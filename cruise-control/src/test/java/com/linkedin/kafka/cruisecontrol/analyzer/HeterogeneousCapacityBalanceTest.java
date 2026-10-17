/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Balancing on a cluster whose brokers have heterogeneous network capacity -- see
 * <a href="https://github.com/cruise-control-for-kafka/cruise-control/issues/2334">issue #2334</a>.
 *
 * <p>Brokers 0 and 1 have a 1G NIC, brokers 2 and 3 have a 10G NIC, and {@code network.outbound.capacity.threshold=0.5}.
 * The cluster serves 7G of outbound traffic in 70 equally sized (100M) single-replica partitions, initially placed as
 * {@code [0.5G, 0.5G, 5G, 1G]}. The capacity-proportional target is ~31.8% (= 7G / 22G) on every broker, i.e.
 * {@code [~0.32G, ~0.32G, ~3.2G, ~3.2G]}.
 */
public class HeterogeneousCapacityBalanceTest {
  // Network capacity is in KB/s: 1G ~ 125,000 KB/s.
  private static final double NW_1G = 125000.0;
  private static final double NW_10G = 10 * NW_1G;
  private static final double PARTITION_NW_OUT = NW_1G / 10;
  private static final Map<Integer, Double> NW_CAPACITY_BY_BROKER = Map.of(0, NW_1G, 1, NW_1G, 2, NW_10G, 3, NW_10G);
  private static final int[] INITIAL_NUM_PARTITIONS_BY_BROKER = {5, 5, 50, 10};
  private static final String TOPIC = "T";

  private static ClusterModel heterogeneousCluster() {
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    NW_CAPACITY_BY_BROKER.forEach((brokerId, nwCapacity) -> {
      String rackId = Integer.toString(brokerId);
      cluster.createRack(rackId);
      Map<Resource, Double> capacity = Map.of(Resource.CPU, 100.0, Resource.DISK, 1_000_000.0,
                                              Resource.NW_IN, 10 * NW_10G, Resource.NW_OUT, nwCapacity);
      cluster.createBroker(rackId, "host" + brokerId, brokerId, new BrokerCapacityInfo(capacity), false);
    });
    int partition = 0;
    for (int brokerId = 0; brokerId < INITIAL_NUM_PARTITIONS_BY_BROKER.length; brokerId++) {
      for (int i = 0; i < INITIAL_NUM_PARTITIONS_BY_BROKER[brokerId]; i++) {
        TopicPartition tp = new TopicPartition(TOPIC, partition++);
        String rackId = Integer.toString(brokerId);
        cluster.createReplica(rackId, brokerId, tp, 0, true);
        AggregatedMetricValues load = KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(0.1, 10.0, PARTITION_NW_OUT, 10.0);
        cluster.setReplicaLoad(rackId, brokerId, tp, load, Collections.singletonList(1L));
      }
    }
    return cluster;
  }

  private static BalancingConstraint constraint(Properties overrides) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG, "0.5");
    overrides.stringPropertyNames().forEach(k -> props.setProperty(k, overrides.getProperty(k)));
    return new BalancingConstraint(new KafkaCruiseControlConfig(props));
  }

  private static Goal goal(Class<? extends Goal> goalClass, BalancingConstraint constraint) throws Exception {
    Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
    constructor.setAccessible(true);
    return constructor.newInstance(constraint);
  }

  /**
   * Optimize the given goals in order, the same way {@link GoalOptimizer} does.
   *
   * @param cluster The state of the cluster.
   * @param constraint Balancing constraint for the given cluster.
   * @param goalClasses Goals by the order of optimization priority.
   * @return {@code true} if the last goal succeeded, {@code false} otherwise.
   */
  private static boolean optimize(ClusterModel cluster, BalancingConstraint constraint, List<Class<? extends Goal>> goalClasses)
      throws Exception {
    Set<Goal> optimizedGoals = new HashSet<>();
    boolean succeeded = false;
    for (Class<? extends Goal> goalClass : goalClasses) {
      Goal goal = goal(goalClass, constraint);
      succeeded = goal.optimize(cluster, optimizedGoals,
                                new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));
      optimizedGoals.add(goal);
    }
    return succeeded;
  }

  private static String describe(ClusterModel cluster) {
    StringJoiner sj = new StringJoiner(", ", "[", "]");
    for (Broker broker : cluster.brokers()) {
      double nwOut = broker.load().expectedUtilizationFor(Resource.NW_OUT);
      sj.add(String.format("b%d: %d replicas, %.2fG (%.1f%%)", broker.id(), broker.replicas().size(), nwOut / NW_1G,
                           100 * nwOut / broker.capacityFor(Resource.NW_OUT)));
    }
    return sj.toString();
  }

  private static boolean isCapacityProportional(ClusterModel cluster, double balancePercentage) {
    double avgUtilizationPercentage = cluster.load().expectedUtilizationFor(Resource.NW_OUT) / cluster.capacityFor(Resource.NW_OUT);
    double margin = (balancePercentage - 1) * 0.9;
    for (Broker broker : cluster.brokers()) {
      double utilizationPercentage = broker.load().expectedUtilizationFor(Resource.NW_OUT) / broker.capacityFor(Resource.NW_OUT);
      if (utilizationPercentage < avgUtilizationPercentage * (1 - margin) || utilizationPercentage > avgUtilizationPercentage * (1 + margin)) {
        return false;
      }
    }
    return true;
  }

  private static String optimizeAndDescribe(ClusterModel cluster, Properties overrides, List<Class<? extends Goal>> goals,
                                           boolean expectSuccess) throws Exception {
    String before = describe(cluster);
    boolean succeeded = optimize(cluster, constraint(overrides), goals);
    String message = "goals: " + goals + " before: " + before + " after: " + describe(cluster);
    assertEquals(message, expectSuccess, succeeded);
    return message;
  }

  /**
   * The resource usage distribution goals already balance the utilization percentage of brokers, i.e. brokers with more
   * capacity receive proportionally more load.
   */
  @Test
  public void testUsageDistributionGoalBalancesByCapacityPercentage() throws Exception {
    ClusterModel cluster = heterogeneousCluster();
    String message = optimizeAndDescribe(cluster, new Properties(),
                                         List.of(NetworkOutboundCapacityGoal.class, NetworkOutboundUsageDistributionGoal.class), true);
    assertTrue(message, isCapacityProportional(cluster, AnalyzerConfig.DEFAULT_NETWORK_OUTBOUND_BALANCE_THRESHOLD));
  }

  /**
   * A higher priority replica distribution goal that expects the same replica count on every broker prevents the usage
   * distribution goal from moving load off the small brokers: they stay at their capacity limit (0.5G) while the rest is
   * unevenly spread over the large brokers. Hence, in clusters with heterogeneous broker capacities, the replica count
   * based distribution goals should be left out of the goal list if resource utilization is to be balanced by capacity.
   */
  @Test
  public void testReplicaDistributionGoalsBlockCapacityProportionalBalance() throws Exception {
    for (Class<? extends Goal> replicaDistributionGoal : List.of(ReplicaDistributionGoal.class, LeaderReplicaDistributionGoal.class)) {
      ClusterModel cluster = heterogeneousCluster();
      String message = optimizeAndDescribe(cluster, new Properties(),
                                           List.of(NetworkOutboundCapacityGoal.class, replicaDistributionGoal,
                                                   NetworkOutboundUsageDistributionGoal.class), false);
      assertFalse(message, isCapacityProportional(cluster, AnalyzerConfig.DEFAULT_NETWORK_OUTBOUND_BALANCE_THRESHOLD));
    }
  }

  /**
   * The balance threshold is the allowed deviation from the cluster-wide average utilization percentage, not the ratio
   * between the most and the least loaded brokers. A large threshold therefore does not weight brokers by capacity; it
   * disables balancing: with 6.0, the limits are [0%, 175%] of each broker's capacity, so the skewed initial distribution
   * is accepted as is.
   */
  @Test
  public void testLargeBalanceThresholdDisablesBalancing() throws Exception {
    ClusterModel cluster = heterogeneousCluster();
    String before = describe(cluster);
    Properties props = new Properties();
    props.setProperty(AnalyzerConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG, "6.0");
    String message = optimizeAndDescribe(cluster, props,
                                         List.of(NetworkOutboundCapacityGoal.class, NetworkOutboundUsageDistributionGoal.class), true);
    assertEquals(message, before, describe(cluster));
  }
}
