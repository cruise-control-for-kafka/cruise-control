/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils.goal;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.RACK_BY_BROKER;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.getHomogeneousCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for ensuring that replica movements honor the explicitly requested destination broker ids (i.e. the
 * {@code destination_broker_ids} parameter of rebalance, proposals, and remove_broker endpoints).
 */
public class RequestedDestinationBrokerIdsTest {
  private static final String TOPIC = "T";
  private static final List<Long> WINDOWS = Collections.singletonList(1L);

  /**
   * Three brokers (0, 1, 2), each partition has a single replica:
   * <ul>
   *   <li>Broker 0 (overloaded): two replicas, each with 80K disk usage.</li>
   *   <li>Broker 1 (underloaded): two replicas, each with 20K disk usage.</li>
   *   <li>Broker 2 (balanced): one replica with 100K disk usage.</li>
   * </ul>
   * Moving a replica from broker 0 to broker 2 would overload broker 2, whereas swapping a replica between brokers 0 and
   * 1 would balance the cluster.
   *
   * @return Cluster model for the tests.
   */
  private static ClusterModel unbalancedDiskCluster() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY, null);
    createReplica(cluster, 0, 0, 80000.0);
    createReplica(cluster, 0, 1, 80000.0);
    createReplica(cluster, 1, 2, 20000.0);
    createReplica(cluster, 1, 3, 20000.0);
    createReplica(cluster, 2, 4, 100000.0);
    return cluster;
  }

  private static void createReplica(ClusterModel cluster, int brokerId, int partition, double diskUsage) {
    TopicPartition tp = new TopicPartition(TOPIC, partition);
    String rack = RACK_BY_BROKER.get(brokerId).toString();
    cluster.createReplica(rack, brokerId, tp, 0, true);
    cluster.setReplicaLoad(rack, brokerId, tp, getAggregatedMetricValues(1.0, 1.0, 1.0, diskUsage), WINDOWS);
  }

  private static OptimizationOptions optimizationOptions(Set<Integer> requestedDestinationBrokerIds) {
    return new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), false,
                                   requestedDestinationBrokerIds);
  }

  private static SortedSet<Replica> eligibleReplicasForSwap(ClusterModel cluster, Set<Integer> requestedDestinationBrokerIds) {
    Replica sourceReplica = cluster.broker(0).replica(new TopicPartition(TOPIC, 0));
    SortedSet<Replica> candidateReplicas = new TreeSet<>(cluster.broker(1).replicas());
    return GoalUtils.eligibleReplicasForSwap(cluster, sourceReplica, candidateReplicas, optimizationOptions(requestedDestinationBrokerIds));
  }

  @Test
  public void testEligibleReplicasForSwapHonorRequestedDestinationBrokerIds() {
    ClusterModel cluster = unbalancedDiskCluster();

    // No requested destination brokers: all candidates are eligible.
    assertEquals(2, eligibleReplicasForSwap(cluster, Collections.emptySet()).size());
    // Both the source and the candidate brokers are requested destination brokers: all candidates are eligible.
    assertEquals(2, eligibleReplicasForSwap(cluster, Set.of(0, 1)).size());
    // The source broker is not a requested destination broker: no candidate is eligible.
    assertTrue(eligibleReplicasForSwap(cluster, Set.of(1, 2)).isEmpty());
    // The candidate broker is not a requested destination broker: no candidate is eligible.
    assertTrue(eligibleReplicasForSwap(cluster, Set.of(0, 2)).isEmpty());
  }

  @Test
  public void testRebalanceDoesNotSwapReplicasToBrokersOtherThanRequestedDestinationBrokers() throws Exception {
    Set<Integer> requestedDestinationBrokerIds = Collections.singleton(2);

    // Sanity check: Without requested destination brokers, the goal balances the cluster by swapping replicas between
    // brokers 0 and 1.
    ClusterModel cluster = unbalancedDiskCluster();
    Goal goal = goal(DiskUsageDistributionGoal.class);
    assertTrue(goal.optimize(cluster, Collections.emptySet(), optimizationOptions(Collections.emptySet())));
    assertTrue(hasReplicaRelocatedTo(cluster, 1));

    // With requested destination brokers, replicas can only be relocated to the requested destination brokers.
    cluster = unbalancedDiskCluster();
    goal = goal(DiskUsageDistributionGoal.class);
    goal.optimize(cluster, Collections.emptySet(), optimizationOptions(requestedDestinationBrokerIds));
    for (Broker broker : cluster.brokers()) {
      if (!requestedDestinationBrokerIds.contains(broker.id())) {
        assertTrue(String.format("Replica relocated to broker %d, which is not among the requested destination brokers %s.",
                                 broker.id(), requestedDestinationBrokerIds), !hasReplicaRelocatedTo(cluster, broker.id()));
      }
    }
  }

  private static boolean hasReplicaRelocatedTo(ClusterModel cluster, int brokerId) {
    Broker broker = cluster.broker(brokerId);
    return broker.replicas().stream().anyMatch(r -> r.originalBroker() != broker);
  }
}
