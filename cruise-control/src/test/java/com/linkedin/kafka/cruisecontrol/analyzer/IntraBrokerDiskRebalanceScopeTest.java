/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;


/**
 * Unit tests for limiting intra-broker (i.e. disk) rebalance to the brokers specified via
 * {@link OptimizationOptions#requestedDestinationBrokerIds()}.
 */
public class IntraBrokerDiskRebalanceScopeTest {
  private static final int NUM_REPLICAS_PER_BROKER = 4;
  private static final double BALANCED_REPLICA_DISK_LOAD = TestConstants.LARGE_BROKER_CAPACITY / 20;
  // Replicas with this load on a single disk exceed the disk capacity limit, but not the broker capacity limit.
  private static final double OVERLOADED_REPLICA_DISK_LOAD = TestConstants.LARGE_BROKER_CAPACITY / 8;
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  @Test
  public void testDiskRebalanceOnAllBrokers() throws KafkaCruiseControlException {
    ClusterModel clusterModel = clusterWithAllReplicasOnFirstDisk(BALANCED_REPLICA_DISK_LOAD, BALANCED_REPLICA_DISK_LOAD);
    Set<ExecutionProposal> proposals = optimize(clusterModel, Collections.emptySet(), diskGoals());
    assertEquals(Set.of(0, 1), brokersWithReplicasToMoveBetweenDisks(proposals));
  }

  @Test
  public void testDiskRebalanceOnRequestedBrokers() throws KafkaCruiseControlException {
    for (int requestedBrokerId : Arrays.asList(0, 1)) {
      ClusterModel clusterModel = clusterWithAllReplicasOnFirstDisk(BALANCED_REPLICA_DISK_LOAD, BALANCED_REPLICA_DISK_LOAD);
      Set<ExecutionProposal> proposals = optimize(clusterModel, Collections.singleton(requestedBrokerId), diskGoals());
      assertEquals(Collections.singleton(requestedBrokerId), brokersWithReplicasToMoveBetweenDisks(proposals));
      // Disks of the brokers that are not requested remain untouched.
      int otherBrokerId = 1 - requestedBrokerId;
      assertEquals(NUM_REPLICAS_PER_BROKER, clusterModel.broker(otherBrokerId).disk(TestConstants.LOGDIR0).replicas().size());
      assertEquals(0, clusterModel.broker(otherBrokerId).disk(TestConstants.LOGDIR1).replicas().size());
    }
  }

  @Test
  public void testDiskCapacityGoalIgnoresBrokersThatAreNotRequested() throws KafkaCruiseControlException {
    // Broker 1 has a disk over its capacity limit, but the rebalance is limited to broker 0.
    ClusterModel clusterModel = clusterWithAllReplicasOnFirstDisk(BALANCED_REPLICA_DISK_LOAD, OVERLOADED_REPLICA_DISK_LOAD);
    List<Goal> capacityGoal = Collections.singletonList(configure(new IntraBrokerDiskCapacityGoal()));
    Set<ExecutionProposal> proposals = optimize(clusterModel, Collections.singleton(0), capacityGoal);
    assertEquals(Collections.emptySet(), brokersWithReplicasToMoveBetweenDisks(proposals));
    assertEquals(NUM_REPLICAS_PER_BROKER, clusterModel.broker(1).disk(TestConstants.LOGDIR0).replicas().size());

    // Rebalancing broker 1 fixes its disk capacity violation.
    clusterModel = clusterWithAllReplicasOnFirstDisk(BALANCED_REPLICA_DISK_LOAD, OVERLOADED_REPLICA_DISK_LOAD);
    capacityGoal = Collections.singletonList(configure(new IntraBrokerDiskCapacityGoal()));
    proposals = optimize(clusterModel, Collections.singleton(1), capacityGoal);
    assertEquals(Collections.singleton(1), brokersWithReplicasToMoveBetweenDisks(proposals));
  }

  @Test
  public void testDiskCapacityGoalFailsForOverloadedRequestedBroker() {
    // Broker 1 is over its broker-level disk capacity limit, which cannot be fixed by moving replicas between its disks.
    ClusterModel clusterModel = clusterWithAllReplicasOnFirstDisk(BALANCED_REPLICA_DISK_LOAD, TestConstants.LARGE_BROKER_CAPACITY / 4);
    List<Goal> capacityGoal = Collections.singletonList(configure(new IntraBrokerDiskCapacityGoal()));
    assertThrows(OptimizationFailureException.class, () -> optimize(clusterModel, Collections.singleton(1), capacityGoal));

    // The overloaded broker does not fail the disk rebalance of other brokers.
    ClusterModel otherClusterModel = clusterWithAllReplicasOnFirstDisk(BALANCED_REPLICA_DISK_LOAD, TestConstants.LARGE_BROKER_CAPACITY / 4);
    List<Goal> otherCapacityGoal = Collections.singletonList(configure(new IntraBrokerDiskCapacityGoal()));
    try {
      optimize(otherClusterModel, Collections.singleton(0), otherCapacityGoal);
    } catch (KafkaCruiseControlException e) {
      throw new AssertionError("Disk rebalance limited to broker 0 should not fail due to broker 1.", e);
    }
  }

  private static List<Goal> diskGoals() {
    return Arrays.asList(configure(new IntraBrokerDiskCapacityGoal()), configure(new IntraBrokerDiskUsageDistributionGoal()));
  }

  private static Goal configure(Goal goal) {
    goal.configure(CONFIG.mergedConfigValues());
    return goal;
  }

  private static Set<ExecutionProposal> optimize(ClusterModel clusterModel, Set<Integer> requestedBrokerIds, List<Goal> goals)
      throws KafkaCruiseControlException {
    GoalOptimizer goalOptimizer = new GoalOptimizer(CONFIG, null, Time.SYSTEM, new MetricRegistry(),
                                                    EasyMock.mock(Executor.class), EasyMock.mock(Admin.class));
    OptimizationOptions optimizationOptions = new OptimizationOptions(Collections.emptySet(), Collections.emptySet(),
                                                                      Collections.emptySet(), false, requestedBrokerIds);
    return goalOptimizer.optimizations(clusterModel, goals, new OperationProgress(), null, optimizationOptions).goalProposals();
  }

  private static Set<Integer> brokersWithReplicasToMoveBetweenDisks(Set<ExecutionProposal> proposals) {
    Set<Integer> brokers = new HashSet<>();
    proposals.forEach(proposal -> brokers.addAll(proposal.replicasToMoveBetweenDisksByBroker().keySet()));
    return brokers;
  }

  /**
   * Two brokers on two racks, each has two disks. Each broker hosts {@link #NUM_REPLICAS_PER_BROKER} single-replica
   * partitions, all of which reside on {@link TestConstants#LOGDIR0}.
   *
   * @param broker0ReplicaDiskLoad Disk load of each replica on broker 0.
   * @param broker1ReplicaDiskLoad Disk load of each replica on broker 1.
   * @return Cluster model for the tests.
   */
  private static ClusterModel clusterWithAllReplicasOnFirstDisk(double broker0ReplicaDiskLoad, double broker1ReplicaDiskLoad) {
    Map<Integer, Integer> rackByBrokerId = Map.of(0, 0, 1, 1);
    Map<Integer, Double> replicaDiskLoadByBrokerId = Map.of(0, broker0ReplicaDiskLoad, 1, broker1ReplicaDiskLoad);
    ClusterModel cluster = DeterministicCluster.getHomogeneousCluster(rackByBrokerId, TestConstants.BROKER_CAPACITY,
                                                                      TestConstants.DISK_CAPACITY);
    int partition = 0;
    for (int brokerId : Arrays.asList(0, 1)) {
      String rack = rackByBrokerId.get(brokerId).toString();
      for (int i = 0; i < NUM_REPLICAS_PER_BROKER; i++) {
        TopicPartition tp = new TopicPartition(DeterministicCluster.T1, partition++);
        cluster.createReplica(rack, brokerId, tp, 0, true, false, TestConstants.LOGDIR0, false);
        cluster.setReplicaLoad(rack, brokerId, tp,
                               getAggregatedMetricValues(1.0, 1.0, 1.0, replicaDiskLoadByBrokerId.get(brokerId)),
                               Collections.singletonList(1L));
      }
    }
    return cluster;
  }
}
