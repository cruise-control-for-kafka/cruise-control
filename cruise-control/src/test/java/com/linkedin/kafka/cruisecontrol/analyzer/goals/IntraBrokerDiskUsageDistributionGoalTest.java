/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link IntraBrokerDiskUsageDistributionGoal}.
 */
public class IntraBrokerDiskUsageDistributionGoalTest {
  private static final String TOPIC = "T";
  private static final String LOGDIR0 = "/mnt/d0";
  private static final String LOGDIR1 = "/mnt/d1";
  private static final String LOGDIR2 = "/mnt/d2";
  private static final double DISK_CAPACITY = 100_000.0;
  // With the balance margin of the goal, a threshold of 1.10 keeps disks within [0.91, 1.09] x the average disk utilization.
  private static final double DISK_BALANCE_THRESHOLD = 1.10;
  private static final double DELTA = 1e-6;
  private static final OptimizationOptions OPTIMIZATION_OPTIONS = new OptimizationOptions(Collections.emptySet(),
                                                                                          Collections.emptySet(),
                                                                                          Collections.emptySet());

  /**
   * A single broker with three disks of capacity 100,000 each (average utilization 40%, balanced range [36.4%, 43.6%]):
   * <ul>
   *   <li>{@link #LOGDIR0}: 46,000 (replicas of 36,000 and 10,000) -- above the upper limit.</li>
   *   <li>{@link #LOGDIR1}: 36,500 (a single replica) -- the least loaded disk, hence the first candidate disk to swap with,
   *   but no swap with it is feasible.</li>
   *   <li>{@link #LOGDIR2}: 37,500 (replicas of 30,000 and 7,500) -- swapping its replica of 7,500 with the replica of 10,000
   *   on {@link #LOGDIR0} balances the broker.</li>
   * </ul>
   * No replica can be moved out of the overloaded disk without violating the limits, so the goal must rely on swaps. The goal
   * is expected to give up on the first candidate disk after a single pass and balance the broker via the second candidate disk.
   */
  @Test
  public void testSwapProceedsToNextCandidateDiskWhenNoSwapIsFeasibleWithFirstCandidate() throws Exception {
    Map<String, List<Double>> diskUsagesByLogdir = new TreeMap<>();
    diskUsagesByLogdir.put(LOGDIR0, List.of(36_000.0, 10_000.0));
    diskUsagesByLogdir.put(LOGDIR1, List.of(36_500.0));
    diskUsagesByLogdir.put(LOGDIR2, List.of(30_000.0, 7_500.0));
    ClusterModel clusterModel = createClusterModel(1, diskUsagesByLogdir);
    AttemptCountingGoal goal = new AttemptCountingGoal(balancingConstraint());

    assertTrue("Goal failed to balance the disks.", goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    Broker broker = clusterModel.broker(0);
    assertEquals(43_500.0, broker.disk(LOGDIR0).utilization(), DELTA);
    assertEquals(36_500.0, broker.disk(LOGDIR1).utilization(), DELTA);
    assertEquals(40_000.0, broker.disk(LOGDIR2).utilization(), DELTA);
    assertNumAttemptsIsBounded(goal, clusterModel);
  }

  /**
   * Brokers with two disks of capacity 100,000 each (average utilization 35%, balanced range [31.85%, 38.15%]), where
   * {@link #LOGDIR0} hosts a single replica of 60,000 and {@link #LOGDIR1} hosts a single replica of 10,000. Neither a move nor a
   * swap can bring the disks within the balanced range, so the goal is expected to give up on each disk as soon as every candidate
   * disk has been tried once -- rather than retrying the same candidate disk until the per disk swap timeout expires.
   */
  @Test
  public void testNoBusyLoopWhenDiskCannotBeBalanced() throws Exception {
    Map<String, List<Double>> diskUsagesByLogdir = new TreeMap<>();
    diskUsagesByLogdir.put(LOGDIR0, List.of(60_000.0));
    diskUsagesByLogdir.put(LOGDIR1, List.of(10_000.0));
    ClusterModel clusterModel = createClusterModel(4, diskUsagesByLogdir);
    AttemptCountingGoal goal = new AttemptCountingGoal(balancingConstraint());

    assertFalse("Goal is not expected to balance the disks.", goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    for (Broker broker : clusterModel.brokers()) {
      assertEquals(60_000.0, broker.disk(LOGDIR0).utilization(), DELTA);
      assertEquals(10_000.0, broker.disk(LOGDIR1).utilization(), DELTA);
    }
    assertNumAttemptsIsBounded(goal, clusterModel);
  }

  /**
   * Verify that the goal has not evaluated the same proposals over and over again. Each disk that is out of the balanced range
   * is expected to (1) try to move each of its replicas to each candidate disk, and (2) try to swap each pair of replicas with each
   * candidate disk at most once before giving up -- i.e. O(numDisks^2 * numReplicas^2) attempts per broker.
   *
   * @param goal The goal that counted its attempts.
   * @param clusterModel The optimized cluster model.
   */
  private static void assertNumAttemptsIsBounded(AttemptCountingGoal goal, ClusterModel clusterModel) {
    int maxExpectedAttempts = 0;
    for (Broker broker : clusterModel.brokers()) {
      int numDisks = broker.disks().size();
      int numReplicas = broker.replicas().size();
      maxExpectedAttempts += 2 * numDisks * (numDisks - 1) * numReplicas * numReplicas;
    }
    assertTrue(String.format("Expected at most %d attempts, but the goal evaluated %d proposals.", maxExpectedAttempts, goal.numAttempts()),
               goal.numAttempts() <= maxExpectedAttempts);
  }

  private static BalancingConstraint balancingConstraint() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, Double.toString(DISK_BALANCE_THRESHOLD));
    return new BalancingConstraint(new KafkaCruiseControlConfig(props));
  }

  /**
   * Create a cluster model with the given number of brokers in a single rack, where each broker has the same disk layout. Each
   * disk usage entry corresponds to a single leader replica of a distinct partition on the given logdir.
   *
   * @param numBrokers Number of brokers.
   * @param diskUsagesByLogdir Disk usage of the replicas to place on each logdir.
   * @return The cluster model.
   */
  private static ClusterModel createClusterModel(int numBrokers, Map<String, List<Double>> diskUsagesByLogdir) {
    Map<Integer, Integer> rackByBroker = new HashMap<>();
    for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
      rackByBroker.put(brokerId, 0);
    }
    Map<String, Double> diskCapacityByLogdir = new HashMap<>();
    diskUsagesByLogdir.keySet().forEach(logdir -> diskCapacityByLogdir.put(logdir, DISK_CAPACITY));
    Map<Resource, Double> brokerCapacity = new HashMap<>(TestConstants.BROKER_CAPACITY);
    brokerCapacity.put(Resource.DISK, DISK_CAPACITY * diskCapacityByLogdir.size());
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(rackByBroker, brokerCapacity, diskCapacityByLogdir);

    int partition = 0;
    for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
      String rackId = rackByBroker.get(brokerId).toString();
      for (Map.Entry<String, List<Double>> entry : diskUsagesByLogdir.entrySet()) {
        for (double diskUsage : entry.getValue()) {
          TopicPartition tp = new TopicPartition(TOPIC, partition++);
          clusterModel.createReplica(rackId, brokerId, tp, 0, true, false, entry.getKey(), false);
          clusterModel.setReplicaLoad(rackId, brokerId, tp, getAggregatedMetricValues(0.0, 0.0, 0.0, diskUsage), Collections.singletonList(1L));
        }
      }
    }
    return clusterModel;
  }

  /**
   * A goal that counts the number of proposals (i.e. intra-broker replica moves and swaps) it evaluates against itself.
   */
  private static final class AttemptCountingGoal extends IntraBrokerDiskUsageDistributionGoal {
    private int _numAttempts;

    AttemptCountingGoal(BalancingConstraint constraint) {
      super(constraint);
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
      _numAttempts++;
      return super.selfSatisfied(clusterModel, action);
    }

    int numAttempts() {
      return _numAttempts;
    }
  }
}
