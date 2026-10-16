/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.diskIORate;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link IntraBrokerDiskIORateDistributionGoal}.
 */
public class IntraBrokerDiskIORateDistributionGoalTest {
  private static final String TOPIC = "T";
  private static final String LOGDIR2 = "/mnt/i02";
  private static final double LOGDIR_CAPACITY = TestConstants.LARGE_BROKER_CAPACITY / 2;
  private static final double DELTA = 1E-6;
  private static final double STRICT_BALANCE_THRESHOLD = 1.02;
  private static final double LOOSE_BALANCE_THRESHOLD = 1.5;
  private static final String HOT_TOPIC = "hot";
  private static final String COLD_TOPIC = "cold";
  private static final String WARM_TOPIC = "warm";
  private static final String FAN_OUT_TOPIC = "fan-out";
  private static final String INGEST_TOPIC = "ingest";
  private static final int NUM_INGEST_PARTITIONS = 13;
  private static final double CPU_USAGE = 1.0;
  private static final double DISK_USAGE = 100.0;
  private static final List<Long> WINDOWS = Collections.singletonList(1L);
  private static final OptimizationOptions OPTIMIZATION_OPTIONS =
      new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

  /**
   * Two leaders of a hot topic share a disk, while two leaders of a cold topic share the other disk. Both disks host the
   * same number of (leader) replicas with the same size, yet the disk hosting hot leaders has a much higher I/O rate.
   * The goal is expected to swap a hot leader with a cold leader, so that each disk hosts one hot leader.
   */
  @Test
  public void testBalanceHotAndColdLeaders() throws OptimizationFailureException {
    ClusterModel clusterModel = hotAndColdLeadersCluster(false);
    IntraBrokerDiskIORateDistributionGoal goal = new IntraBrokerDiskIORateDistributionGoal(balancingConstraint(1.0));

    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    Broker broker = clusterModel.broker(0);
    for (Disk disk : broker.disks()) {
      long numHotReplicas = disk.replicas().stream().filter(r -> r.topicPartition().topic().equals(HOT_TOPIC)).count();
      assertEquals("Disk " + disk.logDir() + " is expected to host exactly one hot leader.", 1, numHotReplicas);
    }
    assertEquals(diskIORate(broker.disk(LOGDIR0), 1.0), diskIORate(broker.disk(LOGDIR1), 1.0), DELTA);
  }

  /**
   * On broker 0, one disk hosts a leader of a topic with a high fan-out (i.e. many consumers), and the other disk hosts
   * followers of write-heavy topics with a single consumer. Counting reads and writes equally, both disks have the same
   * I/O rate. Discounting reads (e.g. served from the page cache), the disk hosting write-only followers is overloaded.
   */
  @Test
  public void testReadWeight() throws OptimizationFailureException {
    // (1) Reads and writes are counted equally: no action is expected.
    ClusterModel clusterModel = fanOutAndIngestCluster();
    Map<TopicPartition, String> initialLogdirs = logdirsByTopicPartition(clusterModel.broker(0));
    IntraBrokerDiskIORateDistributionGoal goal = new IntraBrokerDiskIORateDistributionGoal(balancingConstraint(1.0));
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    assertEquals(initialLogdirs, logdirsByTopicPartition(clusterModel.broker(0)));

    // (2) Reads are discounted: followers are expected to move to the disk hosting the fan-out leader.
    double readWeight = 0.3;
    clusterModel = fanOutAndIngestCluster();
    BalancingConstraint constraint = balancingConstraint(readWeight);
    goal = new IntraBrokerDiskIORateDistributionGoal(constraint);
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    Broker broker = clusterModel.broker(0);
    long numMovedFollowers = broker.disk(LOGDIR1).replicas().stream().filter(r -> r.topicPartition().topic().equals(INGEST_TOPIC)).count();
    assertTrue("Expected some followers to move to " + LOGDIR1, numMovedFollowers > 0);
    double averageDiskIORate = (diskIORate(broker.disk(LOGDIR0), readWeight) + diskIORate(broker.disk(LOGDIR1), readWeight)) / 2;
    double balancePercentage = constraint.intraBrokerDiskIOBalancePercentage();
    for (Disk disk : broker.disks()) {
      double rate = diskIORate(disk, readWeight);
      assertTrue(String.format("Disk %s I/O rate %f is out of balance (average: %f).", disk.logDir(), rate, averageDiskIORate),
                 rate <= averageDiskIORate * balancePercentage && rate >= averageDiskIORate * (2 - balancePercentage));
    }
  }

  @Test
  public void testActionAcceptance() {
    ClusterModel clusterModel = hotAndColdLeadersCluster(true);
    IntraBrokerDiskIORateDistributionGoal goal = new IntraBrokerDiskIORateDistributionGoal(balancingConstraint(1.0));
    goal.initGoalState(clusterModel, OPTIMIZATION_OPTIONS);
    Broker broker = clusterModel.broker(0);
    Disk hotDisk = broker.disk(LOGDIR0);
    Disk coldDisk = broker.disk(LOGDIR1);
    TopicPartition hot = new TopicPartition(HOT_TOPIC, 0);
    TopicPartition cold = new TopicPartition(COLD_TOPIC, 0);
    TopicPartition warm = new TopicPartition(WARM_TOPIC, 0);

    // Moving the replica with a low I/O rate from the hot disk to the cold disk makes disks more balanced.
    assertEquals(ACCEPT, goal.actionAcceptance(
        new BalancingAction(warm, hotDisk, coldDisk, ActionType.INTRA_BROKER_REPLICA_MOVEMENT), clusterModel));
    // Moving a hot leader would push the hot disk below the balance lower limit.
    assertEquals(REPLICA_REJECT, goal.actionAcceptance(
        new BalancingAction(hot, hotDisk, coldDisk, ActionType.INTRA_BROKER_REPLICA_MOVEMENT), clusterModel));
    // Moving a cold leader to the hot disk makes disks less balanced.
    assertEquals(REPLICA_REJECT, goal.actionAcceptance(
        new BalancingAction(cold, coldDisk, hotDisk, ActionType.INTRA_BROKER_REPLICA_MOVEMENT), clusterModel));
    // Swapping a hot leader with a cold leader makes disks balanced.
    assertEquals(ACCEPT, goal.actionAcceptance(
        new BalancingAction(hot, hotDisk, coldDisk, ActionType.INTRA_BROKER_REPLICA_SWAP, cold), clusterModel));
    // Swapping two hot leaders does not change the I/O rate of disks.
    clusterModel.relocateReplica(new TopicPartition(HOT_TOPIC, 1), 0, LOGDIR1);
    assertEquals(ACCEPT, goal.actionAcceptance(
        new BalancingAction(hot, hotDisk, coldDisk, ActionType.INTRA_BROKER_REPLICA_SWAP, new TopicPartition(HOT_TOPIC, 1)),
        clusterModel));
  }

  /**
   * A single broker with three disks (average disk I/O rate 400, balanced range [364, 436]):
   * <ul>
   *   <li>{@link TestConstants#LOGDIR0}: 460 (replicas of 360 and 100) -- above the upper limit.</li>
   *   <li>{@link TestConstants#LOGDIR1}: 365 (a single replica) -- the disk with the lowest I/O rate, hence the first candidate
   *   disk to swap with, but no swap with it is feasible.</li>
   *   <li>{@link #LOGDIR2}: 375 (replicas of 300 and 75) -- swapping its replica of 75 with the replica of 100 on
   *   {@link TestConstants#LOGDIR0} balances the broker.</li>
   * </ul>
   * No replica can be moved out of the overloaded disk without violating the limits, so the goal must rely on swaps. The goal
   * is expected to give up on the first candidate disk after a single pass and balance the broker via the second candidate disk.
   */
  @Test
  public void testSwapProceedsToNextCandidateDiskWhenNoSwapIsFeasibleWithFirstCandidate() throws OptimizationFailureException {
    Map<String, List<Double>> diskIORatesByLogdir = new TreeMap<>();
    diskIORatesByLogdir.put(LOGDIR0, List.of(360.0, 100.0));
    diskIORatesByLogdir.put(LOGDIR1, List.of(365.0));
    diskIORatesByLogdir.put(LOGDIR2, List.of(300.0, 75.0));
    ClusterModel clusterModel = createClusterModel(1, diskIORatesByLogdir);
    AttemptCountingGoal goal = new AttemptCountingGoal(balancingConstraint(1.0));

    assertTrue("Goal failed to balance the disks.", goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    Broker broker = clusterModel.broker(0);
    assertEquals(435.0, diskIORate(broker.disk(LOGDIR0), 1.0), DELTA);
    assertEquals(365.0, diskIORate(broker.disk(LOGDIR1), 1.0), DELTA);
    assertEquals(400.0, diskIORate(broker.disk(LOGDIR2), 1.0), DELTA);
    assertNumAttemptsIsBounded(goal, clusterModel);
  }

  /**
   * Brokers with two disks (average disk I/O rate 350, balanced range [318.5, 381.5]), where {@link TestConstants#LOGDIR0} hosts
   * a single replica with a disk I/O rate of 600 and {@link TestConstants#LOGDIR1} hosts a single replica with a disk I/O rate of
   * 100. Neither a move nor a swap can bring the disks within the balanced range, so the goal is expected to give up on each disk
   * as soon as every candidate disk has been tried once -- rather than retrying the same candidate disk until the per disk swap
   * timeout expires.
   */
  @Test
  public void testNoBusyLoopWhenDiskCannotBeBalanced() throws OptimizationFailureException {
    Map<String, List<Double>> diskIORatesByLogdir = new TreeMap<>();
    diskIORatesByLogdir.put(LOGDIR0, List.of(600.0));
    diskIORatesByLogdir.put(LOGDIR1, List.of(100.0));
    ClusterModel clusterModel = createClusterModel(4, diskIORatesByLogdir);
    AttemptCountingGoal goal = new AttemptCountingGoal(balancingConstraint(1.0));

    assertFalse("Goal is not expected to balance the disks.", goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    for (Broker broker : clusterModel.brokers()) {
      assertEquals(600.0, diskIORate(broker.disk(LOGDIR0), 1.0), DELTA);
      assertEquals(100.0, diskIORate(broker.disk(LOGDIR1), 1.0), DELTA);
    }
    assertNumAttemptsIsBounded(goal, clusterModel);
  }

  @Test
  public void testDiskIOBalanceThresholdFallsBackToDiskBalanceThreshold() {
    BalancingConstraint constraint = balancingConstraint(1.0, LOOSE_BALANCE_THRESHOLD, null);
    assertEquals(LOOSE_BALANCE_THRESHOLD, constraint.intraBrokerDiskIOBalancePercentage(), 0.0);
  }

  @Test
  public void testInvalidDiskIOBalanceThreshold() {
    assertThrows(ConfigException.class, () -> balancingConstraint(1.0, LOOSE_BALANCE_THRESHOLD, 0.9));
  }

  /**
   * A single broker with two disks with estimated disk I/O rates of 120 (replicas of 100, 10 and 10) and 100 (a single replica),
   * i.e. both disks are about 9% away from the average disk I/O rate of 110. The goal is expected to decide whether to balance
   * them based on {@link AnalyzerConfig#INTRA_BROKER_DISK_IO_BALANCE_THRESHOLD_CONFIG} rather than
   * {@link AnalyzerConfig#DISK_BALANCE_THRESHOLD_CONFIG}.
   */
  @Test
  public void testGoalUsesDiskIOBalanceThreshold() throws OptimizationFailureException {
    Map<String, List<Double>> diskIORatesByLogdir = new TreeMap<>();
    diskIORatesByLogdir.put(LOGDIR0, List.of(100.0, 10.0, 10.0));
    diskIORatesByLogdir.put(LOGDIR1, List.of(100.0));

    // (1) A strict disk balance threshold, but a loose disk I/O balance threshold: no action is expected.
    ClusterModel clusterModel = createClusterModel(1, diskIORatesByLogdir);
    Map<TopicPartition, String> initialLogdirs = logdirsByTopicPartition(clusterModel.broker(0));
    IntraBrokerDiskIORateDistributionGoal goal =
        new IntraBrokerDiskIORateDistributionGoal(balancingConstraint(1.0, STRICT_BALANCE_THRESHOLD, LOOSE_BALANCE_THRESHOLD));
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    assertEquals(initialLogdirs, logdirsByTopicPartition(clusterModel.broker(0)));

    // (2) A loose disk balance threshold, but a strict disk I/O balance threshold: a replica of 10 is expected to move.
    clusterModel = createClusterModel(1, diskIORatesByLogdir);
    goal = new IntraBrokerDiskIORateDistributionGoal(balancingConstraint(1.0, LOOSE_BALANCE_THRESHOLD, STRICT_BALANCE_THRESHOLD));
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    Broker broker = clusterModel.broker(0);
    assertEquals(110.0, diskIORate(broker.disk(LOGDIR0), 1.0), DELTA);
    assertEquals(110.0, diskIORate(broker.disk(LOGDIR1), 1.0), DELTA);
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

  private static BalancingConstraint balancingConstraint(double readWeight) {
    return balancingConstraint(readWeight, AnalyzerConfig.DEFAULT_DISK_BALANCE_THRESHOLD, null);
  }

  private static BalancingConstraint balancingConstraint(double readWeight, double diskBalanceThreshold, Double diskIOBalanceThreshold) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.INTRA_BROKER_DISK_IO_READ_WEIGHT_CONFIG, Double.toString(readWeight));
    props.setProperty(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, Double.toString(diskBalanceThreshold));
    if (diskIOBalanceThreshold != null) {
      props.setProperty(AnalyzerConfig.INTRA_BROKER_DISK_IO_BALANCE_THRESHOLD_CONFIG, Double.toString(diskIOBalanceThreshold));
    }
    return new BalancingConstraint(new KafkaCruiseControlConfig(props));
  }

  private static Map<TopicPartition, String> logdirsByTopicPartition(Broker broker) {
    Map<TopicPartition, String> logdirByTopicPartition = new HashMap<>();
    for (Replica replica : broker.replicas()) {
      logdirByTopicPartition.put(replica.topicPartition(), replica.disk().logDir());
    }
    return logdirByTopicPartition;
  }

  /**
   * A single JBOD broker with (1) two hot leaders on {@link TestConstants#LOGDIR0} and (2) two cold leaders on
   * {@link TestConstants#LOGDIR1}. The estimated disk I/O rates are 800 and 80 (reads and writes counted equally).
   *
   * @param withWarmLeader {@code true} to add a leader with a low I/O rate (i.e. 0.5) on {@link TestConstants#LOGDIR0}.
   * @return Cluster model for the tests.
   */
  private static ClusterModel hotAndColdLeadersCluster(boolean withWarmLeader) {
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(Collections.singletonMap(0, 0),
                                                                           TestConstants.BROKER_CAPACITY,
                                                                           TestConstants.DISK_CAPACITY);
    for (int partition = 0; partition < 2; partition++) {
      createReplica(clusterModel, 0, new TopicPartition(HOT_TOPIC, partition), 0, LOGDIR0, 100.0, 300.0);
      createReplica(clusterModel, 0, new TopicPartition(COLD_TOPIC, partition), 0, LOGDIR1, 10.0, 30.0);
    }
    if (withWarmLeader) {
      createReplica(clusterModel, 0, new TopicPartition(WARM_TOPIC, 0), 0, LOGDIR0, 0.125, 0.375);
    }
    return clusterModel;
  }

  /**
   * Two JBOD brokers, where
   * <ul>
   *   <li>Broker 0 hosts (1) the leader of a fan-out topic on {@link TestConstants#LOGDIR1}, with a write rate of 10 and a
   *   read rate of 120 (i.e. replication and eleven consumer groups), and (2) the followers of {@link #NUM_INGEST_PARTITIONS}
   *   ingest topic partitions, each with a write rate of 10, on {@link TestConstants#LOGDIR0}.</li>
   *   <li>Broker 1 hosts (1) the follower of the fan-out topic and (2) the leaders of ingest topic partitions, each with a
   *   write rate of 10 and a read rate of 20 (i.e. replication and a single consumer group), spread across its disks.</li>
   * </ul>
   *
   * @return Cluster model for the tests.
   */
  private static ClusterModel fanOutAndIngestCluster() {
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(Map.of(0, 0, 1, 1),
                                                                           TestConstants.BROKER_CAPACITY,
                                                                           TestConstants.DISK_CAPACITY);
    TopicPartition fanOut = new TopicPartition(FAN_OUT_TOPIC, 0);
    createReplica(clusterModel, 0, fanOut, 0, LOGDIR1, 10.0, 120.0);
    createReplica(clusterModel, 1, fanOut, 1, LOGDIR1, 10.0, 0.0);
    for (int partition = 0; partition < NUM_INGEST_PARTITIONS; partition++) {
      TopicPartition ingest = new TopicPartition(INGEST_TOPIC, partition);
      createReplica(clusterModel, 1, ingest, 0, partition % 2 == 0 ? LOGDIR0 : LOGDIR1, 10.0, 20.0);
      createReplica(clusterModel, 0, ingest, 1, LOGDIR0, 10.0, 0.0);
    }
    return clusterModel;
  }

  /**
   * Create a cluster model with the given number of brokers in a single rack, where each broker has the same disk layout. Each
   * disk I/O rate entry corresponds to a single leader replica of a distinct partition on the given logdir. The replica has no
   * outbound network rate, hence its estimated disk I/O rate is its inbound network rate regardless of the read weight.
   *
   * @param numBrokers Number of brokers.
   * @param diskIORatesByLogdir Estimated disk I/O rates of the replicas to place on each logdir.
   * @return The cluster model.
   */
  private static ClusterModel createClusterModel(int numBrokers, Map<String, List<Double>> diskIORatesByLogdir) {
    Map<Integer, Integer> rackByBroker = new HashMap<>();
    for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
      rackByBroker.put(brokerId, 0);
    }
    Map<String, Double> diskCapacityByLogdir = new HashMap<>();
    diskIORatesByLogdir.keySet().forEach(logdir -> diskCapacityByLogdir.put(logdir, LOGDIR_CAPACITY));
    Map<Resource, Double> brokerCapacity = new HashMap<>(TestConstants.BROKER_CAPACITY);
    brokerCapacity.put(Resource.DISK, LOGDIR_CAPACITY * diskCapacityByLogdir.size());
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(rackByBroker, brokerCapacity, diskCapacityByLogdir);

    int partition = 0;
    for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
      for (Map.Entry<String, List<Double>> entry : diskIORatesByLogdir.entrySet()) {
        for (double diskIORate : entry.getValue()) {
          createReplica(clusterModel, brokerId, new TopicPartition(TOPIC, partition++), 0, entry.getKey(), diskIORate, 0.0);
        }
      }
    }
    return clusterModel;
  }

  private static void createReplica(ClusterModel clusterModel, int brokerId, TopicPartition tp, int index, String logdir,
                                    double nwIn, double nwOut) {
    String rack = clusterModel.broker(brokerId).rack().id();
    clusterModel.createReplica(rack, brokerId, tp, index, index == 0, false, logdir, false);
    clusterModel.setReplicaLoad(rack, brokerId, tp, getAggregatedMetricValues(CPU_USAGE, nwIn, nwOut, DISK_USAGE), WINDOWS);
  }

  /**
   * A goal that counts the number of proposals (i.e. intra-broker replica moves and swaps) it evaluates against itself.
   */
  private static final class AttemptCountingGoal extends IntraBrokerDiskIORateDistributionGoal {
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
