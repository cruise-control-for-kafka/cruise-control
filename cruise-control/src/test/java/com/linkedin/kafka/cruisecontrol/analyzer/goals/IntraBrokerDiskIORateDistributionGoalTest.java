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
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.diskIORate;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link IntraBrokerDiskIORateDistributionGoal}.
 */
public class IntraBrokerDiskIORateDistributionGoalTest {
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
    assertEquals(diskIORate(broker.disk(LOGDIR0), 1.0), diskIORate(broker.disk(LOGDIR1), 1.0), 1E-6);
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
    goal = new IntraBrokerDiskIORateDistributionGoal(balancingConstraint(readWeight));
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));

    Broker broker = clusterModel.broker(0);
    long numMovedFollowers = broker.disk(LOGDIR1).replicas().stream().filter(r -> r.topicPartition().topic().equals(INGEST_TOPIC)).count();
    assertTrue("Expected some followers to move to " + LOGDIR1, numMovedFollowers > 0);
    double averageDiskIORate = (diskIORate(broker.disk(LOGDIR0), readWeight) + diskIORate(broker.disk(LOGDIR1), readWeight)) / 2;
    double balancePercentage = AnalyzerConfig.DEFAULT_DISK_BALANCE_THRESHOLD;
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

  private static BalancingConstraint balancingConstraint(double readWeight) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.INTRA_BROKER_DISK_IO_READ_WEIGHT_CONFIG, Double.toString(readWeight));
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

  private static void createReplica(ClusterModel clusterModel, int brokerId, TopicPartition tp, int index, String logdir,
                                    double nwIn, double nwOut) {
    String rack = clusterModel.broker(brokerId).rack().id();
    clusterModel.createReplica(rack, brokerId, tp, index, index == 0, false, logdir, false);
    clusterModel.setReplicaLoad(rack, brokerId, tp, getAggregatedMetricValues(CPU_USAGE, nwIn, nwOut, DISK_USAGE), WINDOWS);
  }
}
