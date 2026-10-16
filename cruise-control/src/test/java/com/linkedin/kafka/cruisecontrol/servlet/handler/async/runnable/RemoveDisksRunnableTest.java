/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class RemoveDisksRunnableTest {
  private static final double DELTA = 1e-6;
  private static final int BROKER_ID = 0;
  private static final String RACK = "r0";
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  // The broker has two disks, each with half of the broker disk capacity.
  private static final double BROKER_DISK_CAPACITY = TestConstants.LARGE_BROKER_CAPACITY;
  private static final double DISK_CAPACITY = TestConstants.LARGE_BROKER_CAPACITY / 2;
  private static final double REPLICA_DISK_MB = DISK_CAPACITY / 2;
  private static final Map<Integer, Set<String>> LOGDIRS_TO_REMOVE = Collections.singletonMap(BROKER_ID, Collections.singleton(LOGDIR0));
  private static final String LOGDIR2 = "/mnt/i02";

  @Test
  public void testVerboseJsonResponseHasLoadBeforeAndAfterAndDiskMovements() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    Map<String, Object> response = new Gson().fromJson(response(removeDisk(config), config, true),
                                                       new TypeToken<Map<String, Object>>() { }.getType());

    // Before the removal, the disk to remove reports its load relative to its capacity, rather than as a dead disk.
    Map<String, Object> brokerBefore = brokerStats(response, "loadBeforeOptimization");
    assertEquals(BROKER_DISK_CAPACITY, (Double) brokerBefore.get("DiskCapacityMB"), DELTA);
    assertEquals(25.0, (Double) brokerBefore.get("DiskPct"), DELTA);
    assertDiskState(brokerBefore, LOGDIR0, REPLICA_DISK_MB, 50.0, 1);
    assertDiskState(brokerBefore, LOGDIR1, 0.0, 0.0, 0);

    // After the removal, the removed disk is empty and its capacity is dropped from the broker disk capacity.
    Map<String, Object> brokerAfter = brokerStats(response, "loadAfterOptimization");
    assertEquals(DISK_CAPACITY, (Double) brokerAfter.get("DiskCapacityMB"), DELTA);
    assertEquals(50.0, (Double) brokerAfter.get("DiskPct"), DELTA);
    assertDiskState(brokerAfter, LOGDIR0, 0.0, 0.0, 0);
    assertDiskState(brokerAfter, LOGDIR1, REPLICA_DISK_MB, 50.0, 1);

    // The proposal shows the logdirs that the replica moves between, by broker id.
    List<Map<String, Object>> proposals = cast(response.get("proposals"));
    assertEquals(1, proposals.size());
    assertEquals(Map.of(Integer.toString(BROKER_ID), LOGDIR0), proposals.get(0).get("oldReplicaLogdirs"));
    assertEquals(Map.of(Integer.toString(BROKER_ID), LOGDIR1), proposals.get(0).get("newReplicaLogdirs"));
    Map<String, Object> summary = cast(response.get("summary"));
    assertEquals(1, ((Number) summary.get("numIntraBrokerReplicaMovements")).intValue());

    // The disk balance before the removal is taken before the disk to remove is marked for removal: both disks are unbalanced
    // (50% and 0% for a broker at 25%). After the removal, the removed disk has no capacity and is left out.
    Map<String, Object> diskStatisticsBefore = cast(summary.get("diskStatisticsBefore"));
    assertEquals(2, ((Number) diskStatisticsBefore.get("numAliveDisks")).intValue());
    Map<String, Object> usageBefore = cast(diskStatisticsBefore.get("usage"));
    assertEquals(2, ((Number) usageBefore.get("numUnbalancedDisks")).intValue());
    assertEquals(50.0, utilizationPct(usageBefore, "MAX"), DELTA);
    assertEquals(50.0, utilizationPct(usageBefore, "maxSpreadWithinBroker"), DELTA);
    Map<String, Object> diskStatisticsAfter = cast(summary.get("diskStatisticsAfter"));
    assertEquals(1, ((Number) diskStatisticsAfter.get("numAliveDisks")).intValue());
    Map<String, Object> usageAfter = cast(diskStatisticsAfter.get("usage"));
    assertEquals(0, ((Number) usageAfter.get("numUnbalancedDisks")).intValue());
    assertEquals(50.0, utilizationPct(usageAfter, "MAX"), DELTA);
    assertEquals(0.0, utilizationPct(usageAfter, "maxSpreadWithinBroker"), DELTA);
  }

  @Test
  public void testMovesReplicasOfExcludedTopicsOffRemovedDisks() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    // The only replica on the disk to remove belongs to a topic excluded from partition movement: the disk is emptied anyway.
    OptimizerResult result = removeDisk(config, Collections.singleton(TP.topic()));

    assertEquals(Collections.singleton(TP.topic()), result.excludedTopics());
    assertEquals(1, result.goalProposals().size());
    ExecutionProposal proposal = result.goalProposals().iterator().next();
    assertEquals(TP, proposal.topicPartition());
    assertEquals(LOGDIR1, proposal.replicasToMoveBetweenDisksByBroker().get(BROKER_ID).logdir());
  }

  @Test
  public void testOnlyEmptiesTheDisksToRemove() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    // Broker 0 has three disks: the one to remove, one above the capacity limit (80%), and an empty one. Broker 1 also has a disk
    // above the capacity limit.
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    Map<Resource, Double> brokerCapacity = new HashMap<>(TestConstants.BROKER_CAPACITY);
    brokerCapacity.put(Resource.DISK, 3 * DISK_CAPACITY);
    createBroker(clusterModel, 0, new BrokerCapacityInfo(brokerCapacity, Map.of(LOGDIR0, DISK_CAPACITY, LOGDIR1, DISK_CAPACITY,
                                                                                LOGDIR2, DISK_CAPACITY)));
    createBroker(clusterModel, 1, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY));
    TopicPartition replicaToMove = new TopicPartition("topic", 0);
    createReplica(clusterModel, 0, replicaToMove, LOGDIR0, REPLICA_DISK_MB);
    createReplica(clusterModel, 0, new TopicPartition("topic", 1), LOGDIR1, 0.9 * DISK_CAPACITY);
    createReplica(clusterModel, 1, new TopicPartition("topic", 2), LOGDIR0, 0.9 * DISK_CAPACITY);

    OptimizerResult result = removeDisks(config, clusterModel, Collections.singletonMap(0, Collections.singleton(LOGDIR0)),
                                         Collections.emptySet());

    // Only the replica on the disk to remove moves, to the disk of its broker with room for it; the disks above the capacity
    // limit are left as they are, and do not fail the request.
    assertEquals(1, result.goalProposals().size());
    ExecutionProposal proposal = result.goalProposals().iterator().next();
    assertEquals(replicaToMove, proposal.topicPartition());
    assertEquals(Collections.singleton(0), proposal.replicasToMoveBetweenDisksByBroker().keySet());
    assertEquals(LOGDIR2, proposal.replicasToMoveBetweenDisksByBroker().get(0).logdir());
  }

  @Test
  public void testOnlyDiskStatsWhateverTheIntraBrokerGoalsConfig() throws Exception {
    // remove_disks runs its own IntraBrokerDiskCapacityGoal, even when intra.broker.goals does not list it: the goal itself says
    // that it is an intra-broker goal, so its result shows disk-level stats only.
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, IntraBrokerDiskUsageDistributionGoal.class.getName());
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    String response = response(removeDisk(config), config, false);

    assertTrue(response, response.contains("Stats for IntraBrokerDiskCapacityGoal"));
    assertFalse(response, response.contains("AVG:{cpu"));
  }

  @Test
  public void testVerbosePlaintextResponseHasNoDeadDisks() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    String response = response(removeDisk(config), config, false);

    assertTrue(response.contains(String.format("[%d-%s] -> [%d-%s]", BROKER_ID, LOGDIR0, BROKER_ID, LOGDIR1)));
    assertTrue(response.contains("Current load:"));
    assertTrue(response.contains("Cluster load after removing disks"));
    assertFalse(response.contains("DEAD"));
    // The goal of remove_disks is an intra-broker goal, which cannot change broker-level stats: only disk-level stats are shown.
    assertTrue(response.contains("Stats for IntraBrokerDiskCapacityGoal"));
    assertTrue(response.contains("DISKS:{alive: 1}"));
    assertFalse(response.contains("AVG:{cpu"));
    // The removed disk is counted before the removal only.
    assertTrue(response, response.contains("Disk usage balance (before -> after): unbalanced disks 2 of 2 -> 0 of 1, max utilization "
                                           + "50.00% -> 50.00%, max spread within a broker 50.00pp -> 0.00pp."));
  }

  @Test
  public void testRejectsAFailedDisk() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    String message = removeDisksFailure(config, clusterModelWithFailedDisk(true),
                                        Collections.singletonMap(0, Collections.singleton(LOGDIR1)));
    assertTrue(message, message.startsWith(String.format("Cannot remove disk %s of broker 0: the disk is DEAD, and replicas cannot be "
                                                         + "moved off a failed disk", LOGDIR1)));
    // Kafka reports no replica for a failed log dir: the offline replicas of the broker are named instead.
    assertTrue(message, message.endsWith("Offline replicas of broker 0: [topic-1]. Move them to other brokers with "
                                         + "fix_offline_replicas, then take the disk out of the broker's log.dirs."));

    // A failed disk is rejected even if its broker has no offline replica: it cannot be checked to be empty.
    message = removeDisksFailure(config, clusterModelWithFailedDisk(false), Collections.singletonMap(0, Collections.singleton(LOGDIR1)));
    assertTrue(message, message.endsWith("Broker 0 has no offline replica: take the disk out of the broker's log.dirs."));
  }

  @Test
  public void testRejectsWhenNoAliveDiskRemains() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    // The other disk of broker 0 failed, so no disk can take the replicas of the disk to remove.
    String message = removeDisksFailure(config, clusterModelWithFailedDisk(true),
                                        Collections.singletonMap(0, Collections.singleton(LOGDIR0)));
    assertEquals("No alive log dir remaining to move replicas to for broker 0.", message);
  }

  @Test
  public void testFailedDisksAndOfflineReplicasElsewhereDoNotBlock() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    // Broker 0 has three disks: LOGDIR0 to remove, LOGDIR1 failed -- its follower of topic-1 is offline -- and LOGDIR2, empty.
    // Broker 1 is dead, with an offline follower of topic-0. Broker 2 leads topic-1.
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    Map<Resource, Double> brokerCapacity = new HashMap<>(TestConstants.BROKER_CAPACITY);
    brokerCapacity.put(Resource.DISK, 3 * DISK_CAPACITY);
    createBroker(clusterModel, 0, new BrokerCapacityInfo(brokerCapacity, Map.of(LOGDIR0, DISK_CAPACITY, LOGDIR1, DISK_CAPACITY,
                                                                                LOGDIR2, DISK_CAPACITY)));
    clusterModel.handleDeadBroker("1", 1, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY));
    createBroker(clusterModel, 2, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY));
    clusterModel.broker(0).disk(LOGDIR1).setState(Disk.State.DEAD);
    TopicPartition replicaToMove = new TopicPartition("topic", 0);
    createReplica(clusterModel, 0, replicaToMove, LOGDIR0, REPLICA_DISK_MB);
    createOfflineFollower(clusterModel, 1, replicaToMove);
    TopicPartition tp1 = new TopicPartition("topic", 1);
    createReplica(clusterModel, 2, tp1, LOGDIR0, REPLICA_DISK_MB);
    createOfflineFollower(clusterModel, 0, tp1);
    clusterModel.setBrokerState(0, Broker.State.BAD_DISKS);
    clusterModel.setBrokerState(1, Broker.State.DEAD);

    OptimizerResult result = removeDisks(config, clusterModel, Collections.singletonMap(0, Collections.singleton(LOGDIR0)),
                                         Collections.emptySet());

    // The replica on the disk to remove moves to the alive disk; the failed disk and the offline replicas are left alone.
    assertEquals(1, result.goalProposals().size());
    ExecutionProposal proposal = result.goalProposals().iterator().next();
    assertEquals(replicaToMove, proposal.topicPartition());
    assertEquals(Collections.singleton(0), proposal.replicasToMoveBetweenDisksByBroker().keySet());
    assertEquals(LOGDIR2, proposal.replicasToMoveBetweenDisksByBroker().get(0).logdir());
  }

  /**
   * Brokers 0 and 1 with two disks each, where {@link TestConstants#LOGDIR1} of broker 0 failed, and broker 0 leads topic-0 on
   * {@link TestConstants#LOGDIR0}. As in a cluster model built from Kafka, the replicas of the failed disk are offline and on no
   * disk, since Kafka reports no replica for a failed log dir.
   *
   * @param withOfflineReplica {@code true} to have the follower of topic-1 (led by broker 1) on the failed disk.
   * @return The cluster model.
   */
  private static ClusterModel clusterModelWithFailedDisk(boolean withOfflineReplica) {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    for (int brokerId = 0; brokerId < 2; brokerId++) {
      createBroker(clusterModel, brokerId, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY));
    }
    clusterModel.broker(0).disk(LOGDIR1).setState(Disk.State.DEAD);
    createReplica(clusterModel, 0, new TopicPartition("topic", 0), LOGDIR0, REPLICA_DISK_MB);
    if (withOfflineReplica) {
      TopicPartition tp = new TopicPartition("topic", 1);
      createReplica(clusterModel, 1, tp, LOGDIR0, REPLICA_DISK_MB);
      createOfflineFollower(clusterModel, 0, tp);
      clusterModel.setBrokerState(0, Broker.State.BAD_DISKS);
    }
    return clusterModel;
  }

  /**
   * Run {@link #removeDisks(KafkaCruiseControlConfig, ClusterModel, Map, Set)} for a disk removal that is rejected before the
   * optimization.
   *
   * @param config The Kafka Cruise Control config.
   * @param clusterModel The cluster model.
   * @param logdirsToRemove The logdirs of the disks to remove, by broker id.
   * @return The message of the rejection.
   */
  private static String removeDisksFailure(KafkaCruiseControlConfig config,
                                           ClusterModel clusterModel,
                                           Map<Integer, Set<String>> logdirsToRemove) throws Exception {
    try {
      removeDisks(config, clusterModel, logdirsToRemove, Collections.emptySet());
    } catch (KafkaCruiseControlException e) {
      assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof IllegalArgumentException);
      return e.getCause().getMessage();
    }
    fail("The disk removal should be rejected.");
    return null;
  }

  /**
   * Run a dry-run removal of {@link TestConstants#LOGDIR0} from {@link #BROKER_ID}, optimizing the cluster model with the goal
   * optimizer.
   *
   * @param config The Kafka Cruise Control config.
   * @return The optimizer result of the disk removal.
   */
  private static OptimizerResult removeDisk(KafkaCruiseControlConfig config) throws Exception {
    return removeDisk(config, Collections.emptySet());
  }

  /**
   * Same as {@link #removeDisk(KafkaCruiseControlConfig)}, with the given topics excluded from partition movement.
   *
   * @param config The Kafka Cruise Control config.
   * @param excludedTopics Topics excluded from partition movement.
   * @return The optimizer result of the disk removal.
   */
  private static OptimizerResult removeDisk(KafkaCruiseControlConfig config, Set<String> excludedTopics) throws Exception {
    return removeDisks(config, clusterModel(), LOGDIRS_TO_REMOVE, excludedTopics);
  }

  /**
   * Run a dry-run removal of the given disks, optimizing the given cluster model with the goal optimizer.
   *
   * @param config The Kafka Cruise Control config.
   * @param clusterModel The cluster model.
   * @param logdirsToRemove The logdirs of the disks to remove, by broker id.
   * @param excludedTopics Topics excluded from partition movement.
   * @return The optimizer result of the disk removal.
   */
  private static OptimizerResult removeDisks(KafkaCruiseControlConfig config,
                                             ClusterModel clusterModel,
                                             Map<Integer, Set<String>> logdirsToRemove,
                                             Set<String> excludedTopics) throws Exception {
    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(), EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));

    RemoveDisksParameters parameters = EasyMock.niceMock(RemoveDisksParameters.class);
    EasyMock.expect(parameters.dryRun()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.allowCapacityEstimation()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.brokerIdAndLogdirs()).andReturn(logdirsToRemove).anyTimes();

    KafkaCruiseControl kafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(kafkaCruiseControl.config()).andReturn(config).anyTimes();
    EasyMock.expect(kafkaCruiseControl.timeMs()).andReturn(1L).anyTimes();
    kafkaCruiseControl.sanityCheckDryRun(true, false);
    EasyMock.expect(kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    kafkaCruiseControl.sanityCheckBrokerPresence(logdirsToRemove.keySet());
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL), EasyMock.anyLong(),
                                                    EasyMock.anyObject(), EasyMock.eq(true), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(kafkaCruiseControl.executorState())
            .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet()));
    EasyMock.expect(kafkaCruiseControl.excludedTopics(clusterModel, null)).andReturn(excludedTopics);
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.eq(clusterModel), EasyMock.anyObject(), EasyMock.anyObject(),
                                                     EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andAnswer(() -> goalOptimizer.optimizations(clusterModel, EasyMock.getCurrentArgument(1), EasyMock.getCurrentArgument(2),
                                                         null, EasyMock.getCurrentArgument(4), EasyMock.getCurrentArgument(5),
                                                         EasyMock.getCurrentArgument(6)));
    EasyMock.replay(parameters, kafkaCruiseControl);

    RemoveDisksRunnable runnable = new RemoveDisksRunnable(kafkaCruiseControl, new OperationFuture("Remove disks"), parameters, "uuid");
    OptimizerResult result = runnable.computeResult();

    EasyMock.verify(parameters, kafkaCruiseControl);
    return result;
  }

  /**
   * @param optimizerResult The optimizer result of the disk removal.
   * @param config The Kafka Cruise Control config.
   * @param json {@code true} for the JSON response, {@code false} for the plaintext response.
   * @return The verbose response of the disk removal request.
   */
  private static String response(OptimizerResult optimizerResult, KafkaCruiseControlConfig config, boolean json) {
    RemoveDisksParameters parameters = EasyMock.niceMock(RemoveDisksParameters.class);
    EasyMock.expect(parameters.json()).andReturn(json).anyTimes();
    EasyMock.expect(parameters.isVerbose()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.endPoint()).andReturn(CruiseControlEndPoint.REMOVE_DISKS).anyTimes();
    EasyMock.expect(parameters.brokerIdAndLogdirs()).andReturn(LOGDIRS_TO_REMOVE).anyTimes();
    EasyMock.replay(parameters);

    OptimizationResult response = new OptimizationResult(optimizerResult, config);
    response.discardIrrelevantResponse(parameters);
    return response.cachedResponse();
  }

  private static void createBroker(ClusterModel clusterModel, int brokerId, BrokerCapacityInfo capacity) {
    String rack = Integer.toString(brokerId);
    clusterModel.createRack(rack);
    clusterModel.createBroker(rack, rack, brokerId, capacity, true);
  }

  private static void createReplica(ClusterModel clusterModel, int brokerId, TopicPartition tp, String logdir, double diskMB) {
    String rack = Integer.toString(brokerId);
    clusterModel.createReplica(rack, brokerId, tp, 0, true, false, logdir, false);
    clusterModel.setReplicaLoad(rack, brokerId, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, diskMB), Collections.singletonList(1L));
  }

  /**
   * Create an offline follower of the given partition, on no disk, as Kafka reports no replica for a failed log dir or a dead
   * broker.
   *
   * @param clusterModel The cluster model.
   * @param brokerId The id of the broker of the follower.
   * @param tp The partition of the follower.
   */
  private static void createOfflineFollower(ClusterModel clusterModel, int brokerId, TopicPartition tp) {
    String rack = Integer.toString(brokerId);
    clusterModel.createReplica(rack, brokerId, tp, 1, false, true, null, false);
    clusterModel.setReplicaLoad(rack, brokerId, tp, getAggregatedMetricValues(1.0, 10.0, 0.0, 1000.0), Collections.singletonList(1L));
  }

  /**
   * @return A cluster model with a broker having two disks, where the only replica resides on {@link TestConstants#LOGDIR0}.
   */
  private static ClusterModel clusterModel() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    clusterModel.createRack(RACK);
    clusterModel.createBroker(RACK, Integer.toString(BROKER_ID), BROKER_ID,
                              new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY), true);
    clusterModel.createReplica(RACK, BROKER_ID, TP, 0, true, false, LOGDIR0, false);
    clusterModel.setReplicaLoad(RACK, BROKER_ID, TP, getAggregatedMetricValues(40.0, 100.0, 130.0, REPLICA_DISK_MB),
                                Collections.singletonList(1L));
    return clusterModel;
  }

  private static Map<String, Object> brokerStats(Map<String, Object> response, String load) {
    Map<String, Object> brokerStats = cast(response.get(load));
    List<Map<String, Object>> brokers = cast(brokerStats.get("brokers"));
    assertEquals(1, brokers.size());
    assertEquals(BROKER_ID, ((Number) brokers.get(0).get("Broker")).intValue());
    return brokers.get(0);
  }

  private static void assertDiskState(Map<String, Object> brokerStats, String logdir, double diskMB, double diskPct, int numReplicas) {
    Map<String, Map<String, Object>> diskState = cast(brokerStats.get("DiskState"));
    Map<String, Object> disk = diskState.get(logdir);
    assertEquals(diskMB, (Double) disk.get("DiskMB"), DELTA);
    assertEquals(diskPct, (Double) disk.get("DiskPct"), DELTA);
    assertEquals(numReplicas, ((Number) disk.get("NumReplicas")).intValue());
  }

  private static double utilizationPct(Map<String, Object> diskUsageStats, String statistic) {
    Map<String, Object> value = cast(diskUsageStats.get(statistic));
    return (Double) value.get("utilizationPct");
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object object) {
    return (T) object;
  }
}
