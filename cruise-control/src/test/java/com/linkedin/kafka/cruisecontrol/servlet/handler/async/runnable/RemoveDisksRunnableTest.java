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
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    // The proposal shows the logdirs that the replica moves between.
    List<Map<String, Object>> proposals = cast(response.get("proposals"));
    assertEquals(1, proposals.size());
    assertEquals(List.of(LOGDIR0), proposals.get(0).get("oldReplicaLogdirs"));
    assertEquals(List.of(LOGDIR1), proposals.get(0).get("newReplicaLogdirs"));
    Map<String, Object> summary = cast(response.get("summary"));
    assertEquals(1, ((Number) summary.get("numIntraBrokerReplicaMovements")).intValue());
  }

  @Test
  public void testVerbosePlaintextResponseHasNoDeadDisks() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    String response = response(removeDisk(config), config, false);

    assertTrue(response.contains(String.format("[%d-%s] -> [%d-%s]", BROKER_ID, LOGDIR0, BROKER_ID, LOGDIR1)));
    assertTrue(response.contains("Current load:"));
    assertTrue(response.contains("Cluster load after removing disks"));
    assertFalse(response.contains("DEAD"));
  }

  /**
   * Run a dry-run removal of {@link TestConstants#LOGDIR0} from {@link #BROKER_ID}, optimizing the cluster model with the goal
   * optimizer.
   *
   * @param config The Kafka Cruise Control config.
   * @return The optimizer result of the disk removal.
   */
  private static OptimizerResult removeDisk(KafkaCruiseControlConfig config) throws Exception {
    ClusterModel clusterModel = clusterModel();
    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(), EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));

    RemoveDisksParameters parameters = EasyMock.niceMock(RemoveDisksParameters.class);
    EasyMock.expect(parameters.dryRun()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.allowCapacityEstimation()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.brokerIdAndLogdirs()).andReturn(LOGDIRS_TO_REMOVE).anyTimes();

    KafkaCruiseControl kafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(kafkaCruiseControl.config()).andReturn(config).anyTimes();
    EasyMock.expect(kafkaCruiseControl.timeMs()).andReturn(1L).anyTimes();
    kafkaCruiseControl.sanityCheckDryRun(true, false);
    EasyMock.expect(kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    kafkaCruiseControl.sanityCheckBrokerPresence(Collections.singleton(BROKER_ID));
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL), EasyMock.anyLong(),
                                                    EasyMock.anyObject(), EasyMock.eq(true), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(kafkaCruiseControl.executorState())
            .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet()));
    EasyMock.expect(kafkaCruiseControl.excludedTopics(clusterModel, null)).andReturn(Collections.emptySet());
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.eq(clusterModel), EasyMock.anyObject(), EasyMock.anyObject(),
                                                     EasyMock.isNull(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andAnswer(() -> goalOptimizer.optimizations(clusterModel, EasyMock.getCurrentArgument(1), EasyMock.getCurrentArgument(2),
                                                         null, EasyMock.getCurrentArgument(4), EasyMock.getCurrentArgument(5)));
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

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object object) {
    return (T) object;
  }
}
