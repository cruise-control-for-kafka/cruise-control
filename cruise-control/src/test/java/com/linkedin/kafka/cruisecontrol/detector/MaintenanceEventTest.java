/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.generateClusterFromClusterModel;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.BROKERS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.MAINTENANCE_EVENT_TYPE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.TOPICS_WITH_RF_UPDATE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.MAINTENANCE_EVENT;
import static com.linkedin.kafka.cruisecontrol.model.RandomCluster.singleBrokerWithBadDisk;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable.SELF_HEALING_IGNORE_PROPOSAL_CACHE;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable.SELF_HEALING_IS_REBALANCE_DISK_MODE;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DESTINATION_BROKER_IDS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXCLUDED_TOPICS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class MaintenanceEventTest {
  private static final long MOCK_TIME_MS = 100L;
  private static final Set<Integer> MOCK_BROKERS_OBJECT;
  static {
    Set<Integer> mockBrokersObject = new HashSet<>();
    for (int brokerId = 0; brokerId < 2; brokerId++) {
      mockBrokersObject.add(brokerId);
    }
    MOCK_BROKERS_OBJECT = Collections.unmodifiableSet(mockBrokersObject);
  }
  // Note that the use of MOCK_TOPICS_WITH_RF_UPDATE assumes the use of a cluster model, which contains T1.
  private static final Map<Short, String> MOCK_TOPICS_WITH_RF_UPDATE = Collections.singletonMap((short) 2, T1);
  private static final String SELF_HEALING_REASON_PREFIX = String.format("Self healing for %s: ", MAINTENANCE_EVENT);
  private static final String WITH_BROKERS_REASON_SUFFIX = String.format(" for brokers: [%s]}", MOCK_BROKERS_OBJECT);
  private static final String WITH_RF_UPDATE_SUFFIX = String.format(" by desired RF: [%s]}", MOCK_TOPICS_WITH_RF_UPDATE);
  private KafkaCruiseControlConfig _config;
  private KafkaCruiseControl _mockKafkaCruiseControl;
  private ExecutorState _executorState;
  private OptimizerResult _optimizerResult;
  private BrokerStats _brokerStats;

  /**
   * Setup the unit test.
   */
  @Before
  public void setup() {
    _mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    _executorState = ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet());
    _optimizerResult = EasyMock.mock(OptimizerResult.class);
    _brokerStats = EasyMock.mock(BrokerStats.class);

    // Create Kafka Cruise Control config
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    _config = new KafkaCruiseControlConfig(props);
  }

  @Test
  public void testAddBrokerEvent()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    // Populate parameter config overrides.
    Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl,
                                                          BROKERS_OBJECT_CONFIG, MOCK_BROKERS_OBJECT,
                                                          MAINTENANCE_EVENT_TYPE_CONFIG, MaintenanceEventType.ADD_BROKER,
                                                          ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS);

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(3);
    _mockKafkaCruiseControl.sanityCheckDryRun(false, true);
    EasyMock.expect(_mockKafkaCruiseControl.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(_mockKafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject())).andReturn(
        MonitorUtils.combineLoadRequirementOptions(goalsByPriority(getSelfHealingGoalNames(_config), _config)));
    EasyMock.expect(_mockKafkaCruiseControl.getLoadMonitorTaskRunnerState()).andReturn(
        LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    _mockKafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(false));
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    _mockKafkaCruiseControl.sanityCheckBrokerPresence(MOCK_BROKERS_OBJECT);
    ClusterModel clusterModel = unbalanced();
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(_mockKafkaCruiseControl.executorState()).andReturn(_executorState).once();

    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(MOCK_BROKERS_OBJECT),
                                                              EasyMock.eq(true))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(MOCK_BROKERS_OBJECT),
                                                              EasyMock.eq(false))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, SELF_HEALING_EXCLUDED_TOPICS)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject()))
            .andReturn(_optimizerResult);
    EasyMock.expect(_optimizerResult.goalProposals()).andReturn(Collections.singleton(EasyMock.mock(ExecutionProposal.class))).times(2);
    _mockKafkaCruiseControl.executeProposals(EasyMock.anyObject(),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(false),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(null),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS),
                                             EasyMock.eq(SELF_HEALING_REPLICA_MOVEMENT_STRATEGY),
                                             EasyMock.eq(null),
                                             EasyMock.eq(false),
                                             EasyMock.anyString(),
                                             EasyMock.eq(false));
    EasyMock.expect(_optimizerResult.getProposalSummaryForJson()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.statsByGoalName()).andReturn(new LinkedHashMap<>(0)).times(2);
    EasyMock.expect(_optimizerResult.brokerStatsAfterOptimization()).andReturn(_brokerStats).times(2);
    EasyMock.expect(_brokerStats.getJsonStructure()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.getProposalSummary()).andReturn(null);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
    MaintenanceEvent maintenanceEvent = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                      MaintenanceEvent.class,
                                                                      parameterConfigOverrides);

    assertEquals(String.format("%s{Handling %s%s", SELF_HEALING_REASON_PREFIX, MaintenanceEventType.ADD_BROKER,
                               WITH_BROKERS_REASON_SUFFIX), maintenanceEvent.reasonSupplier().get());
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertThrows(IllegalArgumentException.class, maintenanceEvent::hasProposalsToFix);

    assertTrue(maintenanceEvent.fix());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
  }

  @Test
  public void testRemoveBrokerEvent()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    // Populate parameter config overrides.
    Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl,
                                                          BROKERS_OBJECT_CONFIG, MOCK_BROKERS_OBJECT,
                                                          MAINTENANCE_EVENT_TYPE_CONFIG, MaintenanceEventType.REMOVE_BROKER,
                                                          ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS);

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(3);
    _mockKafkaCruiseControl.sanityCheckDryRun(false, true);
    EasyMock.expect(_mockKafkaCruiseControl.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(_mockKafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject())).andReturn(
        MonitorUtils.combineLoadRequirementOptions(goalsByPriority(getSelfHealingGoalNames(_config), _config)));
    EasyMock.expect(_mockKafkaCruiseControl.getLoadMonitorTaskRunnerState()).andReturn(
        LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    _mockKafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(false));
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    ClusterModel clusterModel = unbalanced();
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(_mockKafkaCruiseControl.executorState()).andReturn(_executorState).once();

    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(true))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(false))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, SELF_HEALING_EXCLUDED_TOPICS)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject()))
            .andReturn(_optimizerResult);
    EasyMock.expect(_optimizerResult.goalProposals()).andReturn(Collections.singleton(EasyMock.mock(ExecutionProposal.class))).times(2);
    _mockKafkaCruiseControl.executeRemoval(EasyMock.anyObject(),
                                           EasyMock.eq(false),
                                           EasyMock.eq(MOCK_BROKERS_OBJECT),
                                           EasyMock.eq(false),
                                           EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                           EasyMock.anyObject(),
                                           EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                           EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                           EasyMock.eq(SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS),
                                           EasyMock.eq(SELF_HEALING_REPLICA_MOVEMENT_STRATEGY),
                                           EasyMock.eq(null),
                                           EasyMock.eq(false),
                                           EasyMock.anyString());
    EasyMock.expect(_optimizerResult.getProposalSummaryForJson()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.statsByGoalName()).andReturn(new LinkedHashMap<>(0)).times(2);
    EasyMock.expect(_optimizerResult.brokerStatsAfterOptimization()).andReturn(_brokerStats).times(2);
    EasyMock.expect(_brokerStats.getJsonStructure()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.getProposalSummary()).andReturn(null).times(2);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
    MaintenanceEvent maintenanceEvent = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                      MaintenanceEvent.class,
                                                                      parameterConfigOverrides);

    assertEquals(String.format("%s{Handling %s%s", SELF_HEALING_REASON_PREFIX, MaintenanceEventType.REMOVE_BROKER,
                               WITH_BROKERS_REASON_SUFFIX), maintenanceEvent.reasonSupplier().get());
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertThrows(IllegalArgumentException.class, maintenanceEvent::hasProposalsToFix);

    assertTrue(maintenanceEvent.fix());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
  }

  @Test
  public void testFixOfflineReplicasEvent()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    // Populate parameter config overrides.
    Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl,
                                                          MAINTENANCE_EVENT_TYPE_CONFIG, MaintenanceEventType.FIX_OFFLINE_REPLICAS,
                                                          ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS);

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(3);
    _mockKafkaCruiseControl.sanityCheckDryRun(false, true);
    EasyMock.expect(_mockKafkaCruiseControl.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(_mockKafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject())).andReturn(
        MonitorUtils.combineLoadRequirementOptions(goalsByPriority(getSelfHealingGoalNames(_config), _config)));
    EasyMock.expect(_mockKafkaCruiseControl.getLoadMonitorTaskRunnerState()).andReturn(
        LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    _mockKafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(false));
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    ClusterModel clusterModel = singleBrokerWithBadDisk();
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(_mockKafkaCruiseControl.executorState()).andReturn(_executorState).once();

    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(true))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(false))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, SELF_HEALING_EXCLUDED_TOPICS)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject()))
            .andReturn(_optimizerResult);
    EasyMock.expect(_optimizerResult.goalProposals()).andReturn(Collections.singleton(EasyMock.mock(ExecutionProposal.class))).times(2);
    _mockKafkaCruiseControl.executeProposals(EasyMock.anyObject(),
                                             EasyMock.eq(Collections.emptySet()),
                                             EasyMock.eq(false),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(null),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS),
                                             EasyMock.eq(SELF_HEALING_REPLICA_MOVEMENT_STRATEGY),
                                             EasyMock.eq(null),
                                             EasyMock.eq(false),
                                             EasyMock.anyString(),
                                             EasyMock.eq(false));
    EasyMock.expect(_optimizerResult.getProposalSummaryForJson()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.statsByGoalName()).andReturn(new LinkedHashMap<>(0)).times(2);
    EasyMock.expect(_optimizerResult.brokerStatsAfterOptimization()).andReturn(_brokerStats).times(2);
    EasyMock.expect(_brokerStats.getJsonStructure()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.getProposalSummary()).andReturn(null);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
    MaintenanceEvent maintenanceEvent = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                      MaintenanceEvent.class,
                                                                      parameterConfigOverrides);

    assertEquals(String.format("%s{Handling %s}", SELF_HEALING_REASON_PREFIX, MaintenanceEventType.FIX_OFFLINE_REPLICAS),
                 maintenanceEvent.reasonSupplier().get());
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertThrows(IllegalArgumentException.class, maintenanceEvent::hasProposalsToFix);

    assertTrue(maintenanceEvent.fix());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
  }

  @Test
  public void testRebalanceEvent()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    // Populate parameter config overrides.
    Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl,
                                                          MAINTENANCE_EVENT_TYPE_CONFIG, MaintenanceEventType.REBALANCE,
                                                          ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS);

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(3);
    // This is for rebalance runnable
    _mockKafkaCruiseControl.sanityCheckDryRun(false, true);
    EasyMock.expect(_mockKafkaCruiseControl.hasOngoingExecution()).andReturn(false).once();
    // This is for proposals runnable
    _mockKafkaCruiseControl.sanityCheckDryRun(true, false);
    EasyMock.expect(_mockKafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject())).andReturn(
        MonitorUtils.combineLoadRequirementOptions(goalsByPriority(getSelfHealingGoalNames(_config), _config)));
    EasyMock.expect(_mockKafkaCruiseControl.getLoadMonitorTaskRunnerState()).andReturn(
        LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    _mockKafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(false));
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);

    EasyMock.expect(_mockKafkaCruiseControl
                        .ignoreProposalCache(EasyMock.anyObject(),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(SELF_HEALING_EXCLUDED_TOPICS),
                                             EasyMock.eq(AnomalyDetectorConfig.DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG),
                                             EasyMock.eq(SELF_HEALING_IGNORE_PROPOSAL_CACHE),
                                             EasyMock.eq(true),
                                             EasyMock.eq(SELF_HEALING_DESTINATION_BROKER_IDS),
                                             EasyMock.eq(SELF_HEALING_IS_REBALANCE_DISK_MODE))).andReturn(true);

    EasyMock.expect(_mockKafkaCruiseControl.timeMs()).andReturn(MOCK_TIME_MS);

    ClusterModel clusterModel = unbalanced();
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL),
                                                         EasyMock.eq(MOCK_TIME_MS),
                                                         EasyMock.anyObject(),
                                                         EasyMock.eq(false),
                                                         EasyMock.eq(true),
                                                         EasyMock.anyObject()))
            .andReturn(clusterModel);

    EasyMock.expect(_mockKafkaCruiseControl.executorState()).andReturn(_executorState).once();
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, SELF_HEALING_EXCLUDED_TOPICS)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject()))
            .andReturn(_optimizerResult);
    EasyMock.expect(_optimizerResult.goalProposals()).andReturn(Collections.singleton(EasyMock.mock(ExecutionProposal.class))).times(2);
    _mockKafkaCruiseControl.executeProposals(EasyMock.anyObject(),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(false),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(null),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS),
                                             EasyMock.eq(SELF_HEALING_REPLICA_MOVEMENT_STRATEGY),
                                             EasyMock.eq(null),
                                             EasyMock.eq(null),
                                             EasyMock.eq(false),
                                             EasyMock.anyString(),
                                             EasyMock.eq(false));
    EasyMock.expect(_optimizerResult.getProposalSummaryForJson()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.statsByGoalName()).andReturn(new LinkedHashMap<>(0)).times(2);
    EasyMock.expect(_optimizerResult.brokerStatsAfterOptimization()).andReturn(_brokerStats).times(2);
    EasyMock.expect(_brokerStats.getJsonStructure()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.getProposalSummary()).andReturn(null);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
    MaintenanceEvent maintenanceEvent = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                      MaintenanceEvent.class,
                                                                      parameterConfigOverrides);

    assertEquals(String.format("%s{Handling %s}", SELF_HEALING_REASON_PREFIX, MaintenanceEventType.REBALANCE),
                 maintenanceEvent.reasonSupplier().get());
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertThrows(IllegalArgumentException.class, maintenanceEvent::hasProposalsToFix);

    assertTrue(maintenanceEvent.fix());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
  }

  @Test
  public void testDemoteBrokerEvent()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    // Populate parameter config overrides.
    Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl,
                                                          BROKERS_OBJECT_CONFIG, MOCK_BROKERS_OBJECT,
                                                          MAINTENANCE_EVENT_TYPE_CONFIG, MaintenanceEventType.DEMOTE_BROKER,
                                                          ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS);

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(2);
    _mockKafkaCruiseControl.sanityCheckDryRun(false, true);
    EasyMock.expect(_mockKafkaCruiseControl.hasOngoingExecution()).andReturn(false).once();
    _mockKafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(false));
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    _mockKafkaCruiseControl.sanityCheckBrokerPresence(MOCK_BROKERS_OBJECT);
    ClusterModel clusterModel = unbalanced();
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);

    EasyMock.expect(_mockKafkaCruiseControl.kafkaCluster()).andReturn(generateClusterFromClusterModel(clusterModel)).once();

    EasyMock.expect(_mockKafkaCruiseControl.executorState()).andReturn(_executorState).once();

    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(true))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(false))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, SELF_HEALING_EXCLUDED_TOPICS)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject()))
            .andReturn(_optimizerResult);
    EasyMock.expect(_optimizerResult.goalProposals()).andReturn(Collections.singleton(EasyMock.mock(ExecutionProposal.class))).times(2);
    _mockKafkaCruiseControl.executeDemotion(EasyMock.anyObject(),
                                            EasyMock.eq(MOCK_BROKERS_OBJECT),
                                            EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                            EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                            EasyMock.eq(clusterModel.brokers().size()),
                                            EasyMock.eq(SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS),
                                            EasyMock.eq(SELF_HEALING_REPLICA_MOVEMENT_STRATEGY),
                                            EasyMock.eq(null),
                                            EasyMock.eq(false),
                                            EasyMock.anyString());
    EasyMock.expect(_optimizerResult.getProposalSummaryForJson()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.statsByGoalName()).andReturn(new LinkedHashMap<>(0)).times(2);
    EasyMock.expect(_optimizerResult.brokerStatsAfterOptimization()).andReturn(_brokerStats).times(2);
    EasyMock.expect(_brokerStats.getJsonStructure()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.getProposalSummary()).andReturn(null);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
    MaintenanceEvent maintenanceEvent = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                      MaintenanceEvent.class,
                                                                      parameterConfigOverrides);

    assertEquals(String.format("%s{Handling %s%s", SELF_HEALING_REASON_PREFIX, MaintenanceEventType.DEMOTE_BROKER,
                               WITH_BROKERS_REASON_SUFFIX), maintenanceEvent.reasonSupplier().get());
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertThrows(IllegalArgumentException.class, maintenanceEvent::hasProposalsToFix);

    assertTrue(maintenanceEvent.fix());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
  }

  @Test
  public void testTopicReplicationFactorEvent()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    // Populate parameter config overrides.
    Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl,
                                                          MAINTENANCE_EVENT_TYPE_CONFIG, MaintenanceEventType.TOPIC_REPLICATION_FACTOR,
                                                          ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS,
                                                          TOPICS_WITH_RF_UPDATE_CONFIG, MOCK_TOPICS_WITH_RF_UPDATE);

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(3);
    _mockKafkaCruiseControl.sanityCheckDryRun(false, true);
    EasyMock.expect(_mockKafkaCruiseControl.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(_mockKafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject())).andReturn(
        MonitorUtils.combineLoadRequirementOptions(goalsByPriority(getSelfHealingGoalNames(_config), _config)));
    EasyMock.expect(_mockKafkaCruiseControl.getLoadMonitorTaskRunnerState()).andReturn(
        LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    _mockKafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(false));
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);

    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(true))).andReturn(false);
    EasyMock.expect(_mockKafkaCruiseControl.dropRecentBrokers(EasyMock.eq(Collections.emptySet()),
                                                              EasyMock.eq(false))).andReturn(false);

    ClusterModel clusterModel = unbalanced();
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.eq(true), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(_mockKafkaCruiseControl.kafkaCluster()).andReturn(generateClusterFromClusterModel(clusterModel)).once();

    EasyMock.expect(_mockKafkaCruiseControl.executorState()).andReturn(_executorState).once();
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, SELF_HEALING_EXCLUDED_TOPICS)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject()))
            .andReturn(_optimizerResult);
    EasyMock.expect(_optimizerResult.goalProposals()).andReturn(Collections.singleton(EasyMock.mock(ExecutionProposal.class))).times(2);
    _mockKafkaCruiseControl.executeProposals(EasyMock.anyObject(),
                                             EasyMock.eq(Collections.emptySet()),
                                             EasyMock.eq(false),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.anyObject(),
                                             EasyMock.eq(0),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_CONCURRENT_MOVEMENTS),
                                             EasyMock.eq(SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS),
                                             EasyMock.eq(SELF_HEALING_REPLICA_MOVEMENT_STRATEGY),
                                             EasyMock.eq(null),
                                             EasyMock.eq(false),
                                             EasyMock.anyString(),
                                             EasyMock.eq(true));
    EasyMock.expect(_optimizerResult.getProposalSummaryForJson()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.statsByGoalName()).andReturn(new LinkedHashMap<>(0)).times(2);
    EasyMock.expect(_optimizerResult.brokerStatsAfterOptimization()).andReturn(_brokerStats).times(2);
    EasyMock.expect(_brokerStats.getJsonStructure()).andReturn(Collections.emptyMap());
    EasyMock.expect(_optimizerResult.getProposalSummary()).andReturn(null);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
    MaintenanceEvent maintenanceEvent = _config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                      MaintenanceEvent.class,
                                                                      parameterConfigOverrides);

    assertEquals(String.format("%s{Handling %s%s", SELF_HEALING_REASON_PREFIX, MaintenanceEventType.TOPIC_REPLICATION_FACTOR,
                               WITH_RF_UPDATE_SUFFIX), maintenanceEvent.reasonSupplier().get());
    assertEquals(MAINTENANCE_EVENT, maintenanceEvent.anomalyType());
    assertThrows(IllegalArgumentException.class, maintenanceEvent::hasProposalsToFix);
    assertTrue(maintenanceEvent.fix());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult, _brokerStats);
  }

  @Test
  public void testToStringBeforeConfiguration() {
    assertNotNull(new MaintenanceEvent().toString());
  }

  @Test
  public void testMaintenanceEventEquality() {
    Set<MaintenanceEvent> maintenanceEvents = new HashSet<>();
    Map<String, Object> parameterConfigOverrides = new HashMap<>();
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _mockKafkaCruiseControl);
    parameterConfigOverrides.put(BROKERS_OBJECT_CONFIG, MOCK_BROKERS_OBJECT);
    parameterConfigOverrides.put(TOPICS_WITH_RF_UPDATE_CONFIG, MOCK_TOPICS_WITH_RF_UPDATE);

    int expectedSetSize = 0;
    for (MaintenanceEventType eventType : MaintenanceEventType.cachedValues()) {
      parameterConfigOverrides.put(MAINTENANCE_EVENT_TYPE_CONFIG, eventType);
      // Expect mocks.
      EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).times(6);
      EasyMock.replay(_mockKafkaCruiseControl);
      for (int i = 1; i <= 3; i++) {
        parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, MOCK_TIME_MS * i);
        boolean notDuplicate = maintenanceEvents.add(_config.getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                                                                                   MaintenanceEvent.class,
                                                                                   parameterConfigOverrides));
        assertEquals(i == 1, notDuplicate);
      }
      assertEquals(++expectedSetSize, maintenanceEvents.size());
      EasyMock.verify(_mockKafkaCruiseControl);
      EasyMock.reset(_mockKafkaCruiseControl);
    }
  }
}
