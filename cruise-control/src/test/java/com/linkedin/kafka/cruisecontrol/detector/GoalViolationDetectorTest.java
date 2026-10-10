/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.MAX_BALANCEDNESS_SCORE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.balancednessCostByGoal;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.getHomogeneousCluster;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.anomalyComparator;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test class for goal violation detector.
 */
public class GoalViolationDetectorTest {
  private static final long MOCK_TIME_MS = 100L;
  private static final double DELTA = 1e-6;
  private static final Map<Integer, Integer> RACK_BY_BROKER = Map.of(0, 0, 1, 1);
  private static final int NUM_REPLICAS_ON_BROKER0 = 6;
  private static final int NUM_REPLICAS_ON_BROKER1 = 2;
  // Each replica uses 1/15 of the capacity of a disk.
  private static final double DISK_USAGE_PER_REPLICA = TestConstants.LARGE_BROKER_CAPACITY / 30;
  private static final String INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL = IntraBrokerDiskUsageDistributionGoal.class.getSimpleName();
  private static final String REPLICA_CAPACITY_GOAL = ReplicaCapacityGoal.class.getSimpleName();
  // Broker 0 exceeds the replica capacity, which can be fixed by moving its replicas to broker 1.
  private static final Map<String, String> REPLICA_CAPACITY_VIOLATION_CONFIGS =
      Map.of(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Integer.toString(NUM_REPLICAS_ON_BROKER0 - 1),
             AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, Integer.toString(NUM_REPLICAS_ON_BROKER0 - 1));

  @Test
  public void testDetectIntraBrokerGoalViolations() throws Exception {
    DetectorFixture fixture = new DetectorFixture(Collections.emptyMap());
    fixture.expectHasBrokerWithMultipleLogDirs(true);
    fixture.expectClusterModelWithReplicaPlacementInfo();
    GoalViolationDetector detector = fixture.replayAndDetect();

    // Disks of broker 0 are imbalanced, which can be fixed by moving replicas between its disks. Inter-broker goals are satisfied.
    assertEquals(1, fixture._anomalies.size());
    Anomaly anomaly = fixture._anomalies.poll();
    assertEquals(KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION, anomaly.anomalyType());
    assertEquals(Collections.singletonMap(true, Collections.singletonList(INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL)),
                 ((IntraBrokerGoalViolations) anomaly).violatedGoalsByFixability());
    assertEquals(MAX_BALANCEDNESS_SCORE - balancednessCostWithIntraBrokerGoals(fixture._config).get(INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL),
                 detector.balancednessScore(), DELTA);
    // The semaphore for cluster model generation is acquired once for detecting the violations of inter-broker goals, and once
    // for intra-broker goals. A thread cannot acquire the semaphore again before releasing it.
    assertEquals(2, fixture._semaphore.numAcquired());
    assertFalse(fixture._semaphore.isHeld());

    // The provision status is based on inter-broker goals only.
    DetectorFixture nonJbodFixture = new DetectorFixture(Collections.emptyMap());
    nonJbodFixture.expectHasBrokerWithMultipleLogDirs(false);
    ProvisionStatus provisionStatusWithoutIntraBrokerGoals = nonJbodFixture.replayAndDetect().provisionStatus();
    assertEquals(provisionStatusWithoutIntraBrokerGoals, detector.provisionStatus());
  }

  @Test
  public void testDetectInterAndIntraBrokerGoalViolations() throws Exception {
    DetectorFixture fixture = new DetectorFixture(REPLICA_CAPACITY_VIOLATION_CONFIGS);
    fixture.expectHasBrokerWithMultipleLogDirs(true);
    fixture.expectClusterModelWithReplicaPlacementInfo();
    GoalViolationDetector detector = fixture.replayAndDetect();

    assertEquals(2, fixture._anomalies.size());
    // Goal violations have a higher priority than intra-broker goal violations.
    Anomaly anomaly = fixture._anomalies.poll();
    assertEquals(KafkaAnomalyType.GOAL_VIOLATION, anomaly.anomalyType());
    assertEquals(Collections.singletonMap(true, Collections.singletonList(REPLICA_CAPACITY_GOAL)),
                 ((GoalViolations) anomaly).violatedGoalsByFixability());
    anomaly = fixture._anomalies.poll();
    assertEquals(KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION, anomaly.anomalyType());
    assertEquals(Collections.singletonMap(true, Collections.singletonList(INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL)),
                 ((IntraBrokerGoalViolations) anomaly).violatedGoalsByFixability());
    Map<String, Double> balancednessCost = balancednessCostWithIntraBrokerGoals(fixture._config);
    assertEquals(MAX_BALANCEDNESS_SCORE - balancednessCost.get(REPLICA_CAPACITY_GOAL)
                 - balancednessCost.get(INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL), detector.balancednessScore(), DELTA);
    assertFalse(fixture._semaphore.isHeld());
  }

  @Test
  public void testSkipIntraBrokerGoalViolationDetectionWithoutJbod() throws Exception {
    DetectorFixture fixture = new DetectorFixture(REPLICA_CAPACITY_VIOLATION_CONFIGS);
    // The strict mock fails the test upon generating a cluster model with replica placement info.
    fixture.expectHasBrokerWithMultipleLogDirs(false);
    GoalViolationDetector detector = fixture.replayAndDetect();

    assertEquals(1, fixture._anomalies.size());
    assertEquals(KafkaAnomalyType.GOAL_VIOLATION, fixture._anomalies.poll().anomalyType());
    // The balancedness score is unaffected by intra-broker goals unless their violations are detected.
    Map<String, Double> balancednessCost = balancednessCostByGoal(goals(fixture._config, AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG),
                                                                  priorityWeight(fixture._config), strictnessWeight(fixture._config));
    assertEquals(MAX_BALANCEDNESS_SCORE - balancednessCost.get(REPLICA_CAPACITY_GOAL), detector.balancednessScore(), DELTA);
    assertFalse(fixture._semaphore.isHeld());
  }

  @Test
  public void testSkipIntraBrokerGoalViolationDetectionWithoutIntraBrokerDetectionGoals() throws Exception {
    // The strict mock fails the test upon checking whether the cluster is JBOD.
    DetectorFixture fixture = new DetectorFixture(Map.of(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG, ""));
    GoalViolationDetector detector = fixture.replayAndDetect();

    assertTrue(fixture._anomalies.isEmpty());
    assertEquals(MAX_BALANCEDNESS_SCORE, detector.balancednessScore(), DELTA);
  }

  @Test
  public void testSkipIntraBrokerGoalViolationDetectionUponCapacityResolutionFailure() throws Exception {
    DetectorFixture fixture = new DetectorFixture(REPLICA_CAPACITY_VIOLATION_CONFIGS);
    EasyMock.expect(fixture._loadMonitor.hasBrokerWithMultipleLogDirs(EasyMock.anyBoolean()))
            .andThrow(new BrokerCapacityResolutionException("Failed to resolve capacity."));
    fixture.replayAndDetect();

    // Goal violations are reported regardless.
    assertEquals(1, fixture._anomalies.size());
    assertEquals(KafkaAnomalyType.GOAL_VIOLATION, fixture._anomalies.poll().anomalyType());
  }

  @Test
  public void testIntraBrokerGoalViolationDetectionFailure() throws Exception {
    DetectorFixture fixture = new DetectorFixture(REPLICA_CAPACITY_VIOLATION_CONFIGS);
    fixture.expectHasBrokerWithMultipleLogDirs(true);
    // E.g. the capacity of a broker does not specify the disk capacity by logdir.
    EasyMock.expect(fixture._kafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL),
                                                             EasyMock.eq(MOCK_TIME_MS),
                                                             EasyMock.anyObject(),
                                                             EasyMock.eq(true),
                                                             EasyMock.anyBoolean(),
                                                             EasyMock.anyObject()))
            .andThrow(new IllegalStateException("Missing disk capacity by logdir."));
    fixture.replayAndDetect();

    // Goal violations are reported regardless, and the semaphore for cluster model generation is released.
    assertEquals(1, fixture._anomalies.size());
    assertEquals(KafkaAnomalyType.GOAL_VIOLATION, fixture._anomalies.poll().anomalyType());
    assertFalse(fixture._semaphore.isHeld());
  }

  @Test
  public void testSkipGoalViolationDetectionUponOfflineReplicasInClusterModelWithReplicaPlacementInfo() throws Exception {
    DetectorFixture fixture = new DetectorFixture(REPLICA_CAPACITY_VIOLATION_CONFIGS);
    fixture.expectHasBrokerWithMultipleLogDirs(true);
    // A broker fails after detecting the violations of inter-broker goals.
    ClusterModel clusterModelWithDeadBroker = clusterModel(true);
    clusterModelWithDeadBroker.setBrokerState(1, Broker.State.DEAD);
    EasyMock.expect(fixture._kafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL),
                                                             EasyMock.eq(MOCK_TIME_MS),
                                                             EasyMock.anyObject(),
                                                             EasyMock.eq(true),
                                                             EasyMock.anyBoolean(),
                                                             EasyMock.anyObject()))
            .andReturn(clusterModelWithDeadBroker);
    GoalViolationDetector detector = fixture.replayAndDetect();

    // Offline replicas are reported by the broker failure detector.
    assertTrue(fixture._anomalies.isEmpty());
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), DELTA);
    assertFalse(fixture._semaphore.isHeld());
  }

  /**
   * Two brokers on two racks, each has two disks. Broker 0 has six replicas, all of which reside on one of its disks. Broker 1 has
   * two replicas, one on each of its disks.
   *
   * @param populateReplicaPlacementInfo {@code true} to populate the replica placement over disks, {@code false} otherwise.
   * @return Cluster model for the tests.
   */
  private static ClusterModel clusterModel(boolean populateReplicaPlacementInfo) {
    ClusterModel clusterModel = getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY,
                                                      populateReplicaPlacementInfo ? TestConstants.DISK_CAPACITY : null);
    for (int partition = 0; partition < NUM_REPLICAS_ON_BROKER0 + NUM_REPLICAS_ON_BROKER1; partition++) {
      TopicPartition tp = new TopicPartition(T1, partition);
      int brokerId = partition < NUM_REPLICAS_ON_BROKER0 ? 0 : 1;
      String rackId = RACK_BY_BROKER.get(brokerId).toString();
      String logdir = null;
      if (populateReplicaPlacementInfo) {
        logdir = brokerId == 0 || partition % 2 == 0 ? TestConstants.LOGDIR0 : TestConstants.LOGDIR1;
      }
      clusterModel.createReplica(rackId, brokerId, tp, 0, true, false, logdir, false);
      clusterModel.setReplicaLoad(rackId, brokerId, tp, getAggregatedMetricValues(1.0, 1.0, 1.0, DISK_USAGE_PER_REPLICA),
                                  Collections.singletonList(1L));
    }
    return clusterModel;
  }

  private static List<Goal> goals(KafkaCruiseControlConfig config, String goalsConfig) {
    return config.getConfiguredInstances(goalsConfig, Goal.class);
  }

  private static double priorityWeight(KafkaCruiseControlConfig config) {
    return config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG);
  }

  private static double strictnessWeight(KafkaCruiseControlConfig config) {
    return config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG);
  }

  private static Map<String, Double> balancednessCostWithIntraBrokerGoals(KafkaCruiseControlConfig config) {
    List<Goal> goals = new ArrayList<>(goals(config, AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG));
    goals.addAll(goals(config, AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG));
    return balancednessCostByGoal(goals, priorityWeight(config), strictnessWeight(config));
  }

  /**
   * Mimics the semaphore of {@link LoadMonitor} for cluster model generation, which a thread cannot acquire again before releasing it.
   */
  private static final class ClusterModelSemaphore {
    private int _numAcquired = 0;
    private boolean _isHeld = false;

    LoadMonitor.AutoCloseableSemaphore acquire() {
      if (_isHeld) {
        throw new IllegalStateException("The thread has already acquired the semaphore for cluster model generation.");
      }
      _isHeld = true;
      _numAcquired++;
      AtomicBoolean closed = new AtomicBoolean(false);
      LoadMonitor.AutoCloseableSemaphore semaphore = EasyMock.mock(LoadMonitor.AutoCloseableSemaphore.class);
      semaphore.close();
      EasyMock.expectLastCall().andAnswer(() -> {
        if (closed.compareAndSet(false, true)) {
          _isHeld = false;
        }
        return null;
      }).anyTimes();
      EasyMock.replay(semaphore);
      return semaphore;
    }

    int numAcquired() {
      return _numAcquired;
    }

    boolean isHeld() {
      return _isHeld;
    }
  }

  /**
   * Mocks the dependencies of goal violation detector for a cluster that is ready for goal violation detection.
   */
  private static final class DetectorFixture {
    private final KafkaCruiseControlConfig _config;
    private final KafkaCruiseControl _kafkaCruiseControl;
    private final LoadMonitor _loadMonitor;
    private final ClusterModelSemaphore _semaphore;
    private final Queue<Anomaly> _anomalies;

    DetectorFixture(Map<String, String> configOverrides) throws Exception {
      Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
      props.putAll(configOverrides);
      _config = new KafkaCruiseControlConfig(props);
      _kafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
      _loadMonitor = EasyMock.mock(LoadMonitor.class);
      _semaphore = new ClusterModelSemaphore();
      _anomalies = new PriorityBlockingQueue<>(ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE, anomalyComparator());

      EasyMock.expect(_kafkaCruiseControl.config()).andReturn(_config).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.adminClient()).andReturn(EasyMock.createNiceMock(AdminClient.class)).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.provisioner()).andReturn(EasyMock.createNiceMock(Provisioner.class)).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.loadMonitor()).andReturn(_loadMonitor).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.getLoadMonitorTaskRunnerState())
              .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.executionState()).andReturn(ExecutorState.State.NO_TASK_IN_PROGRESS).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.executorState())
              .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet())).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.timeMs()).andReturn(MOCK_TIME_MS).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andAnswer(_semaphore::acquire).anyTimes();
      EasyMock.expect(_kafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject()))
              .andAnswer(() -> clusterModel(false)).anyTimes();

      EasyMock.expect(_loadMonitor.clusterModelGeneration()).andReturn(new ModelGeneration(1, 1L)).anyTimes();
      EasyMock.expect(_loadMonitor.brokersWithOfflineReplicas(EasyMock.anyLong())).andReturn(Collections.emptySet()).anyTimes();
      EasyMock.expect(_loadMonitor.meetCompletenessRequirements(EasyMock.anyObject())).andReturn(true).anyTimes();
    }

    void expectHasBrokerWithMultipleLogDirs(boolean hasBrokerWithMultipleLogDirs) throws Exception {
      EasyMock.expect(_loadMonitor.hasBrokerWithMultipleLogDirs(
          _config.getBoolean(AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG)))
              .andReturn(hasBrokerWithMultipleLogDirs);
    }

    void expectClusterModelWithReplicaPlacementInfo() throws Exception {
      EasyMock.expect(_kafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL),
                                                       EasyMock.eq(MOCK_TIME_MS),
                                                       EasyMock.anyObject(),
                                                       EasyMock.eq(true),
                                                       EasyMock.eq(_config.getBoolean(
                                                           AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG)),
                                                       EasyMock.anyObject()))
              .andAnswer(() -> clusterModel(true));
    }

    GoalViolationDetector replayAndDetect() {
      EasyMock.replay(_kafkaCruiseControl, _loadMonitor);
      GoalViolationDetector detector = new GoalViolationDetector(_anomalies, _kafkaCruiseControl, new MetricRegistry());
      detector.run();
      EasyMock.verify(_kafkaCruiseControl, _loadMonitor);
      return detector;
    }
  }
}
