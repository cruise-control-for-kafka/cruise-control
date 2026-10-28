/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataAdminClient;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException;
import com.linkedin.kafka.cruisecontrol.executor.persistence.ExecutionStateStore;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionState;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedTask;
import com.linkedin.kafka.cruisecontrol.executor.persistence.ReplicaPlacementFingerprint;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.waitUntilTrue;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.ALL;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.ONGOING_EXECUTION;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Tests persisting the execution state and recovering an execution interrupted by a restart, against an embedded cluster.
 */
public class ExecutorRecoveryTest extends CCKafkaClientsIntegrationTestHarness {
  private static final String TOPIC = "recovery-topic";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
  private static final String UUID = "recovery-test-uuid";
  private static final String REASON = "recovery test";
  private static final long WAIT_TIME_MS = 60_000L;
  private Admin _adminClient;
  private RecordingExecutionStateStore _store;
  private final List<Executor> _executors = new ArrayList<>();

  @Override
  public int clusterSize() {
    return 3;
  }

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    super.setUp();
    _adminClient = newAdminClient();
    _store = new RecordingExecutionStateStore();
  }

  /**
   * Teardown the test.
   */
  @After
  public void tearDown() {
    for (Executor executor : _executors) {
      // Stop the ongoing execution (if any) first, as shutting down the executor waits for the ongoing execution to finish.
      if (executor.hasOngoingExecution()) {
        executor.userTriggeredStopExecution(false);
        waitForExecutionToFinish(executor);
      }
      executor.shutdown();
    }
    if (_adminClient != null) {
      _adminClient.close(Duration.ofSeconds(1));
    }
    super.tearDown();
  }

  private Admin newAdminClient() {
    return KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                                                              bootstrapServers()));
  }

  /**
   * An in-memory execution state store that records every saved state.
   */
  private static final class RecordingExecutionStateStore implements ExecutionStateStore {
    private final List<PersistedExecutionState> _savedStates = new CopyOnWriteArrayList<>();
    private volatile PersistedExecutionState _state;

    @Override
    public synchronized void save(PersistedExecutionState state) {
      _savedStates.add(state);
      _state = state;
    }

    @Override
    public synchronized Optional<PersistedExecutionState> load() {
      return Optional.ofNullable(_state);
    }

    @Override
    public synchronized void delete() {
      _state = null;
    }

    @Override
    public void configure(Map<String, ?> configs) { }

    PersistedExecutionState state() {
      return _state;
    }

    List<PersistedExecutionState> savedStates() {
      return _savedStates;
    }
  }

  private Executor newExecutor(boolean resume,
                               MetadataAdminClient metadataClient,
                               ExecutorNotifier notifier,
                               AnomalyDetectorManager anomalyDetectorManager) {
    return newExecutor(resume, metadataClient, notifier, anomalyDetectorManager, new Properties());
  }

  private Executor newExecutor(boolean resume,
                               MetadataAdminClient metadataClient,
                               ExecutorNotifier notifier,
                               AnomalyDetectorManager anomalyDetectorManager,
                               Properties overrides) {
    Properties props = new Properties();
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, Objects.requireNonNull(
        getClass().getClassLoader().getResource(TestConstants.DEFAULT_BROKER_CAPACITY_CONFIG_FILE)).getFile());
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "400");
    props.setProperty(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "200");
    props.setProperty(ExecutorConfig.RESUME_INTERRUPTED_EXECUTION_ON_STARTUP_CONFIG, Boolean.toString(resume));
    props.putAll(overrides);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    // The executor closes its admin client upon shutdown.
    Admin executorAdminClient = newAdminClient();
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), executorAdminClient,
                                     metadataClient == null ? new MetadataAdminClient(executorAdminClient) : metadataClient,
                                     notifier, anomalyDetectorManager, _store);
    _executors.add(executor);
    return executor;
  }

  private static LoadMonitor mockLoadMonitor() {
    LoadMonitor loadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(loadMonitor.samplingMode()).andReturn(ALL).anyTimes();
    EasyMock.expect(loadMonitor.deadBrokersWithReplicas(anyLong())).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(loadMonitor.brokersWithReplicas(anyLong())).andReturn(Set.of(0, 1, 2)).anyTimes();
    loadMonitor.pauseMetricSampling(isA(String.class), EasyMock.anyBoolean());
    expectLastCall().anyTimes();
    loadMonitor.setSamplingMode(ONGOING_EXECUTION);
    expectLastCall().anyTimes();
    loadMonitor.resumeMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    loadMonitor.setSamplingMode(ALL);
    expectLastCall().anyTimes();
    EasyMock.replay(loadMonitor);
    return loadMonitor;
  }

  /**
   * @param selfHealingCompleteWithError {@code null} if the execution is not expected to report its completion to the anomaly
   *                                     detector (i.e. a resumed execution), whether it completes with error otherwise.
   * @return A mock anomaly detector manager.
   */
  private static AnomalyDetectorManager mockAnomalyDetectorManager(Boolean selfHealingCompleteWithError) {
    AnomalyDetectorManager anomalyDetectorManager = EasyMock.mock(AnomalyDetectorManager.class);
    anomalyDetectorManager.maybeClearOngoingAnomalyDetectionTimeMs();
    expectLastCall().anyTimes();
    anomalyDetectorManager.resetHasUnfixableGoals();
    expectLastCall().anyTimes();
    if (selfHealingCompleteWithError != null) {
      anomalyDetectorManager.markSelfHealingFinished(UUID, selfHealingCompleteWithError);
      expectLastCall().once();
    }
    EasyMock.replay(anomalyDetectorManager);
    return anomalyDetectorManager;
  }

  /**
   * A notifier that records all notifications and alerts.
   */
  private static final class RecordingNotifier implements ExecutorNotifier {
    private final List<String> _messages = new CopyOnWriteArrayList<>();

    @Override
    public void sendNotification(String message) {
      _messages.add(message);
    }

    @Override
    public void sendAlert(String alertMessage) {
      _messages.add(alertMessage);
    }

    @Override
    public void configure(Map<String, ?> configs) { }

    boolean anyMessageContains(String text) {
      return _messages.stream().anyMatch(m -> m.contains(text));
    }
  }

  private void createTopic(Map<Integer, List<Integer>> replicasByPartition) throws Exception {
    _adminClient.createTopics(Collections.singleton(new NewTopic(TOPIC, replicasByPartition))).all().get();
    waitUntilTrue(() -> replicasByPartition.entrySet().stream().allMatch(e -> e.getValue().equals(currentReplicas(e.getKey()))),
                  "Topic is not created", WAIT_TIME_MS, 100L);
  }

  private List<Integer> currentReplicas(int partition) {
    try {
      TopicPartitionInfo info = _adminClient.describeTopics(Collections.singleton(TOPIC)).allTopicNames().get().get(TOPIC)
                                            .partitions().get(partition);
      return info.replicas().stream().map(Node::id).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException e) {
      return Collections.emptyList();
    }
  }

  private static ExecutionProposal proposal(TopicPartition tp, List<Integer> oldReplicas, List<Integer> newReplicas) {
    return new ExecutionProposal(tp, 0, new ReplicaPlacementInfo(oldReplicas.get(0)),
                                 oldReplicas.stream().map(ReplicaPlacementInfo::new).collect(Collectors.toList()),
                                 newReplicas.stream().map(ReplicaPlacementInfo::new).collect(Collectors.toList()));
  }

  /**
   * @param replicationThrottle Replication throttle of the interrupted execution.
   * @param proposals Proposals of the interrupted execution, each with an inter-broker replica task in progress.
   * @return The state of an execution that was just interrupted, with the fingerprint of the cluster as it is now.
   */
  private PersistedExecutionState interruptedExecutionState(Long replicationThrottle, List<ExecutionProposal> proposals) {
    List<PersistedTask> tasks = proposals.stream().map(p -> new PersistedTask(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                                                                              p.topicPartition(), PersistedTask.NO_BROKER_ID,
                                                                              ExecutionTaskState.IN_PROGRESS, 1L, -1L))
                                         .collect(Collectors.toList());
    Set<TopicPartition> partitions = proposals.stream().map(ExecutionProposal::topicPartition).collect(Collectors.toSet());
    return new PersistedExecutionState.Builder(UUID, PersistedExecutionState.Operation.EXECUTE_PROPOSALS)
        .reason(REASON)
        .startTimeMs(System.currentTimeMillis())
        .lastUpdateTimeMs(System.currentTimeMillis())
        .executorState(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS)
        .replicationThrottle(replicationThrottle)
        .outsideExecutionFingerprint(ReplicaPlacementFingerprint.of(new MetadataAdminClient(_adminClient).cluster(), partitions))
        .proposals(proposals)
        .tasks(tasks)
        .build();
  }

  private static Map<String, Object> recovery(Executor executor) {
    Map<String, Object> recovery = executor.state().recovery();
    assertNotNull("The executor state does not report the recovery", recovery);
    return recovery;
  }

  private String brokerRate(int broker, String rateConfig) throws Exception {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(broker));
    ConfigEntry entry = _adminClient.describeConfigs(Collections.singleton(resource)).all().get().get(resource).get(rateConfig);
    return entry == null ? null : entry.value();
  }

  /**
   * Start a reassignment of {@link #TP0} from [0, 1] to [0, 2] that is kept in flight by a tiny replication throttle -- i.e.
   * the in-flight reassignment of an interrupted execution.
   *
   * @param throttle The replication throttle to set.
   * @return The proposal of the reassignment.
   */
  private ExecutionProposal startThrottledReassignment(long throttle) throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    char[] chars = new char[10_000];
    Arrays.fill(chars, 'x');
    String value = new String(chars);
    try (Producer<String, String> producer = createProducer(producerProps)) {
      for (int i = 0; i < 300; i++) {
        producer.send(new ProducerRecord<>(TOPIC, 0, null, value));
      }
      producer.flush();
    }
    ExecutionProposal proposal = proposal(TP0, List.of(0, 1), List.of(0, 2));
    new ReplicationThrottleHelper(_adminClient, throttle).setThrottles(List.of(proposal));
    _adminClient.alterPartitionReassignments(Map.of(TP0, Optional.of(new NewPartitionReassignment(List.of(0, 2))))).all().get();
    assertTrue(_adminClient.listPartitionReassignments().reassignments().get().containsKey(TP0));
    return proposal;
  }

  private static void waitForExecutionToFinish(Executor executor) {
    waitUntilTrue(() -> !executor.hasOngoingExecution() && executor.state().state() == ExecutorState.State.NO_TASK_IN_PROGRESS,
                  "The execution did not finish", WAIT_TIME_MS, 200L);
  }

  private static PersistedTask onlyTask(PersistedExecutionState state) {
    assertEquals(1, state.tasks().size());
    return state.tasks().get(0);
  }

  private boolean hasThrottles() throws Exception {
    List<ConfigResource> resources = new ArrayList<>();
    for (int broker = 0; broker < clusterSize(); broker++) {
      resources.add(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(broker)));
    }
    resources.add(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC));
    for (Config config : _adminClient.describeConfigs(resources).all().get().values()) {
      for (String name : Arrays.asList(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                                       ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                                       ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                                       ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
        if (config.get(name) != null && config.get(name).value() != null && !config.get(name).value().isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  @Test
  public void testExecutionStateIsPersistedUntilCompletion() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    Executor executor = newExecutor(false, null, new RecordingNotifier(), mockAnomalyDetectorManager(false));
    executor.setGeneratingProposalsForExecution(UUID, () -> REASON, false);
    executor.executeProposals(List.of(proposal(TP0, List.of(0, 1), List.of(0, 2))), Collections.emptySet(), null, mockLoadMonitor(),
                              null, null, null, null, null, null, null, null, false, UUID, false, false);
    waitForExecutionToFinish(executor);
    assertEquals(List.of(0, 2), currentReplicas(0));

    List<PersistedExecutionState> savedStates = _store.savedStates();
    // The initial state is persisted before the execution starts.
    PersistedExecutionState initialState = savedStates.get(0);
    assertEquals(UUID, initialState.uuid());
    assertEquals(REASON, initialState.reason());
    assertEquals(ExecutorState.State.INITIALIZING_PROPOSAL_EXECUTION, initialState.executorState());
    assertEquals(ExecutionTaskState.PENDING, onlyTask(initialState).state());
    // The task is persisted as in progress (before it is submitted), then as completed.
    List<ExecutionTaskState> persistedTaskStates = savedStates.stream().map(s -> onlyTask(s).state()).distinct().collect(Collectors.toList());
    assertEquals(List.of(ExecutionTaskState.PENDING, ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.COMPLETED), persistedTaskStates);
    // Every persisted state has the fingerprint of the replicas outside the execution.
    assertTrue(savedStates.stream().allMatch(s -> s.outsideExecutionFingerprint() != null));
    // The persisted state is deleted once the execution finishes.
    assertNull(_store.state());
  }

  @Test
  public void testRecoveryDetectsInProgressTaskCompletedDuringDowntime() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    // A metadata client that never reflects the progress of the execution, so that the task remains in progress.
    Node[] nodes = {new Node(0, "h0", 9092), new Node(1, "h1", 9092), new Node(2, "h2", 9092)};
    Cluster staleCluster = new Cluster("cluster", Arrays.asList(nodes),
                                       Collections.singleton(new PartitionInfo(TOPIC, 0, nodes[0], new Node[]{nodes[0], nodes[1]},
                                                                               new Node[]{nodes[0], nodes[1]})),
                                       Collections.emptySet(), Collections.emptySet());
    MetadataAdminClient staleMetadataClient = EasyMock.mock(MetadataAdminClient.class);
    EasyMock.expect(staleMetadataClient.cluster()).andReturn(staleCluster).anyTimes();
    EasyMock.replay(staleMetadataClient);
    AnomalyDetectorManager anomalyDetectorManager = EasyMock.mock(AnomalyDetectorManager.class);
    anomalyDetectorManager.maybeClearOngoingAnomalyDetectionTimeMs();
    expectLastCall().anyTimes();
    anomalyDetectorManager.resetHasUnfixableGoals();
    expectLastCall().anyTimes();
    anomalyDetectorManager.markSelfHealingFinished(EasyMock.eq(UUID), EasyMock.anyBoolean());
    expectLastCall().anyTimes();
    EasyMock.replay(anomalyDetectorManager);

    Executor executor = newExecutor(false, staleMetadataClient, new RecordingNotifier(), anomalyDetectorManager);
    executor.setGeneratingProposalsForExecution(UUID, () -> REASON, false);
    executor.executeProposals(List.of(proposal(TP0, List.of(0, 1), List.of(0, 2))), Collections.emptySet(), null, mockLoadMonitor(),
                              null, null, null, null, null, null, null, null, false, UUID, false, false);
    waitUntilTrue(() -> _store.state() != null && onlyTask(_store.state()).state() == ExecutionTaskState.IN_PROGRESS,
                  "The task is not persisted as in progress", WAIT_TIME_MS, 100L);
    // Kafka completes the reassignment, but the executor does not see it due to the stale metadata.
    waitUntilTrue(() -> List.of(0, 2).equals(currentReplicas(0)), "Reassignment did not complete", WAIT_TIME_MS, 100L);
    // Simulate a crash: the persisted state is what survives the crash.
    PersistedExecutionState stateAtCrash = _store.state();
    assertEquals(ExecutionTaskState.IN_PROGRESS, onlyTask(stateAtCrash).state());
    assertEquals(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, stateAtCrash.executorState());
    executor.userTriggeredStopExecution(false);
    waitForExecutionToFinish(executor);
    _store.save(stateAtCrash);

    // Upon restart, the in-progress task is found completed in the cluster -- nothing to resume, and the state is cleaned up.
    RecordingNotifier notifier = new RecordingNotifier();
    Executor restartedExecutor = newExecutor(true, null, notifier, mockAnomalyDetectorManager(null));
    restartedExecutor.recoverInterruptedExecution(mockLoadMonitor());
    waitUntilTrue(() -> _store.state() == null, "The interrupted execution is not cleaned up", WAIT_TIME_MS, 100L);
    waitForExecutionToFinish(restartedExecutor);
    assertTrue(notifier.anyMessageContains("there are no remaining tasks to execute"));
    assertTrue(notifier.anyMessageContains("COMPLETED=1"));
  }

  @Test
  public void testUserStopDeletesExecutionState() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    Node[] nodes = {new Node(0, "h0", 9092), new Node(1, "h1", 9092), new Node(2, "h2", 9092)};
    Cluster staleCluster = new Cluster("cluster", Arrays.asList(nodes),
                                       Collections.singleton(new PartitionInfo(TOPIC, 0, nodes[0], new Node[]{nodes[0], nodes[1]},
                                                                               new Node[]{nodes[0], nodes[1]})),
                                       Collections.emptySet(), Collections.emptySet());
    MetadataAdminClient staleMetadataClient = EasyMock.mock(MetadataAdminClient.class);
    EasyMock.expect(staleMetadataClient.cluster()).andReturn(staleCluster).anyTimes();
    EasyMock.replay(staleMetadataClient);
    Executor executor = newExecutor(false, staleMetadataClient, new RecordingNotifier(), mockAnomalyDetectorManager(true));
    executor.setGeneratingProposalsForExecution(UUID, () -> REASON, false);
    executor.executeProposals(List.of(proposal(TP0, List.of(0, 1), List.of(0, 2))), Collections.emptySet(), null, mockLoadMonitor(),
                              null, null, null, null, null, null, null, null, false, UUID, false, false);
    waitUntilTrue(() -> _store.state() != null && onlyTask(_store.state()).state() == ExecutionTaskState.IN_PROGRESS,
                  "The task is not persisted as in progress", WAIT_TIME_MS, 100L);
    executor.userTriggeredStopExecution(false);
    waitForExecutionToFinish(executor);
    // The stop is persisted before the stopped execution finishes, and the state is deleted once it finishes.
    assertTrue(_store.savedStates().stream().anyMatch(s -> s.executorState() == ExecutorState.State.STOPPING_EXECUTION));
    assertNull(_store.state());
  }

  @Test
  public void testRecoveryWithoutResumeRemovesLeftoverThrottles() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    ExecutionProposal proposal = proposal(TP0, List.of(0, 1), List.of(0, 2));
    long throttle = 100_000L;
    // Throttles left behind by the interrupted execution.
    new ReplicationThrottleHelper(_adminClient, throttle).setThrottles(List.of(proposal));
    assertTrue(hasThrottles());
    _store.save(interruptedExecutionState(throttle, List.of(proposal)));

    RecordingNotifier notifier = new RecordingNotifier();
    AnomalyDetectorManager anomalyDetectorManager = mockAnomalyDetectorManager(null);
    Executor executor = newExecutor(false, null, notifier, anomalyDetectorManager);
    executor.recoverInterruptedExecution(mockLoadMonitor());
    waitUntilTrue(() -> _store.state() == null, "The interrupted execution is not cleaned up", WAIT_TIME_MS, 100L);
    waitForExecutionToFinish(executor);

    assertFalse(hasThrottles());
    // The execution is not resumed.
    assertEquals(List.of(0, 1), currentReplicas(0));
    assertEquals("CLEANED_UP", recovery(executor).get("status"));
    assertTrue(notifier.anyMessageContains(ExecutorConfig.RESUME_INTERRUPTED_EXECUTION_ON_STARTUP_CONFIG + " is disabled"));
    EasyMock.verify(anomalyDetectorManager);
  }

  @Test
  public void testResumeInterruptedExecution() throws Exception {
    createTopic(Map.of(0, List.of(0, 1), 1, List.of(2, 1)));
    // TP0 was in progress, but not started in Kafka -- i.e. pending; TP1 was in progress and completed in the meantime.
    ExecutionProposal pending = proposal(TP0, List.of(0, 1), List.of(0, 2));
    ExecutionProposal completed = proposal(TP1, List.of(2, 0), List.of(2, 1));
    _store.save(interruptedExecutionState(null, List.of(pending, completed)));
    int numSavedStatesBeforeResume = _store.savedStates().size();

    RecordingNotifier notifier = new RecordingNotifier();
    AnomalyDetectorManager anomalyDetectorManager = mockAnomalyDetectorManager(null);
    Executor executor = newExecutor(true, null, notifier, anomalyDetectorManager);
    executor.recoverInterruptedExecution(mockLoadMonitor());
    assertTrue(notifier.anyMessageContains("Resuming the execution"));
    waitForExecutionToFinish(executor);

    assertEquals(List.of(0, 2), currentReplicas(0));
    assertEquals(List.of(2, 1), currentReplicas(1));
    assertNull(_store.state());
    // The resumed execution keeps the UUID of the interrupted execution, and only contains the remaining task.
    PersistedExecutionState resumedState = _store.savedStates().get(numSavedStatesBeforeResume);
    assertEquals(UUID, resumedState.uuid());
    assertEquals(REASON, resumedState.reason());
    assertEquals(1, resumedState.resumeCount());
    assertEquals(TP0, onlyTask(resumedState).topicPartition());
    assertEquals(ExecutionTaskState.PENDING, onlyTask(resumedState).state());
    assertTrue(notifier.anyMessageContains("resumed execution is finished"));
    // The resumed execution is not reported to the anomaly detector as a finished self-healing.
    EasyMock.verify(anomalyDetectorManager);
  }

  @Test
  public void testResumeAdoptsOngoingReassignment() throws Exception {
    long throttle = 10_000L;
    // The interrupted execution submitted the reassignment before the restart.
    ExecutionProposal proposal = startThrottledReassignment(throttle);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttle);
    _store.save(interruptedExecutionState(throttle, List.of(proposal)));
    int numSavedStatesBeforeResume = _store.savedStates().size();

    AnomalyDetectorManager anomalyDetectorManager = mockAnomalyDetectorManager(null);
    Executor executor = newExecutor(true, null, new RecordingNotifier(), anomalyDetectorManager);
    executor.recoverInterruptedExecution(mockLoadMonitor());
    // The ongoing reassignment is adopted -- i.e. the resumed execution starts with the task in progress.
    PersistedExecutionState resumedState = _store.savedStates().get(numSavedStatesBeforeResume);
    assertEquals(ExecutionTaskState.IN_PROGRESS, onlyTask(resumedState).state());
    assertTrue(executor.hasOngoingExecution());

    // Let the reassignment complete by removing the throttles (the resumed execution re-applies them when it starts moving replicas).
    waitUntilTrue(() -> {
      try {
        throttleHelper.clearThrottlesForProposals(List.of(proposal), Collections.emptyList());
      } catch (Exception e) {
        // Retry.
      }
      return !executor.hasOngoingExecution();
    }, "The resumed execution did not finish", WAIT_TIME_MS * 3, 500L);
    assertEquals(List.of(0, 2), currentReplicas(0));
    assertNull(_store.state());
    assertFalse(hasThrottles());
    EasyMock.verify(anomalyDetectorManager);
  }

  @Test
  public void testRecoveryKeepsThrottledRateSetByOthers() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    ExecutionProposal proposal = proposal(TP0, List.of(0, 1), List.of(0, 2));
    long throttle = 100_000L;
    new ReplicationThrottleHelper(_adminClient, throttle).setThrottles(List.of(proposal));
    _store.save(interruptedExecutionState(throttle, List.of(proposal)));
    // An operator changes the follower rate of broker 2 while Cruise Control is not running.
    ConfigResource broker2 = new ConfigResource(ConfigResource.Type.BROKER, "2");
    _adminClient.incrementalAlterConfigs(Map.of(broker2, List.of(new AlterConfigOp(
        new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "5000"), AlterConfigOp.OpType.SET))))
                .all().get();
    waitUntilTrue(() -> {
      try {
        return "5000".equals(brokerRate(2, ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG));
      } catch (Exception e) {
        return false;
      }
    }, "The rate is not changed", WAIT_TIME_MS, 100L);

    Executor executor = newExecutor(false, null, new RecordingNotifier(), mockAnomalyDetectorManager(null));
    executor.recoverInterruptedExecution(mockLoadMonitor());
    waitUntilTrue(() -> _store.state() == null, "The interrupted execution is not cleaned up", WAIT_TIME_MS, 100L);
    waitForExecutionToFinish(executor);

    // The rates of the execution are removed; the rate set by the operator is kept and reported.
    assertEquals("5000", brokerRate(2, ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG));
    assertNull(brokerRate(2, ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG));
    assertNull(brokerRate(0, ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG));
    assertNull(brokerRate(1, ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG));
    assertEquals(List.of("broker 2: follower.replication.throttled.rate=5000 (DYNAMIC_BROKER_CONFIG)"),
                 recovery(executor).get("keptThrottles"));
  }

  @Test
  public void testResumeIsRefusedIfReplicasOutsideTheExecutionMoved() throws Exception {
    createTopic(Map.of(0, List.of(0, 1), 1, List.of(2, 1)));
    ExecutionProposal proposal = proposal(TP0, List.of(0, 1), List.of(0, 2));
    _store.save(interruptedExecutionState(null, List.of(proposal)));
    // While Cruise Control is not running, an operator moves a replica of a partition outside the execution to broker 0.
    _adminClient.alterPartitionReassignments(Map.of(TP1, Optional.of(new NewPartitionReassignment(List.of(2, 0))))).all().get();
    waitUntilTrue(() -> List.of(2, 0).equals(currentReplicas(1)), "Reassignment did not complete", WAIT_TIME_MS, 100L);

    RecordingNotifier notifier = new RecordingNotifier();
    Executor executor = newExecutor(true, null, notifier, mockAnomalyDetectorManager(null));
    executor.recoverInterruptedExecution(mockLoadMonitor());
    waitUntilTrue(() -> _store.state() == null, "The interrupted execution is not cleaned up", WAIT_TIME_MS, 100L);
    waitForExecutionToFinish(executor);

    // Not resumed: the partition of the execution stays where it was.
    assertEquals(List.of(0, 1), currentReplicas(0));
    Map<String, Object> recovery = recovery(executor);
    assertTrue(String.valueOf(recovery.get("reasonNotResumed")), String.valueOf(recovery.get("reasonNotResumed"))
                                                                       .contains("the cluster has changed since the execution was interrupted"));
    // Broker 0 gained a replica outside the execution, broker 1 lost one.
    List<?> divergences = (List<?>) recovery.get("divergences");
    assertEquals(String.valueOf(divergences), 2, divergences.size());
    assertReplicaCountChange(String.valueOf(divergences.get(0)), 0, 1);
    assertReplicaCountChange(String.valueOf(divergences.get(1)), 1, -1);
    assertTrue(notifier.anyMessageContains("the cluster has changed since the execution was interrupted"));
  }

  private static void assertReplicaCountChange(String divergence, int broker, int change) {
    Matcher matcher = Pattern.compile("broker (\\d+): (\\d+) -> (\\d+) replicas outside the execution").matcher(divergence);
    assertTrue(divergence, matcher.matches());
    assertEquals(divergence, broker, Integer.parseInt(matcher.group(1)));
    assertEquals(divergence, change, Integer.parseInt(matcher.group(3)) - Integer.parseInt(matcher.group(2)));
  }

  @Test
  public void testResumeIsRefusedIfTheExecutionIsTooOld() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    ExecutionProposal proposal = proposal(TP0, List.of(0, 1), List.of(0, 2));
    _store.save(interruptedExecutionState(null, List.of(proposal)).toBuilder()
                                                                     .lastUpdateTimeMs(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2))
                                                                     .build());
    Executor executor = newExecutor(true, null, new RecordingNotifier(), mockAnomalyDetectorManager(null));
    executor.recoverInterruptedExecution(mockLoadMonitor());
    waitUntilTrue(() -> _store.state() == null, "The interrupted execution is not cleaned up", WAIT_TIME_MS, 100L);
    waitForExecutionToFinish(executor);
    assertEquals(List.of(0, 1), currentReplicas(0));
    assertTrue(String.valueOf(recovery(executor).get("reasonNotResumed")).contains(ExecutorConfig.RESUME_INTERRUPTED_EXECUTION_MAX_AGE_MS_CONFIG));
  }

  @Test
  public void testCleanupIsAnOngoingExecutionUntilStopped() throws Exception {
    long throttle = 10_000L;
    ExecutionProposal proposal = startThrottledReassignment(throttle);
    _store.save(interruptedExecutionState(throttle, List.of(proposal)).toBuilder().removedBrokers(Set.of(1)).build());

    Executor executor = newExecutor(false, null, new RecordingNotifier(), mockAnomalyDetectorManager(null));
    executor.recoverInterruptedExecution(mockLoadMonitor());
    // The cleanup is reported, waits for the in-flight reassignment, and keeps new executions from starting.
    assertTrue(executor.hasOngoingExecution());
    assertEquals(ExecutorState.State.RECOVERING_INTERRUPTED_EXECUTION, executor.state().state());
    waitUntilTrue(() -> List.of(TP0.toString()).equals(executor.state().recovery().get("inFlightPartitionMovements")),
                  "The in-flight reassignment is not reported", WAIT_TIME_MS, 100L);
    assertEquals("WAITING_FOR_IN_FLIGHT_MOVEMENTS", recovery(executor).get("status"));
    assertTrue(executor.ongoingRecoveryDescription().contains(UUID));
    assertThrows(OngoingExecutionException.class, () -> executor.setGeneratingProposalsForExecution("new-uuid", () -> "new", true));
    // A later restart must not resume it either; the broker that it was removing stays recently removed.
    assertTrue(_store.state().cleanupOnly());
    assertTrue(executor.recentlyRemovedBrokers().contains(1));

    // Stopping the cleanup cancels the in-flight reassignment and removes the throttles.
    executor.userTriggeredStopExecution(false);
    waitForExecutionToFinish(executor);
    assertEquals(List.of(0, 1), currentReplicas(0));
    assertFalse(_adminClient.listPartitionReassignments().reassignments().get().containsKey(TP0));
    assertFalse(hasThrottles());
    assertNull(_store.state());
    assertEquals("CLEANED_UP", recovery(executor).get("status"));
    assertNull(executor.ongoingRecoveryDescription());
  }

  @Test
  public void testCleanupTimeoutCancelsInFlightReassignment() throws Exception {
    long throttle = 10_000L;
    ExecutionProposal proposal = startThrottledReassignment(throttle);
    _store.save(interruptedExecutionState(throttle, List.of(proposal)));
    Properties overrides = new Properties();
    overrides.setProperty(ExecutorConfig.INTERRUPTED_EXECUTION_CLEANUP_TIMEOUT_MS_CONFIG, "2000");
    Executor executor = newExecutor(false, null, new RecordingNotifier(), mockAnomalyDetectorManager(null), overrides);
    executor.recoverInterruptedExecution(mockLoadMonitor());
    waitForExecutionToFinish(executor);
    assertEquals(List.of(0, 1), currentReplicas(0));
    assertFalse(hasThrottles());
    assertNull(_store.state());
  }

  @Test
  public void testCleanupOnlyStateIsNotResumed() throws Exception {
    createTopic(Map.of(0, List.of(0, 1)));
    ExecutionProposal proposal = proposal(TP0, List.of(0, 1), List.of(0, 2));
    _store.save(interruptedExecutionState(null, List.of(proposal)).toBuilder().cleanupOnly(true).build());
    Executor executor = newExecutor(true, null, new RecordingNotifier(), mockAnomalyDetectorManager(null));
    executor.recoverInterruptedExecution(mockLoadMonitor());
    waitForExecutionToFinish(executor);
    assertEquals(List.of(0, 1), currentReplicas(0));
    assertNull(_store.state());
    assertTrue(String.valueOf(recovery(executor).get("reasonNotResumed")).contains("already decided not to resume"));
  }
}
