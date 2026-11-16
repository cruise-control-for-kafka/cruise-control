/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataAdminClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.ALL;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.ONGOING_EXECUTION;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the tracking of intra-broker replica movements by {@link Executor} when the brokers do not respond -- e.g. Cruise
 * Control lost its connection to the cluster -- while the movements go on in the brokers.
 */
public class ExecutorIntraBrokerReplicaMovementTest {
  private static final String UUID = "intra-broker-test";
  private static final String LOGDIR_0 = "/d0";
  private static final String LOGDIR_1 = "/d1";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC0, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC0, 1);
  private static final TopicPartitionReplica TPR0 = new TopicPartitionReplica(TOPIC0, 0, 0);
  private static final TopicPartitionReplica TPR1 = new TopicPartitionReplica(TOPIC0, 1, 0);
  private static final Node NODE0 = new Node(0, "host0", 100);
  private static final Node NODE1 = new Node(1, "host1", 100);
  private static final long DEADLINE_MS = TimeUnit.SECONDS.toMillis(60);
  // Movements of these tests do not complete by themselves.
  private static final int ENDLESS = Integer.MAX_VALUE;

  private Executor _executor;
  private LogdirMovingAdminClient _adminClient;
  private final AtomicReference<Cluster> _cluster = new AtomicReference<>(cluster(NODE0, NODE1));
  private int _maxDeadTasks;

  /**
   * Shuts down the executor of the test.
   */
  @After
  public void tearDown() {
    if (_executor != null) {
      _executor.shutdown();
    }
  }

  @Test
  public void testMovementsAreTrackedWhileBrokersDoNotRespond() throws Exception {
    // Two movements on broker 0, one at a time. Each completes after 3 logdir checks.
    start(3, 1, TP0, TP1);
    waitUntil(() -> _adminClient.isMoving(TPR0), "The first movement did not start");
    // The brokers stop responding, while the first movement goes on and completes.
    _adminClient.setReachable(false);
    waitUntil(() -> _adminClient.numUnansweredDescribes() >= 4, "The executor did not check the movement");
    assertEquals(1, _adminClient.alterRequests().size());
    _adminClient.setReachable(true);
    waitForExecutionToFinish();

    // No movement is reported dead, both are done, and the second one started only after the first one completed.
    assertEquals(0, _maxDeadTasks);
    assertEquals(LOGDIR_1, _adminClient.currentLogdir(TPR0));
    assertEquals(LOGDIR_1, _adminClient.currentLogdir(TPR1));
    assertFalse("The concurrency limit was exceeded", _adminClient.hadOverlappingMovements());
  }

  @Test
  public void testMovementWhoseRequestIsLostIsExecutedAgain() throws Exception {
    // The request to start the first movement never reaches the broker.
    _adminClient = new LogdirMovingAdminClient(3);
    _adminClient.loseNextAlterRequest(false);
    start(3, 1, TP0, TP1);
    waitForExecutionToFinish();

    assertEquals(0, _maxDeadTasks);
    assertEquals(LOGDIR_1, _adminClient.currentLogdir(TPR0));
    assertEquals(LOGDIR_1, _adminClient.currentLogdir(TPR1));
    assertEquals(2, _adminClient.alterRequests().stream().filter(request -> request.containsKey(TPR0)).count());
    assertFalse("The concurrency limit was exceeded", _adminClient.hadOverlappingMovements());
  }

  @Test
  public void testMovementWhoseResponseIsLostIsTracked() throws Exception {
    // The broker starts the first movement, but its response is lost.
    _adminClient = new LogdirMovingAdminClient(3);
    _adminClient.loseNextAlterRequest(true);
    start(3, 1, TP0, TP1);
    waitForExecutionToFinish();

    assertEquals(0, _maxDeadTasks);
    assertEquals(LOGDIR_1, _adminClient.currentLogdir(TPR0));
    assertEquals(LOGDIR_1, _adminClient.currentLogdir(TPR1));
    // The ongoing movement is not requested again.
    assertEquals(1, _adminClient.alterRequests().stream().filter(request -> request.containsKey(TPR0)).count());
    assertFalse("The concurrency limit was exceeded", _adminClient.hadOverlappingMovements());
  }

  @Test
  public void testMovementOfBrokerThatIsDownIsDead() throws Exception {
    start(ENDLESS, 1, TP0);
    waitUntil(() -> _adminClient.isMoving(TPR0), "The movement did not start");
    // Broker 0 does not respond, and leaves the cluster.
    _adminClient.setReachable(false);
    _cluster.set(cluster(NODE1));
    // The movement is marked as dead: the execution finishes although the broker never responds.
    waitForExecutionToFinish();

    assertTrue(_adminClient.isMoving(TPR0));
    assertEquals(1, _adminClient.alterRequests().size());
  }

  @Test
  public void testStopRollsBackMovementOnceBrokerResponds() throws Exception {
    start(ENDLESS, 1, TP0);
    waitUntil(() -> _adminClient.isMoving(TPR0), "The movement did not start");
    _adminClient.setReachable(false);
    _executor.userTriggeredStopExecution(false);
    int numUnansweredDescribes = _adminClient.numUnansweredDescribes();
    waitUntil(() -> _adminClient.numUnansweredDescribes() > numUnansweredDescribes, "The executor did not check the movement");
    // The broker responds again within the progress checks that a stop waits for.
    _adminClient.setReachable(true);
    waitForExecutionToFinish();

    // The movement is rolled back: the replica is requested to move to its current logdir.
    assertTrue(_adminClient.alterRequests().contains(Collections.singletonMap(TPR0, LOGDIR_0)));
    assertEquals(LOGDIR_0, _adminClient.currentLogdir(TPR0));
    assertNull(_adminClient.futureLogdir(TPR0));
  }

  @Test
  public void testStopFinishesWhileBrokerDoesNotRespond() throws Exception {
    start(ENDLESS, 1, TP0);
    waitUntil(() -> _adminClient.isMoving(TPR0), "The movement did not start");
    _adminClient.setReachable(false);
    _executor.userTriggeredStopExecution(false);
    waitForExecutionToFinish();

    // The current logdir of the replica is unknown, so no rollback is requested.
    assertEquals(1, _adminClient.alterRequests().size());
  }

  private void start(int checksPerMovement, int intraBrokerConcurrency, TopicPartition... partitions) throws Exception {
    if (_adminClient == null) {
      _adminClient = new LogdirMovingAdminClient(checksPerMovement);
    }
    List<ExecutionProposal> proposals = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      _adminClient.addReplica(new TopicPartitionReplica(tp.topic(), tp.partition(), 0), LOGDIR_0);
      // Move the replica on broker 0 from d0 to d1.
      proposals.add(new ExecutionProposal(tp, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                          List.of(new ReplicaPlacementInfo(0, LOGDIR_0), new ReplicaPlacementInfo(1, LOGDIR_0)),
                                          List.of(new ReplicaPlacementInfo(0, LOGDIR_1), new ReplicaPlacementInfo(1, LOGDIR_0))));
    }
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "200");
    props.setProperty(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "100");
    props.setProperty(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG, "1000");
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    MetadataAdminClient mockMetadataClient = EasyMock.mock(MetadataAdminClient.class);
    EasyMock.expect(mockMetadataClient.cluster()).andAnswer(_cluster::get).anyTimes();
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.taskRunnerState()).andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING).anyTimes();
    EasyMock.expect(mockLoadMonitor.samplingMode()).andReturn(ALL).anyTimes();
    EasyMock.expect(mockLoadMonitor.deadBrokersWithReplicas(anyLong())).andReturn(new HashSet<>()).anyTimes();
    EasyMock.expect(mockLoadMonitor.brokersWithReplicas(anyLong())).andReturn(Set.of(0, 1)).anyTimes();
    mockLoadMonitor.pauseMetricSampling(isA(String.class), EasyMock.anyBoolean());
    expectLastCall().anyTimes();
    mockLoadMonitor.setSamplingMode(ONGOING_EXECUTION);
    expectLastCall().anyTimes();
    mockLoadMonitor.resumeMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    mockLoadMonitor.setSamplingMode(ALL);
    expectLastCall().anyTimes();
    AnomalyDetectorManager mockAnomalyDetectorManager = EasyMock.niceMock(AnomalyDetectorManager.class);
    EasyMock.replay(mockMetadataClient, mockLoadMonitor, mockAnomalyDetectorManager);

    _executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), _adminClient, mockMetadataClient, null,
                             mockAnomalyDetectorManager);
    _executor.setGeneratingProposalsForExecution(UUID, ExecutorIntraBrokerReplicaMovementTest.class::getSimpleName, false);
    _executor.executeProposals(proposals, Collections.emptySet(), null, mockLoadMonitor, null, null, intraBrokerConcurrency,
                               null, null, null, null, null, false, UUID, false, false);
  }

  private void waitForExecutionToFinish() throws InterruptedException {
    waitUntil(() -> {
      ExecutorState state = _executor.state();
      if (state.executionTasksSummary() != null) {
        Map<ExecutionTaskState, Integer> numTasksByState = state.executionTasksSummary().taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        _maxDeadTasks = Math.max(_maxDeadTasks, numTasksByState.getOrDefault(ExecutionTaskState.DEAD, 0));
      }
      return !_executor.hasOngoingExecution() && state.state() == ExecutorState.State.NO_TASK_IN_PROGRESS;
    }, "The execution did not finish");
  }

  private static void waitUntil(BooleanSupplier condition, String message) throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + DEADLINE_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadlineMs) {
        throw new AssertionError(message);
      }
      Thread.sleep(5);
    }
  }

  private static Cluster cluster(Node... nodes) {
    List<PartitionInfo> partitions = new ArrayList<>();
    for (TopicPartition tp : List.of(TP0, TP1)) {
      partitions.add(new PartitionInfo(tp.topic(), tp.partition(), nodes[0], nodes, nodes));
    }
    return new Cluster("id", List.of(nodes), partitions, Collections.emptySet(), Collections.emptySet());
  }

  /**
   * An admin client of brokers that move replicas between their logdirs. A movement completes after a given number of
   * logdir checks, whether or not the brokers respond -- they can be made to not respond to logdir requests.
   */
  private static final class LogdirMovingAdminClient extends MockAdminClient {
    private final int _checksPerMovement;
    private final Map<TopicPartitionReplica, String> _currentLogdir = new HashMap<>();
    private final Map<TopicPartitionReplica, String> _futureLogdir = new HashMap<>();
    private final Map<TopicPartitionReplica, Integer> _remainingChecks = new HashMap<>();
    private final List<Map<TopicPartitionReplica, String>> _alterRequests = new ArrayList<>();
    private boolean _reachable = true;
    private Boolean _processNextLostAlterRequest = null;
    private int _numUnansweredDescribes = 0;
    private boolean _hadOverlappingMovements = false;

    LogdirMovingAdminClient(int checksPerMovement) {
      super(List.of(NODE0, NODE1), NODE0);
      _checksPerMovement = checksPerMovement;
    }

    synchronized void addReplica(TopicPartitionReplica replica, String logdir) {
      _currentLogdir.put(replica, logdir);
    }

    synchronized void setReachable(boolean reachable) {
      _reachable = reachable;
    }

    /**
     * @param processed {@code true} if the broker processes the next request to move replicas but its response is lost,
     *                  {@code false} if the request does not reach the broker.
     */
    synchronized void loseNextAlterRequest(boolean processed) {
      _processNextLostAlterRequest = processed;
    }

    synchronized boolean isMoving(TopicPartitionReplica replica) {
      return _futureLogdir.containsKey(replica);
    }

    synchronized String currentLogdir(TopicPartitionReplica replica) {
      return _currentLogdir.get(replica);
    }

    synchronized String futureLogdir(TopicPartitionReplica replica) {
      return _futureLogdir.get(replica);
    }

    synchronized List<Map<TopicPartitionReplica, String>> alterRequests() {
      return new ArrayList<>(_alterRequests);
    }

    synchronized int numUnansweredDescribes() {
      return _numUnansweredDescribes;
    }

    synchronized boolean hadOverlappingMovements() {
      return _hadOverlappingMovements;
    }

    @Override
    public synchronized DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
                                                                            DescribeReplicaLogDirsOptions options) {
      // Movements go on whether or not the brokers respond.
      for (TopicPartitionReplica replica : new ArrayList<>(_futureLogdir.keySet())) {
        if (_remainingChecks.merge(replica, -1, Integer::sum) <= 0) {
          _currentLogdir.put(replica, _futureLogdir.remove(replica));
          _remainingChecks.remove(replica);
        }
      }
      Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures = new HashMap<>();
      if (!_reachable) {
        _numUnansweredDescribes++;
      }
      for (TopicPartitionReplica replica : replicas) {
        futures.put(replica, _reachable ? KafkaFuture.completedFuture(replicaLogDirInfo(_currentLogdir.get(replica), _futureLogdir.get(replica)))
                                        : noResponse("describeReplicaLogDirs"));
      }
      return newResult(DescribeReplicaLogDirsResult.class, futures);
    }

    @Override
    public synchronized AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
                                                                      AlterReplicaLogDirsOptions options) {
      _alterRequests.add(new HashMap<>(replicaAssignment));
      boolean respond = _reachable && _processNextLostAlterRequest == null;
      boolean process = _reachable && (_processNextLostAlterRequest == null || _processNextLostAlterRequest);
      _processNextLostAlterRequest = null;
      Map<TopicPartitionReplica, KafkaFuture<Void>> futures = new HashMap<>();
      for (Map.Entry<TopicPartitionReplica, String> entry : replicaAssignment.entrySet()) {
        TopicPartitionReplica replica = entry.getKey();
        String logdir = entry.getValue();
        if (process) {
          if (logdir.equals(_currentLogdir.get(replica))) {
            // Moving a replica to its current logdir cancels its ongoing movement.
            _futureLogdir.remove(replica);
            _remainingChecks.remove(replica);
          } else if (!logdir.equals(_futureLogdir.get(replica))) {
            if (_futureLogdir.keySet().stream().anyMatch(r -> r.brokerId() == replica.brokerId() && !r.equals(replica))) {
              _hadOverlappingMovements = true;
            }
            _futureLogdir.put(replica, logdir);
            _remainingChecks.put(replica, _checksPerMovement);
          }
        }
        futures.put(replica, respond ? KafkaFuture.completedFuture(null) : noResponse("alterReplicaLogDirs"));
      }
      return newResult(AlterReplicaLogDirsResult.class, futures);
    }

    private static <T> KafkaFuture<T> noResponse(String call) {
      KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
      future.completeExceptionally(new TimeoutException("Timed out waiting for a node assignment. Call: " + call));
      return future;
    }

    private static <T> T newResult(Class<T> resultClass, Map<TopicPartitionReplica, ?> futures) {
      try {
        Constructor<T> constructor = resultClass.getDeclaredConstructor(Map.class);
        constructor.setAccessible(true);
        return constructor.newInstance(futures);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    private static ReplicaLogDirInfo replicaLogDirInfo(String currentLogdir, String futureLogdir) {
      try {
        Constructor<ReplicaLogDirInfo> constructor =
            ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
        constructor.setAccessible(true);
        return constructor.newInstance(currentLogdir, 0L, futureLogdir, 0L);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
