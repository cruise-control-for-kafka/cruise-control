/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.waitUntilTrue;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.*;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;


public class ExecutorTest extends CCKafkaClientsIntegrationTestHarness {
  private static final int PARTITION = 0;
  private static final TopicPartition TP0 = new TopicPartition(TOPIC0, PARTITION);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC1, PARTITION);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC2, PARTITION);
  private static final TopicPartition TP3 = new TopicPartition(TOPIC3, PARTITION);
  private static final int BROKER_ID_0 = 0;
  private static final int BROKER_ID_1 = 1;
  private static final Random RANDOM = new Random(0xDEADBEEF);
  private static final int MOCK_BROKER_ID_TO_DROP = 1;
  private static final long MOCK_CURRENT_TIME = 1596842708000L;

  private CCContainerizedKraftCluster _cluster;
  private Admin _adminClient;
  private List<String> _brokerAddressList;

  @Override
  public int clusterSize() {
    /*
     * The containerized Kraft cluster we create runs brokers in dual broker/controller mode.
     * Some tests, such as testBrokerDiesBeforeMovingPartition() and testSubmitPreferredLeaderElection(), involve
     * shutting down brokers while moving partitions.
     * To ensure the controllers can maintain quorum during these operations, these tests require at least 3 brokers.
     */
    return 3;
  }

  /**
   * Setup the unit test.
   */
  @Before
  public void setUp() {
    Properties adminClientProps = new Properties();
    adminClientProps.setProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "0");
    setSecurityConfigs(adminClientProps, "admin");

    _cluster = new CCContainerizedKraftCluster(clusterSize(), buildBrokerConfigs(), adminClientProps);
    _cluster.start();
    _brokerAddressList = _cluster.getBrokerAddressList();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();
    _adminClient = _cluster.adminClient();
  }

  /**
   * Tear down the unit test.
   */
  @After
  public void tearDown() {
    if (_cluster != null) {
      _cluster.close();
    }
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":PLAINTEXT"));
    return props;
  }

  @Test
  public void testReplicaReassignment() throws InterruptedException, OngoingExecutionException {
    List<ExecutionProposal> proposalsToExecute = new ArrayList<>();
    List<ExecutionProposal> proposalsToCheck = new ArrayList<>();
    populateProposals(proposalsToExecute, proposalsToCheck, 0);
    executeAndVerifyProposals(_adminClient, proposalsToExecute, proposalsToCheck, false, null, false, true);
  }

  @Test
  public void testReplicaReassignmentProgressWithThrottle() throws InterruptedException, OngoingExecutionException {
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(getExecutorProperties());
    Map<String, Object> adminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(kafkaCruiseControlConfig);
    adminClientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(adminClientConfigs);

    try {
      List<ExecutionProposal> proposalsToExecute = new ArrayList<>();
      List<ExecutionProposal> proposalsToCheck = new ArrayList<>();
      populateProposals(proposalsToExecute, proposalsToCheck, PRODUCE_SIZE_IN_BYTES);
      // Throttle rate is set to the half of the produce size.
      executeAndVerifyProposals(adminClient, proposalsToExecute, proposalsToCheck, false, PRODUCE_SIZE_IN_BYTES / 2, true, true);
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  @Test
  public void testBrokerDiesBeforeMovingPartition() throws Exception {
    Map<String, TopicDescription> topicDescriptions = createTopics((int) PRODUCE_SIZE_IN_BYTES);
    // initialLeader0 will be alive after killing a broker in cluster.
    int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();
    // Kill broker before starting the reassignment.
    int brokerId = initialLeader0 == 0 ? 1 : 0;

    _cluster.shutDownBroker(brokerId);

    ExecutionProposal proposal0 =
      new ExecutionProposal(TP0, PRODUCE_SIZE_IN_BYTES, new ReplicaPlacementInfo(initialLeader0),
                            Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                            Collections.singletonList(new ReplicaPlacementInfo(initialLeader0 == 0 ? 1 : 0)));
    ExecutionProposal proposal1 =
      new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                            Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                          new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0)),
                            Arrays.asList(new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0),
                                          new ReplicaPlacementInfo(initialLeader1)));

    Collection<ExecutionProposal> proposalsToExecute = Arrays.asList(proposal0, proposal1);
    executeAndVerifyProposals(_adminClient, proposalsToExecute, Collections.emptyList(), true, null, false, false);

    // We are doing the rollback. -- The leadership should be on the alive broker.
    TopicDescription topic0 = _adminClient.describeTopics(Collections.singleton(TOPIC0)).topicNameValues().get(TOPIC0).get();
    TopicDescription topic1 = _adminClient.describeTopics(Collections.singleton(TOPIC1)).topicNameValues().get(TOPIC1).get();
    assertEquals(initialLeader0, topic0.partitions().get(PARTITION).leader().id());
    assertEquals(initialLeader0, topic1.partitions().get(PARTITION).leader().id());
  }

  @Test
  public void testSubmitReplicaReassignmentTasksWithDeadTaskAndNoReassignmentInProgress() throws InterruptedException {
    createTopics(0);
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerAddressList.get(0)));

    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    ExecutionProposal proposal = new ExecutionProposal(tp,
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = new ExecutionTask(0,
                                           proposal,
                                           ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);
    task.kill(MOCK_CURRENT_TIME);

    AlterPartitionReassignmentsResult result = ExecutionUtils.submitReplicaReassignmentTasks(adminClient, Collections.singletonList(task));

    assertEquals(1, result.values().size());
    assertTrue(verifyFutureError(result.values().get(tp), Errors.NO_REASSIGNMENT_IN_PROGRESS.exception().getClass()));
  }

  @Test
  public void testSubmitPreferredLeaderElection() throws InterruptedException, ExecutionException, TimeoutException {
    Map<String, TopicDescription> topicDescriptions = createTopics(0);
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerAddressList.get(0)));

    // Case 1: Handle the successful case.
    ReplicaPlacementInfo oldLeader = new ReplicaPlacementInfo(topicDescriptions.get(TP1.topic()).partitions().get(PARTITION).leader().id());
    List<ReplicaPlacementInfo> replicas = new ArrayList<>();
    for (Node node : topicDescriptions.get(TP1.topic()).partitions().get(PARTITION).replicas()) {
      replicas.add(new ReplicaPlacementInfo(node.id()));
    }

    List<ReplicaPlacementInfo> newReplicas = new ArrayList<>(replicas.size());
    for (int i = replicas.size() - 1; i >= 0; i--) {
      newReplicas.add(new ReplicaPlacementInfo(replicas.get(i).brokerId()));
    }

    // Reorder replicas before leadership election to change the preferred replica.
    ExecutionTask swapTask = new ExecutionTask(0,
                                           new ExecutionProposal(TP1, 0, oldLeader, replicas, newReplicas),
                                           ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    swapTask.inProgress(MOCK_CURRENT_TIME);

    AlterPartitionReassignmentsResult swapResult = ExecutionUtils.submitReplicaReassignmentTasks(adminClient, Collections.singletonList(swapTask));

    assertEquals(1, swapResult.values().size());
    // Can retrieve the future if it is successful.
    swapResult.values().get(TP1).get();

    // Leadership task to transfer the leadership to the preferred replica.
    ExecutionTask task = new ExecutionTask(0,
                                           new ExecutionProposal(TP1, 0, oldLeader, newReplicas, newReplicas),
                                           ExecutionTask.TaskType.LEADER_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);

    ElectLeadersResult result = ExecutionUtils.submitPreferredLeaderElection(adminClient, Collections.singletonList(task));
    assertTrue(result.partitions().get().get(TP1).isEmpty());

    // Case 2: Handle "election not needed" -- i.e. the leader is already the preferred replica for TP0.
    oldLeader = new ReplicaPlacementInfo(topicDescriptions.get(TP0.topic()).partitions().get(PARTITION).leader().id());
    replicas.clear();
    for (Node node : topicDescriptions.get(TP0.topic()).partitions().get(PARTITION).replicas()) {
      replicas.add(new ReplicaPlacementInfo(node.id()));
    }
    task = new ExecutionTask(0,
                             new ExecutionProposal(TP0, 0, oldLeader, replicas, replicas),
                             ExecutionTask.TaskType.LEADER_ACTION,
                             EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);
    result = ExecutionUtils.submitPreferredLeaderElection(adminClient, Collections.singletonList(task));
    assertTrue(result.partitions().get().get(TP0).isPresent());
    assertEquals(Errors.ELECTION_NOT_NEEDED.exception().getClass(), result.partitions().get().get(TP0).get().getClass());

    // Case 3: Handle "unknown topic or partition" -- i.e. the partition TP2 does not exist, maybe it existed at some point and then deleted
    task = new ExecutionTask(0,
                             new ExecutionProposal(TP2, 0, oldLeader, replicas, replicas),
                             ExecutionTask.TaskType.LEADER_ACTION,
                             EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);

    result = ExecutionUtils.submitPreferredLeaderElection(adminClient, Collections.singletonList(task));
    assertTrue(result.partitions().get().get(TP2).isPresent());
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getClass(), result.partitions().get().get(TP2).get().getClass());

    // Case 4: Handle the scenario, where we tried to elect the preferred leader but it is offline.
    _cluster.shutDownBroker(0);

    task = new ExecutionTask(0,
                             new ExecutionProposal(TP0, 0, oldLeader, replicas, replicas),
                             ExecutionTask.TaskType.LEADER_ACTION,
                             EXECUTION_ALERTING_THRESHOLD_MS);

    task.inProgress(MOCK_CURRENT_TIME);
    result = ExecutionUtils.submitPreferredLeaderElection(adminClient, Collections.singletonList(task));
    assertTrue(result.partitions().get().get(TP0).isPresent());
    assertEquals(Errors.PREFERRED_LEADER_NOT_AVAILABLE.exception().getClass(), result.partitions().get().get(TP0).get().getClass());
  }

  @Test
  public void testSubmitReplicaReassignmentTasksWithInProgressTaskAndNonExistingTopic() throws InterruptedException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerAddressList.get(0)));

    ExecutionProposal proposal = new ExecutionProposal(TP0,
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Collections.singletonList(new ReplicaPlacementInfo(0)),
                                                       Collections.singletonList(new ReplicaPlacementInfo(1)));

    ExecutionTask task = new ExecutionTask(0,
                                           proposal,
                                           ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);

    AlterPartitionReassignmentsResult result = ExecutionUtils.submitReplicaReassignmentTasks(adminClient, Collections.singletonList(task));

    assertEquals(1, result.values().size());
    // Expect topic deletion -- i.e. or it has never been created.
    assertTrue(verifyFutureError(result.values().get(TP0), Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getClass()));
  }

  @Test
  public void testSubmitReplicaReassignmentTasksWithInProgressTaskAndExistingTopic()
      throws InterruptedException, ExecutionException {
    Map<String, TopicDescription> topicDescriptions = createTopics(0);
    int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    ExecutionProposal proposal0 =
        new ExecutionProposal(TP0, 0, new ReplicaPlacementInfo(initialLeader0),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0 == 0 ? 1 : 0)));

    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerAddressList.get(0)));

    ExecutionTask task = new ExecutionTask(0,
                                           proposal0,
                                           ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);

    AlterPartitionReassignmentsResult result = ExecutionUtils.submitReplicaReassignmentTasks(adminClient, Collections.singletonList(task));

    assertEquals(1, result.values().size());
    // Can retrieve the future if it is successful.
    result.values().get(TP0).get();
  }

  @Test
  public void testSubmitReplicaReassignmentTasksWithEmptyNullOrAllCompletedTasks() {
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    // 1. Verify with an empty task list.
    assertThrows(IllegalArgumentException.class, () -> ExecutionUtils.submitReplicaReassignmentTasks(mockAdminClient, Collections.emptyList()));
    // 2. Verify with null tasks.
    assertThrows(IllegalArgumentException.class, () -> ExecutionUtils.submitReplicaReassignmentTasks(mockAdminClient, null));

    // 2. Verify with all completed tasks.
    TopicPartition tp = new TopicPartition(TOPIC0, 0);
    ExecutionProposal proposal = new ExecutionProposal(tp,
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = new ExecutionTask(0,
                                           proposal,
                                           ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(MOCK_CURRENT_TIME);
    task.completed(MOCK_CURRENT_TIME);
    assertThrows(IllegalArgumentException.class, () -> ExecutionUtils.submitReplicaReassignmentTasks(mockAdminClient,
                                                                                                     Collections.singletonList(task)));
  }

  private static boolean verifyFutureError(Future<?> future, Class<? extends Throwable> exceptionClass) throws InterruptedException {
    if (future == null) {
      return false;
    }
    try {
      future.get();
    } catch (ExecutionException ee) {
      return exceptionClass == ee.getCause().getClass();
    }
    return false;
  }

  @Test
  public void testSetRequestedExecutionProgressCheckIntervalMs() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(config, null, new MetricRegistry(), null, EasyMock.mock(MetadataAdminClient.class),
                                     null, EasyMock.mock(AnomalyDetectorManager.class));
    long minExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);

    // RequestedExecutionProgressCheckIntervalMs has to be larger than minExecutionProgressCheckIntervalMs
    executor.setRequestedExecutionProgressCheckIntervalMs(minExecutionProgressCheckIntervalMs);
    assertEquals(minExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());
    assertThrows(IllegalArgumentException.class,
                 () -> executor.setRequestedExecutionProgressCheckIntervalMs(minExecutionProgressCheckIntervalMs - 1));
  }

  @Test
  public void testSetExecutionProgressCheckIntervalMsWithRequestedValue() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(config, null, new MetricRegistry(), null, EasyMock.mock(MetadataAdminClient.class),
                                     null, EasyMock.mock(AnomalyDetectorManager.class));
    long defaultExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    long minExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);

    // Set requestedExecutionProgressCheckIntervalMs
    long requestedExecutionProgressCheckIntervalMs = 2 * defaultExecutionProgressCheckIntervalMs;
    executor.setRequestedExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);

    // With requested executionProgressCheckIntervalMs, the value should be capped at requestedExecutionProgressCheckIntervalMs, and
    //  set to at least minExecutionProgressCheckIntervalMs
    executor.setExecutionProgressCheckIntervalMs(Long.MAX_VALUE);
    assertEquals(requestedExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());

    executor.setExecutionProgressCheckIntervalMs((long) 0);
    assertEquals(minExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());
  }

  @Test
  public void testSetExecutionProgressCheckIntervalMsWithNoRequestedValue() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(config, null, new MetricRegistry(), null, EasyMock.mock(MetadataAdminClient.class),
                                     null, EasyMock.mock(AnomalyDetectorManager.class));
    long defaultExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    long minExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);

    // With no requested executionProgressCheckIntervalMs, the value should be capped at defaultExecutionProgressCheckIntervalMs, and
    //  set to at least minExecutionProgressCheckIntervalMs
    executor.setExecutionProgressCheckIntervalMs(Long.MAX_VALUE);
    assertEquals(defaultExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());

    executor.setExecutionProgressCheckIntervalMs((long) 0);
    assertEquals(minExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());

    long validValue = minExecutionProgressCheckIntervalMs + (defaultExecutionProgressCheckIntervalMs - minExecutionProgressCheckIntervalMs) / 2;
    executor.setExecutionProgressCheckIntervalMs(validValue);
    assertEquals(validValue, executor.executionProgressCheckIntervalMs());
  }

  @Test
  public void testResetExecutionProgressCheckIntervalMs() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    Executor executor = new Executor(config, null, new MetricRegistry(), null, EasyMock.mock(MetadataAdminClient.class),
        null, EasyMock.mock(AnomalyDetectorManager.class));
    long defaultExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    executor.resetExecutionProgressCheckIntervalMs();
    assertEquals(defaultExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());

    // Set requestedExecutionProgressCheckIntervalMs
    long requestedExecutionProgressCheckIntervalMs = 2 * defaultExecutionProgressCheckIntervalMs;
    executor.setRequestedExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);
    assertEquals(requestedExecutionProgressCheckIntervalMs, executor.executionProgressCheckIntervalMs());
  }

  @Test
  public void testExecutionKnobs() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    assertThrows(IllegalStateException.class,
                 () -> new Executor(config, null, new MetricRegistry(), null, EasyMock.mock(MetadataAdminClient.class), null, null));
    Executor executor = new Executor(config, null, new MetricRegistry(), null, EasyMock.mock(MetadataAdminClient.class),
                                     null, EasyMock.mock(AnomalyDetectorManager.class));

    // Verify correctness of add/drop recently removed/demoted brokers.
    assertFalse(executor.dropRecentlyRemovedBrokers(Collections.emptySet()));
    assertFalse(executor.dropRecentlyDemotedBrokers(Collections.emptySet()));
    executor.addRecentlyRemovedBrokers(Collections.singleton(MOCK_BROKER_ID_TO_DROP));
    executor.addRecentlyDemotedBrokers(Collections.singleton(MOCK_BROKER_ID_TO_DROP));
    assertTrue(executor.dropRecentlyRemovedBrokers(Collections.singleton(MOCK_BROKER_ID_TO_DROP)));
    assertTrue(executor.dropRecentlyDemotedBrokers(Collections.singleton(MOCK_BROKER_ID_TO_DROP)));
  }

  @Test
  public void testTimeoutAndExecutionStop() throws InterruptedException, OngoingExecutionException {
    createTopics(0);
    // The proposal tries to move the leader. We fake the replica list to be unchanged so there is no replica
    // movement, but only leader movement.
    ExecutionProposal proposal =
        new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(1),
                              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)));

    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    Time time = new MockTime();
    MetadataAdminClient mockMetadataClient = EasyMock.mock(MetadataAdminClient.class);
    // Fake the metadata to never change so the leader movement will timeout.
    Node node0 = new Node(BROKER_ID_0, "host0", 100);
    Node node1 = new Node(BROKER_ID_1, "host1", 100);
    Node[] replicas = new Node[2];
    replicas[0] = node0;
    replicas[1] = node1;
    PartitionInfo partitionInfo = new PartitionInfo(TP1.topic(), TP1.partition(), node1, replicas, replicas);
    Cluster cluster = new Cluster("id", Arrays.asList(node0, node1), Collections.singleton(partitionInfo),
                                  Collections.emptySet(), Collections.emptySet());
    EasyMock.expect(mockMetadataClient.cluster()).andReturn(cluster).anyTimes();
    LoadMonitor mockLoadMonitor = getMockLoadMonitor();
    AnomalyDetectorManager mockAnomalyDetectorManager = getMockAnomalyDetector(RANDOM_UUID, false);
    UserTaskManager.UserTaskInfo mockUserTaskInfo = getMockUserTaskInfo();
    // This tests runs two consecutive executions. First one completes w/o error, but the second one with error.
    UserTaskManager mockUserTaskManager = getMockUserTaskManager(RANDOM_UUID, mockUserTaskInfo, Arrays.asList(false, true));
    EasyMock.replay(mockMetadataClient, mockLoadMonitor, mockAnomalyDetectorManager, mockUserTaskInfo, mockUserTaskManager);

    Collection<ExecutionProposal> proposalsToExecute = Collections.singletonList(proposal);
    Executor executor = new Executor(configs, time, new MetricRegistry(), null, mockMetadataClient, null,
                                     mockAnomalyDetectorManager);
    executor.setUserTaskManager(mockUserTaskManager);

    executor.setGeneratingProposalsForExecution(RANDOM_UUID, ExecutorTest.class::getSimpleName, true);
    executor.executeProposals(proposalsToExecute,
                              Collections.emptySet(),
                              null,
                              mockLoadMonitor,
                              null, null,
                              null,
                              null,
                              null,
                              null,
                              null,
                              null,
                              true,
                              RANDOM_UUID,
                              false,
                              false);
    waitUntilTrue(() -> (executor.state().state() == ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS && !executor.inExecutionTasks().isEmpty()),
                  "Leader movement task did not start within the time limit",
                  EXECUTION_DEADLINE_MS, EXECUTION_SHORT_CHECK_MS);

    // Sleep over ExecutorConfig#DEFAULT_LEADER_MOVEMENT_TIMEOUT_MS with some margin for inter-thread synchronization.
    time.sleep(ExecutorConfig.DEFAULT_LEADER_MOVEMENT_TIMEOUT_MS + 1L);
    // The execution should finish.
    waitUntilTrue(() -> (!executor.hasOngoingExecution() && executor.state().state() == ExecutorState.State.NO_TASK_IN_PROGRESS),
                  "Proposal execution did not finish within the time limit",
                  EXECUTION_DEADLINE_MS, EXECUTION_REGULAR_CHECK_MS);

    // The proposal tries to move replicas.
    proposal = new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(1),
                                     Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                     Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(0)));
    Collection<ExecutionProposal> newProposalsToExecute = Collections.singletonList(proposal);

    // Expect exception in case of UUID mismatch between UNKNOWN_UUID and RANDOM_UUID.
    executor.setGeneratingProposalsForExecution(UNKNOWN_UUID, ExecutorTest.class::getSimpleName, true);
    assertThrows(IllegalStateException.class,
                 () -> executor.executeProposals(newProposalsToExecute,
                                                 Collections.emptySet(),
                                                 null,
                                                 mockLoadMonitor,
                                                 null, null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 true,
                                                 RANDOM_UUID,
                                                 false,
                                                 false));
    executor.failGeneratingProposalsForExecution(UNKNOWN_UUID);

    // Now successfully start the execution..
    executor.setGeneratingProposalsForExecution(RANDOM_UUID, ExecutorTest.class::getSimpleName, true);
    executor.executeProposals(newProposalsToExecute,
                              Collections.emptySet(),
                              null,
                              mockLoadMonitor,
                              null, null,
                              null,
                              null,
                              null,
                              null,
                              null,
                              null,
                              true,
                              RANDOM_UUID,
                              false,
                              false);

    waitUntilTrue(() -> (executor.state().state() == ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS),
                  "Inter-broker replica movement task did not start within the time limit",
                  EXECUTION_DEADLINE_MS, EXECUTION_SHORT_CHECK_MS);
    // Stop execution.
    executor.userTriggeredStopExecution(false);
    // The execution should finish.
    waitUntilTrue(() -> (!executor.hasOngoingExecution() && executor.state().state() == ExecutorState.State.NO_TASK_IN_PROGRESS),
                  "Proposal execution did not finish within the time limit",
                  EXECUTION_DEADLINE_MS, EXECUTION_REGULAR_CHECK_MS);
    EasyMock.verify(mockMetadataClient, mockLoadMonitor, mockAnomalyDetectorManager, mockUserTaskInfo, mockUserTaskManager);
  }

  /**
   * Proposal#1: [TPO] move from original broker to the other one -- e.g. 0 -> 1
   * Proposal#2: [TP1] change order and leader -- e.g. [0, 1] -> [1, 0]
   * @param proposalToExecute Proposals to execute.
   * @param proposalToVerify Proposals to verify.
   * @param topicSize Topic size in bytes.
   */
  private void populateProposals(List<ExecutionProposal> proposalToExecute,
                                 List<ExecutionProposal> proposalToVerify,
                                 long topicSize) throws InterruptedException {
    Map<String, TopicDescription> topicDescriptions = createTopics((int) topicSize);
    int initialLeader0 = topicDescriptions.get(TOPIC0).partitions().get(0).leader().id();
    int initialLeader1 = topicDescriptions.get(TOPIC1).partitions().get(0).leader().id();
    // Valid proposals
    ExecutionProposal proposal0 =
        new ExecutionProposal(TP0, topicSize, new ReplicaPlacementInfo(initialLeader0),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0 == 0 ? 1 : 0)));
    ExecutionProposal proposal1 =
        new ExecutionProposal(TP1, 0, new ReplicaPlacementInfo(initialLeader1),
                              Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                            new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0)),
                              Arrays.asList(new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0),
                                            new ReplicaPlacementInfo(initialLeader1)));
    // Invalid proposals, the targeting topics of these proposals does not exist.
    ExecutionProposal proposal2 =
        new ExecutionProposal(TP2, 0, new ReplicaPlacementInfo(initialLeader0),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0)),
                              Collections.singletonList(new ReplicaPlacementInfo(initialLeader0 == 0 ? 1 : 0)));
    ExecutionProposal proposal3 =
        new ExecutionProposal(TP3, 0, new ReplicaPlacementInfo(initialLeader1),
                              Arrays.asList(new ReplicaPlacementInfo(initialLeader1),
                                            new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0)),
                              Arrays.asList(new ReplicaPlacementInfo(initialLeader1 == 0 ? 1 : 0),
                                            new ReplicaPlacementInfo(initialLeader1)));

    proposalToExecute.addAll(Arrays.asList(proposal0, proposal1, proposal2, proposal3));
    proposalToVerify.addAll(Arrays.asList(proposal0, proposal1));
  }

  /**
   * Creates {@link com.linkedin.kafka.cruisecontrol.common.TestConstants#TOPIC0 topic0} with replication factor 1 and
   * {@link com.linkedin.kafka.cruisecontrol.common.TestConstants#TOPIC1 topic1} with replication factor 2. Waits until
   * both brokers in the mock cluster receive the metadata about created topics.
   *
   * @param produceSizeInBytes Size of random data in bytes to produce to
   * {@link com.linkedin.kafka.cruisecontrol.common.TestConstants#TOPIC0 topic0}.
   * @return A map from topic names to their description.
   */
  private Map<String, TopicDescription> createTopics(int produceSizeInBytes) {
    _adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, Collections.singletonMap(0, Collections.singletonList(0))),
                                            new NewTopic(TOPIC1, Collections.singletonMap(0, List.of(0, 1)))));

    // Wait for the metadata of newly created topics to be propagated to all the brokers by ensuring
    //   - Every partition has a leader assigned,
    //   - Every partition has replicas assigned,
    //   - The leader is included in the replicas list,
    //   - Every partition's in-sync replicas list is populated
    // so subsequent Admin operations on these topics can safely assume the metadata is consistent.
    Map<String, TopicDescription> topicDescriptions = _cluster.waitForTopicMetadata(
      List.of(TOPIC0, TOPIC1),
      Duration.ofSeconds(15),
      Duration.ofSeconds(60),
      topicDescription -> {
        return topicDescription.partitions() != null
          && !topicDescription.partitions().isEmpty()
          && topicDescription.partitions().stream().allMatch(p ->
            p.leader() != null
            && !p.replicas().isEmpty()
            && p.replicas().contains(p.leader())
            && !p.isr().isEmpty());
      });

    produceRandomDataToTopic(TOPIC0, produceSizeInBytes);
    return topicDescriptions;
  }

  private void verifyOngoingPartitionReassignments(Set<TopicPartition> partitions) {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerAddressList.get(BROKER_ID_0)));
    try {
      waitUntilTrue(() -> {
                      try {
                        return ExecutionUtils.partitionsBeingReassigned(adminClient).containsAll(partitions);
                      } catch (TimeoutException | InterruptedException | ExecutionException e) {
                        throw new IllegalStateException("Failed to verify the start of a replica reassignment.");
                      }
                    },
                    "Failed to verify the start of a partition reassignment",
                    EXECUTION_DEADLINE_MS, EXECUTION_SHORT_CHECK_MS);
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  private void produceRandomDataToTopic(String topic, int produceSize) {
    if (produceSize > 0) {
      Properties props = new Properties();
      props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
      try (Producer<String, String> producer = createProducer(props)) {
        byte[] randomRecords = new byte[produceSize];
        RANDOM.nextBytes(randomRecords);
        producer.send(new ProducerRecord<>(topic, Arrays.toString(randomRecords)));
      }
    }
  }

  private static UserTaskManager getMockUserTaskManager(String uuid,
                                                        UserTaskManager.UserTaskInfo userTaskInfo,
                                                        List<Boolean> completeWithError) {
    UserTaskManager mockUserTaskManager = EasyMock.mock(UserTaskManager.class);
    // Handle the case that the execution started, but did not finish.
    if (completeWithError.isEmpty()) {
      EasyMock.expect(mockUserTaskManager.markTaskExecutionBegan(uuid)).andReturn(userTaskInfo).once();
    } else {
      // Return as many times as the executions to ensure that the same test can run multiple executions.
      for (boolean completeStatus : completeWithError) {
        EasyMock.expect(mockUserTaskManager.markTaskExecutionBegan(uuid)).andReturn(userTaskInfo).once();
        mockUserTaskManager.markTaskExecutionFinished(uuid, completeStatus);
      }
    }
    return mockUserTaskManager;
  }

  private static LoadMonitor getMockLoadMonitor() {
    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.taskRunnerState())
            .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING)
            .anyTimes();
    EasyMock.expect(mockLoadMonitor.samplingMode()).andReturn(ALL).anyTimes();
    EasyMock.expect(mockLoadMonitor.deadBrokersWithReplicas(anyLong())).andReturn(new HashSet<>()).anyTimes();
    mockLoadMonitor.pauseMetricSampling(isA(String.class), EasyMock.anyBoolean());
    expectLastCall().anyTimes();
    mockLoadMonitor.setSamplingMode(ONGOING_EXECUTION);
    expectLastCall().anyTimes();
    mockLoadMonitor.resumeMetricSampling(isA(String.class));
    expectLastCall().anyTimes();
    mockLoadMonitor.setSamplingMode(ALL);
    expectLastCall().anyTimes();
    EasyMock.expect(mockLoadMonitor.brokersWithReplicas(anyLong())).andReturn(Set.of(BROKER_ID_0, BROKER_ID_1)).anyTimes();
    return mockLoadMonitor;
  }

  private static AnomalyDetectorManager getMockAnomalyDetector(String anomalyId, boolean completeWithError) {
    AnomalyDetectorManager mockAnomalyDetectorManager = EasyMock.mock(AnomalyDetectorManager.class);
    mockAnomalyDetectorManager.maybeClearOngoingAnomalyDetectionTimeMs();
    expectLastCall().anyTimes();
    mockAnomalyDetectorManager.resetHasUnfixableGoals();
    expectLastCall().anyTimes();
    mockAnomalyDetectorManager.markSelfHealingFinished(anomalyId, completeWithError);
    expectLastCall().anyTimes();
    return mockAnomalyDetectorManager;
  }

  private static UserTaskManager.UserTaskInfo getMockUserTaskInfo() {
    UserTaskManager.UserTaskInfo mockUserTaskInfo = EasyMock.mock(UserTaskManager.UserTaskInfo.class);
    // Run it any times to enable consecutive executions in tests.
    EasyMock.expect(mockUserTaskInfo.requestUrl()).andReturn("mock-request").anyTimes();
    expect(mockUserTaskInfo.state()).andReturn(UserTaskManager.TaskState.COMPLETED).anyTimes();
    return mockUserTaskInfo;
  }

  private void executeAndVerifyProposals(Admin adminClient,
                                         Collection<ExecutionProposal> proposalsToExecute,
                                         Collection<ExecutionProposal> proposalsToCheck,
                                         boolean completeWithError,
                                         Long replicationThrottle,
                                         boolean verifyProgress,
                                         boolean isTriggeredByUserRequest)
      throws OngoingExecutionException {
    KafkaCruiseControlConfig configs = new KafkaCruiseControlConfig(getExecutorProperties());
    UserTaskManager.UserTaskInfo mockUserTaskInfo = getMockUserTaskInfo();
    UserTaskManager mockUserTaskManager = isTriggeredByUserRequest ? getMockUserTaskManager(RANDOM_UUID, mockUserTaskInfo,
                                                                                            Collections.singletonList(completeWithError))
                                                                   : null;
    ExecutorNotifier mockExecutorNotifier = EasyMock.mock(ExecutorNotifier.class);
    LoadMonitor mockLoadMonitor = getMockLoadMonitor();
    Capture<String> captureMessage = Capture.newInstance(CaptureType.FIRST);
    AnomalyDetectorManager mockAnomalyDetectorManager = getMockAnomalyDetector(RANDOM_UUID, completeWithError);

    if (completeWithError) {
      mockExecutorNotifier.sendAlert(EasyMock.capture(captureMessage));
    } else {
      mockExecutorNotifier.sendNotification(EasyMock.capture(captureMessage));
    }

    if (isTriggeredByUserRequest) {
      EasyMock.replay(mockUserTaskInfo, mockUserTaskManager, mockExecutorNotifier, mockLoadMonitor,
                      mockAnomalyDetectorManager);
    } else {
      EasyMock.replay(mockUserTaskInfo, mockExecutorNotifier, mockLoadMonitor, mockAnomalyDetectorManager);
    }
    MetricRegistry metricRegistry = new MetricRegistry();
    Executor executor = new Executor(configs, Time.SYSTEM, metricRegistry, (AdminClient) _adminClient,
      new MetadataAdminClient(_adminClient), mockExecutorNotifier, mockAnomalyDetectorManager);
    executor.setUserTaskManager(mockUserTaskManager);
    Map<TopicPartition, Integer> replicationFactors = new HashMap<>();
    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      replicationFactors.put(tp, proposal.oldReplicas().size());
    }

    executor.setGeneratingProposalsForExecution(RANDOM_UUID, ExecutorTest.class::getSimpleName, isTriggeredByUserRequest);
    executor.executeProposals(proposalsToExecute, Collections.emptySet(), null, mockLoadMonitor, null, null,
                              null, null, null, null, null,
                              replicationThrottle, isTriggeredByUserRequest, RANDOM_UUID, false, false);

    if (verifyProgress) {
      verifyOngoingPartitionReassignments(Collections.singleton(TP0));
      assertTrue("Concurrency adjuster is not started during execution", executor.isConcurrencyAdjusterStarted());
      assertTrue("Concurrency manager is not initialized during execution", executor.isConcurrencyManagerInitialized());
    }

    waitUntilTrue(() -> (!executor.hasOngoingExecution() && executor.state().state() == ExecutorState.State.NO_TASK_IN_PROGRESS),
                  "Proposal execution did not finish within the time limit",
                  EXECUTION_DEADLINE_MS, EXECUTION_REGULAR_CHECK_MS);

    // Check notification is sent after execution has finished.
    String notification = captureMessage.getValue();
    assertTrue(notification.contains(RANDOM_UUID));

    for (ExecutionProposal proposal : proposalsToCheck) {
      TopicPartition tp = new TopicPartition(proposal.topic(), proposal.partitionId());
      int expectedReplicationFactor = replicationFactors.get(tp);

      Set<Integer> expectedBrokerIds = proposal.newReplicas().stream()
        .map(ReplicaPlacementInfo::brokerId)
        .collect(Collectors.toSet());

      // Wait for topic metadata change to propagate.
      Map<String, TopicDescription> topicDescriptions = _cluster.waitForTopicMetadata(
        List.of(tp.topic()),
        Duration.ofSeconds(15),
        Duration.ofSeconds(60),
        topicDescription -> {
          TopicPartitionInfo partitionInfo = topicDescription.partitions().get(tp.partition());

          List<Node> replicas = partitionInfo.replicas();
          Set<Integer> actualBrokerIds = replicas.stream().map(Node::id).collect(Collectors.toSet());
          boolean replicasMatch = actualBrokerIds.containsAll(expectedBrokerIds);

          Node leader = partitionInfo.leader();
          boolean leaderMatches = leader != null && leader.id() == proposal.newLeader().brokerId();

          return replicasMatch && leaderMatches;
        });

      TopicDescription topic = topicDescriptions.get(proposal.topic());

      assertEquals("Replication factor for partition " + tp + " should be " + expectedReplicationFactor,
              expectedReplicationFactor, topic.partitions().get(tp.partition()).replicas().size());

      if (proposal.hasReplicaAction()) {
        for (ReplicaPlacementInfo r : proposal.newReplicas()) {
          assertTrue("The partition should have moved for " + tp,
                  topic.partitions().get(tp.partition()).replicas().stream().anyMatch(rep -> r.brokerId().equals(rep.id())));
        }
      }
      assertEquals("The leader should have moved for " + tp,
                   proposal.newLeader().brokerId().intValue(), topic.partitions().get(tp.partition()).leader().id());

    }
    if (isTriggeredByUserRequest) {
      EasyMock.verify(mockUserTaskInfo, mockUserTaskManager, mockExecutorNotifier, mockLoadMonitor, mockAnomalyDetectorManager);
    } else {
      EasyMock.verify(mockUserTaskInfo, mockExecutorNotifier, mockLoadMonitor, mockAnomalyDetectorManager);
    }

    assertNotNull(metricRegistry.meter("Executor.inter-broker-partition-movement-rate"));
    assertNotNull(metricRegistry.meter("Executor.intra-broker-partition-movement-rate"));
    assertNotNull(metricRegistry.meter("Executor.leadership-movement-rate"));
    assertNotNull(metricRegistry.meter("Executor.partition-data-movement-rate-MB"));
    assertNotNull(metricRegistry.gauge("Executor.partition-movement-count-per-second"));
    assertNotNull(metricRegistry.gauge("Executor.partition-movement-MB-per-second"));
    assertFalse("Concurrency adjuster is not shutdown after execution", executor.isConcurrencyAdjusterStarted());
    assertFalse("Concurrency manager is not reset after execution", executor.isConcurrencyManagerInitialized());
  }

  private Properties getExecutorProperties() {
    Properties props = new Properties();
    String capacityConfigFile = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource(DEFAULT_BROKER_CAPACITY_CONFIG_FILE)).getFile();
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "400");
    props.setProperty(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "200");
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    props.setProperty(ExecutorConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(DEMOTION_HISTORY_RETENTION_TIME_MS));
    props.setProperty(ExecutorConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(REMOVAL_HISTORY_RETENTION_TIME_MS));
    props.setProperty(ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG, Long.toString(LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS));
    props.setProperty(ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_CONFIG, Long.toString(LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS));
    props.setProperty(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG, "120000");
    return props;
  }
}
