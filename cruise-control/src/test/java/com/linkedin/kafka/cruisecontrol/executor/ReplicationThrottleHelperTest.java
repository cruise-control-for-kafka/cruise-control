/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ReplicationThrottleHelperTest extends CCKafkaIntegrationTestHarness {
  private static final Config EMPTY_CONFIG = new Config(Collections.emptyList());

  /**
   * The admin client
   */
  private AdminClient _adminClient;

  @Override
  public int clusterSize() {
    return 4;
  }

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    super.setUp();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
  }

  /**
   * Teardown the test.
   */
  @After
  public void tearDown() {
    super.tearDown();
    if (_adminClient != null) {
      _adminClient.close(Duration.ofMillis(1000L));
    }
  }

  private void createTopics() throws ExecutionException, InterruptedException {
    _adminClient.createTopics(Arrays.asList(
        new NewTopic(TOPIC0, 2, (short) 2),
        new NewTopic(TOPIC1, 2, (short) 2)
    )).all().get();
  }

  private void setWildcardThrottleReplicaForTopic(String topicName) throws Exception {
    for (String replicaThrottleProp : Arrays.asList(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                                                    ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
      Collection<AlterConfigOp> configs = Collections.singletonList(
              new AlterConfigOp(new ConfigEntry(replicaThrottleProp, ReplicationThrottleHelper.WILDCARD_ASTERISK), AlterConfigOp.OpType.SET)
      );
      applyTopicConfigs(topicName, configs);
    }
  }

  private void applyBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(ReplicationThrottleHelper.CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private void applyTopicConfigs(String topic, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(ReplicationThrottleHelper.CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private ExecutionTask inProgressTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = new ExecutionTask(id, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(0);
    return task;
  }

  private ExecutionTask completedTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = inProgressTaskForProposal(id, proposal);
    task.completed(1);
    return task;
  }

  @Test
  public void testIsNoOpWhenThrottleIsNull() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(mockAdminClient);

    // Test would fail on any unexpected interactions with the adminClient
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, null);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearThrottleOnNonExistentTopic() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);
    final int partitionId = 0;
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, partitionId),
                                                       100,
                                                       new ReplicaPlacementInfo(brokerId0),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Case 1: a situation where Topic0 does not exist. Hence no property is returned upon read.

    // Before the dynamic broker config is removed for the follower throttle rate
    Config brokerConfig = new Config(Arrays.asList(
            mockConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
            mockConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG))
    );
    // After the dynamic broker config is removed for the follower throttle rate
    Config brokerConfig2 = new Config(Arrays.asList(
            mockConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
            mockConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "300",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG))
    );
    // Expect that only the dynamic throttle rate configs are removed when clearing throttles
    expectDescribeBrokerConfigs(mockAdminClient, brokers, brokerConfig);
    expectIncrementalBrokerConfigs(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers, brokerConfig2);
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    ExecutionTask mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);

    // Case 2: a situation where Topic0 gets deleted after its configs were read.
    EasyMock.reset(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers, brokerConfig);
    expectIncrementalBrokerConfigs(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers, EMPTY_CONFIG);
    String throttledReplicas = brokerId0 + "," + brokerId1;
    Config topicConfigProps = new Config(Arrays.asList(
            new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas),
            new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas)));
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, topicConfigProps, true);
    expectIncrementalTopicConfigs(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    // Expect no exception
    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);
  }

  @Test
  public void testSetThrottleOnNonExistentTopic() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);
    final int partitionId = 0;
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, partitionId),
                                                       100,
                                                       new ReplicaPlacementInfo(brokerId0),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Case 1: a situation where Topic0 does not exist. Hence no property is returned upon read.
    expectDescribeBrokerConfigs(mockAdminClient, brokers);
    // incremental alter for brokers, then verification describe with expected throttle values
    expectIncrementalBrokerConfigs(mockAdminClient);
    Config brokerConfigAfter = new Config(Arrays.asList(
        new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(throttleRate)),
        new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokers, brokerConfigAfter);
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    expectIncrementalTopicConfigs(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    // Expect no exception
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);

    // Case 2: a situation where Topic0 gets deleted after its configs were read. Change configs should not fail.
    EasyMock.reset(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers);
    expectIncrementalBrokerConfigs(mockAdminClient);
    expectDescribeBrokerConfigs(mockAdminClient, brokers, brokerConfigAfter);
    String throttledReplicas = brokerId0 + "," + brokerId1;
    Config topicConfigs = new Config(Arrays.asList(
      new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas),
      new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas)));
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, topicConfigs, true);
    expectIncrementalTopicConfigs(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    // Expect no exception
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testAddingThrottlesWithNoPreExistingThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testAddingThrottlesWithPreExistingThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition(TOPIC0, 0),
        100,
        new ReplicaPlacementInfo(0),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    // Broker 0 has an existing leader throttle rate of 200 and follower throttle rate of 100
    // We expect to overwrite the leader throttle to the desired throttleRate of 100
    long preExistingBroker0ThrottleRate = 200L;
    List<AlterConfigOp> broker0Configs = Arrays.asList(
      new AlterConfigOp(
              new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(preExistingBroker0ThrottleRate)),
              AlterConfigOp.OpType.SET),
      new AlterConfigOp(
              new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(throttleRate)),
              AlterConfigOp.OpType.SET)
    );
    applyBrokerConfigs(0, broker0Configs);

    // Partition 1 (which is not involved in any execution proposal) has pre-existing throttled
    // replicas (on both leaders and followers); we expect these configurations to be merged
    // with our new throttled replicas.
    List<AlterConfigOp> topic0Configs = Arrays.asList(
        new AlterConfigOp(
            new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:0,1:1"),
            AlterConfigOp.OpType.SET),
        new AlterConfigOp(
            new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:0,1:1"),
            AlterConfigOp.OpType.SET));
    applyTopicConfigs(TOPIC0, topic0Configs);

    // Topic 1 is not involved in any execution proposal. It has pre-existing throttled replicas.
    List<AlterConfigOp> topic1Config = Arrays.asList(
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1"), AlterConfigOp.OpType.SET));
    applyTopicConfigs(TOPIC1, topic1Config);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    // Existing throttled replicas are merged with new throttled replicas for topic 0:
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2,1:0,1:1");
    // Existing throttled replicas are unchanged for topic 1:
    assertExpectedThrottledReplicas(TOPIC1, "1:1");

    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    // We expect all throttles related to replica movement to be removed. Specifically,
    // any throttles related to partitions which were not moved will remain.
    // However, we do expect the broker throttles to be removed.
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:1");
    assertExpectedThrottledReplicas(TOPIC1, "1:1");
  }

  @Test
  public void testDoNotModifyExistingWildcardReplicaThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);

    // Set replica throttle config values for both topics
    setWildcardThrottleReplicaForTopic(TOPIC0);
    setWildcardThrottleReplicaForTopic(TOPIC1);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(new TopicPartition(TOPIC0, 1),
                                            100,
                                                        new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2));

    ExecutionTask completedTask = completedTaskForProposal(0, proposal);
    ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    // passing an inProgress task that is not complete should have no effect.
    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    // Completing the in-progress task and the "*" should not be cleaned up.
    inProgressTask.completed(3);
    throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);
  }

  @Test
  public void testDoNotRemoveThrottlesForInProgressTasks() throws Exception {
    createTopics();

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(new TopicPartition(TOPIC0, 1),
                                            100,
                                                        new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2));

    ExecutionTask completedTask = completedTaskForProposal(0, proposal);
    ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2,1:0,1:2,1:3");

    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:2,1:3");

    // passing an inProgress task that is not complete should have no effect.
    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:2,1:3");

    // Completing the in-progress task and clearing the throttles should clean everything up.
    inProgressTask.completed(3);
    throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testRemoveReplicasFromConfigTest() {
    Set<String> replicas = new LinkedHashSet<>();
    replicas.add("foo");
    replicas.add("bar");
    replicas.add("baz");
    String throttleConfig = "foo,bar,qux,qaz,baz";
    String result = ReplicationThrottleHelper.removeReplicasFromConfig(throttleConfig, replicas);
    assertEquals(result, "qux,qaz");
  }

  @Test
  public void testWaitForConfigs() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    int retries = 3;
    // Case 1: queue more responses than RETRIES and expect checkConfigs to throw
    for (int i = 0; i < retries + 1; i++) {
      expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, true);
    }
    EasyMock.replay(mockAdminClient);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, retries);
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    Map<ConfigResource, Collection<AlterConfigOp>> opsByResource = Collections.singletonMap(cf,
            Collections.singletonList(new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOp.OpType.SET)));
    assertThrows(IllegalStateException.class, () -> throttleHelper.waitForConfigs(opsByResource));

    // Case 2: queue a single result and call checkConfigs with matching configs, so it succeeds
    EasyMock.reset(mockAdminClient);
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, EMPTY_CONFIG, true);
    EasyMock.replay(mockAdminClient);
    throttleHelper.waitForConfigs(Collections.singletonMap(cf, Collections.emptyList()));
  }

  @Test
  public void testConfigsEqual() {
    Map<String, String> expectedConfigs = new HashMap<>();
    List<ConfigEntry> entries = new ArrayList<>();

    assertTrue(ReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));
    expectedConfigs.put("name1", "value1");
    assertFalse(ReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));
    entries.add(new ConfigEntry("name1", "value1"));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name2", null);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name2", ""));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name3", null);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name3", null));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name4", "value4");
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name4", "other-value"));
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name4", "other-value");
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));

    // In the case that a dynamic config is deleted and a static config exists, the comparison is skipped
    ConfigEntry mockStaticConfig = mockConfigEntry("name5", "value5", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
    expectedConfigs.put("name5", null);
    entries.add(mockStaticConfig);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    EasyMock.verify(mockStaticConfig);
  }

  private ExecutionTask prepareMockCompleteTask(ExecutionProposal proposal) {
    ExecutionTask mockCompleteTask = EasyMock.mock(ExecutionTask.class);
    EasyMock.expect(mockCompleteTask.state()).andReturn(ExecutionTaskState.COMPLETED).times(2);
    EasyMock.expect(mockCompleteTask.type()).andReturn(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION).once();
    EasyMock.expect(mockCompleteTask.proposal()).andReturn(proposal).once();
    EasyMock.replay(mockCompleteTask);
    return mockCompleteTask;
  }

  private ConfigEntry mockConfigEntry(String name, String value, ConfigEntry.ConfigSource configSource) {
    ConfigEntry configEntry = EasyMock.mock(ConfigEntry.class);
    EasyMock.expect(configEntry.name()).andReturn(name).atLeastOnce();
    EasyMock.expect(configEntry.value()).andReturn(value).atLeastOnce();
    EasyMock.expect(configEntry.source()).andReturn(configSource).atLeastOnce();
    EasyMock.replay(configEntry);
    return configEntry;
  }

  private void expectDescribeTopicConfigs(AdminClient adminClient, String topic, Config topicConfig, boolean topicExists)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Config> topicConfigs = Collections.singletonMap(cf, topicConfig);
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
    if (topicExists) {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(topicConfigs);
    } else {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
              .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));
    }
    EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockFuture);
    EasyMock.expect(adminClient.describeConfigs(Collections.singletonList(cf))).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mockDescribeConfigsResult, mockFuture);
  }

  private void expectIncrementalTopicConfigs(AdminClient adminClient, String topic, boolean topicExists)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    AlterConfigsResult mockAlterConfigsResult = EasyMock.mock(AlterConfigsResult.class);
    KafkaFuture<Void> mockFuture = EasyMock.mock(KafkaFuture.class);
    Map<ConfigResource, KafkaFuture<Void>> futures = Collections.singletonMap(cf, mockFuture);
    EasyMock.expect(mockAlterConfigsResult.values()).andReturn(futures);
    if (topicExists) {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);
    } else {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
              .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));
    }
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.anyObject())).andReturn(mockAlterConfigsResult);
    EasyMock.replay(mockAlterConfigsResult, mockFuture);
  }

  private void expectListTopics(AdminClient adminClient, Set<String> topics)
  throws ExecutionException, InterruptedException, TimeoutException {
    ListTopicsResult mockListTopicsResult = EasyMock.mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockListTopicsResult.names()).andReturn(mockFuture);
    EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(topics);
    EasyMock.expect(adminClient.listTopics()).andReturn(mockListTopicsResult);
    EasyMock.replay(mockListTopicsResult, mockFuture);
  }

  private void expectDescribeBrokerConfigs(AdminClient adminClient, List<Integer> brokers)
  throws ExecutionException, InterruptedException, TimeoutException {
    // All participating brokers have throttled rate set already
    expectDescribeBrokerConfigs(adminClient, brokers, EMPTY_CONFIG);
  }

  private void expectDescribeBrokerConfigs(AdminClient adminClient, List<Integer> brokers, Config brokerConfig)
  throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, Config> brokerConfigs = new HashMap<>();
    for (int i : brokers) {
      ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(i));
      brokerConfigs.put(cf, brokerConfig);
    }
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(brokerConfigs);
    EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockFuture);
    EasyMock.expect(adminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mockDescribeConfigsResult, mockFuture);
  }

  private void expectIncrementalBrokerConfigs(AdminClient adminClient)
  throws ExecutionException, InterruptedException, TimeoutException {
    AlterConfigsResult mockAlterConfigsResult = EasyMock.mock(AlterConfigsResult.class);
    KafkaFuture<Void> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockAlterConfigsResult.all()).andReturn(mockFuture);
    EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.anyObject())).andReturn(mockAlterConfigsResult);
    EasyMock.replay(mockAlterConfigsResult, mockFuture);
  }

  /**
   * With no participating brokers, no broker config alter should be issued.
   */
  @Test
  public void setThrottlesWhenNoParticipatingBrokersThenNoBrokerAlter() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, java.util.Set.of());
    EasyMock.replay(mockAdminClient);
    throttleHelper.setThrottles(Collections.emptyList());
    EasyMock.verify(mockAdminClient);
  }

  /**
   * If wildcard throttles are already set on the topic, operations should be a no-op for topic configuration updates.
   */
  @Test
  public void setThrottledReplicasWhenWildcardPresentThenNoOp() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);

    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
        100,
        new ReplicaPlacementInfo(brokerId0),
        Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
        Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate, java.util.Set.of());

    Config brokerConfigAlready = new Config(Arrays.asList(
        new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(throttleRate)),
        new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokers, brokerConfigAlready);

    Config wildcardTopicConfig = new Config(Arrays.asList(
        new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ReplicationThrottleHelper.WILDCARD_ASTERISK),
        new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ReplicationThrottleHelper.WILDCARD_ASTERISK)));
    expectDescribeTopicConfigs(mockAdminClient, TOPIC0, wildcardTopicConfig, true);

    EasyMock.replay(mockAdminClient);
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);
  }

  /**
   * waitForConfigs should throw when verification exceeds the maximum number of retries.
   */
  @Test
  public void waitForConfigsWhenExceedsRetriesThenThrow() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    int retries = 2;

    ConfigResource topicCf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    ConfigResource brokerCf = new ConfigResource(ConfigResource.Type.BROKER, "1");
    Map<ConfigResource, Collection<AlterConfigOp>> opsByResource = new HashMap<>();
    opsByResource.put(topicCf, Collections.singletonList(new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOp.OpType.SET)));
    opsByResource.put(brokerCf, Collections.singletonList(new AlterConfigOp(new ConfigEntry("a", "b"), AlterConfigOp.OpType.SET)));

    for (int i = 0; i < retries + 1; i++) {
      DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
      KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
      Map<ConfigResource, Config> returned = new HashMap<>();
      returned.put(topicCf, new Config(Collections.emptyList()));
      returned.put(brokerCf, new Config(Collections.emptyList()));
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(returned);
      EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockFuture);
      EasyMock.expect(mockAdminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
      EasyMock.replay(mockDescribeConfigsResult, mockFuture);
    }

    EasyMock.replay(mockAdminClient);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, retries);
    assertThrows(IllegalStateException.class, () -> throttleHelper.waitForConfigs(opsByResource));
  }

  /**
   * waitForConfigs should succeed when actual configs match the expected
   * values for all resources.
   */
  @Test
  public void waitForConfigsWhenConfigsMatchThenSuccess() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    int retries = 2;

    ConfigResource topicCf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    ConfigResource brokerCf = new ConfigResource(ConfigResource.Type.BROKER, "1");
    Map<ConfigResource, Collection<AlterConfigOp>> opsByResource = new HashMap<>();
    opsByResource.put(topicCf, Collections.singletonList(new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOp.OpType.SET)));
    opsByResource.put(brokerCf, Collections.singletonList(new AlterConfigOp(new ConfigEntry("a", "b"), AlterConfigOp.OpType.SET)));

    Map<ConfigResource, Config> returned = new HashMap<>();
    returned.put(topicCf, new Config(Collections.singletonList(new ConfigEntry("k", "v"))));
    returned.put(brokerCf, new Config(Collections.singletonList(new ConfigEntry("a", "b"))));

    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(returned);
    EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockFuture);
    EasyMock.expect(mockAdminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mockDescribeConfigsResult, mockFuture);

    EasyMock.replay(mockAdminClient);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, retries);
    throttleHelper.waitForConfigs(opsByResource);
    EasyMock.verify(mockAdminClient);
  }

  /**
   * configsEqualByResource returns true when all resources match expected values.
   */
  @Test
  public void configsEqualByResourceWhenAllResourcesMatchThenTrue() {
    Map<ConfigResource, Map<String, String>> expectedByResource = new HashMap<>();
    ConfigResource cf1 = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    ConfigResource cf2 = new ConfigResource(ConfigResource.Type.BROKER, "1");
    expectedByResource.put(cf1, Collections.singletonMap("k1", "v1"));
    expectedByResource.put(cf2, Collections.singletonMap("k2", "v2"));

    Map<ConfigResource, Config> actualByResource = new HashMap<>();
    actualByResource.put(cf1, new Config(Collections.singletonList(new ConfigEntry("k1", "v1"))));
    actualByResource.put(cf2, new Config(Collections.singletonList(new ConfigEntry("k2", "v2"))));

    boolean actual = ReplicationThrottleHelper.configsEqualByResource(actualByResource, expectedByResource);
    assertTrue(actual);
  }

  /**
   * configsEqualByResource returns false when any resource mismatches.
   */
  @Test
  public void configsEqualByResourceWhenAnyResourceMismatchesThenFalse() {
    Map<ConfigResource, Map<String, String>> expectedByResource = new HashMap<>();
    ConfigResource cf1 = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    expectedByResource.put(cf1, Collections.singletonMap("k1", "v1"));

    Map<ConfigResource, Config> actualByResource = new HashMap<>();
    actualByResource.put(cf1, new Config(Collections.singletonList(new ConfigEntry("k1", "x"))));

    boolean actual = ReplicationThrottleHelper.configsEqualByResource(actualByResource, expectedByResource);
    assertFalse(actual);
  }

  /**
   * Should retry on partial mismatch and eventually succeed when subsequent
   * describes match the expected values.
   */
  @Test
  public void waitForConfigsWhenPartialMismatchThenRetryAndEventuallySuccess() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    int retries = 2;

    ConfigResource topicCf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    ConfigResource brokerCf = new ConfigResource(ConfigResource.Type.BROKER, "1");
    Map<ConfigResource, Collection<AlterConfigOp>> opsByResource = new HashMap<>();
    opsByResource.put(topicCf, Collections.singletonList(new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOp.OpType.SET)));
    opsByResource.put(brokerCf, Collections.singletonList(new AlterConfigOp(new ConfigEntry("a", "b"), AlterConfigOp.OpType.SET)));

    DescribeConfigsResult firstDescribe = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> firstFuture = EasyMock.mock(KafkaFuture.class);
    Map<ConfigResource, Config> firstReturned = new HashMap<>();
    firstReturned.put(topicCf, new Config(Collections.emptyList()));
    firstReturned.put(brokerCf, new Config(Collections.singletonList(new ConfigEntry("a", "b"))));
    EasyMock.expect(firstFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(firstReturned);
    EasyMock.expect(firstDescribe.all()).andReturn(firstFuture);
    EasyMock.expect(mockAdminClient.describeConfigs(EasyMock.anyObject())).andReturn(firstDescribe);
    EasyMock.replay(firstDescribe, firstFuture);

    DescribeConfigsResult secondDescribe = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> secondFuture = EasyMock.mock(KafkaFuture.class);
    Map<ConfigResource, Config> secondReturned = new HashMap<>();
    secondReturned.put(topicCf, new Config(Collections.singletonList(new ConfigEntry("k", "v"))));
    secondReturned.put(brokerCf, new Config(Collections.singletonList(new ConfigEntry("a", "b"))));
    EasyMock.expect(secondFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(secondReturned);
    EasyMock.expect(secondDescribe.all()).andReturn(secondFuture);
    EasyMock.expect(mockAdminClient.describeConfigs(EasyMock.anyObject())).andReturn(secondDescribe);
    EasyMock.replay(secondDescribe, secondFuture);

    EasyMock.replay(mockAdminClient);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, retries);
    throttleHelper.waitForConfigs(opsByResource);
    EasyMock.verify(mockAdminClient);
  }

  /**
   * configsEqualByResource returns false when an expected resource is missing in
   * the actual result set.
   */
  @Test
  public void configsEqualByResourceWhenMissingResourceInActualThenFalse() {
    Map<ConfigResource, Map<String, String>> expectedByResource = new HashMap<>();
    ConfigResource cf1 = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    ConfigResource cf2 = new ConfigResource(ConfigResource.Type.BROKER, "1");
    expectedByResource.put(cf1, Collections.singletonMap("k1", "v1"));
    expectedByResource.put(cf2, Collections.singletonMap("k2", "v2"));

    Map<ConfigResource, Config> actualByResource = new HashMap<>();
    actualByResource.put(cf1, new Config(Collections.singletonList(new ConfigEntry("k1", "v1"))));

    boolean actual = ReplicationThrottleHelper.configsEqualByResource(actualByResource, expectedByResource);
    assertFalse(actual);
  }

  private void assertExpectedThrottledRateForBroker(int brokerId, Long expectedRate) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Config> brokerConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    String expectedString = expectedRate == null ? null : String.valueOf(expectedRate);
    assertNotNull(brokerConfig.get(cf));
    if (expectedRate == null) {
      assertNull(brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG));
      assertNull(brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG));
    } else {
      assertEquals(expectedString, brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG).value());
      assertEquals(expectedString, brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG).value());
    }
  }

  private void assertExpectedThrottledReplicas(String topic, String expectedReplicas) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Config> topicConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    assertNotNull(topicConfig.get(cf));
    assertEquals(expectedReplicas, topicConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value());
    assertEquals(expectedReplicas, topicConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value());
  }
}
