/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.BYTES_IN_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExecutorAdminUtilsTest {
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  private static final long PARTITION_SIZE_IN_MB = 10L;
  private static final long EXECUTION_ALERTING_THRESHOLD_MS = 100L;
  private static final List<Node> NODES = Arrays.asList(new Node(0, "host0", 100),
                                                        new Node(1, "host1", 100),
                                                        new Node(2, "host2", 100),
                                                        new Node(3, "host3", 100));
  private static final ReplicaPlacementInfo R0 = new ReplicaPlacementInfo(0);
  private static final ReplicaPlacementInfo R1 = new ReplicaPlacementInfo(1);
  private static final ReplicaPlacementInfo R2 = new ReplicaPlacementInfo(2);
  private static final ReplicaPlacementInfo R3 = new ReplicaPlacementInfo(3);

  /**
   * @return A task to move the partition from brokers [0, 1] (leader: 0) to brokers [2, 3].
   */
  private static ExecutionTask interBrokerReplicaTask() {
    ExecutionProposal proposal = new ExecutionProposal(TP, PARTITION_SIZE_IN_MB, R0, Arrays.asList(R0, R1), Arrays.asList(R2, R3));
    return new ExecutionTask(0L, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  /**
   * @return A task to move the replica of the partition on broker 0 from logdir /d1 to logdir /d2.
   */
  private static ExecutionTask intraBrokerReplicaTask() {
    ExecutionProposal proposal = new ExecutionProposal(TP, PARTITION_SIZE_IN_MB, new ReplicaPlacementInfo(0, "/d1"),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, "/d1"), new ReplicaPlacementInfo(1, "/d1")),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, "/d2"), new ReplicaPlacementInfo(1, "/d1")));
    return new ExecutionTask(1L, proposal, 0, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private static Cluster cluster(Node leader) {
    Node[] replicas = {NODES.get(0), NODES.get(1)};
    PartitionInfo partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), leader, replicas, replicas);
    return new Cluster("cluster", NODES, Collections.singleton(partitionInfo), Collections.emptySet(), Collections.emptySet());
  }

  private static Map<String, LogDirDescription> logDir(String path, long sizeInMB, boolean isFuture) {
    return Collections.singletonMap(path, new LogDirDescription(null,
        Collections.singletonMap(TP, new ReplicaInfo(sizeInMB * BYTES_IN_MB, 0L, isFuture))));
  }

  @Test
  public void testUpdateInterBrokerReplicaMovementProgress() {
    ExecutionTask task = interBrokerReplicaTask();
    task.inProgress(0L);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = new HashMap<>();
    logDirsByBrokerId.put(0, logDir("/d1", 300L, false));
    logDirsByBrokerId.put(2, logDir("/d1", 100L, false));
    // The replica has not been created on broker 3 yet.
    logDirsByBrokerId.put(3, Collections.singletonMap("/d1", new LogDirDescription(null, Collections.emptyMap())));

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId);
    assertTrue(task.hasDataMovementProgress());
    assertEquals(100L * BYTES_IN_MB, task.movedDataInBytes());
    // Leader replica size (300 MB) for each of the two replicas being created.
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());
    Map<String, Object> taskJson = task.getJsonStructure();
    assertEquals(100L, taskJson.get("movedDataInMB"));
    assertEquals(600L, taskJson.get("totalDataToMoveInMB"));

    // The replica on broker 3 is created and the leader replica size is unavailable: fall back to the estimated partition size.
    logDirsByBrokerId.put(3, logDir("/d1", 5L, false));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(null), logDirsByBrokerId);
    assertEquals(105L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(2 * PARTITION_SIZE_IN_MB * BYTES_IN_MB, task.totalDataToMoveInBytes());

    // A destination broker fails to respond: keep the last known progress.
    logDirsByBrokerId.remove(3);
    logDirsByBrokerId.put(2, logDir("/d1", 200L, false));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId);
    assertEquals(105L * BYTES_IN_MB, task.movedDataInBytes());

    // Progress is not reported once the task is no longer in progress.
    task.completed(1L);
    assertFalse(task.getJsonStructure().containsKey("movedDataInMB"));
  }

  @Test
  public void testUpdateIntraBrokerReplicaMovementProgress() {
    ExecutionTask task = intraBrokerReplicaTask();
    task.inProgress(0L);
    Map<String, LogDirDescription> logDirs = new HashMap<>(logDir("/d1", 50L, false));
    // The future replica has not been created on the destination disk yet.
    logDirs.put("/d2", new LogDirDescription(null, Collections.emptyMap()));
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = Collections.singletonMap(0, logDirs);

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId);
    assertEquals(0L, task.movedDataInBytes());
    assertEquals(50L * BYTES_IN_MB, task.totalDataToMoveInBytes());

    logDirs.putAll(logDir("/d2", 20L, true));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId);
    assertEquals(20L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(50L * BYTES_IN_MB, task.totalDataToMoveInBytes());
    Map<String, Object> taskJson = task.getJsonStructure();
    assertEquals(20L, taskJson.get("movedDataInMB"));
    assertEquals(50L, taskJson.get("totalDataToMoveInMB"));
  }

  @Test
  public void testIgnoreTasksNotInProgress() {
    ExecutionTask pendingTask = interBrokerReplicaTask();
    ExecutionProposal leaderProposal = new ExecutionProposal(TP, PARTITION_SIZE_IN_MB, R0, Arrays.asList(R0, R1), Arrays.asList(R1, R0));
    ExecutionTask leaderTask = new ExecutionTask(2L, leaderProposal, ExecutionTask.TaskType.LEADER_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    leaderTask.inProgress(0L);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = new HashMap<>();
    for (int brokerId = 0; brokerId < NODES.size(); brokerId++) {
      logDirsByBrokerId.put(brokerId, logDir("/d1", 100L, false));
    }

    ExecutorAdminUtils.updateDataMovementProgress(Arrays.asList(pendingTask, leaderTask), cluster(NODES.get(0)), logDirsByBrokerId);
    assertFalse(pendingTask.hasDataMovementProgress());
    assertFalse(leaderTask.hasDataMovementProgress());
    assertFalse(pendingTask.getJsonStructure().containsKey("movedDataInMB"));
  }

  @Test
  public void testUpdateDataMovementProgressWithAdminClient() {
    ExecutionTask task = interBrokerReplicaTask();
    task.inProgress(0L);

    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futureByBrokerId = new HashMap<>();
    futureByBrokerId.put(0, KafkaFuture.completedFuture(logDir("/d1", 300L, false)));
    futureByBrokerId.put(2, KafkaFuture.completedFuture(logDir("/d1", 100L, false)));
    futureByBrokerId.put(3, KafkaFuture.completedFuture(logDir("/d1", 40L, false)));
    DescribeLogDirsResult describeLogDirsResult = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(describeLogDirsResult.descriptions()).andReturn(futureByBrokerId);
    Admin adminClient = EasyMock.mock(Admin.class);
    // Only the leader and the destination brokers are described.
    EasyMock.expect(adminClient.describeLogDirs(EasyMock.eq(new HashSet<>(Arrays.asList(0, 2, 3)))))
            .andReturn(describeLogDirsResult);
    EasyMock.replay(describeLogDirsResult, adminClient);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), adminClient, config);
    EasyMock.verify(describeLogDirsResult, adminClient);
    assertEquals(140L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());

    // A destination broker fails to respond: keep the last known progress.
    KafkaFutureImpl<Map<String, LogDirDescription>> failedFuture = new KafkaFutureImpl<>();
    failedFuture.completeExceptionally(new TimeoutException("Timed out"));
    futureByBrokerId.put(2, KafkaFuture.completedFuture(logDir("/d1", 200L, false)));
    futureByBrokerId.put(3, failedFuture);
    EasyMock.reset(describeLogDirsResult, adminClient);
    EasyMock.expect(describeLogDirsResult.descriptions()).andReturn(futureByBrokerId);
    EasyMock.expect(adminClient.describeLogDirs(EasyMock.anyObject())).andReturn(describeLogDirsResult);
    EasyMock.replay(describeLogDirsResult, adminClient);

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), adminClient, config);
    EasyMock.verify(describeLogDirsResult, adminClient);
    assertEquals(140L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());
  }

  @Test
  public void testNoDescribeLogDirsRequestWithoutInProgressReplicaTasks() {
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.replay(adminClient);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(interBrokerReplicaTask()), cluster(NODES.get(0)), adminClient, config);
    // No describeLogDirs request is sent, since the only task is pending.
    EasyMock.verify(adminClient);
  }
}
