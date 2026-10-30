/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test class for {@link ExecutorAdminUtils}.
 */
public class ExecutorAdminUtilsTest {
  private static final TopicPartition TP = new TopicPartition(TOPIC0, 0);
  private static final String LOGDIR_0 = "d0";
  private static final String LOGDIR_1 = "d1";
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  @Test
  public void testCancelIntraBrokerReplicaMovements() throws Exception {
    // Move replicas on brokers 0, 1, and 2 from d0 to d1.
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(2, LOGDIR_0)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(2, LOGDIR_1)));
    ExecutionTask task0 = intraBrokerTask(0, proposal, 0);
    ExecutionTask task1 = intraBrokerTask(1, proposal, 1);
    ExecutionTask task2 = intraBrokerTask(2, proposal, 2);
    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TP.topic(), TP.partition(), 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TP.topic(), TP.partition(), 1);

    // Movements of replicas on brokers 0 and 1 are in progress. Logdir information of the replica on broker 2 is unknown.
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask = new HashMap<>();
    logdirInfoByTask.put(task0, replicaLogDirInfo(LOGDIR_0, LOGDIR_1));
    logdirInfoByTask.put(task1, replicaLogDirInfo(LOGDIR_0, LOGDIR_1));

    // Cancellation of the movement on broker 0 succeeds, whereas the one on broker 1 fails.
    KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
    failedFuture.completeExceptionally(new KafkaStorageException("Disk failure"));
    Map<TopicPartitionReplica, KafkaFuture<Void>> futureByReplica = new HashMap<>();
    futureByReplica.put(tpr0, KafkaFuture.completedFuture(null));
    futureByReplica.put(tpr1, failedFuture);
    AlterReplicaLogDirsResult mockResult = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    Capture<Map<TopicPartitionReplica, String>> capturedAssignment = Capture.newInstance();
    EasyMock.expect(mockAdminClient.alterReplicaLogDirs(EasyMock.capture(capturedAssignment))).andReturn(mockResult);
    EasyMock.replay(mockAdminClient, mockResult);

    Set<ExecutionTask> cancelledTasks =
        ExecutorAdminUtils.cancelIntraBrokerReplicaMovements(Arrays.asList(task0, task1, task2), logdirInfoByTask, mockAdminClient, CONFIG);

    // Replicas with known logdir are requested to move back to their current logdir to rollback the ongoing movement.
    Map<TopicPartitionReplica, String> expectedAssignment = new HashMap<>();
    expectedAssignment.put(tpr0, LOGDIR_0);
    expectedAssignment.put(tpr1, LOGDIR_0);
    assertEquals(expectedAssignment, capturedAssignment.getValue());
    // Only the task whose cancellation has been accepted is reported as cancelled.
    assertEquals(Collections.singleton(task0), cancelledTasks);
    EasyMock.verify(mockAdminClient, mockResult);
  }

  @Test
  public void testCancelIntraBrokerReplicaMovementsWithUnknownLogdirs() {
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR_0)),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR_1)));
    ExecutionTask task = intraBrokerTask(0, proposal, 0);

    // No request is sent to the cluster if there is no replica with known logdir.
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.replay(mockAdminClient);

    Set<ExecutionTask> cancelledTasks =
        ExecutorAdminUtils.cancelIntraBrokerReplicaMovements(Collections.singletonList(task), Collections.emptyMap(), mockAdminClient, CONFIG);

    assertTrue(cancelledTasks.isEmpty());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testGetLogdirInfoForExecutionTaskSkipsReplicaWithOfflineLogdir() throws Exception {
    // Move the replicas on brokers 0 and 1 from d0 to d1. Broker 1's logdir is offline.
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_0)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_1)));
    ExecutionTask taskWithOnlineLogdir = intraBrokerTask(0, proposal, 0);
    ExecutionTask taskWithOfflineLogdir = intraBrokerTask(1, proposal, 1);
    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TP.topic(), TP.partition(), 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TP.topic(), TP.partition(), 1);

    // A replica on an offline logdir is reported with a SUCCESSFUL future that carries no logdir at all -- not with a
    // failed future -- so the response has to be filtered on its content rather than on the future completing.
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
    futureByReplica.put(tpr0, KafkaFuture.completedFuture(replicaLogDirInfo(LOGDIR_0, LOGDIR_1)));
    futureByReplica.put(tpr1, KafkaFuture.completedFuture(offlineReplicaLogDirInfo()));
    DescribeReplicaLogDirsResult mockResult = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeReplicaLogDirs(EasyMock.anyObject())).andReturn(mockResult);
    EasyMock.replay(mockAdminClient, mockResult);

    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask =
        ExecutorAdminUtils.getLogdirInfoForExecutionTask(Arrays.asList(taskWithOnlineLogdir, taskWithOfflineLogdir),
                                                         mockAdminClient, CONFIG);

    // Only the replica with a known current logdir is reported. Reporting the other one would make
    // ExecutionUtils#isIntraBrokerReplicaActionDone dereference a null logdir, and would hide the task from
    // Executor#maybeMarkTaskAsDead, whose only condition for an intra-broker task is its absence from this map.
    assertEquals(Collections.singleton(taskWithOnlineLogdir), logdirInfoByTask.keySet());
    assertEquals(LOGDIR_0, logdirInfoByTask.get(taskWithOnlineLogdir).getCurrentReplicaLogDir());
    EasyMock.verify(mockAdminClient, mockResult);
  }

  private static ExecutionTask intraBrokerTask(long executionId, ExecutionProposal proposal, int brokerId) {
    ExecutionTask task = new ExecutionTask(executionId, proposal, brokerId, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(0L);
    return task;
  }

  private static ReplicaLogDirInfo offlineReplicaLogDirInfo() throws Exception {
    // What the admin client hands back for a replica whose logdir is offline: every field unset.
    Constructor<ReplicaLogDirInfo> constructor = ReplicaLogDirInfo.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static ReplicaLogDirInfo replicaLogDirInfo(String currentLogdir, String futureLogdir) throws Exception {
    // Reflectively set constructor from package private to public.
    Constructor<ReplicaLogDirInfo> constructor =
        ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(currentLogdir, 0L, futureLogdir, 0L);
  }
}
