/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.strategy.StrategyOptions;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ExecutionTaskManagerTest {
  private static final Map<ConcurrencyType, Integer> MOCK_DEFAULT_CONCURRENCY;
  static {
    MOCK_DEFAULT_CONCURRENCY =
        Map.of(ConcurrencyType.INTER_BROKER_REPLICA, 4, ConcurrencyType.LEADERSHIP_CLUSTER, 500,
            ConcurrencyType.LEADERSHIP_BROKER, 100, ConcurrencyType.INTRA_BROKER_REPLICA, 2);
  }
  private static ExecutionTaskManager taskManager;

  private Cluster generateExpectedCluster(ExecutionProposal proposal) {
    List<Node> expectedReplicas = new ArrayList<>(proposal.oldReplicas().size());
    expectedReplicas.add(new Node(0, "null", -1));
    expectedReplicas.add(new Node(2, "null", -1));

    Node[] isrArray = new Node[expectedReplicas.size()];
    isrArray = expectedReplicas.toArray(isrArray);

    TopicPartition tp = proposal.topicPartition();
    Set<PartitionInfo> partition = Collections.singleton(new PartitionInfo(tp.topic(), tp.partition(), expectedReplicas.get(1), isrArray, isrArray));

    return new Cluster(null, expectedReplicas, partition, Collections.emptySet(), Collections.emptySet());
  }

  /**
   * Setup the test.
   */
  @BeforeClass
  public static void setup() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.INTER_BROKER_REPLICA)));
    properties.put(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.LEADERSHIP_CLUSTER)));
    properties.put(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.LEADERSHIP_BROKER)));
    properties.put(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.INTRA_BROKER_REPLICA)));
    taskManager = new ExecutionTaskManager(null, new MetricRegistry(), Time.SYSTEM,
                                           new KafkaCruiseControlConfig(properties));
  }

  @Test
  public void testStateChangeSequences() {
    TopicPartition tp = new TopicPartition("topic", 0);

    List<List<ExecutionTaskState>> testSequences = new ArrayList<>();
    // Completed successfully.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.COMPLETED));
    // Rollback succeeded.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.ABORTING, ExecutionTaskState.ABORTED));
    // Rollback failed.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.ABORTING, ExecutionTaskState.DEAD));
    // Cannot rollback.
    testSequences.add(Arrays.asList(ExecutionTaskState.IN_PROGRESS, ExecutionTaskState.DEAD));

    ReplicaPlacementInfo r0 = new ReplicaPlacementInfo(0);
    ReplicaPlacementInfo r1 = new ReplicaPlacementInfo(1);
    ReplicaPlacementInfo r2 = new ReplicaPlacementInfo(2);
    // Make sure the proposal does not involve leader movement.
    ExecutionProposal proposal = new ExecutionProposal(tp, 10, r2, Arrays.asList(r0, r2), Arrays.asList(r2, r1));
    StrategyOptions strategyOptions = new StrategyOptions.Builder(generateExpectedCluster(proposal)).build();

    for (List<ExecutionTaskState> sequence : testSequences) {
      taskManager.clear();
      taskManager.setExecutionModeForTaskTracker(false);
      taskManager.addExecutionProposals(Collections.singletonList(proposal),
                                        Collections.emptySet(),
                                        strategyOptions,
                                        null);
      taskManager.getExecutionConcurrencyManager().setExecutionConcurrencyForAllBrokersOrCluster(null, ConcurrencyType.INTRA_BROKER_REPLICA);
      taskManager.getExecutionConcurrencyManager().setExecutionConcurrencyForAllBrokersOrCluster(null, ConcurrencyType.INTER_BROKER_REPLICA);
      taskManager.getExecutionConcurrencyManager().setExecutionConcurrencyForAllBrokersOrCluster(null, ConcurrencyType.LEADERSHIP_CLUSTER);
      taskManager.getExecutionConcurrencyManager().setExecutionConcurrencyForAllBrokersOrCluster(null, ConcurrencyType.LEADERSHIP_BROKER);
      List<ExecutionTask> tasks = taskManager.getInterBrokerReplicaMovementTasks();
      assertEquals(1, tasks.size());
      ExecutionTask task = tasks.get(0);
      verifyStateChangeSequence(sequence, task, taskManager);
    }

    // Verify that the movement concurrency matches the default configuration
    for (ConcurrencyType concurrencyType : ConcurrencyType.cachedValues()) {
      if (concurrencyType == ConcurrencyType.LEADERSHIP_CLUSTER) {
        continue;
      }
      assertEquals(MOCK_DEFAULT_CONCURRENCY.get(concurrencyType).intValue(),
                   taskManager.getExecutionConcurrencyManager().getExecutionBrokerConcurrency(0, concurrencyType));
      assertEquals(MOCK_DEFAULT_CONCURRENCY.get(concurrencyType).intValue(),
                   taskManager.getExecutionConcurrencyManager().getExecutionBrokerConcurrency(1, concurrencyType));
      assertEquals(MOCK_DEFAULT_CONCURRENCY.get(concurrencyType).intValue(),
                   taskManager.getExecutionConcurrencyManager().getExecutionBrokerConcurrency(2, concurrencyType));
    }
    assertEquals(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.LEADERSHIP_CLUSTER).intValue(),
        taskManager.getExecutionConcurrencyManager().getExecutionClusterLeadershipConcurrency());
  }

  @Test
  public void testGetIntraBrokerReplicaMovementTasksInParallel() throws Exception {
    ExecutionTaskManager intraBrokerTaskManager = intraBrokerTaskManagerWithTwoMovementsOnBrokers0And1();
    // All brokers execute intra-broker replica movements in parallel.
    List<ExecutionTask> tasks = intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks();
    assertEquals(Set.of(0, 1), brokerIds(tasks));
    assertEquals(4, tasks.size());
  }

  @Test
  public void testGetIntraBrokerReplicaMovementTasksBrokerByBroker() throws Exception {
    ExecutionTaskManager intraBrokerTaskManager = intraBrokerTaskManagerWithTwoMovementsOnBrokers0And1();
    intraBrokerTaskManager.setIntraBrokerMovementsBrokerByBroker(true);

    // Only broker 0 executes intra-broker replica movements -- up to its concurrency limit.
    List<ExecutionTask> broker0Tasks = intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks();
    assertEquals(Collections.singleton(0), brokerIds(broker0Tasks));
    assertEquals(2, broker0Tasks.size());
    intraBrokerTaskManager.markTasksInProgress(broker0Tasks);

    // Broker 1 does not start while broker 0 has in-progress intra-broker replica movements.
    assertTrue(intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks().isEmpty());
    intraBrokerTaskManager.markTaskDone(broker0Tasks.get(0));
    assertTrue(intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks().isEmpty());
    intraBrokerTaskManager.markTaskDone(broker0Tasks.get(1));

    // Broker 1 starts once all intra-broker replica movements of broker 0 are finished.
    List<ExecutionTask> broker1Tasks = intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks();
    assertEquals(Collections.singleton(1), brokerIds(broker1Tasks));
    assertEquals(2, broker1Tasks.size());
    intraBrokerTaskManager.markTasksInProgress(broker1Tasks);
    broker1Tasks.forEach(intraBrokerTaskManager::markTaskDone);
    assertTrue(intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks().isEmpty());
    assertEquals(0, intraBrokerTaskManager.numRemainingIntraBrokerPartitionMovements());

    // Clearing the task manager resets the broker by broker execution.
    intraBrokerTaskManager.clear();
    addIntraBrokerProposalsOnBrokers0And1(intraBrokerTaskManager);
    assertEquals(Set.of(0, 1), brokerIds(intraBrokerTaskManager.getIntraBrokerReplicaMovementTasks()));
  }

  private static Set<Integer> brokerIds(List<ExecutionTask> tasks) {
    return tasks.stream().map(ExecutionTask::brokerId).collect(Collectors.toSet());
  }

  /**
   * @return An execution task manager with two intra-broker replica movements on each of brokers 0 and 1, where replicas
   * currently reside on logdir "d0" and are to be moved to logdir "d1".
   */
  private ExecutionTaskManager intraBrokerTaskManagerWithTwoMovementsOnBrokers0And1() throws Exception {
    List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic", 0), new TopicPartition("topic", 1));
    // Mock admin client to report that all replicas currently reside on logdir "d0".
    Constructor<DescribeReplicaLogDirsResult> resultConstructor = DescribeReplicaLogDirsResult.class.getDeclaredConstructor(Map.class);
    resultConstructor.setAccessible(true);
    Constructor<ReplicaLogDirInfo> infoConstructor =
        ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
    infoConstructor.setAccessible(true);
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
    for (TopicPartition tp : partitions) {
      for (int brokerId : Arrays.asList(0, 1)) {
        futureByReplica.put(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId),
                            KafkaFuture.completedFuture(infoConstructor.newInstance("d0", 0L, null, -1L)));
      }
    }
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeReplicaLogDirs(EasyMock.anyObject()))
            .andReturn(resultConstructor.newInstance(futureByReplica)).anyTimes();
    EasyMock.replay(mockAdminClient);

    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG,
                   Integer.toString(MOCK_DEFAULT_CONCURRENCY.get(ConcurrencyType.INTRA_BROKER_REPLICA)));
    ExecutionTaskManager intraBrokerTaskManager = new ExecutionTaskManager(mockAdminClient, new MetricRegistry(), Time.SYSTEM,
                                                                           new KafkaCruiseControlConfig(properties));
    addIntraBrokerProposalsOnBrokers0And1(intraBrokerTaskManager);
    return intraBrokerTaskManager;
  }

  private static void addIntraBrokerProposalsOnBrokers0And1(ExecutionTaskManager intraBrokerTaskManager) {
    ReplicaPlacementInfo r0d0 = new ReplicaPlacementInfo(0, "d0");
    ReplicaPlacementInfo r0d1 = new ReplicaPlacementInfo(0, "d1");
    ReplicaPlacementInfo r1d0 = new ReplicaPlacementInfo(1, "d0");
    ReplicaPlacementInfo r1d1 = new ReplicaPlacementInfo(1, "d1");
    List<ExecutionProposal> proposals = new ArrayList<>();
    for (int partition = 0; partition < 2; partition++) {
      proposals.add(new ExecutionProposal(new TopicPartition("topic", partition), 10, r0d0,
                                          Arrays.asList(r0d0, r1d0), Arrays.asList(r0d1, r1d1)));
    }
    Cluster emptyCluster = new Cluster(null, Collections.emptyList(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    intraBrokerTaskManager.setExecutionModeForTaskTracker(false);
    intraBrokerTaskManager.addExecutionProposals(proposals, Collections.emptySet(), new StrategyOptions.Builder(emptyCluster).build(), null);
    intraBrokerTaskManager.getExecutionConcurrencyManager().initialize(Set.of(0, 1), null, null, null, null);
  }

  private void verifyStateChangeSequence(List<ExecutionTaskState> stateSequence,
                                         ExecutionTask task,
                                         ExecutionTaskManager taskManager) {
    stateSequence.forEach(s -> changeTaskState(s, task, taskManager));
  }

  private void changeTaskState(ExecutionTaskState state, ExecutionTask task, ExecutionTaskManager taskManager) {
    ExecutionTaskTracker.ExecutionTasksSummary executionTasksSummary;
    Map<ExecutionTaskState, Integer> taskStat;
    switch (state) {
      case IN_PROGRESS:
        taskManager.markTasksInProgress(Collections.singletonList(task));
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(1, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(10, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(0, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case ABORTING:
        taskManager.markTaskAborting(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(1, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(10, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(0, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case DEAD:
        taskManager.markTaskDead(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(1, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(0, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(10, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      case ABORTED:
      case COMPLETED:
        ExecutionTaskState origState = task.state();
        taskManager.markTaskDone(task);
        executionTasksSummary = taskManager.getExecutionTasksSummary(Collections.emptySet());
        taskStat = executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.PENDING));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.IN_PROGRESS));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.ABORTING));
        assertEquals(origState == ExecutionTaskState.ABORTING ? 1 : 0, (int) taskStat.get(ExecutionTaskState.ABORTED));
        assertEquals(origState == ExecutionTaskState.ABORTING ? 0 : 1, (int) taskStat.get(ExecutionTaskState.COMPLETED));
        assertEquals(0, (int) taskStat.get(ExecutionTaskState.DEAD));
        assertEquals(0, executionTasksSummary.remainingInterBrokerDataToMoveInMB());
        assertEquals(0, executionTasksSummary.inExecutionInterBrokerDataMovementInMB());
        assertEquals(10, executionTasksSummary.finishedInterBrokerDataMovementInMB());
        break;
      default:
        throw new IllegalArgumentException("Invalid state " + state);
    }
    assertEquals(state, task.state());
  }
}
