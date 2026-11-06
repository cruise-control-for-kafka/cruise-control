/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionRecoveryUtils.RecoveryAction;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionRecoveryUtils.RecoveryPlan;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionState;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedTask;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ExecutionRecoveryUtilsTest {
  private static final List<Node> NODES = Arrays.asList(new Node(0, "h0", 9092), new Node(1, "h1", 9092),
                                                        new Node(2, "h2", 9092), new Node(3, "h3", 9092));
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  private static final String UUID = "uuid";

  // Moves the replica on broker 1 to broker 2 -- i.e. [0, 1] -> [0, 2].
  private static ExecutionProposal interBrokerProposal() {
    return new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0),
                                 Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                 Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
  }

  // Moves the leadership from broker 0 to broker 1 -- i.e. [0, 1] -> [1, 0].
  private static ExecutionProposal leadershipProposal() {
    return new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0),
                                 Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                 Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(0)));
  }

  // Moves the replica on broker 1 from /d1 to /d2.
  private static ExecutionProposal intraBrokerProposal() {
    return new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, "/d1"),
                                 Arrays.asList(new ReplicaPlacementInfo(0, "/d1"), new ReplicaPlacementInfo(1, "/d1")),
                                 Arrays.asList(new ReplicaPlacementInfo(0, "/d1"), new ReplicaPlacementInfo(1, "/d2")));
  }

  private static PersistedExecutionState state(ExecutionProposal proposal,
                                               ExecutorState.State executorState,
                                               PersistedTask... tasks) {
    return new PersistedExecutionState.Builder(UUID, PersistedExecutionState.Operation.EXECUTE_PROPOSALS)
        .executorState(executorState)
        .proposals(Collections.singletonList(proposal))
        .tasks(Arrays.asList(tasks))
        .build();
  }

  private static PersistedTask task(ExecutionTask.TaskType type, ExecutionTaskState state) {
    return new PersistedTask(type, TP, type == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION ? 1 : PersistedTask.NO_BROKER_ID,
                             state, -1L, -1L);
  }

  private static Cluster cluster(Integer... replicas) {
    Node[] replicaNodes = Arrays.stream(replicas).map(NODES::get).toArray(Node[]::new);
    PartitionInfo partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), replicaNodes[0], replicaNodes, replicaNodes);
    return new Cluster("cluster", NODES, Collections.singleton(partitionInfo), Collections.emptySet(), Collections.emptySet());
  }

  private static Cluster emptyCluster() {
    return new Cluster("cluster", NODES, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
  }

  private static Map<TopicPartition, PartitionReassignment> reassigning(TopicPartition tp, List<Integer> targetReplicas,
                                                                        List<Integer> removingReplicas) {
    List<Integer> replicas = new ArrayList<>(targetReplicas);
    removingReplicas.stream().filter(r -> !replicas.contains(r)).forEach(replicas::add);
    List<Integer> adding = new ArrayList<>(targetReplicas);
    adding.removeAll(removingReplicas);
    return Collections.singletonMap(tp, new PartitionReassignment(replicas, adding, removingReplicas));
  }

  private static RecoveryAction interBrokerAction(ExecutionTaskState persistedState,
                                                  Cluster cluster,
                                                  Map<TopicPartition, PartitionReassignment> ongoing) {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, persistedState);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster, ongoing, Collections.emptyMap());
    return plan.actionByTask().get(task);
  }

  @Test
  public void testInterBrokerTaskInProgressAndStillOngoingIsAdopted() {
    // The partition is being reassigned towards the new replicas [0, 2] -- i.e. adding 2, removing 1.
    Map<TopicPartition, PartitionReassignment> ongoing = reassigning(TP, Arrays.asList(0, 2), Collections.singletonList(1));
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 2, 1), ongoing,
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.ADOPT, plan.actionByTask().get(task));
    assertEquals(Set.of(TP), plan.adoptedInterBrokerPartitions());
    assertEquals(Set.of(TP), plan.inFlightInterBrokerPartitions());
    assertTrue(plan.unexpectedOngoingReassignments().isEmpty());
    assertEquals(1, plan.proposalsToResume().size());
    assertNull(plan.reasonToNotResume());
    // A task that was persisted as pending right before being submitted is also adopted.
    assertEquals(RecoveryAction.ADOPT, interBrokerAction(ExecutionTaskState.PENDING, cluster(0, 2, 1), ongoing));
  }

  @Test
  public void testInterBrokerTaskInProgressAndDoneIsCompleted() {
    assertEquals(RecoveryAction.COMPLETED, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(0, 2), Collections.emptyMap()));
    assertEquals(RecoveryAction.COMPLETED, interBrokerAction(ExecutionTaskState.COMPLETED, cluster(0, 2), Collections.emptyMap()));
  }

  @Test
  public void testInterBrokerTaskInProgressButNotStartedIsPending() {
    // E.g. the reassignment was never accepted by Kafka or was rolled back.
    assertEquals(RecoveryAction.PENDING, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(0, 1), Collections.emptyMap()));
    assertEquals(RecoveryAction.PENDING, interBrokerAction(ExecutionTaskState.PENDING, cluster(0, 1), Collections.emptyMap()));
    // Only the replica order remains to be changed.
    assertEquals(RecoveryAction.PENDING, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(2, 0), Collections.emptyMap()));
  }

  @Test
  public void testInterBrokerTaskOfModifiedOrDeletedPartitionIsDropped() {
    assertEquals(RecoveryAction.DROPPED, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(0, 3), Collections.emptyMap()));
    assertEquals(RecoveryAction.DROPPED, interBrokerAction(ExecutionTaskState.PENDING, emptyCluster(), Collections.emptyMap()));
    // Reassigned by someone else towards other replicas.
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 3, 1),
                                                         reassigning(TP, Arrays.asList(0, 3), Collections.singletonList(1)),
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.DROPPED, plan.actionByTask().get(task));
    assertEquals(Set.of(TP), plan.unexpectedOngoingReassignments());
    assertNotNull(plan.reasonToNotResume());
  }

  @Test
  public void testInterBrokerTaskBeingRolledBack() {
    Map<TopicPartition, PartitionReassignment> ongoing = reassigning(TP, Arrays.asList(0, 1), Collections.singletonList(2));
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.DEAD);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 1, 2), ongoing,
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.ROLLBACK_IN_FLIGHT, plan.actionByTask().get(task));
    assertEquals(Set.of(TP), plan.inFlightInterBrokerPartitions());
    assertTrue(plan.unexpectedOngoingReassignments().isEmpty());
    assertNotNull(plan.reasonToNotResume());
    assertEquals(RecoveryAction.FINISHED_WITH_ERROR, interBrokerAction(ExecutionTaskState.ABORTED, cluster(0, 1), Collections.emptyMap()));
  }

  @Test
  public void testUnexpectedOngoingReassignmentPreventsResume() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    TopicPartition otherTp = new TopicPartition("other", 0);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 1),
                                                         reassigning(otherTp, Arrays.asList(0, 1), Collections.emptyList()),
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.PENDING, plan.actionByTask().get(task));
    assertEquals(Set.of(otherTp), plan.unexpectedOngoingReassignments());
    assertNotNull(plan.reasonToNotResume());
  }

  @Test
  public void testStoppingExecutionIsNotResumed() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), ExecutorState.State.STOPPING_EXECUTION, task),
                                                         cluster(0, 1), Collections.emptyMap(), Collections.emptyMap());
    assertEquals(RecoveryAction.PENDING, plan.actionByTask().get(task));
    assertNotNull(plan.reasonToNotResume());
  }

  @Test
  public void testNothingToResume() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 2),
                                                         Collections.emptyMap(), Collections.emptyMap());
    assertTrue(plan.proposalsToResume().isEmpty());
    assertNotNull(plan.reasonToNotResume());
    assertEquals(Integer.valueOf(1), plan.numTasksByAction().get(RecoveryAction.COMPLETED));
  }

  @Test
  public void testLeadershipTask() {
    PersistedTask task = task(ExecutionTask.TaskType.LEADER_ACTION, ExecutionTaskState.IN_PROGRESS);
    PersistedExecutionState state = state(leadershipProposal(), null, task);
    // Leadership moved.
    assertEquals(RecoveryAction.COMPLETED, ExecutionRecoveryUtils.reconcile(state, cluster(1, 0), Collections.emptyMap(),
                                                                            Collections.emptyMap()).actionByTask().get(task));
    // Leadership not moved -- election is retried.
    assertEquals(RecoveryAction.PENDING, ExecutionRecoveryUtils.reconcile(state, cluster(0, 1), Collections.emptyMap(),
                                                                          Collections.emptyMap()).actionByTask().get(task));
    assertEquals(RecoveryAction.DROPPED, ExecutionRecoveryUtils.reconcile(state, emptyCluster(), Collections.emptyMap(),
                                                                          Collections.emptyMap()).actionByTask().get(task));
  }

  private static ReplicaLogDirInfo logDirInfo(String current, String future) {
    ReplicaLogDirInfo info = EasyMock.mock(ReplicaLogDirInfo.class);
    EasyMock.expect(info.getCurrentReplicaLogDir()).andReturn(current).anyTimes();
    EasyMock.expect(info.getFutureReplicaLogDir()).andReturn(future).anyTimes();
    EasyMock.replay(info);
    return info;
  }

  private static RecoveryAction intraBrokerAction(ExecutionTaskState persistedState, ReplicaLogDirInfo info) {
    PersistedTask task = task(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, persistedState);
    Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfo = new HashMap<>();
    if (info != null) {
      logDirInfo.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 1), info);
    }
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(intraBrokerProposal(), null, task), cluster(0, 1), Collections.emptyMap(),
                                                         logDirInfo);
    if (plan.actionByTask().get(task) == RecoveryAction.ADOPT) {
      assertEquals(Set.of(new TopicPartitionReplica(TP.topic(), TP.partition(), 1)), plan.adoptedIntraBrokerReplicas());
    }
    return plan.actionByTask().get(task);
  }

  @Test
  public void testIntraBrokerTask() {
    assertEquals(RecoveryAction.ADOPT, intraBrokerAction(ExecutionTaskState.IN_PROGRESS, logDirInfo("/d1", "/d2")));
    assertEquals(RecoveryAction.COMPLETED, intraBrokerAction(ExecutionTaskState.IN_PROGRESS, logDirInfo("/d2", null)));
    assertEquals(RecoveryAction.PENDING, intraBrokerAction(ExecutionTaskState.IN_PROGRESS, logDirInfo("/d1", null)));
    assertEquals(RecoveryAction.PENDING, intraBrokerAction(ExecutionTaskState.PENDING, null));
    assertEquals(RecoveryAction.FINISHED_WITH_ERROR, intraBrokerAction(ExecutionTaskState.DEAD, logDirInfo("/d1", null)));
  }

  @Test
  public void testReplicaMovementStrategy() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                      PostponeUrpReplicaMovementStrategy.class.getName() + "," + PrioritizeLargeReplicaMovementStrategy.class.getName());
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    assertNull(ExecutionRecoveryUtils.replicaMovementStrategy(null, config));
    assertNull(ExecutionRecoveryUtils.replicaMovementStrategy("UnknownStrategy", config));
    ReplicaMovementStrategy strategy =
        ExecutionRecoveryUtils.replicaMovementStrategy("PostponeUrpReplicaMovementStrategy,PrioritizeLargeReplicaMovementStrategy", config);
    assertEquals("PostponeUrpReplicaMovementStrategy,PrioritizeLargeReplicaMovementStrategy,BaseReplicaMovementStrategy", strategy.name());
    ReplicaMovementStrategy chainedName = ExecutionRecoveryUtils.replicaMovementStrategy(strategy.name(), config);
    assertEquals(strategy.name(), chainedName.name());
  }

  @Test
  public void testResumedReason() {
    String resumedReason = ExecutionRecoveryUtils.resumedReason("rebalance");
    assertEquals(ExecutionRecoveryUtils.RESUMED_REASON_PREFIX + "rebalance", resumedReason);
    assertEquals(resumedReason, ExecutionRecoveryUtils.resumedReason(resumedReason));
  }

  private static List<String> interBrokerDivergences(ExecutionTaskState persistedState,
                                                     Cluster cluster,
                                                     Map<TopicPartition, PartitionReassignment> ongoing) {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, persistedState);
    return ExecutionRecoveryUtils.taskDivergences(state(interBrokerProposal(), null, task), cluster, ongoing, Collections.emptyMap());
  }

  @Test
  public void testInterBrokerTaskDivergences() {
    Map<TopicPartition, PartitionReassignment> none = Collections.emptyMap();
    // As the execution left them.
    assertTrue(interBrokerDivergences(ExecutionTaskState.PENDING, cluster(0, 1), none).isEmpty());
    assertTrue(interBrokerDivergences(ExecutionTaskState.PENDING, cluster(1, 0), none).isEmpty());
    assertTrue(interBrokerDivergences(ExecutionTaskState.IN_PROGRESS, cluster(0, 1), none).isEmpty());
    assertTrue(interBrokerDivergences(ExecutionTaskState.IN_PROGRESS, cluster(0, 2), none).isEmpty());
    assertTrue(interBrokerDivergences(ExecutionTaskState.IN_PROGRESS, cluster(0, 2, 1),
                                      reassigning(TP, Arrays.asList(0, 2), Collections.singletonList(1))).isEmpty());
    assertTrue(interBrokerDivergences(ExecutionTaskState.COMPLETED, cluster(0, 2), none).isEmpty());
    assertTrue(interBrokerDivergences(ExecutionTaskState.DEAD, cluster(0, 1), none).isEmpty());
    // Being rolled back towards the old replicas.
    assertTrue(interBrokerDivergences(ExecutionTaskState.DEAD, cluster(0, 1, 2),
                                      reassigning(TP, Arrays.asList(0, 1), Collections.singletonList(2))).isEmpty());

    // Moved by someone else: a pending task already on its new replicas, a completed task back on its old replicas, a partition
    // on other replicas or being reassigned elsewhere, and a deleted partition.
    assertEquals(1, interBrokerDivergences(ExecutionTaskState.PENDING, cluster(0, 2), none).size());
    assertEquals(1, interBrokerDivergences(ExecutionTaskState.COMPLETED, cluster(0, 1), none).size());
    assertEquals(1, interBrokerDivergences(ExecutionTaskState.IN_PROGRESS, cluster(0, 3), none).size());
    assertEquals(1, interBrokerDivergences(ExecutionTaskState.IN_PROGRESS, cluster(0, 1, 3),
                                           reassigning(TP, Arrays.asList(0, 3), Collections.singletonList(1))).size());
    List<String> divergences = interBrokerDivergences(ExecutionTaskState.PENDING, emptyCluster(), none);
    assertEquals(1, divergences.size());
    assertTrue(divergences.get(0), divergences.get(0).contains("deleted"));
  }

  private static List<String> intraBrokerDivergences(ExecutionTaskState persistedState, String current, String future) {
    PersistedTask task = task(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, persistedState);
    ReplicaLogDirInfo info = logDirInfo(current, future);
    return ExecutionRecoveryUtils.taskDivergences(state(intraBrokerProposal(), null, task), cluster(0, 1), Collections.emptyMap(),
                                                  Map.of(new TopicPartitionReplica(TP.topic(), TP.partition(), 1), info));
  }

  @Test
  public void testIntraBrokerTaskDivergences() {
    assertTrue(intraBrokerDivergences(ExecutionTaskState.PENDING, "/d1", null).isEmpty());
    assertTrue(intraBrokerDivergences(ExecutionTaskState.IN_PROGRESS, "/d1", "/d2").isEmpty());
    assertTrue(intraBrokerDivergences(ExecutionTaskState.IN_PROGRESS, "/d1", null).isEmpty());
    assertTrue(intraBrokerDivergences(ExecutionTaskState.IN_PROGRESS, "/d2", null).isEmpty());
    assertTrue(intraBrokerDivergences(ExecutionTaskState.COMPLETED, "/d2", null).isEmpty());

    assertEquals(1, intraBrokerDivergences(ExecutionTaskState.PENDING, "/d2", null).size());
    assertEquals(1, intraBrokerDivergences(ExecutionTaskState.PENDING, "/d1", "/d3").size());
    assertEquals(1, intraBrokerDivergences(ExecutionTaskState.COMPLETED, "/d1", null).size());
    assertEquals(1, intraBrokerDivergences(ExecutionTaskState.IN_PROGRESS, "/d3", null).size());
    // A replica on an offline logdir, and a replica whose logdir is unknown.
    assertEquals(1, intraBrokerDivergences(ExecutionTaskState.PENDING, null, null).size());
    PersistedTask task = task(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    assertEquals(1, ExecutionRecoveryUtils.taskDivergences(state(intraBrokerProposal(), null, task), cluster(0, 1), Collections.emptyMap(),
                                                           Collections.emptyMap()).size());
  }

  @Test
  public void testLeadershipTaskOnlyDivergesIfThePartitionIsDeleted() {
    PersistedTask task = task(ExecutionTask.TaskType.LEADER_ACTION, ExecutionTaskState.PENDING);
    PersistedExecutionState state = state(leadershipProposal(), null, task);
    // Leaders also move by themselves.
    assertTrue(ExecutionRecoveryUtils.taskDivergences(state, cluster(0, 1), Collections.emptyMap(), Collections.emptyMap()).isEmpty());
    assertTrue(ExecutionRecoveryUtils.taskDivergences(state, cluster(1, 0), Collections.emptyMap(), Collections.emptyMap()).isEmpty());
    assertEquals(1, ExecutionRecoveryUtils.taskDivergences(state, emptyCluster(), Collections.emptyMap(), Collections.emptyMap()).size());
  }

  @Test
  public void testDeadDestinationBrokers() {
    PersistedExecutionState state = state(interBrokerProposal(), null,
                                          task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING));
    assertTrue(ExecutionRecoveryUtils.deadDestinationBrokers(state, cluster(0, 1)).isEmpty());
    // Broker 1 is only a source of the execution, so it may be dead -- e.g. it is being removed.
    List<Node> withoutBroker1 = Arrays.asList(NODES.get(0), NODES.get(2), NODES.get(3));
    assertTrue(ExecutionRecoveryUtils.deadDestinationBrokers(
        state, new Cluster("cluster", withoutBroker1, Collections.emptySet(), Collections.emptySet(), Collections.emptySet())).isEmpty());
    // Broker 2 is a destination.
    List<Node> withoutBroker2 = Arrays.asList(NODES.get(0), NODES.get(1), NODES.get(3));
    List<String> divergences = ExecutionRecoveryUtils.deadDestinationBrokers(
        state, new Cluster("cluster", withoutBroker2, Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));
    assertEquals(List.of("broker 2, a destination of the execution, is not alive"), divergences);
  }

  private static Config rates(String leaderRate, String followerRate) {
    List<ConfigEntry> entries = new ArrayList<>();
    if (leaderRate != null) {
      entries.add(new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, leaderRate,
                                  ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, false, false, Collections.emptyList(), null, null));
    }
    if (followerRate != null) {
      entries.add(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, followerRate,
                                  ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, false, false, Collections.emptyList(), null, null));
    }
    return new Config(entries);
  }

  @Test
  public void testThrottleDivergences() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    PersistedExecutionState state = new PersistedExecutionState.Builder(UUID, PersistedExecutionState.Operation.EXECUTE_PROPOSALS)
        .replicationThrottle(100L).proposals(Collections.singletonList(interBrokerProposal())).tasks(List.of(task)).build();
    Map<TopicPartition, PartitionReassignment> ongoing = reassigning(TP, Arrays.asList(0, 2), Collections.singletonList(1));
    RecoveryPlan adopting = ExecutionRecoveryUtils.reconcile(state, cluster(0, 2, 1), ongoing, Collections.emptyMap());
    // The brokers of an adopted movement must keep the throttle of the execution.
    assertTrue(ExecutionRecoveryUtils.throttleDivergences(state, adopting, Map.of(0, rates("100", "100"), 1, rates("100", "100"),
                                                                                  2, rates("100", "100"))).isEmpty());
    // E.g. someone paused the movement, or removed its throttle.
    assertEquals(1, ExecutionRecoveryUtils.throttleDivergences(state, adopting, Map.of(0, rates("100", "100"), 1, rates("100", "100"),
                                                                                       2, rates("100", "0"))).size());
    assertEquals(2, ExecutionRecoveryUtils.throttleDivergences(state, adopting, Map.of(0, rates("100", "100"), 1, rates("100", "100"),
                                                                                       2, rates(null, null))).size());
    // The brokers of a pending movement may have no rate yet, but not a rate set by someone else.
    RecoveryPlan pending = ExecutionRecoveryUtils.reconcile(state, cluster(0, 1), Collections.emptyMap(), Collections.emptyMap());
    assertTrue(ExecutionRecoveryUtils.throttleDivergences(state, pending, Map.of(0, rates(null, null), 2, rates(null, null))).isEmpty());
    assertEquals(1, ExecutionRecoveryUtils.throttleDivergences(state, pending, Map.of(0, rates(null, null), 2, rates("200", null))).size());
    // An unthrottled execution leaves the rates alone.
    PersistedExecutionState unthrottled = state.toBuilder().replicationThrottle(null).build();
    assertTrue(ExecutionRecoveryUtils.throttleDivergences(unthrottled, pending, Map.of(2, rates("200", "200"))).isEmpty());
  }

  @Test
  public void testFreeSpaceShortfalls() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    // The pending movement adds a 10 MB replica to broker 2.
    PersistedExecutionState state = state(interBrokerProposal(), null, task);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state, cluster(0, 1), Collections.emptyMap(), Collections.emptyMap());
    long mb = 1024L * 1024L;
    Map<Integer, Map<String, LogDirDescription>> enough =
        Map.of(2, Map.of("/d1", new LogDirDescription(null, Collections.emptyMap(), 100 * mb, 6 * mb),
                         "/d2", new LogDirDescription(null, Collections.emptyMap(), 100 * mb, 6 * mb)));
    assertTrue(ExecutionRecoveryUtils.freeSpaceShortfalls(state, plan, enough).isEmpty());
    Map<Integer, Map<String, LogDirDescription>> notEnough =
        Map.of(2, Map.of("/d1", new LogDirDescription(null, Collections.emptyMap(), 100 * mb, 5 * mb),
                         "/d2", new LogDirDescription(null, Collections.emptyMap(), 100 * mb, 5 * mb)));
    List<String> shortfalls = ExecutionRecoveryUtils.freeSpaceShortfalls(state, plan, notEnough);
    assertEquals(1, shortfalls.size());
    assertTrue(shortfalls.get(0), shortfalls.get(0).startsWith("broker 2:"));
    // Unknown usable space is not checked.
    assertTrue(ExecutionRecoveryUtils.freeSpaceShortfalls(state, plan, Collections.emptyMap()).isEmpty());
  }

  @Test
  public void testForeignIntraBrokerReplicaMovements() {
    TopicPartition other = new TopicPartition("other", 0);
    Map<Integer, Map<String, LogDirDescription>> logDirs =
        Map.of(1, Map.of("/d2", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(1L, 0L, true),
                                                                   other, new ReplicaInfo(1L, 0L, true)))));
    Set<TopicPartitionReplica> own = Set.of(new TopicPartitionReplica(TP.topic(), TP.partition(), 1));
    assertEquals(List.of("other-0 on broker 1 is being moved to /d2 by someone else"),
                 ExecutionRecoveryUtils.foreignIntraBrokerReplicaMovements(logDirs, own));
  }

  @Test
  public void testLogDirFingerprint() {
    TopicPartition other = new TopicPartition("other", 0);
    Map<Integer, Map<String, LogDirDescription>> before =
        Map.of(1, Map.of("/d1", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(1L, 0L, false),
                                                                   other, new ReplicaInfo(1L, 0L, false))),
                         "/d2", new LogDirDescription(null, Collections.emptyMap())));
    // The replica of the execution moved, the other did not.
    Map<Integer, Map<String, LogDirDescription>> executionMoved =
        Map.of(1, Map.of("/d1", new LogDirDescription(null, Map.of(other, new ReplicaInfo(1L, 0L, false))),
                         "/d2", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(1L, 0L, false)))));
    // The other replica moved.
    Map<Integer, Map<String, LogDirDescription>> otherMoved =
        Map.of(1, Map.of("/d1", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(1L, 0L, false))),
                         "/d2", new LogDirDescription(null, Map.of(other, new ReplicaInfo(1L, 0L, false)))));
    Map<Integer, Long> fingerprint = ExecutionRecoveryUtils.logDirFingerprintByBroker(before, Set.of(1), Set.of(TP));
    assertTrue(ExecutionRecoveryUtils.logDirFingerprintDivergences(
        fingerprint, ExecutionRecoveryUtils.logDirFingerprintByBroker(executionMoved, Set.of(1), Set.of(TP))).isEmpty());
    assertEquals(1, ExecutionRecoveryUtils.logDirFingerprintDivergences(
        fingerprint, ExecutionRecoveryUtils.logDirFingerprintByBroker(otherMoved, Set.of(1), Set.of(TP))).size());
  }

  @Test
  public void testIsFinishedAndCleanupReason() {
    PersistedTask completed = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.COMPLETED);
    assertTrue(ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, completed), cluster(0, 2), Collections.emptyMap(),
                                                Collections.emptyMap()).isFinished());
    PersistedTask pending = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    assertFalse(ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, pending), cluster(0, 1), Collections.emptyMap(),
                                                 Collections.emptyMap()).isFinished());
    assertEquals(ExecutionRecoveryUtils.CLEANUP_REASON_PREFIX + "r",
                 ExecutionRecoveryUtils.cleanupReason(ExecutionRecoveryUtils.resumedReason("r")));
  }
}
