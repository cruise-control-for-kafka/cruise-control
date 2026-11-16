/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionState;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedTask;
import com.linkedin.kafka.cruisecontrol.executor.persistence.ReplicaPlacementFingerprint;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;


/**
 * Utilities to recover an execution that was interrupted by a Cruise Control restart.
 *
 * The persisted execution state tells what Cruise Control <i>was doing</i> when it stopped, and the Kafka cluster tells
 * what is <i>actually happening now</i>. {@link #reconcile(PersistedExecutionState, Cluster, Map, Map)} combines both to
 * decide what to do with each persisted task (see {@link RecoveryAction}).
 */
final class ExecutionRecoveryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionRecoveryUtils.class);
  static final String RESUMED_REASON_PREFIX = "Resumed after Cruise Control restart: ";
  static final String CLEANUP_REASON_PREFIX = "Cleaning up after Cruise Control restart: ";
  // The usable space of a destination must exceed the data to move to it by this factor for the execution to be resumed.
  static final double FREE_SPACE_MARGIN = 1.1;
  private static final long BYTES_IN_MB = 1024L * 1024L;

  private ExecutionRecoveryUtils() {
  }

  /**
   * What to do with a persisted task upon recovery.
   */
  enum RecoveryAction {
    /** The task was already done before the restart, or got done in the meantime -- nothing to do. */
    COMPLETED,
    /** The task was submitted to Kafka before the restart and Kafka is still executing it -- only monitor it, do not resubmit. */
    ADOPT,
    /** The task has not been started (or its effect is not visible in the cluster) -- execute it if the execution is resumed. */
    PENDING,
    /** The task was being rolled back before the restart and Kafka is still executing the rollback -- wait for it. */
    ROLLBACK_IN_FLIGHT,
    /** The task was aborted or dead before the restart -- it will not be retried. */
    FINISHED_WITH_ERROR,
    /** The partition was deleted, or modified by someone else since the task was generated -- the task is dropped. */
    DROPPED
  }

  /**
   * The result of reconciling a persisted execution state with the current state of the cluster.
   */
  static final class RecoveryPlan {
    private final PersistedExecutionState _state;
    private final Map<PersistedTask, RecoveryAction> _actionByTask;
    private final Set<TopicPartition> _unexpectedOngoingReassignments;

    RecoveryPlan(PersistedExecutionState state,
                 Map<PersistedTask, RecoveryAction> actionByTask,
                 Set<TopicPartition> unexpectedOngoingReassignments) {
      _state = state;
      _actionByTask = Collections.unmodifiableMap(actionByTask);
      _unexpectedOngoingReassignments = Collections.unmodifiableSet(unexpectedOngoingReassignments);
    }

    PersistedExecutionState state() {
      return _state;
    }

    Map<PersistedTask, RecoveryAction> actionByTask() {
      return _actionByTask;
    }

    /**
     * @return Ongoing partition reassignments in the cluster that are not part of the persisted execution.
     */
    Set<TopicPartition> unexpectedOngoingReassignments() {
      return _unexpectedOngoingReassignments;
    }

    private Set<PersistedTask> tasksWith(ExecutionTask.TaskType type, RecoveryAction action) {
      return _actionByTask.entrySet().stream()
                          .filter(e -> e.getKey().type() == type && e.getValue() == action)
                          .map(Map.Entry::getKey)
                          .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return Partitions whose inter-broker replica reassignment was submitted before the restart and is still ongoing.
     */
    Set<TopicPartition> adoptedInterBrokerPartitions() {
      return tasksWith(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, RecoveryAction.ADOPT)
          .stream().map(PersistedTask::topicPartition).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return Replicas whose intra-broker movement was submitted before the restart and is still ongoing.
     */
    Set<TopicPartitionReplica> adoptedIntraBrokerReplicas() {
      return tasksWith(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, RecoveryAction.ADOPT)
          .stream().map(t -> new TopicPartitionReplica(t.topicPartition().topic(), t.topicPartition().partition(), t.brokerId()))
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return Partitions of the persisted execution with an ongoing partition reassignment (i.e. adopted or being rolled back).
     */
    Set<TopicPartition> inFlightInterBrokerPartitions() {
      Set<TopicPartition> inFlight = adoptedInterBrokerPartitions();
      tasksWith(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, RecoveryAction.ROLLBACK_IN_FLIGHT)
          .forEach(t -> inFlight.add(t.topicPartition()));
      return inFlight;
    }

    /**
     * @return Proposals with at least one task to be executed or monitored if the execution is resumed.
     */
    List<ExecutionProposal> proposalsToResume() {
      Set<TopicPartition> partitions = new LinkedHashSet<>();
      _actionByTask.forEach((task, action) -> {
        if (action == RecoveryAction.PENDING || action == RecoveryAction.ADOPT) {
          partitions.add(task.topicPartition());
        }
      });
      return partitions.stream().map(tp -> _state.proposals().get(tp)).collect(Collectors.toList());
    }

    /**
     * @return {@code null} if the execution can be resumed, or the reason why it cannot be resumed otherwise.
     */
    String reasonToNotResume() {
      if (_state.executorState() == ExecutorState.State.STOPPING_EXECUTION) {
        return "the execution was being stopped when Cruise Control stopped";
      }
      if (!_unexpectedOngoingReassignments.isEmpty()) {
        return "there are ongoing partition reassignments that are not part of the execution: " + _unexpectedOngoingReassignments;
      }
      if (_actionByTask.containsValue(RecoveryAction.ROLLBACK_IN_FLIGHT)) {
        return "there are ongoing rollbacks of tasks of the execution";
      }
      if (proposalsToResume().isEmpty()) {
        return "there are no remaining tasks to execute";
      }
      return null;
    }

    /**
     * @return {@code true} if nothing is left of the execution -- i.e. every task is completed (or finished with an error) and no
     * reassignment of the execution is in flight, {@code false} otherwise.
     */
    boolean isFinished() {
      return _actionByTask.values().stream().allMatch(a -> a == RecoveryAction.COMPLETED || a == RecoveryAction.FINISHED_WITH_ERROR);
    }

    /**
     * @return Number of persisted tasks by their recovery action.
     */
    Map<RecoveryAction, Integer> numTasksByAction() {
      Map<RecoveryAction, Integer> numTasksByAction = new EnumMap<>(RecoveryAction.class);
      for (RecoveryAction action : RecoveryAction.values()) {
        numTasksByAction.put(action, 0);
      }
      _actionByTask.values().forEach(action -> numTasksByAction.merge(action, 1, Integer::sum));
      return numTasksByAction;
    }
  }

  /**
   * Decide what to do with each task of the given persisted execution state based on the current state of the cluster.
   *
   * <ul>
   *   <li>Inter-broker replica tasks: A task whose partition has an ongoing reassignment towards the new replicas of the
   *   proposal is {@link RecoveryAction#ADOPT adopted}; a task whose partition has the new replicas (in order) is
   *   {@link RecoveryAction#COMPLETED}; a task whose partition still has the old replicas (or the new replicas in a
   *   different order) is {@link RecoveryAction#PENDING}.</li>
   *   <li>Intra-broker replica tasks: A task whose replica has a future replica on the target logdir is adopted; a task whose
   *   replica is on the target logdir is completed; otherwise, it is pending.</li>
   *   <li>Leadership tasks: A task whose partition has the new leader is completed; otherwise, it is pending.</li>
   * </ul>
   * Any other case indicates that the partition was modified by someone else since the task was generated, so the task
   * is {@link RecoveryAction#DROPPED}. Tasks that were aborted or dead before the restart are never retried.
   *
   * @param state Persisted execution state.
   * @param cluster Current cluster metadata.
   * @param ongoingReassignments Ongoing partition reassignments in the cluster.
   * @param logDirInfoByReplica Current log directory information of the replicas of intra-broker replica tasks (may be
   *                            partial -- a task with unknown log directory information is considered as pending).
   * @return The recovery plan.
   */
  static RecoveryPlan reconcile(PersistedExecutionState state,
                                Cluster cluster,
                                Map<TopicPartition, PartitionReassignment> ongoingReassignments,
                                Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfoByReplica) {
    Map<PersistedTask, RecoveryAction> actionByTask = new LinkedHashMap<>();
    Set<TopicPartition> expectedOngoingReassignments = new HashSet<>();
    for (PersistedTask task : state.tasks()) {
      ExecutionProposal proposal = state.proposals().get(task.topicPartition());
      RecoveryAction action;
      switch (task.type()) {
        case INTER_BROKER_REPLICA_ACTION:
          action = reconcileInterBrokerReplicaTask(task, proposal, cluster, ongoingReassignments.get(task.topicPartition()));
          if (action == RecoveryAction.ADOPT || action == RecoveryAction.ROLLBACK_IN_FLIGHT) {
            expectedOngoingReassignments.add(task.topicPartition());
          }
          break;
        case INTRA_BROKER_REPLICA_ACTION:
          action = reconcileIntraBrokerReplicaTask(task, proposal, cluster, logDirInfoByReplica);
          break;
        case LEADER_ACTION:
          action = reconcileLeadershipTask(task, proposal, cluster);
          break;
        default:
          throw new IllegalStateException("Unsupported task type " + task.type());
      }
      LOG.debug("Recovery action for persisted task {} is {}.", task, action);
      actionByTask.put(task, action);
    }
    Set<TopicPartition> unexpectedOngoingReassignments = new HashSet<>(ongoingReassignments.keySet());
    unexpectedOngoingReassignments.removeAll(expectedOngoingReassignments);
    return new RecoveryPlan(state, actionByTask, unexpectedOngoingReassignments);
  }

  private static boolean isTerminalWithError(ExecutionTaskState state) {
    return state == ExecutionTaskState.ABORTING || state == ExecutionTaskState.ABORTED || state == ExecutionTaskState.DEAD;
  }

  private static RecoveryAction reconcileInterBrokerReplicaTask(PersistedTask task,
                                                                ExecutionProposal proposal,
                                                                Cluster cluster,
                                                                PartitionReassignment ongoingReassignment) {
    PartitionInfo partitionInfo = cluster.partition(task.topicPartition());
    if (partitionInfo == null) {
      return RecoveryAction.DROPPED;
    }
    if (ongoingReassignment != null) {
      List<Integer> targetReplicas = new ArrayList<>(ongoingReassignment.replicas());
      targetReplicas.removeAll(ongoingReassignment.removingReplicas());
      if (isTerminalWithError(task.state())) {
        // The cancellation (i.e. rollback) of the task is still ongoing.
        return RecoveryAction.ROLLBACK_IN_FLIGHT;
      }
      if (task.state() != ExecutionTaskState.COMPLETED
          && new HashSet<>(targetReplicas).equals(brokerIds(proposal.newReplicas()))) {
        return RecoveryAction.ADOPT;
      }
      // An ongoing reassignment of the partition towards some other replicas -- i.e. not started by this execution.
      return RecoveryAction.DROPPED;
    }
    if (isTerminalWithError(task.state())) {
      return RecoveryAction.FINISHED_WITH_ERROR;
    }
    List<Integer> currentReplicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
    if (currentReplicas.equals(orderedBrokerIds(proposal.newReplicas()))) {
      return RecoveryAction.COMPLETED;
    }
    Set<Integer> currentReplicaSet = new HashSet<>(currentReplicas);
    if (currentReplicaSet.equals(brokerIds(proposal.oldReplicas())) || currentReplicaSet.equals(brokerIds(proposal.newReplicas()))) {
      // Either the reassignment has not started (or was reverted), or only the replica order remains to be changed.
      return RecoveryAction.PENDING;
    }
    return RecoveryAction.DROPPED;
  }

  private static RecoveryAction reconcileIntraBrokerReplicaTask(PersistedTask task,
                                                                ExecutionProposal proposal,
                                                                Cluster cluster,
                                                                Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfoByReplica) {
    if (cluster.partition(task.topicPartition()) == null) {
      return RecoveryAction.DROPPED;
    }
    if (isTerminalWithError(task.state())) {
      return RecoveryAction.FINISHED_WITH_ERROR;
    }
    ReplicaPlacementInfo target = proposal.replicasToMoveBetweenDisksByBroker().get(task.brokerId());
    if (target == null) {
      return RecoveryAction.DROPPED;
    }
    ReplicaLogDirInfo info = logDirInfoByReplica.get(
        new TopicPartitionReplica(task.topicPartition().topic(), task.topicPartition().partition(), task.brokerId()));
    if (info == null || info.getCurrentReplicaLogDir() == null) {
      // Unknown log directory -- let the executor figure it out.
      return task.state() == ExecutionTaskState.COMPLETED ? RecoveryAction.COMPLETED : RecoveryAction.PENDING;
    }
    if (target.logdir().equals(info.getCurrentReplicaLogDir()) && info.getFutureReplicaLogDir() == null) {
      return RecoveryAction.COMPLETED;
    }
    if (target.logdir().equals(info.getFutureReplicaLogDir()) && task.state() != ExecutionTaskState.COMPLETED) {
      return RecoveryAction.ADOPT;
    }
    return RecoveryAction.PENDING;
  }

  private static RecoveryAction reconcileLeadershipTask(PersistedTask task, ExecutionProposal proposal, Cluster cluster) {
    PartitionInfo partitionInfo = cluster.partition(task.topicPartition());
    if (partitionInfo == null) {
      return RecoveryAction.DROPPED;
    }
    if (isTerminalWithError(task.state())) {
      return RecoveryAction.FINISHED_WITH_ERROR;
    }
    Node leader = partitionInfo.leader();
    if (leader != null && leader.id() == proposal.newLeader().brokerId()) {
      return RecoveryAction.COMPLETED;
    }
    // Leader election is idempotent and cheap -- simply retry it.
    return RecoveryAction.PENDING;
  }

  /**
   * Find the differences between the persisted state of the tasks and the current state of the cluster that the execution
   * itself cannot explain -- i.e. changes made by someone else while Cruise Control was not running. For each task, the cluster
   * must be where the execution left it:
   * <ul>
   *   <li>a pending task: on its old replicas (inter-broker), or its old logdir (intra-broker) -- the execution never touched it;</li>
   *   <li>an in-progress task: on its old replicas, on its new replicas, or being reassigned towards its new replicas (inter-broker),
   *   or on its old logdir with or without a future replica on its new logdir, or on its new logdir (intra-broker);</li>
   *   <li>a completed task: on its new replicas or logdir;</li>
   *   <li>an aborted or dead task: on its old or new replicas (or logdir), or being rolled back towards its old replicas.</li>
   * </ul>
   * A deleted partition is a difference too. Leadership tasks are only checked for deleted partitions, because leaders also move
   * by themselves -- e.g. by Kafka's automatic preferred leader election.
   *
   * @param state Persisted execution state.
   * @param cluster Current cluster metadata.
   * @param ongoingReassignments Ongoing partition reassignments in the cluster.
   * @param logDirInfoByReplica Current log directory information of the replicas of intra-broker replica tasks.
   * @return Human-readable differences, or an empty list if there are none.
   */
  static List<String> taskDivergences(PersistedExecutionState state,
                                      Cluster cluster,
                                      Map<TopicPartition, PartitionReassignment> ongoingReassignments,
                                      Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfoByReplica) {
    List<String> divergences = new ArrayList<>();
    for (PersistedTask task : state.tasks()) {
      ExecutionProposal proposal = state.proposals().get(task.topicPartition());
      PartitionInfo partitionInfo = cluster.partition(task.topicPartition());
      String divergence;
      if (partitionInfo == null) {
        divergence = String.format("%s was deleted", task.topicPartition());
      } else {
        switch (task.type()) {
          case INTER_BROKER_REPLICA_ACTION:
            divergence = interBrokerTaskDivergence(task, proposal, partitionInfo, ongoingReassignments.get(task.topicPartition()));
            break;
          case INTRA_BROKER_REPLICA_ACTION:
            divergence = intraBrokerTaskDivergence(task, proposal, logDirInfoByReplica);
            break;
          default:
            divergence = null;
            break;
        }
      }
      if (divergence != null) {
        divergences.add(divergence);
      }
    }
    return divergences;
  }

  private static String interBrokerTaskDivergence(PersistedTask task,
                                                  ExecutionProposal proposal,
                                                  PartitionInfo partitionInfo,
                                                  PartitionReassignment ongoingReassignment) {
    Set<Integer> oldReplicas = brokerIds(proposal.oldReplicas());
    Set<Integer> newReplicas = brokerIds(proposal.newReplicas());
    Set<Integer> currentReplicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toSet());
    Set<Integer> targetReplicas = null;
    if (ongoingReassignment != null) {
      targetReplicas = new HashSet<>(ongoingReassignment.replicas());
      targetReplicas.removeAll(ongoingReassignment.removingReplicas());
    }
    boolean expected;
    switch (task.state()) {
      case PENDING:
        expected = targetReplicas == null && currentReplicas.equals(oldReplicas);
        break;
      case IN_PROGRESS:
        expected = targetReplicas == null ? (currentReplicas.equals(oldReplicas) || currentReplicas.equals(newReplicas))
                                          : targetReplicas.equals(newReplicas);
        break;
      case COMPLETED:
        expected = targetReplicas == null && currentReplicas.equals(newReplicas);
        break;
      default:
        // Aborting, aborted or dead -- possibly being rolled back.
        expected = targetReplicas == null ? (currentReplicas.equals(oldReplicas) || currentReplicas.equals(newReplicas))
                                          : targetReplicas.equals(oldReplicas);
        break;
    }
    if (expected) {
      return null;
    }
    return String.format("%s (%s %s -> %s) is %s", task.topicPartition(), task.state(), new TreeSet<>(oldReplicas),
                         new TreeSet<>(newReplicas), targetReplicas == null ? "on " + new TreeSet<>(currentReplicas)
                                                                            : "being reassigned to " + new TreeSet<>(targetReplicas));
  }

  private static String intraBrokerTaskDivergence(PersistedTask task,
                                                  ExecutionProposal proposal,
                                                  Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfoByReplica) {
    TopicPartitionReplica replica = new TopicPartitionReplica(task.topicPartition().topic(), task.topicPartition().partition(),
                                                              task.brokerId());
    ReplicaPlacementInfo target = proposal.replicasToMoveBetweenDisksByBroker().get(task.brokerId());
    if (target == null) {
      return String.format("%s: the execution has no logdir to move it to", replica);
    }
    String source = proposal.oldReplicas().stream().filter(r -> r.brokerId() == task.brokerId()).map(ReplicaPlacementInfo::logdir)
                            .findFirst().orElse(null);
    ReplicaLogDirInfo info = logDirInfoByReplica.get(replica);
    if (info == null) {
      return String.format("%s: its logdir is unknown", replica);
    }
    String current = info.getCurrentReplicaLogDir();
    String future = info.getFutureReplicaLogDir();
    if (current == null) {
      return String.format("%s is on an offline logdir", replica);
    }
    boolean onSource = source == null ? !current.equals(target.logdir()) : current.equals(source);
    boolean onTarget = current.equals(target.logdir());
    boolean expected;
    switch (task.state()) {
      case PENDING:
        expected = onSource && future == null;
        break;
      case COMPLETED:
        expected = onTarget && future == null;
        break;
      default:
        // In progress, aborting, aborted or dead.
        expected = (onSource && (future == null || future.equals(target.logdir()))) || (onTarget && future == null);
        break;
    }
    if (expected) {
      return null;
    }
    return String.format("%s (%s %s -> %s) is on %s%s", replica, task.state(), source, target.logdir(), current,
                         future == null ? "" : " with a future replica on " + future);
  }

  /**
   * @param state Persisted execution state.
   * @param cluster Current cluster metadata.
   * @return Brokers that the execution moves replicas to, which are not alive -- e.g. failed or removed while Cruise Control was not
   * running. Brokers that the execution only moves replicas away from may be dead -- e.g. a broker being removed or healed.
   */
  static List<String> deadDestinationBrokers(PersistedExecutionState state, Cluster cluster) {
    Set<Integer> aliveBrokers = cluster.nodes().stream().map(Node::id).collect(Collectors.toSet());
    Set<Integer> deadDestinationBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : state.proposals().values()) {
      proposal.newReplicas().forEach(r -> {
        if (!aliveBrokers.contains(r.brokerId())) {
          deadDestinationBrokers.add(r.brokerId());
        }
      });
    }
    return deadDestinationBrokers.stream().map(b -> String.format("broker %d, a destination of the execution, is not alive", b))
                                 .collect(Collectors.toList());
  }

  /**
   * @param logDirsByBroker Current logdirs of brokers.
   * @param ownReplicas Replicas of the intra-broker replica tasks of the execution.
   * @return Ongoing intra-broker replica movements that are not part of the execution.
   */
  static List<String> foreignIntraBrokerReplicaMovements(Map<Integer, Map<String, LogDirDescription>> logDirsByBroker,
                                                         Set<TopicPartitionReplica> ownReplicas) {
    Set<String> movements = new TreeSet<>();
    logDirsByBroker.forEach((broker, logDirs) -> logDirs.forEach((logDir, description) -> description.replicaInfos().forEach((tp, info) -> {
      if (info.isFuture() && !ownReplicas.contains(new TopicPartitionReplica(tp.topic(), tp.partition(), broker))) {
        movements.add(String.format("%s on broker %d is being moved to %s by someone else", tp, broker, logDir));
      }
    })));
    return new ArrayList<>(movements);
  }

  /**
   * @param logDirsByBroker Current logdirs of brokers.
   * @param brokers Brokers to compute the fingerprint of -- i.e. the brokers with intra-broker replica movements.
   * @param excludedPartitions Partitions to exclude -- i.e. the partitions of the execution.
   * @return For each of the given brokers whose logdirs are known, a fingerprint of the logdirs of its replicas, except the
   * excluded ones.
   */
  static Map<Integer, Long> logDirFingerprintByBroker(Map<Integer, Map<String, LogDirDescription>> logDirsByBroker,
                                                     Set<Integer> brokers,
                                                     Set<TopicPartition> excludedPartitions) {
    Map<Integer, Long> fingerprintByBroker = new TreeMap<>();
    for (int broker : brokers) {
      Map<String, LogDirDescription> logDirs = logDirsByBroker.get(broker);
      if (logDirs == null) {
        continue;
      }
      long fingerprint = 0L;
      for (Map.Entry<String, LogDirDescription> logDir : logDirs.entrySet()) {
        long logDirHash = ReplicaPlacementFingerprint.hashString(logDir.getKey());
        for (Map.Entry<TopicPartition, ReplicaInfo> replica : logDir.getValue().replicaInfos().entrySet()) {
          TopicPartition tp = replica.getKey();
          if (!replica.getValue().isFuture() && !excludedPartitions.contains(tp)) {
            fingerprint += ReplicaPlacementFingerprint.mix(logDirHash ^ ReplicaPlacementFingerprint.mix(
                ReplicaPlacementFingerprint.hashString(tp.topic()) ^ ReplicaPlacementFingerprint.mix(tp.partition())));
          }
        }
      }
      fingerprintByBroker.put(broker, fingerprint);
    }
    return fingerprintByBroker;
  }

  /**
   * @param persisted The logdir fingerprints persisted when the execution started.
   * @param current The current logdir fingerprints of the same brokers.
   * @return Brokers whose replicas outside the execution were moved between logdirs (or created or deleted).
   */
  static List<String> logDirFingerprintDivergences(Map<Integer, Long> persisted, Map<Integer, Long> current) {
    List<String> divergences = new ArrayList<>();
    persisted.forEach((broker, fingerprint) -> {
      if (!fingerprint.equals(current.get(broker))) {
        divergences.add(String.format("broker %d: replicas outside the execution were moved between its logdirs, created or deleted",
                                      broker));
      }
    });
    return divergences;
  }

  /**
   * The throttled rates of the brokers of the execution must be as the execution left them: the brokers of an adopted (i.e. still
   * ongoing) movement must have the throttle of the execution, and the other brokers either have it or no rate at all. Anything
   * else was set by someone else while Cruise Control was not running -- e.g. a rate of 0 that pauses a movement on purpose.
   *
   * @param state Persisted execution state.
   * @param plan The recovery plan of the execution.
   * @param brokerConfigs Current configs of the brokers of the execution.
   * @return Throttled rates that differ from what the execution left behind, or an empty list if the execution is not throttled.
   */
  static List<String> throttleDivergences(PersistedExecutionState state, RecoveryPlan plan, Map<Integer, Config> brokerConfigs) {
    List<String> divergences = new ArrayList<>();
    Long throttle = state.replicationThrottle();
    if (throttle == null) {
      return divergences;
    }
    Set<Integer> adoptedBrokers = new TreeSet<>();
    plan.adoptedInterBrokerPartitions().forEach(tp -> adoptedBrokers.addAll(participatingBrokers(state.proposals().get(tp))));
    for (Map.Entry<Integer, Config> entry : new TreeMap<>(brokerConfigs).entrySet()) {
      for (String rateConfig : List.of(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                                       ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
        ConfigEntry rate = entry.getValue().get(rateConfig);
        boolean isSet = rate != null && rate.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
        boolean isOwn = isSet && String.valueOf(throttle).equals(rate.value());
        if (isSet ? !isOwn : adoptedBrokers.contains(entry.getKey())) {
          divergences.add(String.format("broker %d: %s is %s, not the throttle of the execution (%d)", entry.getKey(), rateConfig,
                                        isSet ? rate.value() : "not set", throttle));
        }
      }
    }
    return divergences;
  }

  /**
   * @param proposal An execution proposal.
   * @return The brokers of the old and new replicas of the given proposal.
   */
  static Set<Integer> participatingBrokers(ExecutionProposal proposal) {
    Set<Integer> brokers = brokerIds(proposal.oldReplicas());
    brokers.addAll(brokerIds(proposal.newReplicas()));
    return brokers;
  }

  /**
   * The data that the pending replica movements of the execution would add to each destination broker (or logdir, for intra-broker
   * movements) must fit in its usable space, with a margin of {@value #FREE_SPACE_MARGIN} times the data to move. Brokers (or logdirs)
   * whose usable space is unknown -- e.g. not reported by the broker -- are not checked.
   *
   * @param state Persisted execution state.
   * @param plan The recovery plan of the execution.
   * @param logDirsByBroker Current logdirs of brokers.
   * @return Destinations without enough usable space for the pending movements of the execution.
   */
  static List<String> freeSpaceShortfalls(PersistedExecutionState state, RecoveryPlan plan,
                                          Map<Integer, Map<String, LogDirDescription>> logDirsByBroker) {
    Map<Integer, Long> requiredMbByBroker = new TreeMap<>();
    Map<Integer, Map<String, Long>> requiredMbByLogDir = new TreeMap<>();
    plan.actionByTask().forEach((task, action) -> {
      if (action != RecoveryAction.PENDING) {
        return;
      }
      ExecutionProposal proposal = state.proposals().get(task.topicPartition());
      if (task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION) {
        proposal.replicasToAdd().forEach(r -> requiredMbByBroker.merge(r.brokerId(), proposal.partitionSize(), Long::sum));
      } else if (task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION) {
        ReplicaPlacementInfo target = proposal.replicasToMoveBetweenDisksByBroker().get(task.brokerId());
        if (target != null) {
          requiredMbByLogDir.computeIfAbsent(task.brokerId(), b -> new TreeMap<>()).merge(target.logdir(), proposal.partitionSize(), Long::sum);
        }
      }
    });
    List<String> shortfalls = new ArrayList<>();
    requiredMbByBroker.forEach((broker, requiredMb) -> {
      Map<String, LogDirDescription> logDirs = logDirsByBroker.get(broker);
      if (logDirs == null || logDirs.values().stream().anyMatch(d -> d.error() == null && d.usableBytes().isEmpty())) {
        return;
      }
      long usableBytes = logDirs.values().stream().filter(d -> d.error() == null).mapToLong(d -> d.usableBytes().getAsLong()).sum();
      if (requiredMb * BYTES_IN_MB * FREE_SPACE_MARGIN > usableBytes) {
        shortfalls.add(String.format("broker %d: the pending movements add %d MB, but only %d MB are usable", broker, requiredMb,
                                     usableBytes / BYTES_IN_MB));
      }
    });
    requiredMbByLogDir.forEach((broker, requiredMbByDir) -> requiredMbByDir.forEach((logDir, requiredMb) -> {
      LogDirDescription description = logDirsByBroker.getOrDefault(broker, Collections.emptyMap()).get(logDir);
      if (description == null || description.error() != null || description.usableBytes().isEmpty()) {
        return;
      }
      long usableBytes = description.usableBytes().getAsLong();
      if (requiredMb * BYTES_IN_MB * FREE_SPACE_MARGIN > usableBytes) {
        shortfalls.add(String.format("broker %d logdir %s: the pending movements add %d MB, but only %d MB are usable", broker, logDir,
                                     requiredMb, usableBytes / BYTES_IN_MB));
      }
    }));
    return shortfalls;
  }

  private static Set<Integer> brokerIds(List<ReplicaPlacementInfo> replicas) {
    return replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toCollection(HashSet::new));
  }

  private static List<Integer> orderedBrokerIds(List<ReplicaPlacementInfo> replicas) {
    return replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList());
  }

  /**
   * Rebuild the replica movement strategy with the given name (see {@link ReplicaMovementStrategy#name()}) from the
   * strategies in {@link ExecutorConfig#REPLICA_MOVEMENT_STRATEGIES_CONFIG}.
   *
   * @param name Name of the (possibly chained) replica movement strategy, or {@code null}.
   * @param config Cruise Control config.
   * @return The replica movement strategy, or {@code null} (i.e. use the default strategy) if the given name is {@code null}
   * or refers to an unsupported strategy.
   */
  static ReplicaMovementStrategy replicaMovementStrategy(String name, KafkaCruiseControlConfig config) {
    if (name == null || name.isBlank()) {
      return null;
    }
    Map<String, ReplicaMovementStrategy> supportedStrategiesByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    ReplicaMovementStrategy baseStrategy = new BaseReplicaMovementStrategy();
    supportedStrategiesByName.put(baseStrategy.name(), baseStrategy);
    for (ReplicaMovementStrategy strategy : config.getConfiguredInstances(ExecutorConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                                                          ReplicaMovementStrategy.class)) {
      supportedStrategiesByName.put(strategy.name(), strategy);
    }
    ReplicaMovementStrategy strategy = null;
    for (String strategyName : name.split(",")) {
      ReplicaMovementStrategy supportedStrategy = supportedStrategiesByName.get(strategyName.trim());
      if (supportedStrategy == null) {
        LOG.warn("Replica movement strategy {} of the interrupted execution is not supported (supported: {}). Using the default"
                 + " replica movement strategy instead.", strategyName, supportedStrategiesByName.keySet());
        return null;
      }
      strategy = strategy == null ? supportedStrategy : strategy.chain(supportedStrategy);
    }
    return strategy.chainBaseReplicaMovementStrategyIfAbsent();
  }

  /**
   * @param reason Reason of the interrupted execution.
   * @return Reason of cleaning up the interrupted execution.
   */
  static String cleanupReason(String reason) {
    String originalReason = reason.startsWith(RESUMED_REASON_PREFIX) ? reason.substring(RESUMED_REASON_PREFIX.length()) : reason;
    return CLEANUP_REASON_PREFIX + originalReason;
  }

  /**
   * @param reason Reason of the interrupted execution.
   * @return Reason of the resumed execution.
   */
  static String resumedReason(String reason) {
    return reason.startsWith(RESUMED_REASON_PREFIX) ? reason : RESUMED_REASON_PREFIX + reason;
  }
}
