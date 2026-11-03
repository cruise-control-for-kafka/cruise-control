/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;

/**
 * Limits the number of replica movements that read from or write to each logdir of each broker at the same time.
 * <ul>
 *   <li>An inter-broker replica movement reads from the logdir of the partition's current leader replica, and writes to the
 *   logdir that each replica to add is created in. Kafka would pick that logdir itself, so this class picks one that is under
 *   the limit (preferring one with room for the replica, then the least busy, then the one with the most usable bytes), and the
 *   executor pins it before submitting the reassignment.</li>
 *   <li>An intra-broker replica movement reads from the logdir that the replica is in and writes to its destination logdir.</li>
 * </ul>
 * {@link #refresh(Cluster, Collection, Set)} counts the movements in progress on each logdir from the logdirs that Kafka reports,
 * and {@link #tryReserve(ExecutionTask)} admits a movement about to start only if every logdir that it uses is under the limit.
 * A logdir that cannot be described is not limited. Not thread-safe: used by the execution thread only.
 */
final class LogDirMovementLimit {
  private static final Logger LOG = LoggerFactory.getLogger(LogDirMovementLimit.class);
  private final Admin _adminClient;
  private final long _logdirResponseTimeoutMs;
  private final int _maxMovementsPerLogDir;
  private Cluster _cluster;
  // Online logdirs of each described broker.
  private final Map<Integer, Map<String, LogDirState>> _logDirStateByLogDirByBroker;
  // Logdir of each (non-future) replica on each described broker.
  private final Map<Integer, Map<TopicPartition, String>> _logDirByReplicaByBroker;
  // Size of the largest replica of each partition on the described brokers.
  private final Map<TopicPartition, Long> _replicaBytesByPartition;
  // Logdirs picked for the replicas to add of the movements started by this execution, until the replicas exist.
  private final Map<TopicPartitionReplica, String> _pickedLogDirByReplicaToAdd;
  private final Set<Long> _waitingTaskIds;

  /**
   * @param adminClient The adminClient to send describeLogDirs requests.
   * @param config The config object that holds all the Cruise Control related configs.
   * @param maxMovementsPerLogDir The maximum number of replica movements that may use a logdir at the same time (positive).
   */
  LogDirMovementLimit(Admin adminClient, KafkaCruiseControlConfig config, int maxMovementsPerLogDir) {
    if (maxMovementsPerLogDir <= 0) {
      throw new IllegalArgumentException("The maximum number of replica movements per logdir must be positive.");
    }
    _adminClient = adminClient;
    _logdirResponseTimeoutMs = config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG);
    _maxMovementsPerLogDir = maxMovementsPerLogDir;
    _cluster = null;
    _logDirStateByLogDirByBroker = new HashMap<>();
    _logDirByReplicaByBroker = new HashMap<>();
    _replicaBytesByPartition = new HashMap<>();
    _pickedLogDirByReplicaToAdd = new HashMap<>();
    _waitingTaskIds = new HashSet<>();
  }

  /**
   * @return The maximum number of replica movements that may use a logdir at the same time.
   */
  int maxMovementsPerLogDir() {
    return _maxMovementsPerLogDir;
  }

  /**
   * @param tasks Replica movement tasks.
   * @param cluster The current cluster metadata, for the current leader of each partition.
   * @return The brokers whose logdirs the given replica movement tasks may use.
   */
  static Set<Integer> brokersInvolved(Collection<ExecutionTask> tasks, Cluster cluster) {
    Set<Integer> brokers = new HashSet<>();
    for (ExecutionTask task : tasks) {
      if (task.type() == INTER_BROKER_REPLICA_ACTION) {
        brokers.add(task.proposal().oldLeader().brokerId());
        Node leader = cluster == null ? null : cluster.leaderFor(task.proposal().topicPartition());
        if (leader != null) {
          brokers.add(leader.id());
        }
        task.proposal().replicasToAdd().forEach(r -> brokers.add(r.brokerId()));
      } else if (task.type() == INTRA_BROKER_REPLICA_ACTION) {
        brokers.add(task.brokerId());
      }
    }
    return brokers;
  }

  /**
   * Describe the logdirs of the given brokers and count the movements that the given in-execution tasks have on each of them.
   *
   * @param cluster The current cluster metadata, for the current leader of each partition.
   * @param inExecutionTasks Replica movement tasks in progress or being aborted.
   * @param brokers Brokers whose logdirs the in-execution tasks and the tasks about to start may use.
   */
  void refresh(Cluster cluster, Collection<ExecutionTask> inExecutionTasks, Set<Integer> brokers) {
    _cluster = cluster;
    _logDirStateByLogDirByBroker.clear();
    _logDirByReplicaByBroker.clear();
    _replicaBytesByPartition.clear();
    if (!brokers.isEmpty()) {
      for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry
          : _adminClient.describeLogDirs(brokers).descriptions().entrySet()) {
        int brokerId = entry.getKey();
        Map<String, LogDirDescription> descriptionByLogDir;
        try {
          descriptionByLogDir = entry.getValue().get(_logdirResponseTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          LOG.warn("Failed to describe the logdirs of broker {}, hence its logdirs are not limited until the next check.", brokerId, e);
          continue;
        }
        Map<String, LogDirState> stateByLogDir = new HashMap<>();
        Map<TopicPartition, String> logDirByReplica = new HashMap<>();
        descriptionByLogDir.forEach((logDir, description) -> {
          if (description.error() != null) {
            return;
          }
          int numReplicas = 0;
          for (Map.Entry<TopicPartition, ReplicaInfo> replica : description.replicaInfos().entrySet()) {
            if (!replica.getValue().isFuture()) {
              numReplicas++;
              logDirByReplica.put(replica.getKey(), logDir);
              _replicaBytesByPartition.merge(replica.getKey(), replica.getValue().size(), Math::max);
            }
          }
          stateByLogDir.put(logDir, new LogDirState(description.usableBytes().isPresent() ? description.usableBytes().getAsLong() : null,
                                                    numReplicas));
        });
        _logDirStateByLogDirByBroker.put(brokerId, stateByLogDir);
        _logDirByReplicaByBroker.put(brokerId, logDirByReplica);
      }
    }

    Set<TopicPartitionReplica> replicasToAddInExecution = new HashSet<>();
    for (ExecutionTask task : inExecutionTasks) {
      TopicPartition tp = task.proposal().topicPartition();
      if (task.type() == INTER_BROKER_REPLICA_ACTION) {
        int sourceBroker = sourceBroker(task);
        addMovement(sourceBroker, logDirOf(sourceBroker, tp));
        for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
          TopicPartitionReplica tpr = new TopicPartitionReplica(tp.topic(), tp.partition(), replicaToAdd.brokerId());
          replicasToAddInExecution.add(tpr);
          String logDir = logDirOf(replicaToAdd.brokerId(), tp);
          addMovement(replicaToAdd.brokerId(), logDir != null ? logDir : _pickedLogDirByReplicaToAdd.get(tpr));
        }
      } else if (task.type() == INTRA_BROKER_REPLICA_ACTION) {
        int brokerId = task.brokerId();
        String destinationLogDir = task.proposal().replicasToMoveBetweenDisksByBroker().get(brokerId).logdir();
        String logDir = logDirOf(brokerId, tp);
        if (!destinationLogDir.equals(logDir)) {
          addMovement(brokerId, logDir);
          addMovement(brokerId, destinationLogDir);
        }
      }
    }
    _pickedLogDirByReplicaToAdd.keySet().retainAll(replicasToAddInExecution);
  }

  /**
   * Check whether every logdir that the given replica movement task about to start uses is under the limit. If so, count the
   * movement on these logdirs until the next {@link #refresh(Cluster, Collection, Set)}, and for an inter-broker movement, pick
   * the logdir of each replica to add (see {@link #pickedLogDirs(Collection)}).
   *
   * @param task A replica movement task about to start.
   * @return {@code true} if the task can start, {@code false} if it must wait for a logdir.
   */
  boolean tryReserve(ExecutionTask task) {
    TopicPartition tp = task.proposal().topicPartition();
    Map<Integer, String> sourceLogDirByBroker = new HashMap<>();
    Map<Integer, String> destinationLogDirByBroker = new HashMap<>();
    Map<TopicPartitionReplica, String> pickedLogDirByReplica = new HashMap<>();
    if (task.type() == INTER_BROKER_REPLICA_ACTION) {
      int sourceBroker = sourceBroker(task);
      sourceLogDirByBroker.put(sourceBroker, logDirOf(sourceBroker, tp));
      for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
        int brokerId = replicaToAdd.brokerId();
        String logDir = logDirOf(brokerId, tp);
        if (logDir == null) {
          logDir = pickLogDir(brokerId, _replicaBytesByPartition.getOrDefault(tp, 0L));
          if (logDir == null) {
            if (_logDirStateByLogDirByBroker.containsKey(brokerId)) {
              logWaiting(task, String.format("every logdir of destination broker %d has %d movement(s)", brokerId, _maxMovementsPerLogDir));
              return false;
            }
            // The logdirs of the broker are unknown: Kafka picks the logdir.
            continue;
          }
          pickedLogDirByReplica.put(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId), logDir);
        }
        destinationLogDirByBroker.put(brokerId, logDir);
      }
    } else if (task.type() == INTRA_BROKER_REPLICA_ACTION) {
      int brokerId = task.brokerId();
      sourceLogDirByBroker.put(brokerId, logDirOf(brokerId, tp));
      destinationLogDirByBroker.put(brokerId, task.proposal().replicasToMoveBetweenDisksByBroker().get(brokerId).logdir());
    } else {
      return true;
    }
    if (isAnyAtLimit(task, sourceLogDirByBroker) || isAnyAtLimit(task, destinationLogDirByBroker)) {
      return false;
    }
    sourceLogDirByBroker.forEach(this::addMovement);
    destinationLogDirByBroker.forEach(this::addMovement);
    _pickedLogDirByReplicaToAdd.putAll(pickedLogDirByReplica);
    _waitingTaskIds.remove(task.executionId());
    return true;
  }

  /**
   * @param tasks Inter-broker replica movement tasks admitted by {@link #tryReserve(ExecutionTask)}.
   * @return The logdirs picked for the replicas to add of the given tasks, to be pinned before their reassignments are submitted.
   */
  Map<TopicPartitionReplica, String> pickedLogDirs(Collection<ExecutionTask> tasks) {
    Map<TopicPartitionReplica, String> pickedLogDirs = new HashMap<>();
    for (ExecutionTask task : tasks) {
      if (task.type() != INTER_BROKER_REPLICA_ACTION) {
        continue;
      }
      TopicPartition tp = task.proposal().topicPartition();
      for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
        TopicPartitionReplica tpr = new TopicPartitionReplica(tp.topic(), tp.partition(), replicaToAdd.brokerId());
        String logDir = _pickedLogDirByReplicaToAdd.get(tpr);
        if (logDir != null) {
          pickedLogDirs.put(tpr, logDir);
        }
      }
    }
    return pickedLogDirs;
  }

  /**
   * Forget the logdir picked for a replica to add whose logdir could not be pinned: Kafka picks the logdir, and the next
   * {@link #refresh(Cluster, Collection, Set)} counts the movement where the replica is created.
   *
   * @param replica A replica to add.
   */
  void forgetPickedLogDir(TopicPartitionReplica replica) {
    _pickedLogDirByReplicaToAdd.remove(replica);
  }

  /**
   * @return The number of movements counted on each logdir with at least one movement, by broker.
   */
  Map<Integer, Map<String, Integer>> numMovementsByLogDirByBroker() {
    Map<Integer, Map<String, Integer>> numMovements = new HashMap<>();
    _logDirStateByLogDirByBroker.forEach((brokerId, stateByLogDir) -> stateByLogDir.forEach((logDir, state) -> {
      if (state._numMovements > 0) {
        numMovements.computeIfAbsent(brokerId, b -> new HashMap<>()).put(logDir, state._numMovements);
      }
    }));
    return numMovements;
  }

  private boolean isAnyAtLimit(ExecutionTask task, Map<Integer, String> logDirByBroker) {
    for (Map.Entry<Integer, String> entry : logDirByBroker.entrySet()) {
      LogDirState state = state(entry.getKey(), entry.getValue());
      if (state != null && state._numMovements >= _maxMovementsPerLogDir) {
        logWaiting(task, String.format("logdir %s of broker %d has %d movement(s)", entry.getValue(), entry.getKey(), state._numMovements));
        return true;
      }
    }
    return false;
  }

  private int sourceBroker(ExecutionTask task) {
    Node leader = _cluster == null ? null : _cluster.leaderFor(task.proposal().topicPartition());
    return leader != null ? leader.id() : task.proposal().oldLeader().brokerId();
  }

  private String logDirOf(int brokerId, TopicPartition tp) {
    return _logDirByReplicaByBroker.getOrDefault(brokerId, Collections.emptyMap()).get(tp);
  }

  private LogDirState state(int brokerId, String logDir) {
    return logDir == null ? null : _logDirStateByLogDirByBroker.getOrDefault(brokerId, Collections.emptyMap()).get(logDir);
  }

  private void addMovement(int brokerId, String logDir) {
    LogDirState state = state(brokerId, logDir);
    if (state != null) {
      state._numMovements++;
    }
  }

  /**
   * Pick the logdir to create a replica in: an online logdir under the limit, preferring one with room for the replica, then
   * the one with the fewest movements, then the one with the most usable bytes, then the one with the fewest replicas.
   *
   * @param brokerId The destination broker.
   * @param replicaBytes The size of the replica.
   * @return The logdir to create the replica in, or {@code null} if every logdir is at the limit or the logdirs are unknown.
   */
  private String pickLogDir(int brokerId, long replicaBytes) {
    Map<String, LogDirState> stateByLogDir = _logDirStateByLogDirByBroker.get(brokerId);
    if (stateByLogDir == null) {
      return null;
    }
    Comparator<Map.Entry<String, LogDirState>> preference =
        Comparator.<Map.Entry<String, LogDirState>, Boolean>comparing(e -> !e.getValue().hasRoomFor(replicaBytes))
                  .thenComparingInt(e -> e.getValue()._numMovements)
                  .thenComparingLong(e -> e.getValue()._usableBytes == null ? 0L : -e.getValue()._usableBytes)
                  .thenComparingInt(e -> e.getValue()._numReplicas)
                  .thenComparing(Map.Entry::getKey);
    return stateByLogDir.entrySet().stream()
                        .filter(e -> e.getValue()._numMovements < _maxMovementsPerLogDir)
                        .min(preference)
                        .map(Map.Entry::getKey)
                        .orElse(null);
  }

  private void logWaiting(ExecutionTask task, String reason) {
    if (_waitingTaskIds.add(task.executionId())) {
      LOG.info("Task {} waits for a logdir under the limit of {} movement(s) per logdir: {}.", task, _maxMovementsPerLogDir, reason);
    }
  }

  private static final class LogDirState {
    private final Long _usableBytes;
    private final int _numReplicas;
    private int _numMovements;

    private LogDirState(Long usableBytes, int numReplicas) {
      _usableBytes = usableBytes;
      _numReplicas = numReplicas;
      _numMovements = 0;
    }

    private boolean hasRoomFor(long replicaBytes) {
      return _usableBytes == null || _usableBytes > replicaBytes;
    }
  }
}
