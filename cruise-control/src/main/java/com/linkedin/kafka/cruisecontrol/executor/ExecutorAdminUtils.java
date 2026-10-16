/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.BYTES_IN_MB;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

public final class ExecutorAdminUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorAdminUtils.class);

  private ExecutorAdminUtils() {

  }

  /**
   * Fetch the logdir information for subject replicas in intra-broker replica movement tasks.
   *
   * @param tasks The tasks to check.
   * @param adminClient The adminClient to send describeReplicaLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs
   * @return Replica logdir information by task.
   */
  static Map<ExecutionTask, ReplicaLogDirInfo> getLogdirInfoForExecutionTask(Collection<ExecutionTask> tasks,
                                                                             Admin adminClient,
                                                                             KafkaCruiseControlConfig config) {
    Set<TopicPartitionReplica> replicasToCheck = new HashSet<>();
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask = new HashMap<>();
    Map<TopicPartitionReplica, ExecutionTask> taskByReplica = new HashMap<>();
    tasks.forEach(t -> {
      TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
      replicasToCheck.add(tpr);
      taskByReplica.put(tpr, t);
    });
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> logDirsByReplicas = adminClient.describeReplicaLogDirs(replicasToCheck).values();
    for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> entry : logDirsByReplicas.entrySet()) {
      try {
        ReplicaLogDirInfo info = entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
        logdirInfoByTask.put(taskByReplica.get(entry.getKey()), info);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.warn("Encounter exception {} when fetching logdir information for replica {}", e.getMessage(), entry.getKey());
      }
    }
    return logdirInfoByTask;
  }

  /**
   * Update the data movement progress of the in-progress replica movement tasks among the given tasks, using the replica
   * sizes reported by the brokers involved in these tasks. A single describeLogDirs request is sent to each such broker
   * (i.e. the destination brokers and the current leader for inter-broker replica movements, and the broker whose replica
   * is moving across disks for intra-broker replica movements), and the requests to different brokers are sent in parallel.
   * Tasks whose progress cannot be determined (e.g. a broker fails to respond in time) keep their last known progress.
   *
   * @param tasks The tasks whose data movement progress will be updated -- tasks other than in-progress inter-broker or
   *              intra-broker replica movement tasks are ignored.
   * @param cluster The current cluster metadata.
   * @param adminClient The adminClient to send describeLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs
   * @param nowMs The current time in milliseconds.
   * @return {@code true} if describeLogDirs requests have been sent, {@code false} if there is no in-progress replica
   * movement task to check.
   */
  static boolean updateDataMovementProgress(Collection<ExecutionTask> tasks,
                                            Cluster cluster,
                                            Admin adminClient,
                                            KafkaCruiseControlConfig config,
                                            long nowMs) {
    List<ExecutionTask> tasksToUpdate = tasks.stream().filter(ExecutorAdminUtils::shouldTrackDataMovementProgress).collect(Collectors.toList());
    Set<Integer> brokersToDescribe = new HashSet<>();
    for (ExecutionTask task : tasksToUpdate) {
      if (task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION) {
        task.proposal().replicasToAdd().forEach(r -> brokersToDescribe.add(r.brokerId()));
        Node leader = cluster.leaderFor(task.proposal().topicPartition());
        if (leader != null) {
          brokersToDescribe.add(leader.id());
        }
      } else {
        brokersToDescribe.add(task.brokerId());
      }
    }
    if (brokersToDescribe.isEmpty()) {
      return false;
    }

    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = new HashMap<>();
    long timeoutMs = config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG);
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    try {
      Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futureByBrokerId = adminClient.describeLogDirs(brokersToDescribe).descriptions();
      for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : futureByBrokerId.entrySet()) {
        try {
          // All brokers share the same deadline, so an unresponsive (e.g. offline) broker delays the check by at most the timeout.
          long remainingTimeMs = Math.max(0L, deadlineMs - System.currentTimeMillis());
          logDirsByBrokerId.put(entry.getKey(), entry.getValue().get(remainingTimeMs, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          LOG.warn("Failed to describe logdirs of broker {} to track the progress of partition movements.", entry.getKey(), e);
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to describe logdirs of brokers {} to track the progress of partition movements.", brokersToDescribe, e);
      return true;
    }
    updateDataMovementProgress(tasksToUpdate, cluster, logDirsByBrokerId, nowMs);
    return true;
  }

  /**
   * Update the data movement progress of the in-progress replica movement tasks among the given tasks, using the given
   * logdir descriptions.
   *
   * <ul>
   *   <li>{@code INTER_BROKER_REPLICA_ACTION}: The moved data is the total size of the replicas being created on the
   *   destination brokers. The total data to move is the size of the leader replica multiplied by the number of replicas
   *   being created.</li>
   *   <li>{@code INTRA_BROKER_REPLICA_ACTION}: The moved data is the size of the future replica being created on the
   *   destination disk. The total data to move is the size of the current replica.</li>
   * </ul>
   *
   * If a destination broker failed to respond, the task keeps its last known progress. If a replica being created may
   * be on an offline logdir, the task is considered to have made no progress since the last observation. If the size of
   * the source replica is unavailable (e.g. the partition has no leader), the last known (or estimated) total is used.
   *
   * @param tasks The tasks whose data movement progress will be updated -- tasks other than in-progress inter-broker or
   *              intra-broker replica movement tasks are ignored.
   * @param cluster The current cluster metadata.
   * @param logDirsByBrokerId Logdir descriptions by broker id, as returned by a describeLogDirs request. Brokers that
   *                          failed to respond are expected to be missing.
   * @param nowMs The current time in milliseconds.
   */
  static void updateDataMovementProgress(Collection<ExecutionTask> tasks,
                                         Cluster cluster,
                                         Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId,
                                         long nowMs) {
    for (ExecutionTask task : tasks) {
      if (!shouldTrackDataMovementProgress(task)) {
        continue;
      }
      if (task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION) {
        updateInterBrokerDataMovementProgress(task, cluster, logDirsByBrokerId, nowMs);
      } else {
        updateIntraBrokerDataMovementProgress(task, logDirsByBrokerId, nowMs);
      }
    }
  }

  private static void updateInterBrokerDataMovementProgress(ExecutionTask task,
                                                            Cluster cluster,
                                                            Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId,
                                                            long nowMs) {
    TopicPartition tp = task.proposal().topicPartition();
    Set<ReplicaPlacementInfo> replicasToAdd = task.proposal().replicasToAdd();
    long movedDataInBytes = 0L;
    boolean isReplicaToAddUnavailable = false;
    for (ReplicaPlacementInfo replicaToAdd : replicasToAdd) {
      Map<String, LogDirDescription> logDirs = logDirsByBrokerId.get(replicaToAdd.brokerId());
      if (logDirs == null) {
        // The destination broker failed to respond.
        return;
      }
      Map.Entry<String, ReplicaInfo> replica = findReplica(logDirs, tp, false);
      if (replica != null) {
        movedDataInBytes += replica.getValue().size();
      } else if (hasOfflineLogDir(logDirs)) {
        isReplicaToAddUnavailable = true;
      }
      // Otherwise, the replica has not been created on the destination broker yet.
    }
    if (isReplicaToAddUnavailable && task.hasDataMovementProgress()) {
      movedDataInBytes = task.movedDataInBytes();
    }
    Node leader = cluster.leaderFor(tp);
    Map.Entry<String, ReplicaInfo> leaderReplica = leader == null ? null : findReplica(logDirsByBrokerId.get(leader.id()), tp, false);
    long totalDataToMoveInBytes = leaderReplica != null ? leaderReplica.getValue().size() * replicasToAdd.size()
                                                        : lastKnownOrEstimatedTotalDataToMoveInBytes(task);
    task.updateDataMovementProgress(movedDataInBytes, totalDataToMoveInBytes, nowMs);
  }

  private static void updateIntraBrokerDataMovementProgress(ExecutionTask task,
                                                            Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId,
                                                            long nowMs) {
    Map<String, LogDirDescription> logDirs = logDirsByBrokerId.get(task.brokerId());
    if (logDirs == null) {
      // The broker failed to respond.
      return;
    }
    TopicPartition tp = task.proposal().topicPartition();
    String destinationLogDir = task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId()).logdir();
    Map.Entry<String, ReplicaInfo> currentReplica = findReplica(logDirs, tp, false);
    Map.Entry<String, ReplicaInfo> futureReplica = findReplica(logDirs, tp, true);
    long movedDataInBytes;
    long totalDataToMoveInBytes;
    if (currentReplica != null && currentReplica.getKey().equals(destinationLogDir)) {
      // The future replica has replaced the current replica since the last check of the task's completion.
      movedDataInBytes = currentReplica.getValue().size();
      totalDataToMoveInBytes = movedDataInBytes;
    } else {
      if (futureReplica != null) {
        movedDataInBytes = futureReplica.getValue().size();
      } else if (hasOfflineLogDir(logDirs) && task.hasDataMovementProgress()) {
        movedDataInBytes = task.movedDataInBytes();
      } else {
        // The future replica has not been created on the destination disk yet.
        movedDataInBytes = 0L;
      }
      totalDataToMoveInBytes = currentReplica != null ? currentReplica.getValue().size() : lastKnownOrEstimatedTotalDataToMoveInBytes(task);
    }
    task.updateDataMovementProgress(movedDataInBytes, totalDataToMoveInBytes, nowMs);
  }

  private static long lastKnownOrEstimatedTotalDataToMoveInBytes(ExecutionTask task) {
    if (task.hasDataMovementProgress()) {
      return task.totalDataToMoveInBytes();
    }
    long estimatedDataToMoveInMB = task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION
                                   ? task.proposal().interBrokerDataToMoveInMB() : task.proposal().intraBrokerDataToMoveInMB();
    return estimatedDataToMoveInMB * BYTES_IN_MB;
  }

  private static boolean shouldTrackDataMovementProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS && task.type() != ExecutionTask.TaskType.LEADER_ACTION;
  }

  /**
   * @param logDirs Logdir descriptions of a broker, or {@code null} if unavailable.
   * @param tp The topic partition whose replica is requested.
   * @param isFuture {@code true} to get the future replica (i.e. the replica being moved to another disk), {@code false}
   *                 to get the current replica.
   * @return The requested replica of the given partition along with the logdir hosting it, or {@code null} if it does
   * not exist in any online logdir.
   */
  private static Map.Entry<String, ReplicaInfo> findReplica(Map<String, LogDirDescription> logDirs, TopicPartition tp, boolean isFuture) {
    if (logDirs != null) {
      for (Map.Entry<String, LogDirDescription> logDir : logDirs.entrySet()) {
        ReplicaInfo replicaInfo = logDir.getValue().error() == null ? logDir.getValue().replicaInfos().get(tp) : null;
        if (replicaInfo != null && replicaInfo.isFuture() == isFuture) {
          return Map.entry(logDir.getKey(), replicaInfo);
        }
      }
    }
    return null;
  }

  private static boolean hasOfflineLogDir(Map<String, LogDirDescription> logDirs) {
    return logDirs.values().stream().anyMatch(logDir -> logDir.error() != null);
  }

  /**
   * Execute intra-broker replica movement tasks by sending alterReplicaLogDirs request.
   *
   * @param tasksToExecute The tasks to execute.
   * @param adminClient The adminClient to send alterReplicaLogDirs request.
   * @param executionTaskManager The task manager to do bookkeeping for task execution state.
   * @param config The config object that holds all the Cruise Control related configs
   */
  static void executeIntraBrokerReplicaMovements(List<ExecutionTask> tasksToExecute,
                                                 Admin adminClient,
                                                 ExecutionTaskManager executionTaskManager,
                                                 KafkaCruiseControlConfig config) {
    Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();
    Map<TopicPartitionReplica, ExecutionTask> replicaToTask = new HashMap<>();
    tasksToExecute.forEach(t -> {
      TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
      replicaAssignment.put(tpr, t.proposal().replicasToMoveBetweenDisksByBroker().get(t.brokerId()).logdir());
      replicaToTask.put(tpr, t);
    });
    for (Map.Entry<TopicPartitionReplica, KafkaFuture<Void>> entry: adminClient.alterReplicaLogDirs(replicaAssignment).values().entrySet()) {
      try {
        entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException | LogDirNotFoundException | KafkaStorageException
          | ReplicaNotAvailableException e) {
        LOG.warn("Encounter exception {} when trying to execute task {}, mark task dead.", e.getMessage(), replicaToTask.get(entry.getKey()));
        executionTaskManager.markTaskAborting(replicaToTask.get(entry.getKey()));
        executionTaskManager.markTaskDead(replicaToTask.get(entry.getKey()));
      }
    }
  }

  /**
   * Check whether there is ongoing intra-broker replica movement.
   * @param adminClient The adminClient to send describeLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs
   * @return {@code true} if there is ongoing intra-broker replica movement.
   */
  static boolean hasOngoingIntraBrokerReplicaMovement(Admin adminClient,
                                                      KafkaCruiseControlConfig config)
      throws InterruptedException, ExecutionException, TimeoutException {
    Collection<Integer> brokersToCheck = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> logDirsByBrokerId = adminClient.describeLogDirs(brokersToCheck).descriptions();
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : logDirsByBrokerId.entrySet()) {
      Map<String, LogDirDescription> logInfos = entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
      for (LogDirDescription info : logInfos.values()) {
        if (info.error() == null) {
          if (info.replicaInfos().values().stream().anyMatch(ReplicaInfo::isFuture)) {
            return true;
          }
        }
      }
    }
    return false;
  }
}

