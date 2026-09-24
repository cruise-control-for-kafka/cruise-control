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

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

public final class ExecutorAdminUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorAdminUtils.class);

    private ExecutorAdminUtils() {
    }

    /**
     * Fetch the logdir information for subject replicas in intra-broker replica movement tasks.
     *
     * @param tasks       The tasks to check.
     * @param adminClient The adminClient to send describeReplicaLogDirs request.
     * @param config      The config object that holds all the Cruise Control related configs
     * @return Replica logdir information by task.
     */
    static Map<ExecutionTask, ReplicaLogDirInfo> getLogDirInfoForExecutionTask(Collection<ExecutionTask> tasks,
                                                                               AdminClient adminClient,
                                                                               KafkaCruiseControlConfig config) {
        Set<TopicPartitionReplica> replicasToCheck = new HashSet<>();
        Map<ExecutionTask, ReplicaLogDirInfo> logDirInfoByTask = new HashMap<>();
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
                logDirInfoByTask.put(taskByReplica.get(entry.getKey()), info);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn("Encounter exception {} when fetching logdir information for replica {}", e.getMessage(), entry.getKey());
            }
        }
        return logDirInfoByTask;
    }

    /**
     * Execute intra-broker replica movement tasks by sending alterReplicaLogDirs request.
     *
     * @param tasksToExecute       The tasks to execute.
     * @param adminClient          The adminClient to send alterReplicaLogDirs request.
     * @param executionTaskManager The task manager to do bookkeeping for task execution state.
     * @param config               The config object that holds all the Cruise Control related configs
     */
    static void executeIntraBrokerReplicaMovements(List<ExecutionTask> tasksToExecute,
                                                   AdminClient adminClient,
                                                   ExecutionTaskManager executionTaskManager,
                                                   KafkaCruiseControlConfig config) {
        Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();
        Map<TopicPartitionReplica, ExecutionTask> replicaToTask = new HashMap<>();
        tasksToExecute.forEach(t -> {
            TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
            replicaAssignment.put(tpr, t.proposal().replicasToMoveBetweenDisksByBroker().get(t.brokerId()).logdir());
            replicaToTask.put(tpr, t);
        });
        for (Map.Entry<TopicPartitionReplica, KafkaFuture<Void>> entry : adminClient.alterReplicaLogDirs(replicaAssignment).values().entrySet()) {
            try {
                entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException | LogDirNotFoundException |
                     KafkaStorageException
                     | ReplicaNotAvailableException e) {
                LOG.warn("Encounter exception {} when trying to execute task {}, mark task dead.", e.getMessage(), replicaToTask.get(entry.getKey()));
                executionTaskManager.markTaskAborting(replicaToTask.get(entry.getKey()));
                executionTaskManager.markTaskDead(replicaToTask.get(entry.getKey()));
            }
        }
    }

    /**
     * Check whether there is ongoing intra-broker replica movement.
     *
     * @param adminClient The adminClient to send describeLogDirs request.
     * @param config      The config object that holds all the Cruise Control related configs
     * @return {@code true} if there is ongoing intra-broker replica movement.
     */
    static boolean hasOngoingIntraBrokerReplicaMovement(AdminClient adminClient,
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

