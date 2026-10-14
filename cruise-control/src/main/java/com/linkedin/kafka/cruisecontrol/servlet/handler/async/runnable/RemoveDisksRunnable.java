/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.computeOptimizationOptions;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;

public class RemoveDisksRunnable extends GoalBasedOperationRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveDisksRunnable.class);
    private static final int CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS = 0;
    private static final int MAX_INTER_BROKER_PARTITION_MOVEMENTS = 0;
    private static final int CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS = 1;
    private static final int CLUSTER_CONCURRENT_LEADER_MOVEMENTS = 1;
    private static final int BROKER_CONCURRENT_LEADER_MOVEMENTS = 1;
    private static final int MAX_OFFLINE_REPLICAS_TO_REPORT = 10;
    private final Map<Integer, Set<String>> _brokerIdAndLogdirs;

    public RemoveDisksRunnable(KafkaCruiseControl kafkaCruiseControl,
                               OperationFuture future,
                               RemoveDisksParameters parameters,
                               String uuid) {
        super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), false,
                uuid, parameters::reason);
        _brokerIdAndLogdirs = parameters.brokerIdAndLogdirs();
    }

    @Override
    protected OptimizationResult getResult() throws Exception {
        return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
    }

    @Override
    protected void init() {
        _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
        Goal intraBrokerDiskCapacityGoal = new IntraBrokerDiskCapacityGoal(true);
        intraBrokerDiskCapacityGoal.configure(_kafkaCruiseControl.config().mergedConfigValues());
        _goalsByPriority = new ArrayList<>(Collections.singletonList(intraBrokerDiskCapacityGoal));

        _operationProgress = _future.operationProgress();
        if (_stopOngoingExecution) {
            maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _operationProgress);
        }
        _combinedCompletenessRequirements = _goalsByPriority.get(0).clusterModelCompletenessRequirements();
    }

    @Override
    protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, NotEnoughValidWindowsException, TimeoutException {
        Set<Integer> brokersToCheckPresence = new HashSet<>(_brokerIdAndLogdirs.keySet());
        _kafkaCruiseControl.sanityCheckBrokerPresence(brokersToCheckPresence);
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(
                DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                _kafkaCruiseControl.timeMs(),
                _combinedCompletenessRequirements,
                true,
                _allowCapacityEstimation,
                _operationProgress
        );

        checkCanRemoveDisks(_brokerIdAndLogdirs, clusterModel);

        OptimizationOptions optimizationOptions = computeOptimizationOptions(clusterModel,
                false,
                _kafkaCruiseControl,
                Collections.emptySet(),
                _dryRun,
                _excludeRecentlyDemotedBrokers,
                _excludeRecentlyRemovedBrokers,
                _excludedTopics,
                Collections.emptySet(),
                false,
                _fastMode
        );

        // Marking disks for removal drops their capacity, hence the load and stats before the disk removal are taken beforehand.
        BrokerStats brokerStatsBeforeRemoval = clusterModel.brokerStats(null);
        ClusterModelStats clusterModelStatsBeforeRemoval =
                clusterModel.getClusterStats(new BalancingConstraint(_kafkaCruiseControl.config()), optimizationOptions);
        _brokerIdAndLogdirs.forEach((brokerId, logDirsToRemove) ->
                logDirsToRemove.forEach(logDir -> clusterModel.markDiskForRemoval(brokerId, logDir))
        );

        OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions,
                brokerStatsBeforeRemoval, clusterModelStatsBeforeRemoval);
        if (!_dryRun) {
            _kafkaCruiseControl.executeProposals(
                    result.goalProposals(),
                    Collections.emptySet(),
                    isKafkaAssignerMode(_goals),
                    CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS,
                    MAX_INTER_BROKER_PARTITION_MOVEMENTS,
                    CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS,
                    CLUSTER_CONCURRENT_LEADER_MOVEMENTS,
                    BROKER_CONCURRENT_LEADER_MOVEMENTS,
                    SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS,
                    SELF_HEALING_REPLICA_MOVEMENT_STRATEGY,
                    _kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG),
                    _isTriggeredByUserRequest,
                    _uuid,
                    false
            );
        }

        return result;
    }

    private void checkCanRemoveDisks(Map<Integer, Set<String>> brokerIdAndLogdirs, ClusterModel clusterModel) {
        BalancingConstraint balancingConstraint = new BalancingConstraint(_kafkaCruiseControl.config());
        double capacityThreshold = balancingConstraint.capacityThreshold(Resource.DISK);
        for (Map.Entry<Integer, Set<String>> entry : brokerIdAndLogdirs.entrySet()) {
            Integer brokerId = entry.getKey();
            Set<String> logDirsToRemove = entry.getValue();
            Broker broker = clusterModel.broker(brokerId);
            Set<String> brokerLogDirs = broker.disks().stream().map(Disk::logDir).collect(Collectors.toCollection(TreeSet::new));
            if (!brokerLogDirs.containsAll(logDirsToRemove)) {
                LOG.error("Invalid log dirs provided for broker {}.", brokerId);
                throw new IllegalArgumentException(String.format("Invalid log dirs provided for broker %d, whose log dirs are %s.",
                                                                 brokerId, brokerLogDirs));
            }
            // Replicas cannot be moved off a failed disk, and Kafka reports no replica for a failed log dir: such a disk can be
            // neither emptied nor checked to be empty.
            for (String logDir : logDirsToRemove) {
                Disk disk = broker.disk(logDir);
                if (!disk.isAlive()) {
                    LOG.error("Cannot remove disk {} of broker {}, which is {}.", logDir, brokerId, disk.state());
                    throw new IllegalArgumentException(String.format("Cannot remove disk %s of broker %d: the disk is %s, and "
                                                                     + "replicas cannot be moved off a failed disk to another disk of "
                                                                     + "the broker. %s", logDir, brokerId, disk.state(),
                                                                     offlineReplicasAdvice(broker)));
                }
            }
            List<Disk> remainingDisks = broker.disks().stream()
                                              .filter(disk -> disk.isAlive() && !logDirsToRemove.contains(disk.logDir()))
                                              .collect(Collectors.toList());
            if (remainingDisks.isEmpty()) {
                LOG.error("No alive log dir remaining to move replicas to for broker {}.", brokerId);
                throw new IllegalArgumentException(String.format("No alive log dir remaining to move replicas to for broker %d.",
                                                                 brokerId));
            }

            // The replicas of failed disks are offline, on no disk: only the replicas on alive disks are moved.
            double futureUsage = broker.disks().stream().filter(Disk::isAlive).mapToDouble(Disk::utilization).sum();
            double remainingCapacity = remainingDisks.stream().mapToDouble(Disk::capacity).sum();
            if (futureUsage > remainingCapacity * capacityThreshold) {
                LOG.error("Not enough remaining capacity to move replicas to for broker {}.", brokerId);
                throw new IllegalArgumentException(String.format("Not enough remaining capacity to move replicas to for broker %d.", brokerId));
            }
        }
    }

    /**
     * @param broker A broker with a failed disk.
     * @return What to do with the offline replicas of the broker -- e.g. the replicas of its failed disks -- before its failed disk
     * can be taken out of the broker.
     */
    private static String offlineReplicasAdvice(Broker broker) {
        List<String> offlineReplicas = broker.replicas().stream().filter(Replica::isCurrentOffline)
                                             .map(replica -> replica.topicPartition().toString()).sorted().collect(Collectors.toList());
        if (offlineReplicas.isEmpty()) {
            return String.format("Broker %d has no offline replica: take the disk out of the broker's log.dirs.", broker.id());
        }
        String replicas = offlineReplicas.size() <= MAX_OFFLINE_REPLICAS_TO_REPORT
                          ? offlineReplicas.toString()
                          : String.format("%s and %d more", offlineReplicas.subList(0, MAX_OFFLINE_REPLICAS_TO_REPORT),
                                          offlineReplicas.size() - MAX_OFFLINE_REPLICAS_TO_REPORT);
        return String.format("Offline replicas of broker %d: %s. Move them to other brokers with fix_offline_replicas, then take the "
                             + "disk out of the broker's log.dirs.", broker.id(), replicas);
    }

    @Override
    protected boolean shouldWorkWithClusterModel() {
        return true;
    }

    @Override
    protected OptimizerResult workWithoutClusterModel() {
        return null;
    }
}
