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
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;

public class RemoveDisksRunnable extends GoalBasedOperationRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveDisksRunnable.class);
    private static final int CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS = 0;
    private static final int MAX_INTER_BROKER_PARTITION_MOVEMENTS = 0;
    private static final int CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS = 1;
    private static final int CLUSTER_CONCURRENT_LEADER_MOVEMENTS = 1;
    private static final int BROKER_CONCURRENT_LEADER_MOVEMENTS = 1;
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

        _brokerIdAndLogdirs.forEach((brokerId, logDirsToRemove) ->
                logDirsToRemove.forEach(logDir -> clusterModel.broker(brokerId).disk(logDir).markDiskForRemoval())
        );

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

        OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
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
            Set<String> brokerLogDirs = broker.disks().stream().map(Disk::logDir).collect(Collectors.toSet());
            if (!brokerLogDirs.containsAll(logDirsToRemove)) {
                LOG.error("Invalid log dirs provided for broker {}.", brokerId);
                throw new IllegalArgumentException(String.format("Invalid log dirs provided for broker %d.", brokerId));
            }
            if (broker.disks().size() == logDirsToRemove.size()) {
                LOG.error("No log dir remaining to move replicas to for broker {}.", brokerId);
                throw new IllegalArgumentException(String.format("No log dir remaining to move replicas to for broker %d.", brokerId));
            }

            double futureUsage = 0.0;
            double remainingCapacity = 0.0;
            for (Disk disk : broker.disks()) {
                futureUsage += disk.utilization();
                if (!logDirsToRemove.contains(disk.logDir())) {
                    remainingCapacity += disk.capacity();
                }
            }
            if (futureUsage / remainingCapacity > capacityThreshold) {
                LOG.error("Not enough remaining capacity to move replicas to for broker {}.", brokerId);
                throw new IllegalArgumentException(String.format("Not enough remaining capacity to move replicas to for broker %d.", brokerId));
            }
        }
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
