/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.*;


/**
 * The async runnable for fixing offline replicas -- i.e. move offline replicas to alive brokers/disks.
 */
public class FixOfflineReplicasRunnable extends GoalBasedOperationRunnable {
    protected final Integer _concurrentInterBrokerPartitionMovements;
    protected final Integer _maxInterBrokerPartitionMovements;
    protected final Integer _clusterLeaderMovementConcurrency;
    protected final Integer _brokerLeaderMovementConcurrency;
    protected final Long _executionProgressCheckIntervalMs;
    protected final ReplicaMovementStrategy _replicaMovementStrategy;
    protected final Long _replicationThrottle;
    protected static final boolean SKIP_AUTO_REFRESHING_CONCURRENCY = false;

    /**
     * Constructor to be used for creating a runnable for self-healing.
     */
    public FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                                      List<String> selfHealingGoals,
                                      boolean allowCapacityEstimation,
                                      boolean excludeRecentlyDemotedBrokers,
                                      boolean excludeRecentlyRemovedBrokers,
                                      String anomalyId,
                                      Supplier<String> reasonSupplier,
                                      boolean stopOngoingExecution) {
        super(kafkaCruiseControl, new OperationFuture("Fixing Offline Replicas for Self-Healing"), selfHealingGoals, allowCapacityEstimation,
                excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, anomalyId, reasonSupplier, stopOngoingExecution);
        _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _maxInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _clusterLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _brokerLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
        _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
        _replicationThrottle = kafkaCruiseControl.config().getLong(DEFAULT_REPLICATION_THROTTLE_CONFIG);
    }

    public FixOfflineReplicasRunnable(KafkaCruiseControl kafkaCruiseControl,
                                      OperationFuture future,
                                      FixOfflineReplicasParameters parameters,
                                      String uuid) {
        super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
                uuid, parameters::reason);
        _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
        _maxInterBrokerPartitionMovements = parameters.maxInterBrokerPartitionMovements();
        _clusterLeaderMovementConcurrency = parameters.clusterLeaderMovementConcurrency();
        _brokerLeaderMovementConcurrency = parameters.brokerLeaderMovementConcurrency();
        _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
        _replicaMovementStrategy = parameters.replicaMovementStrategy();
        _replicationThrottle = parameters.replicationThrottle();
    }

    @Override
    protected OptimizationResult getResult() throws Exception {
        return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
    }

    @Override
    protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
        // Ensure that the generated cluster model contains offline replicas.
        sanityCheckOfflineReplicaPresence(clusterModel);
        if (clusterModel.isClusterAlive()) {
            throw new IllegalArgumentException("All brokers are dead in the cluster.");
        }

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
                _fastMode);

        OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
        if (!_dryRun) {
            _kafkaCruiseControl.executeProposals(result.goalProposals(),
                    Collections.emptySet(),
                    false,
                    _concurrentInterBrokerPartitionMovements,
                    _maxInterBrokerPartitionMovements,
                    null,
                    _clusterLeaderMovementConcurrency,
                    _brokerLeaderMovementConcurrency,
                    _executionProgressCheckIntervalMs,
                    _replicaMovementStrategy,
                    _replicationThrottle,
                    _isTriggeredByUserRequest,
                    _uuid,
                    SKIP_AUTO_REFRESHING_CONCURRENCY);
        }
        return result;
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
