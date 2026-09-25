/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.*;


/**
 * The async runnable for broker addition.
 */
public class AddBrokersRunnable extends GoalBasedOperationRunnable {
    protected final Set<Integer> _brokerIds;
    protected final boolean _throttleAddedBrokers;
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
    public AddBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                              Set<Integer> brokerIds,
                              List<String> selfHealingGoals,
                              boolean allowCapacityEstimation,
                              boolean excludeRecentlyDemotedBrokers,
                              boolean excludeRecentlyRemovedBrokers,
                              String anomalyId,
                              Supplier<String> reasonSupplier,
                              boolean stopOngoingExecution) {
        super(kafkaCruiseControl, new OperationFuture("Broker Addition for Self-Healing"), selfHealingGoals, allowCapacityEstimation,
                excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, anomalyId, reasonSupplier, stopOngoingExecution);
        _brokerIds = brokerIds;
        _throttleAddedBrokers = false;
        _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _maxInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _clusterLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _brokerLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
        _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
        _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    }

    public AddBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                              OperationFuture future,
                              AddBrokerParameters parameters,
                              String uuid) {
        super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
                uuid, parameters::reason);
        _brokerIds = parameters.brokerIds();
        _throttleAddedBrokers = parameters.throttleAddedBrokers();
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
        _kafkaCruiseControl.sanityCheckBrokerPresence(_brokerIds);
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
        sanityCheckBrokersHavingOfflineReplicasOnBadDisks(_goals, clusterModel);
        _brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.NEW));

        if (clusterModel.isClusterAlive()) {
            throw new IllegalArgumentException("All brokers are dead in the cluster.");
        }

        OptimizationOptions optimizationOptions = computeOptimizationOptions(clusterModel,
                false,
                _kafkaCruiseControl,
                _brokerIds,
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
                    _throttleAddedBrokers ? Collections.emptySet() : _brokerIds,
                    isKafkaAssignerMode(_goals),
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
