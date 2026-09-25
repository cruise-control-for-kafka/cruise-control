/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ensureDisjoint;
import static com.linkedin.kafka.cruisecontrol.model.Disk.State.DEMOTED;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;


/**
 * The async runnable for broker demotion.
 * Demote given brokers by making all the replicas on these brokers the least preferred replicas for leadership election
 * within their corresponding partitions and then triggering a preferred leader election on the partitions to migrate
 * the leader replicas off the brokers.
 * <p>
 * The result of the broker demotion is not guaranteed to be able to move all the leaders away from the
 * given brokers. The operation is with best effort. There are various possibilities that some leaders
 * cannot be migrated (e.g. no other broker is in the ISR).
 * <p>
 * This operation can be configured to be stateful for self-healing operations and user requests -- see
 * {@link com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig#DEMOTION_HISTORY_RETENTION_TIME_MS_DOC},
 * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig#SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC},
 * and {@link com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils#EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM}.
 */
public class DemoteBrokerRunnable extends GoalBasedOperationRunnable {
    protected final Set<Integer> _brokerIds;
    protected final Integer _clusterLeaderMovementConcurrency;
    protected final Integer _brokerLeaderMovementConcurrency;
    protected final Long _executionProgressCheckIntervalMs;
    protected final boolean _skipUrpDemotion;
    protected final boolean _excludeFollowerDemotion;
    protected final ReplicaMovementStrategy _replicaMovementStrategy;
    protected final Long _replicationThrottle;
    protected final Map<Integer, Set<String>> _brokerIdAndLogdirs;

    /**
     * Constructor to be used for creating a runnable for self-healing.
     */
    public DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                                Set<Integer> demotedBrokerIds,
                                boolean allowCapacityEstimation,
                                boolean excludeRecentlyDemotedBrokers,
                                String anomalyId,
                                Supplier<String> reasonSupplier,
                                boolean stopOngoingExecution) {
        super(kafkaCruiseControl, new OperationFuture("Broker Demotion for Self-Healing"), SELF_HEALING_DRYRUN, null,
                stopOngoingExecution, null, true, null,
                allowCapacityEstimation, excludeRecentlyDemotedBrokers, false, anomalyId, reasonSupplier,
                SELF_HEALING_IS_TRIGGERED_BY_USER_REQUEST, SELF_HEALING_FAST_MODE);
        _brokerIds = demotedBrokerIds;
        _clusterLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _brokerLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
        _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
        _skipUrpDemotion = SELF_HEALING_SKIP_URP_DEMOTION;
        _excludeFollowerDemotion = SELF_HEALING_EXCLUDE_FOLLOWER_DEMOTION;
        _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
        _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
        _brokerIdAndLogdirs = Collections.emptyMap();
    }

    public DemoteBrokerRunnable(KafkaCruiseControl kafkaCruiseControl,
                                OperationFuture future,
                                String uuid,
                                DemoteBrokerParameters parameters) {

        super(kafkaCruiseControl, future, parameters.dryRun(), null,
                parameters.stopOngoingExecution(), null, true, null,
                parameters.allowCapacityEstimation(), parameters.excludeRecentlyDemotedBrokers(), false,
                uuid, parameters::reason, true, false);
        _brokerIds = parameters.brokerIds();
        _clusterLeaderMovementConcurrency = parameters.clusterLeaderMovementConcurrency();
        _brokerLeaderMovementConcurrency = parameters.brokerLeaderMovementConcurrency();
        _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
        _skipUrpDemotion = parameters.skipUrpDemotion();
        _excludeFollowerDemotion = parameters.excludeFollowerDemotion();
        _replicaMovementStrategy = parameters.replicaMovementStrategy();
        _replicationThrottle = parameters.replicationThrottle();
        _brokerIdAndLogdirs = parameters.brokerIdAndLogdirs();
    }

    @Override
    protected OptimizationResult getResult() throws Exception {
        return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
    }

    /**
     * Perform the initializations before {@link #computeResult()}.
     */
    @Override
    protected void init() {
        _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
        _goalsByPriority = new ArrayList<>(1);
        _goalsByPriority.add(new PreferredLeaderElectionGoal(_skipUrpDemotion,
                _excludeFollowerDemotion,
                _skipUrpDemotion ? _kafkaCruiseControl.kafkaCluster() : null));

        _operationProgress = _future.operationProgress();
        if (_stopOngoingExecution) {
            maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _operationProgress);
        }
        _combinedCompletenessRequirements = _goalsByPriority.get(0).clusterModelCompletenessRequirements();
    }

    protected void setDemoteState(ClusterModel clusterModel) {
        if (clusterModel.isClusterAlive()) {
            throw new IllegalArgumentException("All brokers are dead in the cluster.");
        }
        _brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEMOTED));
        _brokerIdAndLogdirs.forEach((brokerid, logdirs) -> {
            Broker broker = clusterModel.broker(brokerid);
            for (String logdir : logdirs) {
                if (broker.disk(logdir) == null) {
                    throw new IllegalStateException(String.format("Broker %d does not have logdir %s.", brokerid, logdir));
                }
                broker.disk(logdir).setState(DEMOTED);
            }
        });
    }

    @Override
    protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
        ensureDisjoint(_brokerIds, _brokerIdAndLogdirs.keySet(),
                "Attempt to demote the broker and its disk in the same request is not allowed.");
        Set<Integer> brokersToCheckPresence = new HashSet<>(_brokerIds);
        brokersToCheckPresence.addAll(_brokerIdAndLogdirs.keySet());
        _kafkaCruiseControl.sanityCheckBrokerPresence(brokersToCheckPresence);
        ClusterModel clusterModel = _brokerIdAndLogdirs.isEmpty() ? _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements,
                _allowCapacityEstimation,
                _operationProgress)
                : _kafkaCruiseControl.clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                _kafkaCruiseControl.timeMs(),
                _combinedCompletenessRequirements,
                true,
                _allowCapacityEstimation,
                _operationProgress);
        setDemoteState(clusterModel);
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
            _kafkaCruiseControl.executeDemotion(result.goalProposals(), _brokerIds, _clusterLeaderMovementConcurrency,
                    _brokerLeaderMovementConcurrency, clusterModel.brokers().size(),
                    _executionProgressCheckIntervalMs, _replicaMovementStrategy, _replicationThrottle,
                    _isTriggeredByUserRequest, _uuid);
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
