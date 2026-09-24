/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DESTINATION_BROKER_IDS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;


/**
 * The async runnable for balancing the cluster load.
 */
public class RebalanceRunnable extends GoalBasedOperationRunnable {
  public static final boolean SELF_HEALING_IGNORE_PROPOSAL_CACHE = false;
  public static final boolean SELF_HEALING_IS_REBALANCE_DISK_MODE = false;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _maxInterBrokerPartitionMovements;
  protected final Integer _concurrentIntraBrokerPartitionMovements;
  protected final Integer _clusterLeaderMovementConcurrency;
  protected final Integer _brokerLeaderMovementConcurrency;
  protected final Long _executionProgressCheckIntervalMs;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
  protected final boolean _ignoreProposalCache;
  protected final Set<Integer> _destinationBrokerIds;
  protected final boolean _isRebalanceDiskMode;
  protected static final boolean SKIP_AUTO_REFRESHING_CONCURRENCY = false;

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                           List<String> selfHealingGoals,
                           boolean allowCapacityEstimation,
                           boolean excludeRecentlyDemotedBrokers,
                           boolean excludeRecentlyRemovedBrokers,
                           String anomalyId,
                           Supplier<String> reasonSupplier,
                           boolean stopOngoingExecution) {
    super(kafkaCruiseControl, new OperationFuture("Rebalance for Self-Healing"), selfHealingGoals, allowCapacityEstimation,
          excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, anomalyId, reasonSupplier, stopOngoingExecution);
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _maxInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentIntraBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _clusterLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _brokerLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _ignoreProposalCache = SELF_HEALING_IGNORE_PROPOSAL_CACHE;
    _destinationBrokerIds = SELF_HEALING_DESTINATION_BROKER_IDS;
    _isRebalanceDiskMode = SELF_HEALING_IS_REBALANCE_DISK_MODE;
  }

  public RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                           List<String> selfHealingGoals,
                           boolean allowCapacityEstimation,
                           boolean excludeRecentlyDemotedBrokers,
                           boolean excludeRecentlyRemovedBrokers,
                           String anomalyId,
                           Supplier<String> reasonSupplier,
                           boolean stopOngoingExecution,
                           boolean skipHardGoalCheck,
                           boolean ignoreProposalCache,
                           boolean isRebalanceDiskMode) {
    super(kafkaCruiseControl, new OperationFuture("Rebalance for Self-Healing"), selfHealingGoals, allowCapacityEstimation,
            excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, anomalyId, reasonSupplier, stopOngoingExecution, skipHardGoalCheck);
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _maxInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _concurrentIntraBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _clusterLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _brokerLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _ignoreProposalCache = ignoreProposalCache;
    _destinationBrokerIds = SELF_HEALING_DESTINATION_BROKER_IDS;
    _isRebalanceDiskMode = isRebalanceDiskMode;
  }

  public RebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                           OperationFuture future,
                           RebalanceParameters parameters,
                           String uuid) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
          uuid, parameters::reason);
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _maxInterBrokerPartitionMovements = parameters.maxInterBrokerPartitionMovements();
    _concurrentIntraBrokerPartitionMovements = parameters.concurrentIntraBrokerPartitionMovements();
    _clusterLeaderMovementConcurrency = parameters.clusterConcurrentLeaderMovements();
    _brokerLeaderMovementConcurrency = parameters.brokerConcurrentLeaderMovements();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _ignoreProposalCache = parameters.ignoreProposalCache();
    _destinationBrokerIds = parameters.destinationBrokerIds();
    _isRebalanceDiskMode = parameters.isRebalanceDiskMode();
  }

  @Override
  protected OptimizationResult getResult() throws Exception {
    return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
  }

  @Override
  protected void init() {
    _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
    if (_stopOngoingExecution) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _future.operationProgress());
    }
  }

  @Override
  protected boolean shouldWorkWithClusterModel() {
    return false;
  }

  @Override
  protected OptimizerResult workWithClusterModel() {
    return null;
  }

  @Override
  protected OptimizerResult workWithoutClusterModel() throws KafkaCruiseControlException {
    ProposalsRunnable proposalsRunnable = new ProposalsRunnable(_kafkaCruiseControl, _future, _goals, _modelCompletenessRequirements,
                                                                _allowCapacityEstimation, _excludedTopics, _excludeRecentlyDemotedBrokers,
                                                                _excludeRecentlyRemovedBrokers, _ignoreProposalCache, _destinationBrokerIds,
                                                                _isRebalanceDiskMode, _skipHardGoalCheck, !_isTriggeredByUserRequest,
                                                                _fastMode);
    OptimizerResult result = proposalsRunnable.computeResult();
    if (!_dryRun) {
      _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), isKafkaAssignerMode(_goals),
          _concurrentInterBrokerPartitionMovements, _maxInterBrokerPartitionMovements,
          _concurrentIntraBrokerPartitionMovements, _clusterLeaderMovementConcurrency, _brokerLeaderMovementConcurrency,
          _executionProgressCheckIntervalMs, _replicaMovementStrategy,
          _replicationThrottle, _isTriggeredByUserRequest, _uuid, SKIP_AUTO_REFRESHING_CONCURRENCY);
    }
    return result;
  }
}
