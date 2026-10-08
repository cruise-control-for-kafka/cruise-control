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
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DESTINATION_BROKER_IDS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_CONCURRENT_MOVEMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.computeOptimizationOptions;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;


/**
 * The async runnable for broker decommission.
 */
public class RemoveBrokersRunnable extends GoalBasedOperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveBrokersRunnable.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  protected final Set<Integer> _removedBrokerIds;
  protected final Set<Integer> _destinationBrokerIds;
  protected final boolean _throttleRemovedBrokers;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _maxInterBrokerPartitionMovements;
  protected final Integer _clusterLeaderMovementConcurrency;
  protected final Integer _brokerLeaderMovementConcurrency;
  protected final Long _executionProgressCheckIntervalMs;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public RemoveBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                               Set<Integer> removedBrokerIds,
                               List<String> selfHealingGoals,
                               boolean allowCapacityEstimation,
                               boolean excludeRecentlyDemotedBrokers,
                               boolean excludeRecentlyRemovedBrokers,
                               String anomalyId,
                               Supplier<String> reasonSupplier,
                               boolean stopOngoingExecution) {
    super(kafkaCruiseControl, new OperationFuture("Broker Removal for Self-Healing"), selfHealingGoals, allowCapacityEstimation,
          excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, anomalyId, reasonSupplier, stopOngoingExecution);
    _removedBrokerIds = removedBrokerIds;
    _throttleRemovedBrokers = false;
    _destinationBrokerIds = SELF_HEALING_DESTINATION_BROKER_IDS;
    _concurrentInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _maxInterBrokerPartitionMovements = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _clusterLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _brokerLeaderMovementConcurrency = SELF_HEALING_CONCURRENT_MOVEMENTS;
    _executionProgressCheckIntervalMs = SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS;
    _replicaMovementStrategy = SELF_HEALING_REPLICA_MOVEMENT_STRATEGY;
    _replicationThrottle = kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
  }

  public RemoveBrokersRunnable(KafkaCruiseControl kafkaCruiseControl,
                               OperationFuture future,
                               RemoveBrokerParameters parameters,
                               String uuid) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
          uuid, parameters::reason);
    _removedBrokerIds = parameters.brokerIds();
    _throttleRemovedBrokers = parameters.throttleRemovedBrokers();
    _destinationBrokerIds = parameters.destinationBrokerIds();
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
    try {
      return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
    } catch (Exception e) {
      LOG.error("User task {}: failed to remove brokers due to {}", _uuid, e);
      throw e;
    }
  }

  @Override
  protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
    long start = System.currentTimeMillis();
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
    sanityCheckBrokersHavingOfflineReplicasOnBadDisks(_goals, clusterModel);
    _removedBrokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
    if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }
    if (!_destinationBrokerIds.isEmpty()) {
      _kafkaCruiseControl.sanityCheckBrokerPresence(_destinationBrokerIds);
    }
    OptimizationOptions optimizationOptions = computeOptimizationOptions(clusterModel,
                                                                         false,
                                                                         _kafkaCruiseControl,
                                                                         _destinationBrokerIds,
                                                                         _dryRun,
                                                                         _excludeRecentlyDemotedBrokers,
                                                                         _excludeRecentlyRemovedBrokers,
                                                                         _excludedTopics,
                                                                         _destinationBrokerIds,
                                                                         false,
                                                                         _fastMode);
    LOG.info("User task {}: Optimization options: {}", _uuid, optimizationOptions);
    OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
    long goalProposalGenerationTimeMs = System.currentTimeMillis() - start;
    LOG.info("User task {}: Time in proposals generation: {}ms; Optimization result: {}", _uuid,
        goalProposalGenerationTimeMs, result.getProposalSummary());
    Set<ExecutionProposal> goalProposals = result.goalProposals();
    OPERATION_LOG.info("User task {}: Goal proposals: {}", _uuid, goalProposals);
    if (!_dryRun) {
      LOG.info("User task {}: Execute broker removal. throttleRemovedBrokers={}, "
          + "removedBrokerIds={}, "
          + "concurrentInterBrokerPartitionMovements={}, "
          + "maxInterBrokerPartitionMovements={}, "
          + "clusterLeaderMovementConcurrency={}, "
          + "brokerLeaderMovementConcurrency={}, "
          + "executionProgressCheckIntervalMs={}, "
          + "replicaMovementStrategy={}, "
          + "replicationThrottle={}, "
          + "isTriggeredByUserRequest={}",
          _uuid,
          _throttleRemovedBrokers,
          _removedBrokerIds,
          _concurrentInterBrokerPartitionMovements,
          _maxInterBrokerPartitionMovements,
          _clusterLeaderMovementConcurrency,
          _brokerLeaderMovementConcurrency,
          _executionProgressCheckIntervalMs,
          _replicaMovementStrategy,
          _replicationThrottle,
          _isTriggeredByUserRequest);
      _kafkaCruiseControl.executeRemoval(goalProposals, _throttleRemovedBrokers, _removedBrokerIds, isKafkaAssignerMode(_goals),
                                         _concurrentInterBrokerPartitionMovements, _maxInterBrokerPartitionMovements,
                                         _clusterLeaderMovementConcurrency, _brokerLeaderMovementConcurrency,
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
