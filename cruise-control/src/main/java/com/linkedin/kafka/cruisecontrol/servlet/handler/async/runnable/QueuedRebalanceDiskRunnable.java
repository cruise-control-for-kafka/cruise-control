/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;

/**
 * The disk rebalance that a rebalance request with {@code queue_rebalance_disk=true} queues after its own execution. It runs once
 * that execution has completed, while the executor is still reserved for the same user task:
 * <ol>
 *   <li>It waits until the load monitor meets the model completeness requirements of a disk rebalance (partition metrics are not
 *   sampled during an execution), for at most two partition metric windows.</li>
 *   <li>It generates the disk rebalance proposals from a fresh cluster model, exactly as a request with {@code rebalance_disk=true}
 *   and the same parameters would.</li>
 *   <li>It executes them with the same UUID, or finishes the user task if there is nothing to move.</li>
 * </ol>
 * A stop request, a failure, or a shutdown before the execution starts finishes the user task with an error.
 */
public class QueuedRebalanceDiskRunnable implements Runnable {
  public static final String TYPE = "REBALANCE_DISK";
  private static final Logger LOG = LoggerFactory.getLogger(QueuedRebalanceDiskRunnable.class);
  private static final long LOAD_MONITOR_CHECK_INTERVAL_MS = 5000L;
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final String _uuid;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Pattern _excludedTopics;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final Set<Integer> _destinationBrokerIds;
  private final boolean _fastMode;
  private final Integer _concurrentIntraBrokerPartitionMovements;
  private final Long _executionProgressCheckIntervalMs;
  private final Long _replicationThrottle;
  private final boolean _isTriggeredByUserRequest;

  /**
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param rebalance The rebalance that queues this disk rebalance; this one inherits its applicable parameters.
   */
  QueuedRebalanceDiskRunnable(KafkaCruiseControl kafkaCruiseControl, RebalanceRunnable rebalance) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _uuid = rebalance._uuid;
    _modelCompletenessRequirements = rebalance._modelCompletenessRequirements;
    _allowCapacityEstimation = rebalance._allowCapacityEstimation;
    _excludedTopics = rebalance._excludedTopics;
    _excludeRecentlyDemotedBrokers = rebalance._excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = rebalance._excludeRecentlyRemovedBrokers;
    _destinationBrokerIds = rebalance._destinationBrokerIds;
    _fastMode = rebalance._fastMode;
    _concurrentIntraBrokerPartitionMovements = rebalance._concurrentIntraBrokerPartitionMovements;
    _executionProgressCheckIntervalMs = rebalance._executionProgressCheckIntervalMs;
    _replicationThrottle = rebalance._replicationThrottle;
    _isTriggeredByUserRequest = rebalance._isTriggeredByUserRequest;
  }

  @Override
  public void run() {
    try {
      if (!waitForLoadMonitor()) {
        return;
      }
      List<String> goals = ParameterUtils.rebalanceDiskModeGoals();
      ProposalsRunnable proposalsRunnable =
          new ProposalsRunnable(_kafkaCruiseControl, new OperationFuture("Queued disk rebalance"), goals, _modelCompletenessRequirements,
                                _allowCapacityEstimation, _excludedTopics, _excludeRecentlyDemotedBrokers, _excludeRecentlyRemovedBrokers,
                                true, _destinationBrokerIds, true, true, false, _fastMode);
      OptimizerResult result = proposalsRunnable.computeResult();
      LOG.info("User task {}: Queued disk rebalance proposals: {}", _uuid, result.getProposalSummary());
      if (result.goalProposals().isEmpty()) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, false, "the disks are already balanced");
        return;
      }
      if (_kafkaCruiseControl.isQueuedExecutionCancelled(_uuid)) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, true, "a stop was requested before it started");
        return;
      }
      _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), false, null, null,
                                           _concurrentIntraBrokerPartitionMovements, null, null, _executionProgressCheckIntervalMs,
                                           null, _replicationThrottle, _isTriggeredByUserRequest, _uuid,
                                           RebalanceRunnable.SKIP_AUTO_REFRESHING_CONCURRENCY);
    } catch (InterruptedException ie) {
      _kafkaCruiseControl.finishQueuedExecution(_uuid, true, "Cruise Control is shutting down");
    } catch (Exception e) {
      LOG.warn("User task {}: The queued disk rebalance failed.", _uuid, e);
      _kafkaCruiseControl.finishQueuedExecution(_uuid, true, String.format("it failed: %s", e.getMessage()));
    }
  }

  /**
   * Wait until the load monitor meets the model completeness requirements of a disk rebalance, for at most two partition metric
   * windows, or until a stop is requested.
   *
   * @return {@code true} if the requirements are met, {@code false} if the queued disk rebalance is finished instead.
   */
  private boolean waitForLoadMonitor() throws InterruptedException {
    ModelCompletenessRequirements requirements =
        _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority(ParameterUtils.rebalanceDiskModeGoals(),
                                                                          _kafkaCruiseControl.config()))
                           .weaker(_modelCompletenessRequirements);
    long maxWaitMs = 2 * _kafkaCruiseControl.config().getLong(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
    long deadlineMs = System.currentTimeMillis() + maxWaitMs;
    boolean logged = false;
    while (!_kafkaCruiseControl.loadMonitor().meetCompletenessRequirements(requirements)) {
      if (_kafkaCruiseControl.isQueuedExecutionCancelled(_uuid)) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, true, "a stop was requested before it started");
        return false;
      }
      if (System.currentTimeMillis() >= deadlineMs) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, true, String.format(
            "the load monitor did not meet the model completeness requirements %s within %d ms", requirements, maxWaitMs));
        return false;
      }
      if (!logged) {
        LOG.info("User task {}: The queued disk rebalance waits for the load monitor to meet {} (at most {} ms).", _uuid, requirements,
                 maxWaitMs);
        logged = true;
      }
      Thread.sleep(LOAD_MONITOR_CHECK_INTERVAL_MS);
    }
    return true;
  }
}
