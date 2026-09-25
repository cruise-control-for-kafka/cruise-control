/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.AtomicDouble;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataAdminClient;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ConcurrencyAdjustingRecommendation;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.StrategyOptions;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;

import static com.linkedin.cruisecontrol.CruiseControlUtils.currentUtcDate;
import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.*;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionUtils.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorAdminUtils.*;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.SamplingMode.*;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * Executor for Kafka GoalOptimizer.
 * <p>
 * The executor class is responsible for talking to the Kafka cluster to execute the rebalance proposals.
 *
 * The executor is thread-safe.
 */
public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private static final long EXECUTION_PROGRESS_CHECK_INTERVAL_ADJUSTING_MS = 1000;
  // The execution progress is controlled by the ExecutionTaskManager.
  private final ExecutionTaskManager _executionTaskManager;
  private final MetadataAdminClient _metadataClient;
  private volatile long _executionProgressCheckIntervalMs;
  private final long _defaultExecutionProgressCheckIntervalMs;
  private Long _requestedExecutionProgressCheckIntervalMs;
  private final ExecutorService _proposalExecutor;
  private final AdminClient _adminClient;
  private final double _leaderMovementTimeoutMs;

  private static final int NO_STOP_EXECUTION = 0;
  private static final int STOP_EXECUTION = 1;

  // Some state for external service to query
  private final AtomicInteger _stopSignal;
  private final Time _time;
  private volatile boolean _hasOngoingExecution;
  private final Semaphore _flipOngoingExecutionMutex;
  private final Semaphore _noOngoingExecutionSemaphore;
  private volatile ExecutorState _executorState;
  private volatile String _uuid;
  private volatile Supplier<String> _reasonSupplier;
  private final ExecutorNotifier _executorNotifier;

  private final AtomicInteger _numExecutionStopped;
  private final AtomicInteger _numExecutionStoppedByUser;
  private final AtomicBoolean _executionStoppedByUser;
  private final AtomicBoolean _ongoingExecutionIsBeingModified;
  private final AtomicInteger _numExecutionStartedInKafkaAssignerMode;
  private final AtomicInteger _numExecutionStartedInNonKafkaAssignerMode;
  private volatile boolean _isKafkaAssignerMode;
  private volatile boolean _skipInterBrokerReplicaConcurrencyAdjustment;
  // TODO: Execution history is currently kept in memory, but ideally we should move it to a persistent store.
  private final long _demotionHistoryRetentionTimeMs;
  private final long _removalHistoryRetentionTimeMs;
  private final ConcurrentMap<Integer, Long> _latestDemoteStartTimeMsByBrokerId;
  private final ConcurrentMap<Integer, Long> _latestRemoveStartTimeMsByBrokerId;
  private final ScheduledExecutorService _executionHistoryScannerExecutor;
  private UserTaskManager _userTaskManager;
  private final AnomalyDetectorManager _anomalyDetectorManager;
  private final ConcurrencyAdjuster _concurrencyAdjuster;
  private final ScheduledExecutorService _concurrencyAdjusterExecutor;
  private final ConcurrentMap<ConcurrencyType, Boolean> _concurrencyAdjusterEnabled;
  private volatile boolean _concurrencyAdjusterMinIsrCheckEnabled;
  private final TopicMinIsrCache _topicMinIsrCache;
  private final long _minExecutionProgressCheckIntervalMs;
  private final long _slowTaskAlertingBackoffTimeMs;
  private final KafkaCruiseControlConfig _config;
  private final AtomicDouble _partitionMovementCountPerSec;
  private final AtomicDouble _partitionMovementMbPerSec;
  // The timers _proposalExecutionTimerInvolveBrokerRemoval and _proposalExecutionTimerInvolveBrokerDemotionOnly
  // would be useful in estimating the cluster recover time during broker failures.
  // Proposal execution for broker demotion would be faster compared to that for broker removal in general,
  // so we have two metrics:
  // (1) timer for broker removal (does not matter whether there is also broker demotion or not)
  // (2) timer for broker demotion only
  // Timer of executing proposal involved with broker removal (may or may not with broker demotion).
  private final Timer _proposalExecutionTimerInvolveBrokerRemoval;
  // Timer of executing proposal involved with broker demotion only (no broker removal).
  private final Timer _proposalExecutionTimerInvolveBrokerDemotionOnly;
  private final Timer _proposalExecutionTimer;

  /**
   * The executor class that execute the proposals generated by optimizer.
   *
   * @param config The configurations for Cruise Control.
   */
  public Executor(KafkaCruiseControlConfig config,
                  Time time,
                  MetricRegistry dropwizardMetricRegistry,
                  AnomalyDetectorManager anomalyDetectorManager) {
    this(config, time, dropwizardMetricRegistry, null, null, null, anomalyDetectorManager);
  }

  /**
   * The executor class that execute the proposals generated by optimizer.
   * Package private for unit test.
   *
   * @param config The configurations for Cruise Control.
   */
  Executor(KafkaCruiseControlConfig config,
           Time time,
           MetricRegistry dropwizardMetricRegistry,
           AdminClient adminClient,
           MetadataAdminClient metadataClient,
           ExecutorNotifier executorNotifier,
           AnomalyDetectorManager anomalyDetectorManager) {
    _numExecutionStopped = new AtomicInteger(0);
    _numExecutionStoppedByUser = new AtomicInteger(0);
    _executionStoppedByUser = new AtomicBoolean(false);
    _ongoingExecutionIsBeingModified = new AtomicBoolean(false);
    _numExecutionStartedInKafkaAssignerMode = new AtomicInteger(0);
    _numExecutionStartedInNonKafkaAssignerMode = new AtomicInteger(0);
    _partitionMovementCountPerSec = new AtomicDouble(0);
    _partitionMovementMbPerSec = new AtomicDouble(0);
    _isKafkaAssignerMode = false;
    _skipInterBrokerReplicaConcurrencyAdjustment = false;
    ExecutionUtils.init(config);
    _config = config;

    _time = time;
    _adminClient = adminClient != null ? adminClient
      : KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _executionTaskManager = new ExecutionTaskManager(_adminClient, dropwizardMetricRegistry, time, config);
    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);
    _metadataClient = metadataClient != null ? metadataClient : new MetadataAdminClient(_adminClient);
    _defaultExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _executionProgressCheckIntervalMs = _defaultExecutionProgressCheckIntervalMs;
    _leaderMovementTimeoutMs = config.getLong(ExecutorConfig.LEADER_MOVEMENT_TIMEOUT_MS_CONFIG);
    _requestedExecutionProgressCheckIntervalMs = null;
    _proposalExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("ProposalExecutor", false, LOG));
    _latestDemoteStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _latestRemoveStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
    _stopSignal = new AtomicInteger(NO_STOP_EXECUTION);
    _hasOngoingExecution = false;
    _flipOngoingExecutionMutex = new Semaphore(1);
    _noOngoingExecutionSemaphore = new Semaphore(1);
    _uuid = null;
    _reasonSupplier = null;
    _executorNotifier = executorNotifier != null ? executorNotifier
                                                 : config.getConfiguredInstance(ExecutorConfig.EXECUTOR_NOTIFIER_CLASS_CONFIG,
                                                                                ExecutorNotifier.class);
    // Must be set before execution via #setUserTaskManager(UserTaskManager)
    _userTaskManager = null;
    if (anomalyDetectorManager == null) {
      throw new IllegalStateException("Anomaly detector manager cannot be null.");
    }
    _anomalyDetectorManager = anomalyDetectorManager;
    _demotionHistoryRetentionTimeMs = config.getLong(ExecutorConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG);
    _removalHistoryRetentionTimeMs = config.getLong(ExecutorConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG);
    _minExecutionProgressCheckIntervalMs = config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _slowTaskAlertingBackoffTimeMs = config.getLong(ExecutorConfig.SLOW_TASK_ALERTING_BACKOFF_TIME_MS_CONFIG);
    _concurrencyAdjusterEnabled = new ConcurrentHashMap<>(ConcurrencyType.cachedValues().size());
    _concurrencyAdjusterEnabled.put(ConcurrencyType.INTER_BROKER_REPLICA,
                                    config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_INTER_BROKER_REPLICA_ENABLED_CONFIG));
    _concurrencyAdjusterEnabled.put(ConcurrencyType.LEADERSHIP_CLUSTER,
                                    config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_LEADERSHIP_ENABLED_CONFIG));
    _concurrencyAdjusterEnabled.put(ConcurrencyType.LEADERSHIP_BROKER,
                                    config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_LEADERSHIP_PER_BROKER_ENABLED_CONFIG));
    // Support for intra-broker replica movement is pending https://github.com/linkedin/cruise-control/issues/1299.
    _concurrencyAdjusterEnabled.put(ConcurrencyType.INTRA_BROKER_REPLICA, false);
    _concurrencyAdjusterMinIsrCheckEnabled = config.getBoolean(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_ISR_CHECK_ENABLED_CONFIG);
    _concurrencyAdjusterExecutor = Executors.newSingleThreadScheduledExecutor(
        new KafkaCruiseControlThreadFactory(ConcurrencyAdjuster.class.getSimpleName()));
    int numMinIsrCheck = config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_NUM_MIN_ISR_CHECK_CONFIG);
    long intervalMs = config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG) / numMinIsrCheck;
    _concurrencyAdjuster = new ConcurrencyAdjuster(numMinIsrCheck);
    _topicMinIsrCache = new TopicMinIsrCache(Duration.ofMillis(config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_ISR_RETENTION_MS_CONFIG)),
                                             config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_ISR_CACHE_SIZE_CONFIG),
                                             ExecutionUtils.MIN_ISR_CACHE_CLEANER_PERIOD,
                                             ExecutionUtils.MIN_ISR_CACHE_CLEANER_INITIAL_DELAY,
                                             _time);
    _concurrencyAdjusterExecutor.scheduleAtFixedRate(_concurrencyAdjuster, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    _executionHistoryScannerExecutor = Executors.newSingleThreadScheduledExecutor(
        new KafkaCruiseControlThreadFactory(ExecutionHistoryScanner.class.getSimpleName()));
    _executionHistoryScannerExecutor.scheduleAtFixedRate(new ExecutionHistoryScanner(),
                                                         ExecutionUtils.EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS,
                                                         ExecutionUtils.EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS,
                                                         TimeUnit.SECONDS);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, PARTITION_MOVEMENT_PER_SEC),
                                      (Gauge<Double>) _partitionMovementCountPerSec::get);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, PARTITION_MOVEMENT_MB_PER_SEC),
                                      (Gauge<Double>) _partitionMovementMbPerSec::get);
    _proposalExecutionTimerInvolveBrokerRemoval = dropwizardMetricRegistry.timer(MetricRegistry.name(EXECUTOR_SENSOR,
        TIMER_PROPOSAL_EXECUTION_TIME_INVOLVE_BROKER_REMOVAL));
    _proposalExecutionTimerInvolveBrokerDemotionOnly = dropwizardMetricRegistry.timer(MetricRegistry.name(EXECUTOR_SENSOR,
        TIMER_PROPOSAL_EXECUTION_TIME_INVOLVE_BROKER_DEMOTION_ONLY));
    _proposalExecutionTimer = dropwizardMetricRegistry.timer(MetricRegistry.name(EXECUTOR_SENSOR,
        TIMER_PROPOSAL_EXECUTION_TIME));
  }

  /**
   * @return The tasks that are {@link ExecutionTaskState#IN_PROGRESS} or {@link ExecutionTaskState#ABORTING} for all task types.
   */
  public Set<ExecutionTask> inExecutionTasks() {
    return _executionTaskManager.inExecutionTasks();
  }

  /**
   * Dynamically set the interval between checking and updating (if needed) the progress of an initiated execution.
   * To prevent setting this value to a very small value by mistake, ensure that the requested interval is greater than
   * the {@link #_minExecutionProgressCheckIntervalMs}.
   *
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use {@link #_defaultExecutionProgressCheckIntervalMs}).
   */
  public synchronized void setRequestedExecutionProgressCheckIntervalMs(Long requestedExecutionProgressCheckIntervalMs) {
    if (requestedExecutionProgressCheckIntervalMs != null
        && requestedExecutionProgressCheckIntervalMs < _minExecutionProgressCheckIntervalMs) {
      throw new IllegalArgumentException("Attempt to set execution progress check interval ["
                                         + requestedExecutionProgressCheckIntervalMs
                                         + "ms] to smaller than the minimum execution progress check interval in cluster ["
                                         + _minExecutionProgressCheckIntervalMs + "ms].");
    }
    _requestedExecutionProgressCheckIntervalMs = requestedExecutionProgressCheckIntervalMs;
    setExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);
  }

  /**
   * Set _executionProgressCheckIntervalMs to initial value. _executionProgressCheckIntervalMs is currently used by both
   * inter broker move tasks and leadership move task. _executionProgressCheckIntervalMs is adjusted dynamically
   * at runtime when running inter broker move tasks based on whether a task batch has been completed within one check
   * interval, while it is not adjusted when running leadership move tasks. So we need to reset it to initial value after
   * inter broker tasks to have leadership move task running safely.
   */
  public synchronized void resetExecutionProgressCheckIntervalMs() {
    _executionProgressCheckIntervalMs = _requestedExecutionProgressCheckIntervalMs == null ? _defaultExecutionProgressCheckIntervalMs
        : _requestedExecutionProgressCheckIntervalMs;
    LOG.info("ExecutionProgressCheckInterval has reset to {}, which equals requestedExecutionProgressCheckIntervalMs, "
        + "or _defaultExecutionProgressCheckIntervalMs if no requested value", _executionProgressCheckIntervalMs);
  }

  /**
   * Dynamically set the interval between checking and updating (if needed) the progress of an initiated execution.
   * The value is rectified to _minExecutionProgressCheckIntervalMs if it is too small, and rectified to user's requested value if it is too big.
   *
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *    *                                                  execution
   */
  public synchronized void setExecutionProgressCheckIntervalMs(Long executionProgressCheckIntervalMs) {
    if (executionProgressCheckIntervalMs == null) {
      return;
    }

    final long prevExecutionProgressCheckIntervalMs = _executionProgressCheckIntervalMs;

    // Cap the check interval to requestedExecutionProgressCheckIntervalMs, or _defaultExecutionProgressCheckIntervalMs if no requested value
    _executionProgressCheckIntervalMs = Math.min(
        _requestedExecutionProgressCheckIntervalMs != null ? _requestedExecutionProgressCheckIntervalMs : _defaultExecutionProgressCheckIntervalMs,
        executionProgressCheckIntervalMs);

    // Make sure the check interval is not smaller than _minExecutionProgressCheckIntervalMs.
    _executionProgressCheckIntervalMs = Math.max(_minExecutionProgressCheckIntervalMs, _executionProgressCheckIntervalMs);

    if (prevExecutionProgressCheckIntervalMs != _executionProgressCheckIntervalMs) {
      LOG.info("ExecutionProgressCheckInterval changed from {} to {}", prevExecutionProgressCheckIntervalMs, _executionProgressCheckIntervalMs);
    }
  }

  /**
   * @return The interval between checking and updating (if needed) the progress of an initiated execution.
   */
  public long executionProgressCheckIntervalMs() {
    return _executionProgressCheckIntervalMs;
  }

  /**
   * Register gauge sensors.
   * @param dropwizardMetricRegistry The metric registry that holds all the metrics for monitoring Cruise Control.
   */
  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          ExecutionUtils.GAUGE_EXECUTION_STOPPED),
                                      (Gauge<Integer>) this::numExecutionStopped);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          ExecutionUtils.GAUGE_EXECUTION_STOPPED_BY_USER),
                                      (Gauge<Integer>) this::numExecutionStoppedByUser);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          ExecutionUtils.GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) this::numExecutionStartedInKafkaAssignerMode);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          ExecutionUtils.GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) this::numExecutionStartedInNonKafkaAssignerMode);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          ExecutionUtils.GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          ExecutionUtils.GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_GLOBAL_CAP),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager().maxClusterLeadershipMovements());
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_MAX_CONCURRENCY),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_MIN_CONCURRENCY),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_AVG_CONCURRENCY),
                                      (Gauge<Double>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_MAX_CONCURRENCY),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_MIN_CONCURRENCY),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_AVG_CONCURRENCY),
                                      (Gauge<Double>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_MAX_CONCURRENCY),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_MIN_CONCURRENCY),
                                      (Gauge<Integer>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR,
                                                          GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_AVG_CONCURRENCY),
                                      (Gauge<Double>) () -> _executionTaskManager
                                          .getExecutionConcurrencyManager()
                                          .getExecutionConcurrencySummary()
                                          .getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
  }

  private void removeExpiredDemotionHistory() {
    LOG.debug("Remove expired demotion history");
    _latestDemoteStartTimeMsByBrokerId.entrySet().removeIf(entry -> {
      long startTime = entry.getValue();
      return startTime != ExecutionUtils.PERMANENT_TIMESTAMP && startTime + _demotionHistoryRetentionTimeMs < _time.milliseconds();
    });
  }

  private void removeExpiredRemovalHistory() {
    LOG.debug("Remove expired broker removal history");
    _latestRemoveStartTimeMsByBrokerId.entrySet().removeIf(entry -> {
      long startTime = entry.getValue();
      return startTime != ExecutionUtils.PERMANENT_TIMESTAMP && startTime + _removalHistoryRetentionTimeMs < _time.milliseconds();
    });
  }

  /**
   * A runnable class to remove expired execution history.
   */
  private class ExecutionHistoryScanner implements Runnable {
    @Override
    public void run() {
      try {
        removeExpiredDemotionHistory();
        removeExpiredRemovalHistory();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to expire execution history.", t);
      }
    }
  }

  /**
   * A runnable class to auto-adjust the allowed reassignment concurrency for ongoing executions based on a version of
   * additive-increase/multiplicative-decrease (AIMD) feedback control algorithm. The adjustment decisions are made using:
   * <ul>
   *   <li>(At/Under)MinISR status of partitions (i.e. every time this runnable is called), and</li>
   *   <li>Selected metrics (i.e. only once for every {@link #_numMinIsrCheck} checks of this runnable and only when MinISR-based check
   *   does not generate a recommendation)</li>
   * </ul>
   */
  public class ConcurrencyAdjuster implements Runnable {
    private final int _numMinIsrCheck;
    private LoadMonitor _loadMonitor;
    private int _numChecks;
    private final ExecutionConcurrencyManager _executionConcurrencyManager;
    private volatile boolean _started;

    public ConcurrencyAdjuster(int numMinIsrCheck) {
      _numMinIsrCheck = numMinIsrCheck;
      _loadMonitor = null;
      _numChecks = 0;
      _executionConcurrencyManager = _executionTaskManager.getExecutionConcurrencyManager();
    }

    /**
     * Initialize the reassignment concurrency adjustment with the load monitor and the initially requested reassignment concurrency.
     *
     * @param loadMonitor Load monitor.
     * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
     *                                                         per broker(if null, use num.concurrent.partition.movements.per.broker).
     * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements
     *                                                         per broker(if null, use num.concurrent.intra.broker.partition.movements).
     * @param requestedClusterLeadershipMovementConcurrency The maximum number of concurrent leader movements in a cluster
     *                                               (if null, use num.concurrent.leader.movements).
     * @param requestedBrokerLeadershipMovementConcurrency The maximum number of concurrent leader movements involved in a broker
     *                                               (if null, use num.concurrent.leader.movements.per.broker).
     */
    public synchronized void initAdjustment(LoadMonitor loadMonitor,
                                            Integer requestedInterBrokerPartitionMovementConcurrency,
                                            Integer requestedIntraBrokerPartitionMovementConcurrency,
                                            Integer requestedClusterLeadershipMovementConcurrency,
                                            Integer requestedBrokerLeadershipMovementConcurrency) {
      _loadMonitor = loadMonitor;
      _executionConcurrencyManager.initialize(loadMonitor.brokersWithReplicas(MAX_METADATA_WAIT_MS),
                                              requestedInterBrokerPartitionMovementConcurrency,
                                              requestedIntraBrokerPartitionMovementConcurrency,
                                              requestedClusterLeadershipMovementConcurrency,
                                              requestedBrokerLeadershipMovementConcurrency);
      _started = true;
    }

    /**
     * Concurrency should be reset after each execution is done.
     */
    public synchronized void clearAdjustment() {
      _started = false;
      _executionConcurrencyManager.reset();
    }

    /**
     * Check if concurrency adjuster is started
     * @return true if concurrency adjuster is started.
     */
    public boolean isStarted() {
      return _started;
    }

    private boolean canRefreshConcurrency(ConcurrencyType concurrencyType) {
      LOG.debug("ConcurrencyAdjuster.canRefreshConcurrency, {}, {}, {}", _concurrencyAdjusterEnabled.get(concurrencyType),
               _loadMonitor, _stopSignal.get());
      if (!_concurrencyAdjusterEnabled.get(concurrencyType) || _loadMonitor == null || _stopSignal.get() != NO_STOP_EXECUTION) {
        return false;
      }
      switch (concurrencyType) {
        case LEADERSHIP_CLUSTER:
        case LEADERSHIP_BROKER:
          return _executorState.state() == LEADER_MOVEMENT_TASK_IN_PROGRESS;
        case INTER_BROKER_REPLICA:
          return _executorState.state() == ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS
                 && !_skipInterBrokerReplicaConcurrencyAdjustment;
        default:
          throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
      }
    }

    private synchronized void refreshConcurrency(boolean canRunMetricsBasedCheck, ConcurrencyType concurrencyType) {
      if (canRefreshConcurrency(concurrencyType)) {
        ConcurrencyAdjustingRecommendation concurrencyAdjustingRecommendation = minIsrBasedConcurrency();

        if (concurrencyAdjustingRecommendation.shouldStopExecution()) {
          LOG.info("Stop the ongoing execution as per recommendation of Concurrency Adjuster.");
          stopExecution();
          return;
        }

        // Only if ISR metrics suggest no change in concurrency, we will run broker-metric-based concurrency adjusting.
        // That is, if ISR metrics suggesting to decrease concurrency, will not check broker metrics for further adjusting.
        if (concurrencyAdjustingRecommendation.noChangeRecommended() && canRunMetricsBasedCheck) {
          concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(_loadMonitor.currentBrokerMetricValues());
        }

        for (int broker: concurrencyAdjustingRecommendation.getBrokersToIncreaseConcurrency()) {
          increaseExecutionBrokerConcurrency(broker, concurrencyType);
        }
        for (int broker: concurrencyAdjustingRecommendation.getBrokersToDecreaseConcurrency()) {
          decreaseExecutionBrokerConcurrency(broker, concurrencyType);
        }

        if (concurrencyType == ConcurrencyType.LEADERSHIP_BROKER && canRefreshConcurrency(ConcurrencyType.LEADERSHIP_CLUSTER)) {
          if (concurrencyAdjustingRecommendation.shouldIncreaseClusterConcurrency()) {
            increaseExecutionClusterLeadershipConcurrency();
          } else if (concurrencyAdjustingRecommendation.shouldDecreaseClusterConcurrency()) {
            decreaseExecutionClusterLeadershipConcurrency();
          }
        }
      }
    }

    private void maybeRetrieveAndCacheTopicMinIsr(Set<String> topicsToCheck) {
      if (topicsToCheck.isEmpty()) {
        return;
      }

      Set<ConfigResource> topicResourcesToCheck = new HashSet<>();
      topicsToCheck.forEach(t -> topicResourcesToCheck.add(new ConfigResource(ConfigResource.Type.TOPIC, t)));
      DescribeConfigsResult describeConfigsResult = _adminClient.describeConfigs(topicResourcesToCheck);
      _topicMinIsrCache.putTopicMinIsr(describeConfigsResult);
    }

    /**
     * Determine whether to stop execution, decrease concurrency, or no change based on (At/Under)MinISR status of partitions.
     * Note: no increase of concurrency would be recommended based on ISR.
     * @return {@code ConcurrencyAdjustingRecommendation.NO_CHANGE_RECOMMENDED} to indicate recommendation for no change in allowed
     * movement concurrency, {@code ConcurrencyAdjustingRecommendation.SHOULD_STOP_EXECUTION} to indicate recommendation to
     * cancel the execution
     */
    private ConcurrencyAdjustingRecommendation minIsrBasedConcurrency() {
      if (!_concurrencyAdjusterMinIsrCheckEnabled) {
        return ConcurrencyAdjustingRecommendation.NO_CHANGE_RECOMMENDED;
      }
      Cluster cluster = _loadMonitor.kafkaCluster();
      Map<String, TopicMinIsrCache.MinIsrWithTime> minIsrWithTimeByTopic = _topicMinIsrCache.minIsrWithTimeByTopic();
      Set<String> topicsToCheck = new HashSet<>(cluster.topics());
      topicsToCheck.removeAll(minIsrWithTimeByTopic.keySet());
      maybeRetrieveAndCacheTopicMinIsr(topicsToCheck);

      return ExecutionUtils.recommendedConcurrency(cluster, minIsrWithTimeByTopic);
    }

    private void decreaseExecutionBrokerConcurrency(int brokerId, ConcurrencyType concurrencyType) {
      int currentMovementConcurrency = _executionConcurrencyManager.getExecutionBrokerConcurrency(brokerId, concurrencyType);
      int decreasedConcurrency = getDecreasedConcurrency(currentMovementConcurrency, concurrencyType);
      if (decreasedConcurrency != currentMovementConcurrency) {
        _executionConcurrencyManager.setExecutionConcurrencyForBroker(brokerId, decreasedConcurrency, concurrencyType);
        LOG.info("Concurrency adjuster decreased the {} movement concurrency to {} for broker {}.",
            concurrencyType, decreasedConcurrency, brokerId);
      }
    }

    private void decreaseExecutionClusterLeadershipConcurrency() {
      ConcurrencyType concurrencyType = ConcurrencyType.LEADERSHIP_CLUSTER;
      int currentMovementConcurrency = _executionConcurrencyManager.getExecutionClusterLeadershipConcurrency();
      int decreasedConcurrency = getDecreasedConcurrency(currentMovementConcurrency, concurrencyType);
      if (decreasedConcurrency != currentMovementConcurrency) {
        _executionConcurrencyManager.setExecutionConcurrencyForAllBrokersOrCluster(decreasedConcurrency, concurrencyType);
        LOG.info("Concurrency adjuster decreased the {} movement concurrency to {}.", concurrencyType, decreasedConcurrency);
      }
    }

    private int getDecreasedConcurrency(int currentConcurrency, ConcurrencyType concurrencyType) {
      int minMovementsConcurrency = MIN_CONCURRENCY.get(concurrencyType);
      if (currentConcurrency > minMovementsConcurrency) {
        // Multiplicative-decrease reassignment concurrency (MIN: minMovementsConcurrency).
        return Math.max(minMovementsConcurrency, currentConcurrency / MULTIPLICATIVE_DECREASE.get(concurrencyType));
      }
      return currentConcurrency;
    }

    private void increaseExecutionBrokerConcurrency(int brokerId, ConcurrencyType concurrencyType) {
      int currentMovementConcurrency = _executionConcurrencyManager.getExecutionBrokerConcurrency(brokerId, concurrencyType);
      int increasedConcurrency = getIncreasedConcurrency(currentMovementConcurrency, concurrencyType);
      if (increasedConcurrency != currentMovementConcurrency) {
        _executionConcurrencyManager.setExecutionConcurrencyForBroker(brokerId, increasedConcurrency, concurrencyType);
        LOG.info("Concurrency adjuster increased the {} movement concurrency to {} for broker {}.",
            concurrencyType, increasedConcurrency, brokerId);
      }
    }

    private void increaseExecutionClusterLeadershipConcurrency() {
      ConcurrencyType concurrencyType = ConcurrencyType.LEADERSHIP_CLUSTER;
      int currentMovementConcurrency = _executionConcurrencyManager.getExecutionClusterLeadershipConcurrency();
      int increasedConcurrency = getIncreasedConcurrency(currentMovementConcurrency, concurrencyType);
      if (increasedConcurrency != currentMovementConcurrency) {
        _executionConcurrencyManager.setExecutionConcurrencyForAllBrokersOrCluster(increasedConcurrency, concurrencyType);
        LOG.info("Concurrency adjuster increased the {} movement concurrency to {}.", concurrencyType, increasedConcurrency);
      }
    }

    private int getIncreasedConcurrency(int currentConcurrency, ConcurrencyType concurrencyType) {
      int maxMovementsConcurrency = MAX_CONCURRENCY.get(concurrencyType);
      if (currentConcurrency < maxMovementsConcurrency) {
        // Additive-increase reassignment concurrency (MAX: maxMovementsConcurrency).
        return Math.min(maxMovementsConcurrency, currentConcurrency + ADDITIVE_INCREASE.get(concurrencyType));
      }
      return currentConcurrency;
    }

    @Override
    public void run() {
      try {
        if (_started) {
          boolean canRunMetricsBasedCheck = (_numChecks++ % _numMinIsrCheck) == 0;
          refreshConcurrency(canRunMetricsBasedCheck, ConcurrencyType.INTER_BROKER_REPLICA);
          // Both broker and cluster leadership movement concurrency can be refreshed with call below.
          refreshConcurrency(canRunMetricsBasedCheck, ConcurrencyType.LEADERSHIP_BROKER);
        }
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to adjust reassignment concurrency.", t);
      }
    }
  }

  /**
   * Recently demoted brokers are the ones
   * <ul>
   *   <li>for which a broker demotion was started, regardless of how the corresponding process was completed, or</li>
   *   <li>that are explicitly marked so -- e.g. via a pluggable component using {@link #addRecentlyDemotedBrokers(Set)}.</li>
   * </ul>
   *
   * @return IDs of recently demoted brokers -- i.e. demoted within the last {@link #_demotionHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyDemotedBrokers() {
    return Collections.unmodifiableSet(_latestDemoteStartTimeMsByBrokerId.keySet());
  }

  /**
   * Recently removed brokers are the ones
   * <ul>
   *   <li>for which a broker removal was started, regardless of how the corresponding process was completed, or</li>
   *   <li>that are explicitly marked so -- e.g. via a pluggable component using {@link #addRecentlyRemovedBrokers(Set)}.</li>
   * </ul>
   *
   * @return IDs of recently removed brokers -- i.e. removed within the last {@link #_removalHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyRemovedBrokers() {
    return Collections.unmodifiableSet(_latestRemoveStartTimeMsByBrokerId.keySet());
  }

  /**
   * Drop the given brokers from the recently removed brokers.
   *
   * @param brokersToDrop Brokers to drop from the {@link #_latestRemoveStartTimeMsByBrokerId}.
   * @return {@code true} if any elements were removed from {@link #_latestRemoveStartTimeMsByBrokerId}.
   */
  public boolean dropRecentlyRemovedBrokers(Set<Integer> brokersToDrop) {
    return _latestRemoveStartTimeMsByBrokerId.entrySet().removeIf(entry -> (brokersToDrop.contains(entry.getKey())));
  }

  /**
   * Drop the given brokers from the recently demoted brokers.
   *
   * @param brokersToDrop Brokers to drop from the {@link #_latestDemoteStartTimeMsByBrokerId}.
   * @return {@code true} if any elements were removed from {@link #_latestDemoteStartTimeMsByBrokerId}.
   */
  public boolean dropRecentlyDemotedBrokers(Set<Integer> brokersToDrop) {
    return _latestDemoteStartTimeMsByBrokerId.entrySet().removeIf(entry -> (brokersToDrop.contains(entry.getKey())));
  }

  /**
   * Add the given brokers to the recently removed brokers permanently -- i.e. until they are explicitly dropped by user.
   * If given set has brokers that were already removed recently, make them a permanent part of recently removed brokers.
   *
   * @param brokersToAdd Brokers to add to the {@link #_latestRemoveStartTimeMsByBrokerId}.
   */
  public void addRecentlyRemovedBrokers(Set<Integer> brokersToAdd) {
    brokersToAdd.forEach(brokerId -> _latestRemoveStartTimeMsByBrokerId.put(brokerId, ExecutionUtils.PERMANENT_TIMESTAMP));
  }

  /**
   * Add the given brokers from the recently demoted brokers permanently -- i.e. until they are explicitly dropped by user.
   * If given set has brokers that were already demoted recently, make them a permanent part of recently demoted brokers.
   *
   * @param brokersToAdd Brokers to add to the {@link #_latestDemoteStartTimeMsByBrokerId}.
   */
  public void addRecentlyDemotedBrokers(Set<Integer> brokersToAdd) {
    brokersToAdd.forEach(brokerId -> _latestDemoteStartTimeMsByBrokerId.put(brokerId, ExecutionUtils.PERMANENT_TIMESTAMP));
  }

  /**
   * @return The current executor state.
   */
  public ExecutorState state() {
    return _executorState;
  }

  /**
   * Enable or disable concurrency adjuster for the given concurrency type in the executor.
   *
   * <ul>
   *   <li>TODO: Support for concurrency adjuster of {@link ConcurrencyType#INTRA_BROKER_REPLICA intra-broker replica movement}
   *   is pending <a href="https://github.com/linkedin/cruise-control/issues/1299">#1299</a>.</li>
   * </ul>
   * @param concurrencyType Type of concurrency for which to enable or disable concurrency adjuster.
   * @param isConcurrencyAdjusterEnabled {@code true} if concurrency adjuster is enabled for the given type, {@code false} otherwise.
   * @return {@code true} if concurrency adjuster was enabled before for the given concurrency type, {@code false} otherwise.
   */
  public Boolean setConcurrencyAdjusterFor(ConcurrencyType concurrencyType, boolean isConcurrencyAdjusterEnabled) {
    if (concurrencyType != ConcurrencyType.INTER_BROKER_REPLICA && concurrencyType != ConcurrencyType.LEADERSHIP_CLUSTER
        && concurrencyType != ConcurrencyType.LEADERSHIP_BROKER) {
      throw new IllegalArgumentException(String.format("Concurrency adjuster for %s is not yet supported.", concurrencyType));
    }
    return _concurrencyAdjusterEnabled.put(concurrencyType, isConcurrencyAdjusterEnabled);
  }

  /**
   * Enable or disable (At/Under)MinISR-based concurrency adjustment.
   *
   * @param isMinIsrBasedConcurrencyAdjustmentEnabled {@code true} to enable (At/Under)MinISR-based concurrency adjustment, {@code false} otherwise.
   * @return {@code true} if (At/Under)MinISR-based concurrency adjustment was enabled before, {@code false} otherwise.
   */
  public boolean setConcurrencyAdjusterMinIsrCheck(boolean isMinIsrBasedConcurrencyAdjustmentEnabled) {
    boolean oldValue = _concurrencyAdjusterMinIsrCheckEnabled;
    _concurrencyAdjusterMinIsrCheckEnabled = isMinIsrBasedConcurrencyAdjustmentEnabled;
    return oldValue;
  }

  /**
   * Initialize proposal execution and start execution.
   *
   * @param proposals Proposals to be executed.
   * @param unthrottledBrokers Brokers that are not throttled in terms of the number of in/out replica movements.
   * @param removedBrokers Removed brokers, null if no brokers has been removed.
   * @param loadMonitor Load monitor.
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker(if null, use num.concurrent.partition.movements.per.broker).
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements
   *                                                         (if null, use num.concurrent.intra.broker.partition.movements).
   * @param requestedMaxClusterPartitionMovements The upper bound of concurrent inter broker partition movements in cluster
   *                                              (if null, use max.num.cluster.partition.movements).
   * @param requestedClusterLeadershipMovementConcurrency The maximum number of concurrent leader movements in a cluster
   *                                               (if null, use num.concurrent.leader.movements).
   * @param requestedBrokerLeadershipMovementConcurrency The maximum number of concurrent leader movements involved in a broker
   *                                               (if null, use num.concurrent.leader.movements.per.broker).
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing a proposal (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   * @param skipInterBrokerReplicaConcurrencyAdjustment {@code true} to skip auto adjusting concurrency of inter-broker
   * replica movements even if the concurrency adjuster is enabled, {@code false} otherwise.
   */
  public synchronized void executeProposals(Collection<ExecutionProposal> proposals,
                                            Set<Integer> unthrottledBrokers,
                                            Set<Integer> removedBrokers,
                                            LoadMonitor loadMonitor,
                                            Integer requestedInterBrokerPartitionMovementConcurrency,
                                            Integer requestedMaxClusterPartitionMovements,
                                            Integer requestedIntraBrokerPartitionMovementConcurrency,
                                            Integer requestedClusterLeadershipMovementConcurrency,
                                            Integer requestedBrokerLeadershipMovementConcurrency,
                                            Long requestedExecutionProgressCheckIntervalMs,
                                            ReplicaMovementStrategy replicaMovementStrategy,
                                            Long replicationThrottle,
                                            boolean isTriggeredByUserRequest,
                                            String uuid,
                                            boolean isKafkaAssignerMode,
                                            boolean skipInterBrokerReplicaConcurrencyAdjustment) throws OngoingExecutionException {
    setExecutionMode(isKafkaAssignerMode);
    sanityCheckExecuteProposals(loadMonitor, uuid);
    _skipInterBrokerReplicaConcurrencyAdjustment = skipInterBrokerReplicaConcurrencyAdjustment;
    try {
      initProposalExecution(proposals, unthrottledBrokers, requestedInterBrokerPartitionMovementConcurrency, requestedMaxClusterPartitionMovements,
                            requestedIntraBrokerPartitionMovementConcurrency, requestedClusterLeadershipMovementConcurrency,
                            requestedBrokerLeadershipMovementConcurrency, requestedExecutionProgressCheckIntervalMs, replicaMovementStrategy,
                            isTriggeredByUserRequest, loadMonitor);
      startExecution(loadMonitor, null, removedBrokers, replicationThrottle, isTriggeredByUserRequest);
    } catch (Exception e) {
      if (e instanceof OngoingExecutionException) {
        LOG.info("User task {}: Broker removal operation aborted due to ongoing execution", uuid);
      } else {
        LOG.error("User task {}: Broker removal operation failed due to exception", uuid, e);
      }
      processExecuteProposalsFailure();
      throw e;
    }
  }

  private void sanityCheckExecuteProposals(LoadMonitor loadMonitor, String uuid) throws OngoingExecutionException {
    if (_hasOngoingExecution) {
      throw new OngoingExecutionException("Cannot execute new proposals while there is an ongoing execution.");
    }
    if (loadMonitor == null) {
      throw new IllegalArgumentException("Load monitor cannot be null.");
    }
    if (_executorState.state() != GENERATING_PROPOSALS_FOR_EXECUTION) {
      throw new IllegalStateException(String.format("Unexpected executor state %s. Initializing proposal execution requires"
                                                    + " generating proposals for execution.", _executorState.state()));
    }
    if (uuid == null || !uuid.equals(_uuid)) {
      throw new IllegalStateException(String.format("Attempt to initialize proposal execution with a UUID %s that differs from"
                                                    + " the UUID used for generating proposals for execution %s.", uuid, _uuid));
    }
  }

  private synchronized void initProposalExecution(Collection<ExecutionProposal> proposals,
                                                  Collection<Integer> brokersToSkipConcurrencyCheck,
                                                  Integer requestedInterBrokerPartitionMovementConcurrency,
                                                  Integer requestedMaxInterBrokerPartitionMovements,
                                                  Integer requestedIntraBrokerPartitionMovementConcurrency,
                                                  Integer requestedClusterLeadershipMovementConcurrency,
                                                  Integer requestedBrokerLeadershipMovementConcurrency,
                                                  Long requestedExecutionProgressCheckIntervalMs,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  boolean isTriggeredByUserRequest,
                                                  LoadMonitor loadMonitor) {
    _executorState = ExecutorState.initializeProposalExecution(_uuid, _reasonSupplier.get(), recentlyDemotedBrokers(),
                                                               recentlyRemovedBrokers(), isTriggeredByUserRequest);
    _executionTaskManager.setExecutionModeForTaskTracker(_isKafkaAssignerMode);
    // Get a snapshot of (1) cluster and (2) minIsr with time by topic name.
    StrategyOptions strategyOptions = new StrategyOptions.Builder(_metadataClient.cluster())
        .minIsrWithTimeByTopic(_topicMinIsrCache.minIsrWithTimeByTopic()).build();
    _executionTaskManager.addExecutionProposals(proposals, brokersToSkipConcurrencyCheck, strategyOptions, replicaMovementStrategy);
    _concurrencyAdjuster.initAdjustment(loadMonitor,
                                        requestedInterBrokerPartitionMovementConcurrency,
                                        requestedIntraBrokerPartitionMovementConcurrency,
                                        requestedClusterLeadershipMovementConcurrency,
                                        requestedBrokerLeadershipMovementConcurrency);
    setRequestedMaxInterBrokerPartitionMovements(requestedMaxInterBrokerPartitionMovements);
    setRequestedExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);
  }

  /**
   * Initialize proposal execution and start demotion.
   *
   * @param proposals Proposals to be executed.
   * @param demotedBrokers Demoted brokers.
   * @param loadMonitor Load monitor.
   * @param concurrentSwaps The number of concurrent swap operations per broker.
   * @param requestedClusterLeadershipMovementConcurrency The maximum number of concurrent leader movements in a cluster
   *                                                      (if null, use num.concurrent.leader.movements).
   * @param requestedBrokerLeadershipMovementConcurrency The maximum number of concurrent leader movements involved in a broker
   *                                                     (if null, use num.concurrent.leader.movements.per.broker).
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use execution.progress.check.interval.ms).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            while executing demotion proposals (if null, no throttling is applied).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   */
  public synchronized void executeDemoteProposals(Collection<ExecutionProposal> proposals,
                                                  Collection<Integer> demotedBrokers,
                                                  LoadMonitor loadMonitor,
                                                  Integer concurrentSwaps,
                                                  Integer requestedClusterLeadershipMovementConcurrency,
                                                  Integer requestedBrokerLeadershipMovementConcurrency,
                                                  Long requestedExecutionProgressCheckIntervalMs,
                                                  ReplicaMovementStrategy replicaMovementStrategy,
                                                  Long replicationThrottle,
                                                  boolean isTriggeredByUserRequest,
                                                  String uuid) throws OngoingExecutionException {
    setExecutionMode(false);
    sanityCheckExecuteProposals(loadMonitor, uuid);
    _skipInterBrokerReplicaConcurrencyAdjustment = true;
    try {
      initProposalExecution(proposals, demotedBrokers, concurrentSwaps, null, 0,
                            requestedClusterLeadershipMovementConcurrency, requestedBrokerLeadershipMovementConcurrency,
                            requestedExecutionProgressCheckIntervalMs, replicaMovementStrategy, isTriggeredByUserRequest, loadMonitor);
      startExecution(loadMonitor, demotedBrokers, null, replicationThrottle, isTriggeredByUserRequest);
    } catch (Exception e) {
      processExecuteProposalsFailure();
      throw e;
    }
  }

  /**
   * Dynamically set the per broker movement concurrency of the given type for all brokers or set the cluster concurrency.
   *
   * @param concurrency The maximum number of concurrent movements.
   * @param concurrencyType The type of concurrency for which the requested movement concurrency will be set.
   */
  public synchronized void setExecutionConcurrencyForAllBrokersOrCluster(Integer concurrency, ConcurrencyType concurrencyType) {
    _executionTaskManager.getExecutionConcurrencyManager().setExecutionConcurrencyForAllBrokersOrCluster(concurrency, concurrencyType);
  }

  /**
   * Dynamically set the movement concurrency of the given type.
   *
   * @param brokerId The brokerId to set execution concurrency
   * @param concurrency The maximum number of concurrent movements.
   * @param concurrencyType The type of concurrency for which the requested movement concurrency will be set.
   */
  public synchronized void setExecutionConcurrencyForBroker(int brokerId, Integer concurrency, ConcurrencyType concurrencyType) {
    _executionTaskManager.getExecutionConcurrencyManager().setExecutionConcurrencyForBroker(brokerId, concurrency, concurrencyType);
  }

  /**
   * Dynamically set the max inter-broker partition movements per cluster.
   *
   * @param requestedMaxInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements
   *                                                         per cluster.
   */
  public void setRequestedMaxInterBrokerPartitionMovements(Integer requestedMaxInterBrokerPartitionMovements) {
    _executionTaskManager.getExecutionConcurrencyManager().
        setClusterInterBrokerPartitionMovementConcurrency(requestedMaxInterBrokerPartitionMovements);
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   */
  private void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Allow a reference to {@link UserTaskManager} to be set externally and allow executor to retrieve information
   * about the request that generated execution.
   * @param userTaskManager a reference to {@link UserTaskManager} instance.
   */
  public void setUserTaskManager(UserTaskManager userTaskManager) {
    _userTaskManager = userTaskManager;
  }

  private int numExecutionStopped() {
    return _numExecutionStopped.get();
  }

  private int numExecutionStoppedByUser() {
    return _numExecutionStoppedByUser.get();
  }

  private int numExecutionStartedInKafkaAssignerMode() {
    return _numExecutionStartedInKafkaAssignerMode.get();
  }

  private int numExecutionStartedInNonKafkaAssignerMode() {
    return _numExecutionStartedInNonKafkaAssignerMode.get();
  }

  /**
   * Pause the load monitor and kick off the execution.
   *
   * @param loadMonitor Load monitor.
   * @param demotedBrokers Brokers to be demoted, null if no broker has been demoted.
   * @param removedBrokers Brokers to be removed, null if no broker has been removed.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            while moving partitions (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   */
  private void startExecution(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle,
                              boolean isTriggeredByUserRequest) throws OngoingExecutionException {
    _executionStoppedByUser.set(false);
    sanityCheckOngoingMovement();

    try {
      _flipOngoingExecutionMutex.acquire();
      try {
        _hasOngoingExecution = true;
        _stopSignal.set(NO_STOP_EXECUTION);
        try {
          // Wait if Executor is shutting down.
          _noOngoingExecutionSemaphore.acquire();
          // Block Executor from shutting down until ongoing execution stopped.
        } catch (final InterruptedException ie) {
          _hasOngoingExecution = false;
          throw new IllegalStateException("Interrupted while shutting down executor.");
        }
      } finally {
        _flipOngoingExecutionMutex.release();
      }
    } catch (final InterruptedException ie) {
      // handle acquire failure for _flipOngoingExecutionMutex here
      _hasOngoingExecution = false;
      throw new IllegalStateException("Interrupted while shutting down executor.");
    }

    _anomalyDetectorManager.maybeClearOngoingAnomalyDetectionTimeMs();
    _anomalyDetectorManager.resetHasUnfixableGoals();
    _executionStoppedByUser.set(false);
    if (_isKafkaAssignerMode) {
      _numExecutionStartedInKafkaAssignerMode.incrementAndGet();
    } else {
      _numExecutionStartedInNonKafkaAssignerMode.incrementAndGet();
    }
    _proposalExecutor.execute(
        new ProposalExecutionRunnable(loadMonitor, demotedBrokers, removedBrokers, replicationThrottle, isTriggeredByUserRequest));
  }

  /**
   * Sanity check whether there are ongoing inter-broker or intra-broker replica movements.
   * This check ensures lack of ongoing movements started by external agents -- i.e. not started by this Executor.
   */
  private void sanityCheckOngoingMovement() throws OngoingExecutionException {
    boolean hasOngoingPartitionReassignments;
    Map<TopicPartition, PartitionReassignment> ongoingPartitionReassignments;
    try {
       ongoingPartitionReassignments = ExecutionUtils.ongoingPartitionReassignments(_adminClient);
       hasOngoingPartitionReassignments = !ongoingPartitionReassignments.keySet().isEmpty();
    } catch (TimeoutException | InterruptedException | ExecutionException e) {
      // This may indicate transient (e.g. network) issues.
      throw new IllegalStateException("Failed to retrieve if there are already ongoing partition reassignments.", e);
    }
    // Note that in case there is an ongoing partition reassignment, we do not unpause metric sampling.
    if (hasOngoingPartitionReassignments) {
      throw new OngoingExecutionException("There are ongoing inter-broker partition movements: " + ongoingPartitionReassignments);
    } else {
      boolean hasOngoingIntraBrokerReplicaMovement;
      try {
        hasOngoingIntraBrokerReplicaMovement = hasOngoingIntraBrokerReplicaMovement(_adminClient, _config);
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        // This may indicate transient (e.g. network) issues.
        throw new IllegalStateException("Failed to retrieve if there are already ongoing intra-broker replica reassignments.", e);
      }
      if (hasOngoingIntraBrokerReplicaMovement) {
        throw new OngoingExecutionException("There are ongoing intra-broker partition movements.");
      }
    }
  }

  private void processExecuteProposalsFailure() {
    _executionTaskManager.clear();
    _uuid = null;
    _reasonSupplier = null;
    _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
  }

  /**
   * Notify the executor on starting to generate proposals for execution with the given uuid and reason supplier.
   * The executor must be in {@link ExecutorState.State#NO_TASK_IN_PROGRESS} state for this operation to succeed.
   *
   * @param uuid UUID of the current execution.
   * @param reasonSupplier Reason supplier for the execution.
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   */
  public synchronized void setGeneratingProposalsForExecution(String uuid, Supplier<String> reasonSupplier, boolean isTriggeredByUserRequest)
      throws OngoingExecutionException {
    ExecutorState.State currentExecutorState = _executorState.state();
    if (currentExecutorState != NO_TASK_IN_PROGRESS) {
      throw new OngoingExecutionException(String.format("Cannot generate proposals while the executor is in %s state.", currentExecutorState));
    }

    _uuid = validateNotNull(uuid, "UUID of the execution cannot be null.");
    _reasonSupplier = validateNotNull(reasonSupplier, "Reason supplier cannot be null.");
    _executorState = ExecutorState.generatingProposalsForExecution(_uuid, _reasonSupplier.get(), recentlyDemotedBrokers(),
                                                                   recentlyRemovedBrokers(), isTriggeredByUserRequest);
  }

  /**
   * Notify the executor on the failure to generate proposals for execution with the given uuid.
   * The executor may stuck in {@link ExecutorState.State#GENERATING_PROPOSALS_FOR_EXECUTION} state if this call is omitted.
   *
   * This is a no-op if called when
   * <ul>
   *   <li>Not generating proposals for execution: This is because all other execution failures after generating proposals
   *   for execution are already handled by the executor.</li>
   *   <li>Generating proposals with a different UUID: This indicates an error in the caller-side logic, and is not
   *   supposed to happen because at any given time proposals shall be generated for only one request -- for detecting
   *   such presumably benign misuse (if any), this case is logged as a warning.</li>
   * </ul>
   * @param uuid UUID of the failed proposal generation for execution.
   */
  public synchronized void failGeneratingProposalsForExecution(String uuid) {
    if (_executorState.state() == GENERATING_PROPOSALS_FOR_EXECUTION) {
      if (uuid != null && uuid.equals(_uuid)) {
        LOG.warn("Failed to generate proposals for execution (UUID: {} reason: {}).", uuid, _reasonSupplier.get());
        _uuid = null;
        _reasonSupplier = null;
        _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
      } else {
        LOG.warn("UUID mismatch in attempt to report failure to generate proposals (received: {} expected: {})", uuid, _uuid);
      }
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @param stopExternalAgent Whether to stop ongoing execution started by external agents.
   */
  public synchronized void userTriggeredStopExecution(boolean stopExternalAgent) {
    if (stopExecution()) {
      LOG.info("User task {}: User requested to stop the ongoing proposal execution.", _uuid);
      _numExecutionStoppedByUser.incrementAndGet();
      _executionStoppedByUser.set(true);
    }
    if (stopExternalAgent) {
      if (maybeStopExternalAgent()) {
        LOG.info("User task {}: The request to stop ongoing external agent partition reassignment is submitted successfully.",
            _uuid);
      }
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @return {@code true} if the flag to stop the execution is set after the call (i.e. was not set already), {@code false} otherwise.
   */
  private synchronized boolean stopExecution() {
    if (_stopSignal.compareAndSet(NO_STOP_EXECUTION, STOP_EXECUTION)) {
      _numExecutionStopped.incrementAndGet();
      _executionTaskManager.setStopRequested();
      return true;
    }
    return false;
  }

  /**
   * Shutdown the executor.
   */
  public synchronized void shutdown() {
    LOG.info("Shutting down executor.");

    try {
      _flipOngoingExecutionMutex.acquire();
      try {
        if (_hasOngoingExecution) {
          LOG.warn("Shutdown executor may take long because execution is still in progress.");
          stopExecution();
        }
        try {
          LOG.info("Waiting for ongoing execution to stop completely if any.");
          _noOngoingExecutionSemaphore.acquire();
          LOG.info("Blocking new execution from proceeding.");
        } catch (final InterruptedException ie) {
          LOG.warn("Interrupted while waiting for ongoing execution to stop.");
        }
      } finally {
        _flipOngoingExecutionMutex.release();
      }
    } catch (final InterruptedException ie) {
      // handle acquire failure for _flipOngoingExecutionMutex
      LOG.warn("Interrupted while checking if there is ongoing execution to stop.");
    }

    _proposalExecutor.shutdown();

    try {
      _proposalExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for anomaly detector to shutdown.");
    }
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    _executionHistoryScannerExecutor.shutdownNow();
    _concurrencyAdjusterExecutor.shutdownNow();
    _topicMinIsrCache.shutdown();
    LOG.info("Executor shutdown completed.");
  }

  /**
   * Let executor know the intention regarding modifying the ongoing execution. Only one request at a given time is
   * allowed to modify the ongoing execution.
   *
   * @param modify {@code true} to indicate, {@code false} to cancel the intention to modify
   * @return {@code true} if the intention changes the state known by executor, {@code false} otherwise.
   */
  public boolean modifyOngoingExecution(boolean modify) {
    return _ongoingExecutionIsBeingModified.compareAndSet(!modify, modify);
  }

  /**
   * Whether there is an ongoing operation triggered by current Cruise Control deployment.
   *
   * @return {@code true} if there is an ongoing execution.
   */
  public boolean hasOngoingExecution() {
    return _hasOngoingExecution;
  }

  /**
   * List ongoing partition reassignment on Kafka cluster.
   *
   * Note that an empty set response does not guarantee lack of an ongoing execution because when there is an ongoing
   * execution inside Cruise Control, partition reassignment task batches are sent to Kafka periodically. So, there will
   * be intervals without partition reassignments.
   *
   * @return TopicPartitions of the ongoing partition reassignment on Kafka cluster.
   */
  public Set<TopicPartition> listPartitionsBeingReassigned() throws InterruptedException, ExecutionException, TimeoutException {
    return ExecutionUtils.partitionsBeingReassigned(_adminClient);
  }

  /**
   * Check whether there is an ongoing partition reassignment on Kafka cluster.
   *
   * Note that a {@code false} response does not guarantee lack of an ongoing execution because when there is an ongoing
   * execution inside Cruise Control, partition reassignment task batches are sent to Kafka periodically. So, there will
   * be intervals without partition reassignments.
   *
   * @return {@code true} if there is an ongoing partition reassignment on Kafka cluster.
   */
  public boolean hasOngoingPartitionReassignments() throws InterruptedException, ExecutionException, TimeoutException {
    return !ExecutionUtils.partitionsBeingReassigned(_adminClient).isEmpty();
  }

  /**
   * Stop the inter-broker replica reassignments started by an external agent.
   *
   * @return {@code true} if the stop ongoing external agent partition reassignment request is submitted successfully,
   * {@code false} if there isn't any ongoing external agent partition reassignments on Kafka cluster.
   */
  public boolean maybeStopExternalAgent() {
    if (_hasOngoingExecution) {
      // Current CC is executing. The chances are low for any external agents to execute. Skip the rest of the check.
      return false;
    }
    return ExecutionUtils.maybeStopPartitionReassignment(_adminClient) != null;
  }

  /**
   * Check if the concurrency manager is initialized
   * @return true if concurrency manager is initialized
   */
  public boolean isConcurrencyManagerInitialized() {
    return _executionTaskManager.getExecutionConcurrencyManager().isInitialized();
  }

  /**
   * Check if concurrency adjuster is started
   * @return true if concurrency adjuster is started.
   */
  public boolean isConcurrencyAdjusterStarted() {
    return _concurrencyAdjuster.isStarted();
  }

  /**
   * This class is thread safe.
   *
   * Note that once the thread for {@link ProposalExecutionRunnable} is submitted for running, the variable
   * _executionTaskManager can only be written within this inner class, but not from the outer Executor class.
   */
  private class ProposalExecutionRunnable implements Runnable {
    private final LoadMonitor _loadMonitor;
    private final Set<Integer> _recentlyDemotedBrokers;
    private final Set<Integer> _recentlyRemovedBrokers;
    private final Long _replicationThrottle;
    private Throwable _executionException;
    private final boolean _isTriggeredByUserRequest;
    private long _lastSlowTaskReportingTimeMs;
    private static final boolean FORCE_PAUSE_SAMPLING = true;
    private final Timer _executionTimerInvolveBrokerRemovalOrDemotion;

    private final Collection<Integer> _demotedBrokers;

    private final Collection<Integer> _removedBrokers;

    ProposalExecutionRunnable(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle,
                              boolean isTriggeredByUserRequest) {
      _loadMonitor = loadMonitor;
      _demotedBrokers = demotedBrokers;
      _removedBrokers = removedBrokers;
      _executionException = null;
      if (isTriggeredByUserRequest && _userTaskManager == null) {
        processExecuteProposalsFailure();
        _hasOngoingExecution = false;
        _noOngoingExecutionSemaphore.release();
        _stopSignal.set(NO_STOP_EXECUTION);
        _executionStoppedByUser.set(false);
        LOG.error("User task {}: Failed to initialize proposal execution.", _uuid);
        throw new IllegalStateException("User task manager cannot be null.");
      }
      if (_demotedBrokers != null) {
        // Add/overwrite the latest demotion time of (non-permanent) demoted brokers (if any).
        _demotedBrokers.forEach(id -> {
          Long demoteStartTime = _latestDemoteStartTimeMsByBrokerId.get(id);
          if (demoteStartTime == null || demoteStartTime != ExecutionUtils.PERMANENT_TIMESTAMP) {
            _latestDemoteStartTimeMsByBrokerId.put(id, _time.milliseconds());
          }
        });
      }
      if (_removedBrokers != null) {
        // Add/overwrite the latest removal time of (non-permanent) removed brokers (if any).
        _removedBrokers.forEach(id -> {
          Long removeStartTime = _latestRemoveStartTimeMsByBrokerId.get(id);
          if (removeStartTime == null || removeStartTime != ExecutionUtils.PERMANENT_TIMESTAMP) {
            _latestRemoveStartTimeMsByBrokerId.put(id, _time.milliseconds());
          }
        });
      }
      _recentlyDemotedBrokers = recentlyDemotedBrokers();
      _recentlyRemovedBrokers = recentlyRemovedBrokers();
      _replicationThrottle = replicationThrottle;
      _isTriggeredByUserRequest = isTriggeredByUserRequest;
      _lastSlowTaskReportingTimeMs = -1L;
      if (_removedBrokers != null && !_removedBrokers.isEmpty()) {
        _executionTimerInvolveBrokerRemovalOrDemotion = _proposalExecutionTimerInvolveBrokerRemoval;
      } else if (_demotedBrokers != null && !_demotedBrokers.isEmpty()) {
        _executionTimerInvolveBrokerRemovalOrDemotion = _proposalExecutionTimerInvolveBrokerDemotionOnly;
      } else {
        _executionTimerInvolveBrokerRemovalOrDemotion = null;
      }
    }

    public void run() {
      LOG.info("User task {}: Starting executing balancing proposals.", _uuid);
      final long start = System.currentTimeMillis();
      try {
        UserTaskManager.UserTaskInfo userTaskInfo = initExecution();
        execute(userTaskInfo);
      } catch (Exception e) {
        LOG.error("User task {}: ProposalExecutionRunnable got exception during run", _uuid, e);
      } finally {
        final long duration = System.currentTimeMillis() - start;
        _proposalExecutionTimer.update(duration, TimeUnit.MILLISECONDS);
        if (_executionTimerInvolveBrokerRemovalOrDemotion != null) {
          _executionTimerInvolveBrokerRemovalOrDemotion.update(duration, TimeUnit.MILLISECONDS);
        }
        String executionStatusString = String.format("removed brokers: %s; demoted brokers: %s; total time used: %dms.",
                                               _removedBrokers,
                                               _demotedBrokers,
                                               duration
                                               );
        if (_executionException != null) {
          LOG.info("User task {}: Execution failed: {}. Exception: {}", _uuid, executionStatusString, _executionException.getMessage());
        } else {
          LOG.info("User task {}: Execution succeeded: {}. ", _uuid, executionStatusString);
        }
        // Clear completed execution.
        clearCompletedExecution();
      }
    }

    private UserTaskManager.UserTaskInfo initExecution() {
      UserTaskManager.UserTaskInfo userTaskInfo = null;
      // If the task is triggered from a user request, mark the task to be in-execution state in user task manager and
      // retrieve the associated user task information.
      if (_isTriggeredByUserRequest) {
        userTaskInfo = _userTaskManager.markTaskExecutionBegan(_uuid);
      }
      String reason = _reasonSupplier.get();
      _executorState = ExecutorState.executionStarting(_uuid, reason, _recentlyDemotedBrokers, _recentlyRemovedBrokers, _isTriggeredByUserRequest);
      OPERATION_LOG.info("Task [{}] execution starts. The reason of execution is {}.", _uuid, reason);
      _ongoingExecutionIsBeingModified.set(false);
      return userTaskInfo;
    }

    /**
     * Unless the metric sampling is already in the desired mode:
     * <ul>
     *   <li>Pause the metric sampling</li>
     *   <li>Set the metric sampling mode to retrieve desired samples</li>
     *   <li>Resume the metric sampling</li>
     * </ul>
     */
    private void adjustSamplingModeBeforeExecution() throws InterruptedException {
      // Pause the partition metric sampling to avoid the loss of accuracy during execution.
      while (_loadMonitor.samplingMode() != ONGOING_EXECUTION) {
        try {
          String reasonForPause = String.format("Paused-By-Cruise-Control-Before-Starting-Execution (Date: %s)", currentUtcDate());
          _loadMonitor.pauseMetricSampling(reasonForPause, FORCE_PAUSE_SAMPLING);
          // Set the metric sampling mode to retrieve broker samples only.
          _loadMonitor.setSamplingMode(ONGOING_EXECUTION);
          break;
        } catch (IllegalStateException e) {
          Thread.sleep(executionProgressCheckIntervalMs());
          LOG.debug("Waiting for the load monitor to be ready to adjust sampling mode.", e);
        }
      }
      // Resume the metric sampling.
      _loadMonitor.resumeMetricSampling(String.format("Resumed-By-Cruise-Control-Before-Starting-Execution (Date: %s)", currentUtcDate()));
    }

    /**
     * Start the actual execution of the proposals in order:
     * <ol>
     *   <li>Inter-broker move replicas.</li>
     *   <li>Intra-broker move replicas.</li>
     *   <li>Transfer leadership.</li>
     * </ol>
     *
     * @param userTaskInfo The task information if the task is triggered from a user request, {@code null} otherwise.
     */
    private void execute(UserTaskManager.UserTaskInfo userTaskInfo) {
      try {
        // Ensure that sampling mode is adjusted properly to avoid the loss of partition metric accuracy
        // and enable the collection of broker metric samples during an ongoing execution.
        adjustSamplingModeBeforeExecution();

        // 1. Inter-broker move replicas if possible.
        if (_executorState.state() == STARTING_EXECUTION) {
          _executorState = ExecutorState.operationInProgress(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(INTER_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.getExecutionConcurrencyManager()
                                                                                  .getExecutionConcurrencySummary(),
                                                             _uuid,
                                                             _reasonSupplier.get(),
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers,
                                                             _isTriggeredByUserRequest);
          interBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 2. Intra-broker move replicas if possible.
        if (_executorState.state() == INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _executorState = ExecutorState.operationInProgress(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.getExecutionConcurrencyManager()
                                                                                  .getExecutionConcurrencySummary(),
                                                             _uuid,
                                                             _reasonSupplier.get(),
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers,
                                                             _isTriggeredByUserRequest);
          intraBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 3. Transfer leadership if possible.
        if (_executorState.state() == INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _executorState = ExecutorState.operationInProgress(LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(LEADER_ACTION)),
                                                             _executionTaskManager.getExecutionConcurrencyManager()
                                                                                  .getExecutionConcurrencySummary(),
                                                             _uuid,
                                                             _reasonSupplier.get(),
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers,
                                                             _isTriggeredByUserRequest);
          moveLeaderships();
          updateOngoingExecutionState();
        }
      } catch (Throwable t) {
        LOG.error("User task {}: Executor got exception during execution", _uuid, t);
        _executionException = t;
      } finally {
        notifyFinishedTask(userTaskInfo);
      }
    }

    private void notifyFinishedTask(UserTaskManager.UserTaskInfo userTaskInfo) {
      // If the finished task was triggered by a user request, update task status in user task manager; if task is triggered
      // by an anomaly self-healing, update the task status in anomaly detector.
      boolean completeWithError = (_executorState.state() == STOPPING_EXECUTION || _executionException != null);
      if (userTaskInfo != null) {
        _userTaskManager.markTaskExecutionFinished(_uuid, completeWithError);
      } else {
        _anomalyDetectorManager.markSelfHealingFinished(_uuid, completeWithError);
      }

      String prefix = String.format("Task [%s] %s execution is ", _uuid,
                                    userTaskInfo != null ? ("user" + userTaskInfo.requestUrl()) : "self-healing");

      if (_executorState.state() == STOPPING_EXECUTION) {
        notifyExecutionFinished(String.format("%sstopped by %s.", prefix, _executionStoppedByUser.get() ? "user" : "Cruise Control"),
                                true);
      } else if (_executionException != null) {
        notifyExecutionFinished(String.format("%sinterrupted with exception %s.", prefix, _executionException.getMessage()),
                                true);
      } else {
        notifyExecutionFinished(String.format("%sfinished.", prefix), false);
      }
    }

    private void notifyExecutionFinished(String message, boolean isWarning) {
      if (isWarning) {
        _executorNotifier.sendAlert(message);
        OPERATION_LOG.warn(message);
      } else {
        _executorNotifier.sendNotification(message);
        OPERATION_LOG.info(message);
      }
    }

    private void clearCompletedExecution() {
      _executionTaskManager.clear();
      _uuid = null;
      _reasonSupplier = null;
      _executorState = ExecutorState.noTaskInProgress(_recentlyDemotedBrokers, _recentlyRemovedBrokers);
      _hasOngoingExecution = false;
      _noOngoingExecutionSemaphore.release();
      _stopSignal.set(NO_STOP_EXECUTION);
      _executionStoppedByUser.set(false);
      // Ensure that sampling mode is adjusted properly to continue collecting partition metrics after execution.
      _loadMonitor.setSamplingMode(ALL);
      _concurrencyAdjuster.clearAdjustment();
    }

    private void updateOngoingExecutionState() {
      if (_stopSignal.get() == NO_STOP_EXECUTION) {
        switch (_executorState.state()) {
          case LEADER_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(LEADER_ACTION)),
                                                               _executionTaskManager.getExecutionConcurrencyManager()
                                                                                    .getExecutionConcurrencySummary(),
                                                               _uuid,
                                                               _reasonSupplier.get(),
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers,
                                                               _isTriggeredByUserRequest);
            break;
          case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(INTER_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.getExecutionConcurrencyManager()
                                                                                    .getExecutionConcurrencySummary(),
                                                               _uuid,
                                                               _reasonSupplier.get(),
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers,
                                                               _isTriggeredByUserRequest);
            break;
          case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.getExecutionConcurrencyManager()
                                                                                    .getExecutionConcurrencySummary(),
                                                               _uuid,
                                                               _reasonSupplier.get(),
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers,
                                                               _isTriggeredByUserRequest);
            break;
          default:
            throw new IllegalStateException("Unexpected ongoing execution state " + _executorState.state());
        }
      } else {
        Set<ExecutionTask.TaskType> taskTypesToGetFullList = new HashSet<>(ExecutionTask.TaskType.cachedValues());
        _executorState = ExecutorState.operationInProgress(STOPPING_EXECUTION,
                                                           _executionTaskManager.getExecutionTasksSummary(taskTypesToGetFullList),
                                                           _executionTaskManager.getExecutionConcurrencyManager()
                                                                                .getExecutionConcurrencySummary(),
                                                           _uuid,
                                                           _reasonSupplier.get(),
                                                           _recentlyDemotedBrokers,
                                                           _recentlyRemovedBrokers,
                                                           _isTriggeredByUserRequest);
      }
    }

    private void interBrokerMoveReplicas() throws InterruptedException, ExecutionException, TimeoutException {
      Set<Integer> currentDeadBrokersWithReplicas = _loadMonitor.deadBrokersWithReplicas(MAX_METADATA_WAIT_MS);
      ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, _replicationThrottle,
          currentDeadBrokersWithReplicas);
      int numTotalPartitionMovements = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingInterBrokerDataToMoveInMB();
      long startTime = System.currentTimeMillis();
      LOG.info("User task {}: Starting {} inter-broker partition movements.", _uuid, numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      Set<Integer> throttledBrokers = new HashSet<>();

      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !inExecutionTasks().isEmpty()) && _stopSignal.get() == NO_STOP_EXECUTION) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getInterBrokerReplicaMovementTasks();
        LOG.info("User task {}: Executor will execute {} task(s)", _uuid, tasksToExecute.size());

        AlterPartitionReassignmentsResult result = null;
        if (!tasksToExecute.isEmpty()) {
            // Set throttles only if not already set for brokers involved
            List<ExecutionProposal> proposals = tasksToExecute.stream().map(ExecutionTask::proposal).collect(Collectors.toList());
            for (ExecutionProposal proposal : proposals) {
                int brokerId = proposal.oldLeader().brokerId();
                if (!throttledBrokers.contains(brokerId)) {
                    throttleHelper.setThrottles(Collections.singletonList(proposal));
                    throttledBrokers.add(brokerId);
                }
            }

          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          result = ExecutionUtils.submitReplicaReassignmentTasks(_adminClient, tasksToExecute);
        }

        // Wait indefinitely for partition movements to finish.
        List<ExecutionTask> completedTasks = waitForInterBrokerReplicaTasksToFinish(result);
        partitionsToMove = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedInterBrokerPartitionMovements();
        long finishedDataMovementInMB = _executionTaskManager.finishedInterBrokerDataMovementInMB();
        updatePartitionMovementMetrics(numFinishedPartitionMovements, finishedDataMovementInMB, System.currentTimeMillis() - startTime);
        LOG.info("User task {}: {}/{} ({}%) inter-broker partition movements completed. {}/{} ({}%) MB have been moved.",
                 _uuid,
                 numFinishedPartitionMovements, numTotalPartitionMovements,
                 String.format("%.2f", numFinishedPartitionMovements * UNIT_INTERVAL_TO_PERCENTAGE / numTotalPartitionMovements),
                 finishedDataMovementInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : String.format("%.2f", finishedDataMovementInMB * UNIT_INTERVAL_TO_PERCENTAGE
                                                                        / totalDataToMoveInMB));
        List<ExecutionTask> inProgressTasks = tasksToExecute.stream()
            .filter(t -> t.state() == ExecutionTaskState.IN_PROGRESS)
            .collect(Collectors.toList());
        inProgressTasks.addAll(inExecutionTasks());

          // Clear throttles only after all tasks are complete for the brokers involved
          if (partitionsToMove == 0) {
              throttleHelper.clearThrottles(completedTasks, inProgressTasks);
              throttledBrokers.clear();
          }
      }

      // Currently, _executionProgressCheckIntervalMs is only runtime adjusted for inter broker move tasks, not
      // in leadership move task. Thus reset it to initial value once interBrokerMoveReplicas has stopped to
      // have it been safely used in following leadership move tasks.
      resetExecutionProgressCheckIntervalMs();

      // At this point it is guaranteed that there are no in execution tasks to wait -- i.e. all tasks are completed or dead.
      if (_stopSignal.get() == NO_STOP_EXECUTION) {
        LOG.info("User task {}: Inter-broker partition movements finished", _uuid);
      } else {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTaskState, Integer> partitionMovementTasksByState = executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
        LOG.info("User task {}: Inter-broker partition movements stopped. For inter-broker partition movements {} tasks cancelled, "
                + "{} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for intra-broker "
                 + "partition movement {} tasks cancelled; for leadership movements {} tasks cancelled.",
                 _uuid,
                 partitionMovementTasksByState.getOrDefault(ExecutionTaskState.PENDING, 0),
                 partitionMovementTasksByState.getOrDefault(ExecutionTaskState.IN_PROGRESS, 0),
                 partitionMovementTasksByState.getOrDefault(ExecutionTaskState.ABORTING, 0),
                 partitionMovementTasksByState.getOrDefault(ExecutionTaskState.ABORTED, 0),
                 partitionMovementTasksByState.getOrDefault(ExecutionTaskState.DEAD, 0),
                 partitionMovementTasksByState.getOrDefault(ExecutionTaskState.COMPLETED, 0),
                 executionTasksSummary.remainingInterBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION).getOrDefault(ExecutionTaskState.PENDING, 0),
                 executionTasksSummary.taskStat().get(LEADER_ACTION).getOrDefault(ExecutionTaskState.PENDING, 0));
      }
  }

    private void intraBrokerMoveReplicas() {
      int numTotalPartitionMovements = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingIntraBrokerDataToMoveInMB();
      long startTime = System.currentTimeMillis();
      LOG.info("User task {}: Starting {} intra-broker partition movements.", _uuid, numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !inExecutionTasks().isEmpty()) && _stopSignal.get() == NO_STOP_EXECUTION) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getIntraBrokerReplicaMovementTasks();
        LOG.info("User task {}: Executor will execute {} task(s)", _uuid, tasksToExecute.size());

        if (!tasksToExecute.isEmpty()) {
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          executeIntraBrokerReplicaMovements(tasksToExecute, _adminClient, _executionTaskManager, _config);
        }
        // Wait indefinitely for partition movements to finish.
        waitForIntraBrokerReplicaTasksToFinish();
        partitionsToMove = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedIntraBrokerPartitionMovements();
        long finishedDataToMoveInMB = _executionTaskManager.finishedIntraBrokerDataToMoveInMB();
        updatePartitionMovementMetrics(numFinishedPartitionMovements, finishedDataToMoveInMB, System.currentTimeMillis() - startTime);
        LOG.info("User task {}: {}/{} ({}%) intra-broker partition movements completed. {}/{} ({}%) MB have been moved.",
                 _uuid,
                 numFinishedPartitionMovements, numTotalPartitionMovements,
                 String.format("%.2f", numFinishedPartitionMovements * UNIT_INTERVAL_TO_PERCENTAGE / numTotalPartitionMovements),
                 finishedDataToMoveInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : String.format("%.2f", finishedDataToMoveInMB * UNIT_INTERVAL_TO_PERCENTAGE
                                                                        / totalDataToMoveInMB));
      }
      Set<ExecutionTask> inExecutionTasks = inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("User task {}: Waiting for {} tasks moving {} MB to finish", _uuid, inExecutionTasks.size(),
                 _executionTaskManager.inExecutionIntraBrokerDataMovementInMB());
        waitForIntraBrokerReplicaTasksToFinish();
        inExecutionTasks = inExecutionTasks();
      }
      if (inExecutionTasks().isEmpty()) {
        LOG.info("User task {}: Intra-broker partition movements finished.", _uuid);
      } else if (_stopSignal.get() != NO_STOP_EXECUTION) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTaskState, Integer> partitionMovementTasksByState = executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
        LOG.info("User task {}: Intra-broker partition movements stopped. For intra-broker partition movements {} tasks cancelled, "
                 + "{} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for leadership "
                 + "movements {} tasks cancelled.",
                 _uuid,
                 partitionMovementTasksByState.get(ExecutionTaskState.PENDING),
                 partitionMovementTasksByState.get(ExecutionTaskState.IN_PROGRESS),
                 partitionMovementTasksByState.get(ExecutionTaskState.ABORTING),
                 partitionMovementTasksByState.get(ExecutionTaskState.ABORTED),
                 partitionMovementTasksByState.get(ExecutionTaskState.DEAD),
                 partitionMovementTasksByState.get(ExecutionTaskState.COMPLETED),
                 executionTasksSummary.remainingIntraBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTaskState.PENDING));
      }
    }

    /**
     * Executes leadership movement tasks.
     */
    private void moveLeaderships() {
      int numTotalLeadershipMovements = _executionTaskManager.numRemainingLeadershipMovements();
      LOG.info("User task {}: Starting {} leadership movements.", _uuid, numTotalLeadershipMovements);
      int numFinishedLeadershipMovements = 0;
      while (_executionTaskManager.numRemainingLeadershipMovements() != 0 && _stopSignal.get() == NO_STOP_EXECUTION) {
        updateOngoingExecutionState();
        numFinishedLeadershipMovements += moveLeadershipInBatch();
        LOG.info("User task {}: {}/{} ({}%) leadership movements completed.", _uuid, numFinishedLeadershipMovements,
                 numTotalLeadershipMovements, numFinishedLeadershipMovements * 100 / numTotalLeadershipMovements);
      }
      if (inExecutionTasks().isEmpty()) {
        LOG.info("Leadership movements finished.");
      } else if (_stopSignal.get() != NO_STOP_EXECUTION) {
        Map<ExecutionTaskState, Integer> leadershipMovementTasksByState =
            _executionTaskManager.getExecutionTasksSummary(Collections.emptySet()).taskStat().get(LEADER_ACTION);
        LOG.info("User task {}: Leadership movements stopped. {} tasks cancelled, {} tasks in-progress, {} tasks aborting, {} tasks aborted, "
                 + "{} tasks dead, {} tasks completed.",
                 _uuid,
                 leadershipMovementTasksByState.get(ExecutionTaskState.PENDING),
                 leadershipMovementTasksByState.get(ExecutionTaskState.IN_PROGRESS),
                 leadershipMovementTasksByState.get(ExecutionTaskState.ABORTING),
                 leadershipMovementTasksByState.get(ExecutionTaskState.ABORTED),
                 leadershipMovementTasksByState.get(ExecutionTaskState.DEAD),
                 leadershipMovementTasksByState.get(ExecutionTaskState.COMPLETED));
      }
    }

    private int moveLeadershipInBatch() {
      List<ExecutionTask> leadershipMovementTasks = _executionTaskManager.getLeadershipMovementTasks();
      int numLeadershipToMove = leadershipMovementTasks.size();
      LOG.debug("Executing {} leadership movements in a batch.", numLeadershipToMove);
      // Execute the leadership movements.
      if (!leadershipMovementTasks.isEmpty() && _stopSignal.get() == NO_STOP_EXECUTION) {
        // Mark leadership movements in progress.
        _executionTaskManager.markTasksInProgress(leadershipMovementTasks);

        ElectLeadersResult electLeadersResult = ExecutionUtils.submitPreferredLeaderElection(_adminClient, leadershipMovementTasks);
        LOG.trace("Waiting for leadership movement batch to finish.");
        while (!inExecutionTasks().isEmpty() && _stopSignal.get() == NO_STOP_EXECUTION) {
          waitForLeadershipTasksToFinish(electLeadersResult);
        }
      }
      return numLeadershipToMove;
    }

    /**
     * First waits for {@link #executionProgressCheckIntervalMs} for the execution to make progress, then retrieves the
     * cluster state for the progress check.
     *
     * @return The cluster state after waiting for the execution progress.
     */
    private Cluster getClusterForExecutionProgressCheck() {
      try {
        Thread.sleep(executionProgressCheckIntervalMs());
      } catch (InterruptedException e) {
        // let it go
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Tasks in execution: {}", inExecutionTasks());
      }
      return _metadataClient.cluster();
    }

    /**
     * Periodically checks the metadata to see if inter-broker replica reassignment has finished or not.
     * @param result the result of a request to alter partition reassignments -- can be {@code null} if no new tasks
     *               for the execution are submitted.
     *
     * @return Finished tasks.
     */
    private List<ExecutionTask> waitForInterBrokerReplicaTasksToFinish(AlterPartitionReassignmentsResult result)
        throws InterruptedException, ExecutionException, TimeoutException {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      Set<Long> stoppedTaskIds = new HashSet<>();
      Set<Long> deletedTaskIds = new HashSet<>();
      Set<Long> deadTaskIds = new HashSet<>();

      // Process result to ensure acceptance of reassignment request on broker-side and identify dead/deleted tasks.
      Set<TopicPartition> deletedUponSubmission = new HashSet<>();
      Set<TopicPartition> deadUponSubmission = new HashSet<>();
      Set<TopicPartition> noReassignmentToCancel = new HashSet<>(0);
      ExecutionUtils.processAlterPartitionReassignmentsResult(result, deletedUponSubmission, deadUponSubmission, noReassignmentToCancel);
      if (!noReassignmentToCancel.isEmpty()) {
        throw new IllegalStateException(String.format("Attempt to cancel reassignment of partitions %s during regular execution.",
                                                      noReassignmentToCancel));
      }

      boolean retry;
      do {
        Cluster cluster = getClusterForExecutionProgressCheck();
        List<ExecutionTask> deadInterBrokerReplicaTasks = new ArrayList<>();
        List<ExecutionTask> stoppedInterBrokerReplicaTasks = new ArrayList<>();
        List<ExecutionTask> slowTasksToReport = new ArrayList<>();
        final int numInExecutionTasks = inExecutionTasks().size();
        // numFinishedOrDeletedTasks instead of finishedTasks.size() is used to decide whether to dynamically adjust
        // executionProgressCheckIntervalMs.
        // If the task is completed or the related topic is deleted, numFinishedOrDeletedTasks is increased.
        // If the destination broker is dead, it may not be safe to increase numFinishedOrDeletedTasks as it may not be safe
        // to speed up inter broker replica move with new broker being down.
        int numFinishedOrDeletedTasks = 0;
        boolean shouldReportSlowTasks = _time.milliseconds() - _lastSlowTaskReportingTimeMs > _slowTaskAlertingBackoffTimeMs;
        for (ExecutionTask task : inExecutionTasks()) {
          TopicPartition tp = task.proposal().topicPartition();
          if (_stopSignal.get() != NO_STOP_EXECUTION) {
            // If the execution is stopped during an ongoing inter-broker replica reassignment, the
            // executor will not silently wait for the completion of the current in-progress tasks. Instead, it will mark in
            // progress tasks as dead, and rollback the ongoing reassignment of gracefully stopped inter-broker replica reassignment.
            LOG.debug("Task {} is marked as dead to stop the execution with a rollback.", task);
            finishedTasks.add(task);
            stoppedTaskIds.add(task.executionId());
            _executionTaskManager.markTaskDead(task);
            stoppedInterBrokerReplicaTasks.add(task);
          } else if (cluster.partition(tp) == null || deletedUponSubmission.contains(tp)) {
            numFinishedOrDeletedTasks++;
            handleProgressWithTopicDeletion(task, finishedTasks, deletedTaskIds);
          } else if (ExecutionUtils.isInterBrokerReplicaActionDone(cluster, task)) {
            numFinishedOrDeletedTasks++;
            handleProgressWithCompletion(task, finishedTasks);
          } else {
            if (shouldReportSlowTasks) {
              task.maybeReportExecutionTooSlow(_time.milliseconds(), slowTasksToReport);
            }
            if (maybeMarkTaskAsDead(cluster, null, task, deadUponSubmission)) {
              deadTaskIds.add(task.executionId());
              // Add dead inter-broker replica tasks for handling.
              deadInterBrokerReplicaTasks.add(task);
              finishedTasks.add(task);
            }
          }
        }

        // Dynamically adjust the _executionProgressCheckIntervalMs based on execution result
        // 1. If all inExecutionTasks are completed check interval, then we should reduce the interval to avoid unnecessary wait time.
        // 2. Else, we should increase the interval, to give the tasks more time to complete.
        if (numFinishedOrDeletedTasks == numInExecutionTasks) {
          setExecutionProgressCheckIntervalMs(_executionProgressCheckIntervalMs - EXECUTION_PROGRESS_CHECK_INTERVAL_ADJUSTING_MS);
        } else {
          setExecutionProgressCheckIntervalMs(_executionProgressCheckIntervalMs + EXECUTION_PROGRESS_CHECK_INTERVAL_ADJUSTING_MS);
        }

        sendSlowExecutionAlert(slowTasksToReport);
        handleDeadInterBrokerReplicaTasks(deadInterBrokerReplicaTasks, stoppedInterBrokerReplicaTasks);
        updateOngoingExecutionState();

        retry = !inExecutionTasks().isEmpty() && finishedTasks.isEmpty();
        // If there is no finished tasks, we need to check if anything is blocked.
        if (retry) {
          maybeReexecuteInterBrokerReplicaTasks(deletedUponSubmission, deadUponSubmission);
        }
      } while (retry);

      LOG.info("User task {}: Finished tasks: {}.{}{}{}", _uuid, finishedTasks,
               stoppedTaskIds.isEmpty() ? "" : String.format(". [Stopped: %s]", stoppedTaskIds),
               deletedTaskIds.isEmpty() ? "" : String.format(". [Deleted: %s]", deletedTaskIds),
               deadTaskIds.isEmpty() ? "" : String.format(". [Dead: %s]", deadTaskIds));

      return finishedTasks;
    }

    private void handleProgressWithTopicDeletion(ExecutionTask task, List<ExecutionTask> finishedTasks, Set<Long> deletedTaskIds) {
      // Handle topic deletion during the execution.
      LOG.debug("Task {} is marked as finished because the topic has been deleted.", task);
      finishedTasks.add(task);
      deletedTaskIds.add(task.executionId());
      _executionTaskManager.markTaskAborting(task);
      _executionTaskManager.markTaskDone(task);
    }

    private void handleProgressWithCompletion(ExecutionTask task, List<ExecutionTask> finishedTasks) {
      // Check to see if the task is done.
      finishedTasks.add(task);
      _executionTaskManager.markTaskDone(task);
    }

    /**
     * Periodically checks the metadata to see if leadership reassignment has finished or not.
     * @param result Elect leaders result.
     */
    private void waitForLeadershipTasksToFinish(ElectLeadersResult result) {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      Set<Long> stoppedTaskIds = new HashSet<>();
      Set<Long> deletedTaskIds = new HashSet<>();
      Set<Long> deadTaskIds = new HashSet<>();

      // Process result to ensure acceptance of leader election request on broker-side and identify deleted tasks.
      Set<TopicPartition> deletedUponSubmission = new HashSet<>();
      ExecutionUtils.processElectLeadersResult(result, deletedUponSubmission);

      boolean retry;
      do {
        Cluster cluster = getClusterForExecutionProgressCheck();

        List<ExecutionTask> slowTasksToReport = new ArrayList<>();
        boolean shouldReportSlowTasks = _time.milliseconds() - _lastSlowTaskReportingTimeMs > _slowTaskAlertingBackoffTimeMs;
        for (ExecutionTask task : inExecutionTasks()) {
          TopicPartition tp = task.proposal().topicPartition();
          if (_stopSignal.get() != NO_STOP_EXECUTION) {
            // If the execution is stopped, the executor will mark all in progress tasks as dead
            LOG.debug("Task {} is marked as dead to stop the execution.", task);
            finishedTasks.add(task);
            stoppedTaskIds.add(task.executionId());
            _executionTaskManager.markTaskDead(task);
          } else if (cluster.partition(tp) == null || deletedUponSubmission.contains(tp)) {
            handleProgressWithTopicDeletion(task, finishedTasks, deletedTaskIds);
          } else if (ExecutionUtils.isLeadershipMovementDone(cluster, task)) {
            handleProgressWithCompletion(task, finishedTasks);
          } else {
            if (shouldReportSlowTasks) {
              task.maybeReportExecutionTooSlow(_time.milliseconds(), slowTasksToReport);
            }
            if (maybeMarkTaskAsDead(cluster, null, task, null)) {
              deadTaskIds.add(task.executionId());
              finishedTasks.add(task);
            }
          }
        }
        sendSlowExecutionAlert(slowTasksToReport);
        updateOngoingExecutionState();

        retry = !inExecutionTasks().isEmpty() && finishedTasks.isEmpty();
        // If there is no finished tasks, we need to check if anything is blocked.
        if (retry) {
          maybeReexecuteLeadershipTasks(deletedUponSubmission);
        }
      } while (retry);

      LOG.info("User task {}: Finished tasks: {}.{}{}{}", _uuid, finishedTasks,
               stoppedTaskIds.isEmpty() ? "" : String.format(". [Stopped: %s]", stoppedTaskIds),
               deletedTaskIds.isEmpty() ? "" : String.format(". [Deleted: %s]", deletedTaskIds),
               deadTaskIds.isEmpty() ? "" : String.format(". [Dead: %s]", deadTaskIds));
    }

    /**
     * Periodically checks the metadata to see if intra-broker replica reassignment has finished or not.
     */
    private void waitForIntraBrokerReplicaTasksToFinish() {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      Set<Long> deletedTaskIds = new HashSet<>();
      Set<Long> deadTaskIds = new HashSet<>();
      do {
        // If there is no finished tasks, we need to check if anything is blocked.
        maybeReexecuteIntraBrokerReplicaTasks();
        Cluster cluster = getClusterForExecutionProgressCheck();
        Map<ExecutionTask, ReplicaLogDirInfo> logDirInfoByTask = getLogdirInfoForExecutionTask(
            _executionTaskManager.inExecutionTasks(Collections.singleton(INTRA_BROKER_REPLICA_ACTION)),
            _adminClient, _config);

        List<ExecutionTask> slowTasksToReport = new ArrayList<>();
        boolean shouldReportSlowTasks = _time.milliseconds() - _lastSlowTaskReportingTimeMs > _slowTaskAlertingBackoffTimeMs;
        for (ExecutionTask task : inExecutionTasks()) {
          TopicPartition tp = task.proposal().topicPartition();
          if (cluster.partition(tp) == null) {
            handleProgressWithTopicDeletion(task, finishedTasks, deletedTaskIds);
          } else if (ExecutionUtils.isIntraBrokerReplicaActionDone(logDirInfoByTask, task)) {
            handleProgressWithCompletion(task, finishedTasks);
          } else {
            if (shouldReportSlowTasks) {
              task.maybeReportExecutionTooSlow(_time.milliseconds(), slowTasksToReport);
            }
            if (maybeMarkTaskAsDead(cluster, logDirInfoByTask, task, null)) {
              deadTaskIds.add(task.executionId());
              finishedTasks.add(task);
            }
          }
        }
        sendSlowExecutionAlert(slowTasksToReport);
        updateOngoingExecutionState();
      } while (!inExecutionTasks().isEmpty() && finishedTasks.isEmpty());

      LOG.info("User task {}: Finished tasks: {}.{}{}", _uuid, finishedTasks,
               deletedTaskIds.isEmpty() ? "" : String.format(". [Deleted: %s]", deletedTaskIds),
               deadTaskIds.isEmpty() ? "" : String.format(". [Dead: %s]", deadTaskIds));
    }

    /**
     * Attempts to cancel/rollback the ongoing reassignment of dead / stopped inter-broker replica actions and stops the
     * execution if not already requested so by the user.
     *
     * If all dead tasks are due to stopped inter-broker replica tasks, it waits until the rollback is completed.
     * Otherwise, it will not wait for the actual rollback to complete to avoid being blocked on a potentially stuck
     * reassignment operation due to dead brokers in the cluster. If by the time the next execution is attempted, the
     * rollback is still in progress on Kafka server-side, the executor will detect the ongoing server-side execution
     * and will not start a new execution (see {@link #sanityCheckOngoingMovement}).
     *
     * @param deadInterBrokerReplicaTasks Inter-broker replica tasks that are expected to be marked as dead due to dead
     *                                    destination brokers.
     * @param stoppedInterBrokerReplicaTasks Inter-broker replica tasks that are expected to be marked as dead due to
     *                                       being stopped by user.
     */
    private void handleDeadInterBrokerReplicaTasks(List<ExecutionTask> deadInterBrokerReplicaTasks,
                                                   List<ExecutionTask> stoppedInterBrokerReplicaTasks)
        throws InterruptedException, ExecutionException, TimeoutException {
      List<ExecutionTask> tasksToCancel = new ArrayList<>(deadInterBrokerReplicaTasks);
      tasksToCancel.addAll(stoppedInterBrokerReplicaTasks);

      if (!tasksToCancel.isEmpty()) {
        // Sanity check to ensure that all tasks are marked as dead.
        tasksToCancel.stream().filter(task -> task.state() != ExecutionTaskState.DEAD).forEach(task -> {
          throw new IllegalArgumentException(String.format("Unexpected state for task %s (expected: %s).", task, ExecutionTaskState.DEAD));
        });

        // Cancel/rollback reassignment of dead inter-broker replica tasks.
        AlterPartitionReassignmentsResult result = ExecutionUtils.submitReplicaReassignmentTasks(_adminClient, tasksToCancel);
        // Process the partition reassignment result.
        Set<TopicPartition> deleted = new HashSet<>();
        Set<TopicPartition> dead = new HashSet<>();
        Set<TopicPartition> noReassignmentToCancel = new HashSet<>();
        ExecutionUtils.processAlterPartitionReassignmentsResult(result, deleted, dead, noReassignmentToCancel);
        LOG.debug("Handling dead inter-broker replica tasks {} (deleted: {} dead: {} noReassignmentToCancel: {})",
                  tasksToCancel, deleted, dead, noReassignmentToCancel);

        if (_stopSignal.get() == NO_STOP_EXECUTION) {
          // If there are dead tasks, Cruise Control stops the execution.
          LOG.info("User task {}: Stop the execution due to {} dead tasks: {}.", _uuid, tasksToCancel.size(), tasksToCancel);
          stopExecution();
        }

        if (deadInterBrokerReplicaTasks.isEmpty()) {
          // All dead tasks are due to stopped inter-broker replica tasks. Wait until the tasks being cancelled are done.
          Set<TopicPartition> beingCancelled = tasksToCancel.stream().map(task -> task.proposal().topicPartition()).collect(Collectors.toSet());
          beingCancelled.removeAll(deleted);
          beingCancelled.removeAll(dead);
          beingCancelled.removeAll(noReassignmentToCancel);

          while (true) {
            Set<TopicPartition> intersection = new HashSet<>(ExecutionUtils.partitionsBeingReassigned(_adminClient));
            intersection.retainAll(beingCancelled);
            if (intersection.isEmpty()) {
              // All tasks have been rolled back.
              break;
            }
            try {
              LOG.info("User task {}: Waiting for the rollback of ongoing inter-broker replica reassignments for {}.",
                  _uuid, intersection);
              Thread.sleep(executionProgressCheckIntervalMs());
            } catch (InterruptedException e) {
              // let it go
            }
          }
        }
      }
    }

    private void sendSlowExecutionAlert(List<ExecutionTask> slowTasksToReport) {
      if (!slowTasksToReport.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("Slow tasks are detected:\n");
        for (ExecutionTask task: slowTasksToReport) {
          sb.append(String.format("\tID: %s\tstart_time:%s\tdetail:%s%n", task.executionId(), utcDateFor(task.startTimeMs()), task));
        }
        _executorNotifier.sendAlert(sb.toString());
        _lastSlowTaskReportingTimeMs = _time.milliseconds();
      }
    }

    /**
     * Mark the task as dead if if it satisfies any of the constraints specified for its type:
     *
     * <ul>
     *   <li>{@code LEADER_ACTION}: Target broker/disk hosting the prospective leader is down or leadership transition
     *   took longer than the configured limit.</li>
     *   <li>{@code INTER_BROKER_REPLICA_ACTION}: Destination broker is down.</li>
     *   <li>{@code INTRA_BROKER_REPLICA_ACTION}: Destination disk is down.</li>
     * </ul>
     *
     * @param cluster the kafka cluster
     * @param logdirInfoByTask  disk information for ongoing intra-broker replica movement tasks
     * @param task the task to check
     * @param deadInterBrokerReassignments the set of partitions that were dead upon submission of the corresponding
     *                                     inter-broker replica reassignment task (ignored for other task types).
     * @return {@code true} if the task is marked as dead, {@code false} otherwise.
     */
    private boolean maybeMarkTaskAsDead(Cluster cluster,
                                        Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                                        ExecutionTask task,
                                        Set<TopicPartition> deadInterBrokerReassignments) {
      // Only check tasks with IN_PROGRESS or ABORTING state.
      if (task.state() == ExecutionTaskState.IN_PROGRESS || task.state() == ExecutionTaskState.ABORTING) {
        switch (task.type()) {
          case LEADER_ACTION:
            if (cluster.nodeById(task.proposal().newLeader().brokerId()) == null) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("User task {}: Killing execution for task {} because the target leader is down.", _uuid, task);
              return true;
            } else if (_time.milliseconds() > task.startTimeMs() + _leaderMovementTimeoutMs) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("User task {}: Killing execution for task {} because it took longer than {} to finish.",
                  _uuid, task, _leaderMovementTimeoutMs);
              return true;
            }
            break;

          case INTER_BROKER_REPLICA_ACTION:
            for (ReplicaPlacementInfo broker : task.proposal().newReplicas()) {
              if (cluster.nodeById(broker.brokerId()) == null
                  || deadInterBrokerReassignments.contains(task.proposal().topicPartition())) {
                _executionTaskManager.markTaskDead(task);
                LOG.warn("User task {}: Killing execution for task {} because the new replica {} is down.", _uuid, task, broker);
                return true;
              }
            }
            break;

          case INTRA_BROKER_REPLICA_ACTION:
            if (!logdirInfoByTask.containsKey(task)) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("User task {}: Killing execution for task {} because the destination disk is down.", _uuid, task);
              return true;
            }
            break;

          default:
            throw new IllegalStateException("Unknown task type " + task.type());
        }
      }
      return false;
    }

    /**
     * Identifies if there is a need for re-execution, if so, ensures re-execution of inter-broker replica reassignments
     * -- e.g. in case there is a controller failover.
     *
     * @param deleted if re-execution is needed, a set to populate with partitions that were deleted upon submission of
     *                the corresponding inter-broker replica reassignment tasks. No change otherwise.
     * @param dead if re-execution is needed, a set to populate with partitions that were dead upon submission of the
     *             corresponding inter-broker replica reassignment tasks.
     */
    private void maybeReexecuteInterBrokerReplicaTasks(Set<TopicPartition> deleted, Set<TopicPartition> dead) {
      List<ExecutionTask> candidateInterBrokerReplicaTasksToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(INTER_BROKER_REPLICA_ACTION)));
      List<ExecutionTask> tasksToReexecute;
      try {
        tasksToReexecute = ExecutionUtils.getInterBrokerReplicaTasksToReexecute(ExecutionUtils.partitionsBeingReassigned(_adminClient),
            candidateInterBrokerReplicaTasksToReexecute);
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        // This may indicate transient (e.g. network) issues.
        LOG.warn("User task {}: Failed to retrieve partitions being reassigned. Skipping reexecution check for inter-broker replica actions.",
            _uuid, e);
        tasksToReexecute = Collections.emptyList();
      }
      if (!tasksToReexecute.isEmpty()) {
        AlterPartitionReassignmentsResult result = ExecutionUtils.submitReplicaReassignmentTasks(_adminClient, tasksToReexecute);
        // Process the partition reassignment result.
        Set<TopicPartition> noReassignmentToCancel = new HashSet<>();
        ExecutionUtils.processAlterPartitionReassignmentsResult(result, deleted, dead, noReassignmentToCancel);
        if (!noReassignmentToCancel.isEmpty()) {
          throw new IllegalStateException(String.format("Attempt to cancel reassignment of partitions %s during re-execution.",
                                                        noReassignmentToCancel));
        }
      }
    }

    /**
     * Identifies if there is a need for re-execution, if so, ensures re-execution of intra-broker replica reassignments
     * -- e.g. in case there is a controller failover.
     */
    private void maybeReexecuteIntraBrokerReplicaTasks() {
      List<ExecutionTask> intraBrokerReplicaTasksToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(INTRA_BROKER_REPLICA_ACTION)));
      getLogdirInfoForExecutionTask(intraBrokerReplicaTasksToReexecute, _adminClient, _config).forEach((k, v) -> {
        String targetLogdir = k.proposal().replicasToMoveBetweenDisksByBroker().get(k.brokerId()).logdir();
        // If task is completed or in-progress, do not reexecute the task.
        if (targetLogdir.equals(v.getCurrentReplicaLogDir()) || targetLogdir.equals(v.getFutureReplicaLogDir())) {
          intraBrokerReplicaTasksToReexecute.remove(k);
        }
      });
      if (!intraBrokerReplicaTasksToReexecute.isEmpty()) {
        LOG.info("User task {}: Reexecuting tasks {}", _uuid, intraBrokerReplicaTasksToReexecute);
        executeIntraBrokerReplicaMovements(intraBrokerReplicaTasksToReexecute, _adminClient, _executionTaskManager, _config);
      }
    }

    /**
     * Due to the race condition between the controller and Cruise Control, some of the submitted tasks may be
     * deleted by controller without being executed. We will resubmit those tasks in that case.
     *
     * @param deleted if re-execution is needed, a set to populate with partitions that were deleted upon submission of
     *                the corresponding leadership tasks. No change otherwise.
     */
    private void maybeReexecuteLeadershipTasks(Set<TopicPartition> deleted) {
      List<ExecutionTask> leaderActionsToReexecute = new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(LEADER_ACTION)));
      if (!leaderActionsToReexecute.isEmpty()) {
        LOG.info("User task {}: Reexecuting tasks {}", _uuid, leaderActionsToReexecute);
        ElectLeadersResult electLeadersResult = ExecutionUtils.submitPreferredLeaderElection(_adminClient, leaderActionsToReexecute);
        ExecutionUtils.processElectLeadersResult(electLeadersResult, deleted);
      }
    }

    private void updatePartitionMovementMetrics(long numTotalPartitionMovements, long totalDataToMoveInMB, long executionTimeMs) {
      if (executionTimeMs != 0) {
        _partitionMovementMbPerSec.set(totalDataToMoveInMB / (double) executionTimeMs * 1000);
        _partitionMovementCountPerSec.set(numTotalPartitionMovements / (double) executionTimeMs * 1000);
      }
    }
  }
}
