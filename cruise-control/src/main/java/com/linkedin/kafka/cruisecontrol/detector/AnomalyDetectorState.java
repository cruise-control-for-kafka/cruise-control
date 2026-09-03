/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ANOMALY_DETECTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.SEC_TO_MS;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.*;


@JsonResponseClass
public class AnomalyDetectorState {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorState.class);
  @JsonResponseField
  private static final String SELF_HEALING_ENABLED = "selfHealingEnabled";
  @JsonResponseField
  private static final String SELF_HEALING_DISABLED = "selfHealingDisabled";
  @JsonResponseField
  private static final String SELF_HEALING_ENABLED_RATIO = "selfHealingEnabledRatio";
  @JsonResponseField
  private static final String RECENT_GOAL_VIOLATIONS = "recentGoalViolations";
  @JsonResponseField
  private static final String RECENT_INTRA_BROKER_GOAL_VIOLATIONS = "recentIntraBrokerGoalViolations";
  @JsonResponseField
  private static final String RECENT_BROKER_FAILURES = "recentBrokerFailures";
  @JsonResponseField
  private static final String RECENT_METRIC_ANOMALIES = "recentMetricAnomalies";
  @JsonResponseField
  private static final String RECENT_TOPIC_ANOMALIES = "recentTopicAnomalies";
  @JsonResponseField
  private static final String RECENT_MAINTENANCE_EVENTS = "recentMaintenanceEvents";
  @JsonResponseField
  private static final String RECENT_DISK_FAILURES = "recentDiskFailures";
  @JsonResponseField(required = false)
  private static final String ONGOING_SELF_HEALING_ANOMALY = "ongoingSelfHealingAnomaly";
  @JsonResponseField
  private static final String METRICS = "metrics";
  @JsonResponseField
  private static final String BALANCEDNESS_SCORE = "balancednessScore";
  private static final double INITIAL_BALANCEDNESS_SCORE = 100.0;
  static final String NUM_SELF_HEALING_STARTED = "numSelfHealingStarted";
  private static final long NO_ONGOING_ANOMALY_FLAG = -1L;

  // Recent anomalies with anomaly state by the anomaly type.
  private final Map<AnomalyType, Map<String, AnomalyState>> _recentAnomaliesByType;
  private Anomaly _ongoingSelfHealingAnomaly;
  private final AnomalyNotifier _anomalyNotifier;
  private SelfHealingEnabledRatio _selfHealingEnabledRatio;
  // Maximum number of anomalies to keep in the anomaly detector state.
  private final int _numCachedRecentAnomalyStates;
  private AnomalyMetrics _metrics;
  // The detection time of the earliest ongoing anomaly. Expected to be cleared upon start of a proposal execution.
  private volatile long _ongoingAnomalyDetectionTimeMs;
  // The count during which there is at least one ongoing anomaly.
  private long _ongoingAnomalyCount;
  private double _ongoingAnomalyDurationSumForAverageMs;
  private final Time _time;
  private final AtomicLong _numSelfHealingStarted;
  private final AtomicLong _numSelfHealingFailedToStart;
  private final Map<AnomalyType, Meter> _anomalyRateByType;
  private double _balancednessScore;
  private boolean _hasUnfixableGoals;
  private final Map<AnomalyType, Timer> _anomalyDetectToFixCompleteTimer;

  public AnomalyDetectorState(Time time,
                              AnomalyNotifier anomalyNotifier,
                              int numCachedRecentAnomalyStates,
                              MetricRegistry dropwizardMetricRegistry) {
    _time = time;
    _numCachedRecentAnomalyStates = numCachedRecentAnomalyStates;
    _recentAnomaliesByType = new HashMap<>();
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      _recentAnomaliesByType.put(anomalyType, new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, AnomalyState> eldest) {
          return this.size() > _numCachedRecentAnomalyStates;
        }
      });
    }
    _anomalyNotifier = anomalyNotifier;
    _selfHealingEnabledRatio = null;
    _ongoingSelfHealingAnomaly = null;
    _ongoingAnomalyDetectionTimeMs = NO_ONGOING_ANOMALY_FLAG;
    _ongoingAnomalyCount = 0L;
    _ongoingAnomalyDurationSumForAverageMs = 0;
    _numSelfHealingStarted = new AtomicLong(0L);
    _numSelfHealingFailedToStart = new AtomicLong(0L);

    Map<AnomalyType, Double> meanTimeBetweenAnomaliesMs = new HashMap<>();
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      meanTimeBetweenAnomaliesMs.put(anomalyType, 0.0);
    }
    _metrics = new AnomalyMetrics(meanTimeBetweenAnomaliesMs, 0.0, 0L, 0L, 0L);
    _anomalyDetectToFixCompleteTimer = new HashMap<>();
    if (dropwizardMetricRegistry != null) {
      dropwizardMetricRegistry.register(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "mean-time-to-start-fix-ms"),
                                        (Gauge<Double>) this::meanTimeToStartFixMs);
      dropwizardMetricRegistry.register(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "number-of-self-healing-started"),
                                        (Gauge<Long>) this::numSelfHealingStarted);
      dropwizardMetricRegistry.register(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "number-of-self-healing-failed-to-start"),
                                        (Gauge<Long>) this::numSelfHealingFailedToStart);
      dropwizardMetricRegistry.register(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "ongoing-anomaly-duration-ms"),
                                        (Gauge<Long>) this::ongoingAnomalyDurationMs);
      dropwizardMetricRegistry.register(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, String.format("%s-has-unfixable-goals", GOAL_VIOLATION)),
                                        (Gauge<Integer>) () -> hasUnfixableGoals() ? 1 : 0);

      _anomalyRateByType = new HashMap<>();
      _anomalyRateByType.put(BROKER_FAILURE,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "broker-failure-rate")));
      _anomalyRateByType.put(GOAL_VIOLATION,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "goal-violation-rate")));
      _anomalyRateByType.put(INTRA_BROKER_GOAL_VIOLATION,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "intra-broker-goal-violation-rate")));
      _anomalyRateByType.put(METRIC_ANOMALY,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "metric-anomaly-rate")));
      _anomalyRateByType.put(DISK_FAILURE,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "disk-failure-rate")));
      _anomalyRateByType.put(TOPIC_ANOMALY,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "topic-anomaly-rate")));
      _anomalyRateByType.put(MAINTENANCE_EVENT,
                             dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "maintenance-event-rate")));
      for (KafkaAnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
        Timer timer = dropwizardMetricRegistry.timer(
            MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, String.format("%s-detect-to-fix-complete-timer", anomalyType.toString().toLowerCase())));
        _anomalyDetectToFixCompleteTimer.put(anomalyType, timer);
      }
    } else {
      _anomalyRateByType = new HashMap<>();
      KafkaAnomalyType.cachedValues().forEach(anomalyType -> _anomalyRateByType.put(anomalyType, new Meter()));
    }
    _balancednessScore = INITIAL_BALANCEDNESS_SCORE;
    _hasUnfixableGoals = false;
  }

  /**
   * Mark the occurrence of an anomaly.
   *
   * @param anomalyType Type of anomaly.
   */
  void markAnomalyRate(AnomalyType anomalyType) {
    _anomalyRateByType.get(anomalyType).mark();
  }

  /**
   * Refreshes the anomaly detector cache that indicates whether unfixable goals were detected in the cluster.
   *
   * @param goalViolations Goal violation to check whether there are unfixable goals.
   */
  void refreshHasUnfixableGoal(GoalViolations goalViolations) {
    _hasUnfixableGoals = AnomalyDetectorUtils.hasUnfixableGoals(goalViolations);
  }

  /**
   * Refreshes the anomaly detector cache that indicates whether unfixable goals were detected in the cluster.
   *
   * @param goalViolations Goal violation to check whether there are unfixable goals.
   */
  void refreshHasUnfixableGoal(IntraBrokerGoalViolations goalViolations) {
    _hasUnfixableGoals = AnomalyDetectorUtils.hasUnfixableGoals(goalViolations);
  }

  /**
   * @return {@code true} if the latest goal violation contains unfixable goals, {@code false} if either the latest goal violation
   * contains no unfixable goals or if any execution was started after the latest goal violation.
   */
  public boolean hasUnfixableGoals() {
    return _hasUnfixableGoals;
  }

  /**
   * Resets the state corresponding to {@link #hasUnfixableGoals()}.
   */
  void resetHasUnfixableGoals() {
    _hasUnfixableGoals = false;
  }

  /**
   * Refresh the anomaly metrics.
   *
   * @param selfHealingEnabledRatio The ratio
   * @param balancednessScore A metric to quantify how well the load distribution on a cluster satisfies the anomaly
   * detection goals.
   */
  synchronized void refreshMetrics(Map<AnomalyType, Float> selfHealingEnabledRatio, double balancednessScore) {
    if (selfHealingEnabledRatio == null) {
      throw new IllegalArgumentException("Attempt to set selfHealingEnabledRatio with null.");
    }

    // Retrieve mean time between anomalies, record the time in ms.
    Map<AnomalyType, Double> meanTimeBetweenAnomaliesMs = new HashMap<>();
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      meanTimeBetweenAnomaliesMs.put(anomalyType, _anomalyRateByType.get(anomalyType).getMeanRate() * SEC_TO_MS);
    }

    _metrics = new AnomalyMetrics(meanTimeBetweenAnomaliesMs, meanTimeToStartFixMs(), numSelfHealingStarted(),
                                  numSelfHealingFailedToStart(), ongoingAnomalyDurationMs());
    _selfHealingEnabledRatio = new SelfHealingEnabledRatio(selfHealingEnabledRatio.size());
    selfHealingEnabledRatio.forEach((key, value) -> _selfHealingEnabledRatio.put(key, value));
    _balancednessScore = balancednessScore;
  }

  /**
   * @return Duration of the ongoing anomaly in ms if there is one, 0 otherwise. This method is intentionally not
   * thread-safe to avoid the synchronization latency; hence, it can potentially be stale.
   */
  private long ongoingAnomalyDurationMs() {
    return _ongoingAnomalyDetectionTimeMs != NO_ONGOING_ANOMALY_FLAG ? _time.milliseconds() - _ongoingAnomalyDetectionTimeMs : 0L;
  }

  /**
   * @return Mean time to start a fix in ms. This method is intentionally not thread-safe to avoid the synchronization
   * latency; hence, it can potentially be stale.
   */
  private double meanTimeToStartFixMs() {
    long fixedAnomalyDurations = _ongoingAnomalyDetectionTimeMs == NO_ONGOING_ANOMALY_FLAG ? _ongoingAnomalyCount
                                                                                           : _ongoingAnomalyCount - 1;

    return fixedAnomalyDurations == 0L ? 0 : _ongoingAnomalyDurationSumForAverageMs / fixedAnomalyDurations;
  }

  /**
   * If there is an ongoing anomaly, clear the earliest detection time to indicate start of an ongoing fix.
   */
  synchronized void maybeClearOngoingAnomalyDetectionTimeMs() {
    if (_ongoingAnomalyDetectionTimeMs != NO_ONGOING_ANOMALY_FLAG) {
      double elapsed = _time.milliseconds() - _ongoingAnomalyDetectionTimeMs;
      _ongoingAnomalyDurationSumForAverageMs += elapsed;
      // Clear ongoing anomaly detection time
      _ongoingAnomalyDetectionTimeMs = NO_ONGOING_ANOMALY_FLAG;
    }
  }

  /**
   * The {@link #_ongoingAnomalyDetectionTimeMs} is updated only when there is no earlier ongoing anomaly.
   * See {@link #maybeClearOngoingAnomalyDetectionTimeMs()} for clearing the ongoing anomaly detection time.
   */
  synchronized void maybeSetOngoingAnomalyDetectionTimeMs() {
    if (_ongoingAnomalyDetectionTimeMs == NO_ONGOING_ANOMALY_FLAG) {
      _ongoingAnomalyDetectionTimeMs = _time.milliseconds();
      _ongoingAnomalyCount++;
    }
  }

  /**
   * @return Number of anomaly fixes started by the anomaly detector for self healing.
   */
  long numSelfHealingStarted() {
    return _numSelfHealingStarted.get();
  }

  /**
   * Increment the number of self healing actions started successfully.
   */
  void incrementNumSelfHealingStarted() {
    _numSelfHealingStarted.incrementAndGet();
  }

  /**
   * @return Number of anomaly fixes failed to start despite the anomaly in progress being ready to fix. This typically
   * indicates the need for expanding the cluster or relaxing the constraints of self-healing goals.
   */
  long numSelfHealingFailedToStart() {
    return _numSelfHealingFailedToStart.get();
  }

  /**
   * Increment the number of self healing actions failed to start despite the anomaly in progress being ready to fix.
   */
  void incrementNumSelfHealingFailedToStart() {
    _numSelfHealingFailedToStart.incrementAndGet();
  }

  /**
   * Package private for testing
   * @return Metrics as a JSON structure.
   */
  Map<String, Object> metrics() {
    return _metrics.getJsonStructure();
  }

  private SelfHealingEnabledRatio selfHealingEnabledRatio() {
    return _selfHealingEnabledRatio == null ? new SelfHealingEnabledRatio(0) : _selfHealingEnabledRatio;
  }

  /**
   * Update anomaly status once associated self-healing operation has finished.
   *
   * @param anomalyId Unique id of anomaly which triggered self-healing operation.
   * @param completeWithError Whether the task execution finished with error.
   */
  public synchronized void markSelfHealingFinished(String anomalyId, boolean completeWithError) {
    if (_ongoingSelfHealingAnomaly == null || !_ongoingSelfHealingAnomaly.anomalyId().equals(anomalyId)) {
      throw new IllegalStateException(String.format("Anomaly %s is not marked as %s state in AnomalyDetector.",
                                                    anomalyId, AnomalyState.Status.FIX_STARTED));
    }
    long totalTime = _time.milliseconds() - _ongoingSelfHealingAnomaly.detectionTimeMs();
    LOG.info("Self-healing for anomalyId {} completed{}. Total time: {} ms", anomalyId, completeWithError
        ? " with error" : " successfully", totalTime);
    if (!completeWithError) {
      // Time the duration if the self-healing is completed successfully.
      // completeWithError is true if the proposal execution is interrupted (receive stop signal) or encountered exception.
      // execution-stopped metrics track the number of stopped execution.
      _anomalyDetectToFixCompleteTimer.get(_ongoingSelfHealingAnomaly.anomalyType()).update(totalTime, TimeUnit.MILLISECONDS);
    }
    _ongoingSelfHealingAnomaly = null;
  }

  /**
   * Add detected anomaly to the anomaly detector state.
   *
   * @param anomalyType Type of the detected anomaly.
   * @param anomaly The detected anomaly.
   */
  void addAnomalyDetection(AnomalyType anomalyType, Anomaly anomaly) {
    _recentAnomaliesByType.get(anomalyType).put(anomaly.anomalyId(), new AnomalyState(anomaly));
  }

  /**
   * Update state regarding how the anomaly has been handled.
   *
   * @param anomaly The anomaly to handle.
   * @param status A status information regarding how the anomaly was handled.
   */
  synchronized void onAnomalyHandle(Anomaly anomaly, AnomalyState.Status status) {
    AnomalyType anomalyType = anomaly.anomalyType();
    String anomalyId = anomaly.anomalyId();

    if (status == AnomalyState.Status.FIX_STARTED) {
      _ongoingSelfHealingAnomaly = anomaly;
    }

    AnomalyState recentAnomalyState = _recentAnomaliesByType.get(anomalyType).get(anomalyId);
    if (recentAnomalyState != null) {
      recentAnomalyState.setStatus(status);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Anomaly (type: {}, anomalyId: {}) is no longer in the anomaly detector state cache.", anomalyType, anomalyId);
    }
  }

  /**
   * Set self healing for the given anomaly type.
   *
   * @param anomalyType Type of anomaly.
   * @param isSelfHealingEnabled {@code true} if self healing is enabled, {@code false} otherwise.
   * @return The old value of self healing for the given anomaly type.
   */
  public synchronized boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    return _anomalyNotifier.setSelfHealingFor(anomalyType, isSelfHealingEnabled);
  }

  /**
   * Package private for unit tests.
   * @return Recent anomalies by type.
   */
  Map<AnomalyType, Map<String, AnomalyState>> recentAnomaliesByType() {
    return _recentAnomaliesByType;
  }

  private Set<Map<String, Object>> recentAnomalies(AnomalyType anomalyType, boolean isJson) {
    Map<String, AnomalyState> anomaliesById = _recentAnomaliesByType.get(anomalyType);
    Set<Map<String, Object>> recentAnomalies = new HashSet<>();
    for (Map.Entry<String, AnomalyState> entry: anomaliesById.entrySet()) {
      recentAnomalies.add(new AnomalyDetails(entry.getValue(), anomalyType, false, isJson).populateAnomalyDetails());
    }
    return recentAnomalies;
  }

  private Map<Boolean, Set<String>> getSelfHealingByEnableStatus() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = Map.of(true, new HashSet<>(), false, new HashSet<>());
    _anomalyNotifier.selfHealingEnabled().forEach((key, value) -> selfHealingByEnableStatus.get(value).add(key.toString()));
    return selfHealingByEnableStatus;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public synchronized Map<String, Object> getJsonStructure() {
    Map<String, Object> anomalyDetectorState = new HashMap<>();
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    anomalyDetectorState.put(SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true));
    anomalyDetectorState.put(SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false));
    anomalyDetectorState.put(SELF_HEALING_ENABLED_RATIO, selfHealingEnabledRatio().getJsonStructure());
    anomalyDetectorState.put(RECENT_GOAL_VIOLATIONS, recentAnomalies(GOAL_VIOLATION, true));
    anomalyDetectorState.put(RECENT_INTRA_BROKER_GOAL_VIOLATIONS, recentAnomalies(INTRA_BROKER_GOAL_VIOLATION, true));
    anomalyDetectorState.put(RECENT_BROKER_FAILURES, recentAnomalies(BROKER_FAILURE, true));
    anomalyDetectorState.put(RECENT_METRIC_ANOMALIES, recentAnomalies(METRIC_ANOMALY, true));
    anomalyDetectorState.put(RECENT_DISK_FAILURES, recentAnomalies(DISK_FAILURE, true));
    anomalyDetectorState.put(RECENT_TOPIC_ANOMALIES, recentAnomalies(TOPIC_ANOMALY, true));
    anomalyDetectorState.put(RECENT_MAINTENANCE_EVENTS, recentAnomalies(MAINTENANCE_EVENT, true));
    anomalyDetectorState.put(METRICS, metrics());
    if (_ongoingSelfHealingAnomaly != null) {
      anomalyDetectorState.put(ONGOING_SELF_HEALING_ANOMALY, _ongoingSelfHealingAnomaly.anomalyId());
    }
    anomalyDetectorState.put(BALANCEDNESS_SCORE, _balancednessScore);
    return anomalyDetectorState;
  }

  @Override
  public synchronized String toString() {
    Map<Boolean, Set<String>> selfHealingByEnableStatus = getSelfHealingByEnableStatus();
    return String.format("{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%.3f}%n",
                         SELF_HEALING_ENABLED, selfHealingByEnableStatus.get(true),
                         SELF_HEALING_DISABLED, selfHealingByEnableStatus.get(false),
                         SELF_HEALING_ENABLED_RATIO, selfHealingEnabledRatio().getJsonStructure(),
                         RECENT_GOAL_VIOLATIONS, recentAnomalies(GOAL_VIOLATION, false),
                         RECENT_INTRA_BROKER_GOAL_VIOLATIONS, recentAnomalies(INTRA_BROKER_GOAL_VIOLATION, false),
                         RECENT_BROKER_FAILURES, recentAnomalies(BROKER_FAILURE, false),
                         RECENT_METRIC_ANOMALIES, recentAnomalies(METRIC_ANOMALY, false),
                         RECENT_DISK_FAILURES, recentAnomalies(DISK_FAILURE, false),
                         RECENT_TOPIC_ANOMALIES, recentAnomalies(TOPIC_ANOMALY, false),
                         RECENT_MAINTENANCE_EVENTS, recentAnomalies(MAINTENANCE_EVENT, false),
                         METRICS, _metrics,
                         ONGOING_SELF_HEALING_ANOMALY, _ongoingSelfHealingAnomaly == null
                                                       ? "None" : _ongoingSelfHealingAnomaly.anomalyId(),
                         BALANCEDNESS_SCORE, _balancednessScore);
  }
}
