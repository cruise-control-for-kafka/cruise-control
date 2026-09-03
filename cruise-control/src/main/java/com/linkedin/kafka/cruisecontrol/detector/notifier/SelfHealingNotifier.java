/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.IntraBrokerGoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent;
import com.linkedin.kafka.cruisecontrol.detector.TopicAnomaly;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.hasUnfixableGoals;


/**
 * This class implements a logic of self-healing when anomaly is detected.
 *
 * <ul>
 * <li>The goal violations will trigger an immediate fix.</li>
 * <li>The metric anomalies will trigger an immediate fix.</li>
 * <li>The disk failures will trigger an immediate fix.</li>
 * <li>The topic anomalies will trigger an immediate fix.</li>
 * <li>The maintenance events will trigger an immediate fix.</li>
 * <li>The broker failures are handled in the following way:
 *   <ol>
 *     <li>If a broker disappears from a cluster at timestamp <b>T</b>, the detector will start down counting.</li>
 *     <li>If the broker did not re-join the cluster within broker.failure.alert.threshold.ms since <b>T</b>,
 *      the broker will be defined as dead and an alert will be triggered.</li>
 *     <li>If the broker did not re-join the cluster within broker.failure.self.healing.threshold.ms since <b>T</b>, an auto
 *      remediation will be performed.</li>
 *   </ol>
 * </li>
 * </ul>
 *
 * <ul>
 * <li>{@link #SELF_HEALING_ENABLED_CONFIG}: Enable self healing for all anomaly detectors, unless the particular
 * anomaly detector is explicitly disabled.</li>
 * <li>{@link #SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG}: Enable self healing for broker failure detector.</li>
 * <li>{@link #SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG}: Enable self healing for goal violation detector.</li>
 * <li>{@link #SELF_HEALING_INTRA_BROKER_GOAL_VIOLATION_ENABLED_CONFIG}: Enable self healing for intra broker goal violation detector.</li>
 * <li>{@link #SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG}: Enable self healing for metric anomaly detector.</li>
 * <li>{@link #SELF_HEALING_DISK_FAILURE_ENABLED_CONFIG}: Enable self healing for disk failure detector.</li>
 * <li>{@link #SELF_HEALING_TOPIC_ANOMALY_ENABLED_CONFIG}: Enable self healing for topic anomaly detector.</li>
 * <li>{@link #SELF_HEALING_MAINTENANCE_EVENT_ENABLED_CONFIG}: Enable self healing for maintenance event detector.</li>
 * </ul>
 */
public class SelfHealingNotifier implements AnomalyNotifier {
  public static final String BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG = "broker.failure.alert.threshold.ms";
  public static final String SELF_HEALING_ENABLED_CONFIG = "self.healing.enabled";
  public static final String SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG = "self.healing.broker.failure.enabled";
  public static final String SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG = "self.healing.goal.violation.enabled";
  public static final String SELF_HEALING_INTRA_BROKER_GOAL_VIOLATION_ENABLED_CONFIG = "self.healing.intra.broker.goal.violation.enabled";
  public static final String SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG = "self.healing.metric.anomaly.enabled";
  public static final String SELF_HEALING_DISK_FAILURE_ENABLED_CONFIG = "self.healing.disk.failure.enabled";
  public static final String SELF_HEALING_TOPIC_ANOMALY_ENABLED_CONFIG = "self.healing.topic.anomaly.enabled";
  public static final String SELF_HEALING_MAINTENANCE_EVENT_ENABLED_CONFIG = "self.healing.maintenance.event.enabled";
  public static final String BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG = "broker.failure.self.healing.threshold.ms";
  static final long DEFAULT_ALERT_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(15);
  static final long DEFAULT_AUTO_FIX_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(30);

  private static final Logger LOG = LoggerFactory.getLogger(SelfHealingNotifier.class);
  protected final Time _time;
  protected final long _notifierStartTimeMs;
  protected final Map<AnomalyType, Boolean> _selfHealingEnabled;
  protected final Map<Boolean, Map<AnomalyType, Long>> _selfHealingStateChangeTimeMs;
  protected final Map<AnomalyType, Long> _selfHealingEnabledHistoricalDurationMs;
  protected long _brokerFailureAlertThresholdMs;
  protected long _selfHealingThresholdMs;
  // A cache that keeps the most recent broker failure for each broker.
  protected final Map<Boolean, Map<Integer, Long>> _latestFailedBrokersByAutoFixTriggered;

  public SelfHealingNotifier() {
    this(Time.SYSTEM);
  }

  /**
   * Package private constructor for unit test.
   */
  SelfHealingNotifier(Time time) {
    _time = time;
    _notifierStartTimeMs = _time.milliseconds();
    _selfHealingEnabled = new HashMap<>();
    // Init self-healing state change time.
    _selfHealingStateChangeTimeMs = new HashMap<>();
    _selfHealingStateChangeTimeMs.put(true, new HashMap<>());
    _selfHealingStateChangeTimeMs.put(false, new HashMap<>());
    // Init self-healing historical duration.
    _selfHealingEnabledHistoricalDurationMs = new HashMap<>();
    KafkaAnomalyType.cachedValues().forEach(anomalyType -> _selfHealingEnabledHistoricalDurationMs.put(anomalyType, 0L));
    _latestFailedBrokersByAutoFixTriggered = new HashMap<>();
    _latestFailedBrokersByAutoFixTriggered.put(true, new HashMap<>());
    _latestFailedBrokersByAutoFixTriggered.put(false, new HashMap<>());
  }

  @Override
  public AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations) {
    boolean autoFixTriggered = _selfHealingEnabled.get(KafkaAnomalyType.GOAL_VIOLATION);
    boolean selfHealingTriggered = autoFixTriggered && !hasUnfixableGoals(goalViolations);
    alert(goalViolations, selfHealingTriggered, _time.milliseconds(), KafkaAnomalyType.GOAL_VIOLATION);

    if (autoFixTriggered) {
      if (selfHealingTriggered) {
        return AnomalyNotificationResult.fix();
      }
      // If there are unfixable goals, do not self heal even when it is enabled and selfHealing goals include the unfixable goal.
      LOG.warn("Self-healing is not possible due to unfixable goals: {}{}.",
               goalViolations.violatedGoalsByFixability().get(false),
               goalViolations.provisionResponse() == null ? "" : String.format(", Provision: %s.", goalViolations.provisionResponse()));
    }

    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onIntraBrokerGoalViolation(IntraBrokerGoalViolations goalViolations) {
    boolean autoFixTriggered = _selfHealingEnabled.get(KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION);
    boolean selfHealingTriggered = autoFixTriggered && !hasUnfixableGoals(goalViolations);
    alert(goalViolations, selfHealingTriggered, _time.milliseconds(), KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION);

    if (autoFixTriggered) {
      if (selfHealingTriggered) {
        return AnomalyNotificationResult.fix();
      }
      // If there are unfixable goals, do not self heal even when it is enabled and selfHealing goals include the unfixable goal.
      LOG.warn("Self-healing is not possible due to unfixable goals: {}{}.",
              goalViolations.violatedGoalsByFixability().get(false),
              goalViolations.provisionResponse() == null ? "" : String.format(", Provision: %s.", goalViolations.provisionResponse()));
    }

    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly) {
    boolean autoFixTriggered = _selfHealingEnabled.get(KafkaAnomalyType.METRIC_ANOMALY) && metricAnomaly.fixable();
    alert(metricAnomaly, autoFixTriggered, _time.milliseconds(), KafkaAnomalyType.METRIC_ANOMALY);
    return autoFixTriggered ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onTopicAnomaly(TopicAnomaly topicAnomaly) {
    boolean autoFixTriggered = _selfHealingEnabled.get(KafkaAnomalyType.TOPIC_ANOMALY);
    alert(topicAnomaly, autoFixTriggered, _time.milliseconds(), KafkaAnomalyType.TOPIC_ANOMALY);
    return autoFixTriggered ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMaintenanceEvent(MaintenanceEvent maintenanceEvent) {
    boolean autoFixTriggered = _selfHealingEnabled.get(KafkaAnomalyType.MAINTENANCE_EVENT);
    alert(maintenanceEvent, autoFixTriggered, _time.milliseconds(), KafkaAnomalyType.MAINTENANCE_EVENT);
    return autoFixTriggered ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onDiskFailure(DiskFailures diskFailures) {
    alert(diskFailures, _selfHealingEnabled.get(KafkaAnomalyType.DISK_FAILURE), _time.milliseconds(), KafkaAnomalyType.DISK_FAILURE);
    return _selfHealingEnabled.get(KafkaAnomalyType.DISK_FAILURE) ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
  }

  @Override
  public Map<AnomalyType, Boolean> selfHealingEnabled() {
    return Collections.unmodifiableMap(_selfHealingEnabled);
  }

  @Override
  public synchronized boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    Boolean oldValue = _selfHealingEnabled.put(anomalyType, isSelfHealingEnabled);
    updateSelfHealingStateChange(anomalyType, oldValue, isSelfHealingEnabled);
    return oldValue;
  }

  private void updateSelfHealingStateChange(AnomalyType anomalyType, Boolean oldValue, boolean isSelfHealingEnabled) {
    if (oldValue == null) {
      throw new IllegalStateException(String.format("No previous value is associated with %s.", anomalyType));
    }
    // Update self-healing state change time if there has been a change.
    if (oldValue != isSelfHealingEnabled) {
      // 1. Update the historical duration of old state.
      long oldStateChangeMs = _selfHealingStateChangeTimeMs.get(oldValue).get(anomalyType);
      long newStateChangeMs = _time.milliseconds();

      if (!isSelfHealingEnabled) {
        _selfHealingEnabledHistoricalDurationMs.merge(anomalyType, newStateChangeMs - oldStateChangeMs, Long::sum);
      }

      // 2. Update new state start time.
      _selfHealingStateChangeTimeMs.get(isSelfHealingEnabled).put(anomalyType, newStateChangeMs);
    }
  }

  private synchronized long enabledTimeMs(AnomalyType anomalyType, long nowMs) {
    long enabledTimeMs = _selfHealingEnabledHistoricalDurationMs.get(anomalyType);
    if (_selfHealingEnabled.get(anomalyType)) {
      // Add current duration during which the self-healing is enabled.
      Long currentEnabledSelfHealingStartTime = _selfHealingStateChangeTimeMs.get(true).get(anomalyType);
      enabledTimeMs += nowMs - (currentEnabledSelfHealingStartTime == null ? _notifierStartTimeMs : currentEnabledSelfHealingStartTime);
    }
    return enabledTimeMs;
  }

  @Override
  public synchronized Map<AnomalyType, Float> selfHealingEnabledRatio() {
    Map<AnomalyType, Float> selfHealingEnabledRatio = new HashMap<>();
    long nowMs = _time.milliseconds();
    long uptimeMs = uptimeMs(nowMs);

    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      long enabledTimeMs = enabledTimeMs(anomalyType, nowMs);
      selfHealingEnabledRatio.put(anomalyType, ((float) enabledTimeMs / uptimeMs));
    }

    return selfHealingEnabledRatio;
  }

  /**
   * Check whether the given broker failures with the corresponding autoFixTriggered contains a new failure to alert.
   *
   * @param brokerFailures The detected broker failures
   * @param autoFixTriggered {@code true} if auto fix has been triggered, {@code false} otherwise.
   * @return {@code true} if any of the broker failures with the corresponding autoFixTriggered has not been alerted, {@code false} otherwise.
   */
  private boolean hasNewFailureToAlert(BrokerFailures brokerFailures, boolean autoFixTriggered) {
    Map<Integer, Long> failedBrokers = _latestFailedBrokersByAutoFixTriggered.get(autoFixTriggered);
    boolean containsNewAlert = false;

    for (Map.Entry<Integer, Long> entry : brokerFailures.failedBrokers().entrySet()) {
      Long failureTime = failedBrokers.get(entry.getKey());
      if (failureTime == null || failureTime.longValue() != entry.getValue().longValue()) {
        failedBrokers.put(entry.getKey(), entry.getValue());
        containsNewAlert = true;
      }
    }

    return containsNewAlert;
  }

  @Override
  public AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures) {
    long earliestFailureTimeMs = Long.MAX_VALUE;
    for (long t : brokerFailures.failedBrokers().values()) {
      earliestFailureTimeMs = Math.min(earliestFailureTimeMs, t);
    }
    long nowMs = _time.milliseconds();
    long alertTimeMs = earliestFailureTimeMs + _brokerFailureAlertThresholdMs;
    long selfHealingTimeMs = earliestFailureTimeMs + _selfHealingThresholdMs;
    AnomalyNotificationResult result = null;
    if (nowMs < alertTimeMs) {
      // Not reaching alerting threshold yet.
      long delayMs = alertTimeMs - nowMs;
      result = AnomalyNotificationResult.check(delayMs);
    } else if (nowMs < selfHealingTimeMs) {
      // Reached alert threshold. Alert but do not fix.
      if (hasNewFailureToAlert(brokerFailures, false)) {
        alert(brokerFailures, false, selfHealingTimeMs, KafkaAnomalyType.BROKER_FAILURE);
      }
      long delayMs = selfHealingTimeMs - nowMs;
      result = AnomalyNotificationResult.check(delayMs);
    } else {
      // Reached auto fix threshold. Alert and fix if self healing is enabled and anomaly is fixable.
      boolean autoFixTriggered = _selfHealingEnabled.get(KafkaAnomalyType.BROKER_FAILURE) && brokerFailures.fixable();
      if (hasNewFailureToAlert(brokerFailures, autoFixTriggered)) {
        alert(brokerFailures, autoFixTriggered, selfHealingTimeMs, KafkaAnomalyType.BROKER_FAILURE);
      }
      result = autoFixTriggered ? AnomalyNotificationResult.fix() : AnomalyNotificationResult.ignore();
    }
    return result;
  }

  /**
   * Alert on anomaly.
   *
   * @param anomaly Detected anomaly.
   * @param autoFixTriggered {@code true} if auto fix has been triggered, {@code false} otherwise.
   * @param selfHealingStartTime The time that the self healing started.
   * @param anomalyType Type of anomaly.
   */
  public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
    LOG.warn("{} detected {}. Self healing {}.", anomalyType, anomaly,
             _selfHealingEnabled.get(anomalyType) ? String.format("start time %s", utcDateFor(selfHealingStartTime)) : "is disabled");

    if (autoFixTriggered) {
      LOG.warn("Self-healing has been triggered.");
    }
  }

  @Override
  public void configure(Map<String, ?> config) {
    String alertThreshold = (String) config.get(BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG);
    _brokerFailureAlertThresholdMs = alertThreshold == null ? DEFAULT_ALERT_THRESHOLD_MS : Long.parseLong(alertThreshold);
    String fixThreshold = (String) config.get(BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG);
    _selfHealingThresholdMs = fixThreshold == null ? DEFAULT_AUTO_FIX_THRESHOLD_MS : Long.parseLong(fixThreshold);
    if (_brokerFailureAlertThresholdMs > _selfHealingThresholdMs) {
      throw new IllegalArgumentException(String.format("The failure detection threshold %d cannot be larger than "
                                                       + "the auto fix threshold. %d",
                                                       _brokerFailureAlertThresholdMs, _selfHealingThresholdMs));
    }
    // Global config for self healing.
    String selfHealingEnabledString = (String) config.get(SELF_HEALING_ENABLED_CONFIG);
    boolean selfHealingAllEnabled = Boolean.parseBoolean(selfHealingEnabledString);
    // Per anomaly detector configs for self healing.
    String selfHealingBrokerFailureEnabledString = (String) config.get(SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.BROKER_FAILURE, selfHealingBrokerFailureEnabledString == null
                                                             ? selfHealingAllEnabled
                                                             : Boolean.parseBoolean(selfHealingBrokerFailureEnabledString));
    String selfHealingGoalViolationEnabledString = (String) config.get(SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.GOAL_VIOLATION, selfHealingGoalViolationEnabledString == null
                                                             ? selfHealingAllEnabled
                                                             : Boolean.parseBoolean(selfHealingGoalViolationEnabledString));
    String selfHealingIntraBrokersGoalViolationEnabledString = (String) config.get(SELF_HEALING_INTRA_BROKER_GOAL_VIOLATION_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION, selfHealingIntraBrokersGoalViolationEnabledString == null
                                                                          ? selfHealingAllEnabled
                                                                          : Boolean.parseBoolean(selfHealingIntraBrokersGoalViolationEnabledString));
    String selfHealingMetricAnomalyEnabledString = (String) config.get(SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.METRIC_ANOMALY, selfHealingMetricAnomalyEnabledString == null
                                                             ? selfHealingAllEnabled
                                                             : Boolean.parseBoolean(selfHealingMetricAnomalyEnabledString));
    String selfHealingDiskFailuresEnabledString = (String) config.get(SELF_HEALING_DISK_FAILURE_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.DISK_FAILURE, selfHealingDiskFailuresEnabledString == null
                                                           ? selfHealingAllEnabled
                                                           : Boolean.parseBoolean(selfHealingDiskFailuresEnabledString));
    String selfHealingTopicAnomalyEnabledString = (String) config.get(SELF_HEALING_TOPIC_ANOMALY_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.TOPIC_ANOMALY, selfHealingTopicAnomalyEnabledString == null
                                                            ? selfHealingAllEnabled
                                                            : Boolean.parseBoolean(selfHealingTopicAnomalyEnabledString));
    String selfHealingMaintenanceEventEnabledString = (String) config.get(SELF_HEALING_MAINTENANCE_EVENT_ENABLED_CONFIG);
    _selfHealingEnabled.put(KafkaAnomalyType.MAINTENANCE_EVENT, selfHealingMaintenanceEventEnabledString == null
                                                                ? selfHealingAllEnabled
                                                                : Boolean.parseBoolean(selfHealingMaintenanceEventEnabledString));
    // Set self-healing current state start time.
    _selfHealingEnabled.forEach((key, value) -> _selfHealingStateChangeTimeMs.get(value).put(key, _notifierStartTimeMs));
  }

  @Override
  public long uptimeMs(long nowMs) {
    return nowMs - _notifierStartTimeMs;
  }
}
