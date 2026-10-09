/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

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


/**
 * A no-op notifier.
 */
public class NoopNotifier implements AnomalyNotifier {
  private final Map<AnomalyType, Boolean> _selfHealingEnabled;

  public NoopNotifier() {
    _selfHealingEnabled = new HashMap<>();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    KafkaAnomalyType.cachedValues().forEach(anomalyType -> _selfHealingEnabled.put(anomalyType, false));
  }

  @Override
  public AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onIntraBrokerGoalViolation(IntraBrokerGoalViolations goalViolations) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onTopicAnomaly(TopicAnomaly topicAnomaly) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onMaintenanceEvent(MaintenanceEvent maintenanceEvent) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public AnomalyNotificationResult onDiskFailure(DiskFailures diskFailures) {
    return AnomalyNotificationResult.ignore();
  }

  @Override
  public Map<AnomalyType, Boolean> selfHealingEnabled() {
    return Collections.unmodifiableMap(_selfHealingEnabled);
  }

  @Override
  public boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    return false;
  }

  @Override
  public Map<AnomalyType, Float> selfHealingEnabledRatio() {
    Map<AnomalyType, Float> selfHealingEnabledRatio = new HashMap<>();
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      selfHealingEnabledRatio.put(anomalyType, 0.0f);
    }

    return selfHealingEnabledRatio;
  }

  @Override
  public long uptimeMs(long nowMs) {
    return 0;
  }
}
