/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.IntraBrokerGoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent;
import com.linkedin.kafka.cruisecontrol.detector.TopicAnomaly;
import java.util.Map;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * An interface to handle notifications corresponding to different anomalies and their self-healing status.
 */
@InterfaceStability.Evolving
public interface AnomalyNotifier extends CruiseControlConfigurable {

  /**
   * When a particular goal is violated this method will be called.
   *
   * @param goalViolations The detected goal violations.
   * @return The notification result that asks Cruise Control to perform a particular
   * {@link com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult.Action}.
   */
  AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations);

  /**
   * When a particular intra broker goal is violated this method will be called.
   *
   * @param goalViolations The detected intra broker goal violations.
   * @return The notification result that asks Cruise Control to perform a particular
   * {@link com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult.Action}.
   */
  AnomalyNotificationResult onIntraBrokerGoalViolation(IntraBrokerGoalViolations goalViolations);

  /**
   * The method will be called when a broker failure has been detected.
   *
   * @param brokerFailures the detected broker failures
   * @return The notification result that asks Cruise Control to perform a particular
   * {@link com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult.Action}.
   */
  AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures);

  /**
   * This method is called when a metric anomaly is detected.
   *
   * @param metricAnomaly the detected metric anomaly.
   * @return The notification result that asks Cruise Control to perform a particular
   * {@link com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult.Action}.
   */
  AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly);

  /**
   * This method is called when a topic anomaly is detected.
   *
   * @param topicAnomaly the detected topic anomaly.
   * @return The notification result that asks Cruise Control to perform a particular
   * {@link com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult.Action}.
   */
  AnomalyNotificationResult onTopicAnomaly(TopicAnomaly topicAnomaly);

  /**
   * This method is called when a maintenance event is detected.
   *
   * @param maintenanceEvent the detected maintenance event.
   * @return The notification result that asks Cruise Control to perform a particular
   * {@link com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult.Action}.
   */
  AnomalyNotificationResult onMaintenanceEvent(MaintenanceEvent maintenanceEvent);

  /**
   * This method is called when a disk failure is detected.
   *
   * @param diskFailures the detected disk failures.
   * @return The notification result that asks Cruise Control to perform one of the following behaviors: ignore, fix or
   * perform a delayed check.
   */
  AnomalyNotificationResult onDiskFailure(DiskFailures diskFailures);

  /**
   * Check whether the self healing is enabled for different anomaly types.
   *
   * @return A map from anomaly type to whether the self healing is enabled for that anomaly type or not.
   */
  Map<AnomalyType, Boolean> selfHealingEnabled();

  /**
   * Enable or disable self healing for the given anomaly type.
   *
   * @param anomalyType Type of anomaly for which to enable or disable self healing.
   * @param isSelfHealingEnabled {@code true} if self healing is enabled, {@code false} otherwise.
   * @return The old value of self healing for the given anomaly type.
   */
  boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled);

  /**
   * Get the ratio during which the self-healing is enabled over the total operating time.
   *
   * @return The ratio during which the self-healing is enabled over the total operating time for each anomaly type.
   */
  Map<AnomalyType, Float> selfHealingEnabledRatio();

  /**
   * @param nowMs Current time in ms.
   * @return Uptime until now in ms.
   */
  long uptimeMs(long nowMs);
}
