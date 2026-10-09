/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION;

@JsonResponseClass
public class AnomalyDetails {
  @JsonResponseField
  protected static final String STATUS_UPDATE_MS = "statusUpdateMs";
  protected static final String STATUS_UPDATE_DATE = "statusUpdateDate";
  @JsonResponseField
  protected static final String DETECTION_MS = "detectionMs";
  protected static final String DETECTION_DATE = "detectionDate";
  @JsonResponseField
  protected static final String STATUS = "status";
  @JsonResponseField
  protected static final String ANOMALY_ID = "anomalyId";
  @JsonResponseField(required = false)
  protected static final String FIXABLE_VIOLATED_GOALS = "fixableViolatedGoals";
  @JsonResponseField(required = false)
  protected static final String UNFIXABLE_VIOLATED_GOALS = "unfixableViolatedGoals";
  @JsonResponseField(required = false)
  protected static final String FAILED_BROKERS_BY_TIME_MS = "failedBrokersByTimeMs";
  @JsonResponseField(required = false)
  protected static final String FAILED_DISKS_BY_TIME_MS = "failedDisksByTimeMs";
  @JsonResponseField(required = false)
  protected static final String OPTIMIZATION_RESULT = "optimizationResult";
  @JsonResponseField(required = false)
  protected static final String DESCRIPTION = "description";

  protected AnomalyState _anomalyState;
  protected AnomalyType _anomalyType;
  protected boolean _hasFixStarted;
  protected boolean _isJson;

  AnomalyDetails(AnomalyState anomalyState, AnomalyType anomalyType, boolean hasFixStarted, boolean isJson) {
    _anomalyState = anomalyState;
    _anomalyType = anomalyType;
    _hasFixStarted = hasFixStarted;
    _isJson = isJson;
  }

  /**
   * @return An object that can be further used to encode into JSON to represent anomaly data
   */
  public Map<String, Object> populateAnomalyDetails() {
    // Goal violation has one more field than other anomaly types.
    Map<String, Object> anomalyDetails = new HashMap<>();
    anomalyDetails.put(_isJson ? DETECTION_MS : DETECTION_DATE,
                       _isJson ? _anomalyState.detectionMs() : utcDateFor(_anomalyState.detectionMs()));
    anomalyDetails.put(STATUS, _anomalyState.status());
    anomalyDetails.put(ANOMALY_ID, _anomalyState.anomalyId());
    anomalyDetails.put(_isJson ? STATUS_UPDATE_MS : STATUS_UPDATE_DATE,
                       _isJson ? _anomalyState.statusUpdateMs() : utcDateFor(_anomalyState.statusUpdateMs()));
    switch ((KafkaAnomalyType) _anomalyType) {
      case GOAL_VIOLATION:
        GoalViolations goalViolations = (GoalViolations) _anomalyState.anomaly();
        Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
        anomalyDetails.put(FIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(true, Collections.emptyList()));
        anomalyDetails.put(UNFIXABLE_VIOLATED_GOALS, violatedGoalsByFixability.getOrDefault(false, Collections.emptyList()));
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, goalViolations.optimizationResult(_isJson));
        }
        break;
      case INTRA_BROKER_GOAL_VIOLATION:
        IntraBrokerGoalViolations intraBrokerGoalViolations = (IntraBrokerGoalViolations) _anomalyState.anomaly();
        Map<Boolean, List<String>> violatedIntraBrokerGoalsByFixability = intraBrokerGoalViolations.violatedGoalsByFixability();
        anomalyDetails.put(FIXABLE_VIOLATED_GOALS, violatedIntraBrokerGoalsByFixability.getOrDefault(true, Collections.emptyList()));
        anomalyDetails.put(UNFIXABLE_VIOLATED_GOALS, violatedIntraBrokerGoalsByFixability.getOrDefault(false, Collections.emptyList()));
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, intraBrokerGoalViolations.optimizationResult(_isJson));
        }
        break;
      case BROKER_FAILURE:
        BrokerFailures brokerFailures = (BrokerFailures) _anomalyState.anomaly();
        anomalyDetails.put(FAILED_BROKERS_BY_TIME_MS, brokerFailures.failedBrokers());
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, brokerFailures.optimizationResult(_isJson));
        }
        break;
      case DISK_FAILURE:
        DiskFailures diskFailures = (DiskFailures) _anomalyState.anomaly();
        anomalyDetails.put(FAILED_DISKS_BY_TIME_MS, diskFailures.failedDisks());
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, diskFailures.optimizationResult(_isJson));
        }
        break;
      case METRIC_ANOMALY:
        KafkaMetricAnomaly metricAnomaly = (KafkaMetricAnomaly) _anomalyState.anomaly();
        anomalyDetails.put(DESCRIPTION, metricAnomaly.description());
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, metricAnomaly.optimizationResult(_isJson));
        }
        break;
      case TOPIC_ANOMALY:
        TopicAnomaly topicAnomaly = (TopicAnomaly) _anomalyState.anomaly();
        anomalyDetails.put(DESCRIPTION, topicAnomaly.toString());
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, topicAnomaly.optimizationResult(_isJson));
        }
        break;
      case MAINTENANCE_EVENT:
        MaintenanceEvent maintenanceEvent = (MaintenanceEvent) _anomalyState.anomaly();
        anomalyDetails.put(DESCRIPTION, maintenanceEvent.toString());
        if (_hasFixStarted) {
          anomalyDetails.put(OPTIMIZATION_RESULT, maintenanceEvent.optimizationResult(_isJson));
        }
        break;
      default:
        throw new IllegalStateException("Unrecognized anomaly type " + _anomalyType);
    }
    return anomalyDetails;
  }
}
