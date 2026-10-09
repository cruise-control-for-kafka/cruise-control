/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toPrettyDuration;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.METRIC_ANOMALY;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.BROKER_FAILURE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.DISK_FAILURE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.TOPIC_ANOMALY;

@JsonResponseClass
public class AnomalyMetrics {
  @JsonResponseField
  private static final String MEAN_TIME_BETWEEN_ANOMALIES_MS = "meanTimeBetweenAnomaliesMs";
  @JsonResponseField
  private static final String MEAN_TIME_TO_START_FIX_MS = "meanTimeToStartFixMs";
  @JsonResponseField
  private static final String ONGOING_ANOMALY_DURATION_MS = "ongoingAnomalyDurationMs";
  @JsonResponseField
  private static final String NUM_SELF_HEALING_STARTED = "numSelfHealingStarted";
  @JsonResponseField
  private static final String NUM_SELF_HEALING_FAILED_TO_START = "numSelfHealingFailedToStart";

  private final MeanTimeBetweenAnomaliesMs _meanTimeBetweenAnomaliesMs;
  private final double _meanTimeToStartFixMs;
  private final long _numSelfHealingStarted;
  private final long _numSelfHealingFailedToStart;
  private final long _ongoingAnomalyDurationMs;

  /**
   * <ol>
   *   <li>Mean time between anomalies: The mean time between detected anomalies, while there is no ongoing execution.</li>
   *   <li>Mean time to start fix: Mean time between the detection of earliest ongoing anomaly and start of a proposal
   *   execution. This metric covers executions started both by anomaly detector and as a result of on-demand requests.
   *   This metric excludes the time of the current ongoing unfixed anomaly.</li>
   *   <li>Number of self healing started: Successful self-healing fixes started by anomaly detector.</li>
   *   <li>Ongoing anomaly duration: The time (in milliseconds) from the start of the earliest unfixed (ongoing) anomaly
   *   to the current time for which no fix has been started. 0, if there is no unfixed (ongoing) anomaly.</li>
   * </ol>
   *
   * @param meanTimeBetweenAnomaliesMs Mean time between anomalies by the corresponding type.
   * @param meanTimeToStartFixMs Mean time to start fix for any anomaly.
   * @param numSelfHealingStarted Number of fixes started by the anomaly detector as a result of self healing.
   * @param numSelfHealingFailedToStart Number of anomaly fixes failed to start despite the anomaly in progress being ready
   * to fix. This typically indicates the need for expanding the cluster or relaxing the constraints of self-healing goals.
   * @param ongoingAnomalyDurationMs The duration of the ongoing (unfixed/unfixable) anomaly if there is any, 0 otherwise.
   */
  public AnomalyMetrics(Map<AnomalyType, Double> meanTimeBetweenAnomaliesMs,
                        double meanTimeToStartFixMs,
                        long numSelfHealingStarted,
                        long numSelfHealingFailedToStart,
                        long ongoingAnomalyDurationMs) {
    if (meanTimeBetweenAnomaliesMs == null) {
      throw new IllegalArgumentException("Attempt to set meanTimeBetweenAnomaliesMs with null.");
    }
    for (AnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      if (!meanTimeBetweenAnomaliesMs.containsKey(anomalyType)) {
        throw new IllegalArgumentException(anomalyType + " is missing in meanTimeBetweenAnomaliesMs metric.");
      }
    }
    _meanTimeBetweenAnomaliesMs = new MeanTimeBetweenAnomaliesMs(meanTimeBetweenAnomaliesMs);
    _meanTimeToStartFixMs = meanTimeToStartFixMs;
    _numSelfHealingStarted = numSelfHealingStarted;
    _numSelfHealingFailedToStart = numSelfHealingFailedToStart;
    _ongoingAnomalyDurationMs = ongoingAnomalyDurationMs;
  }

  /**
   * @return Mean time to start a fix.
   */
  public double meanTimeToStartFixMs() {
    return _meanTimeToStartFixMs;
  }

  /**
   * @return Mean time between anomalies.
   */
  public MeanTimeBetweenAnomaliesMs meanTimeBetweenAnomaliesMs() {
    return _meanTimeBetweenAnomaliesMs;
  }

  /**
   * @return Number of self healing operations started.
   */
  public long numSelfHealingStarted() {
    return _numSelfHealingStarted;
  }

  /**
   * @return Number of anomaly fixes failed to start despite the anomaly in progress being ready to fix.
   * This typically indicates the need for expanding the cluster or relaxing the constraints of self-healing goals.
   */
  public long numSelfHealingFailedToStart() {
    return _numSelfHealingFailedToStart;
  }

  /**
   * @return The duration of ongoing anomaly in milliseconds.
   */
  public long ongoingAnomalyDurationMs() {
    return _ongoingAnomalyDurationMs;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    return Map.of(MEAN_TIME_BETWEEN_ANOMALIES_MS, meanTimeBetweenAnomaliesMs().getJsonStructure(),
                  MEAN_TIME_TO_START_FIX_MS, meanTimeToStartFixMs(), NUM_SELF_HEALING_STARTED, numSelfHealingStarted(),
                  NUM_SELF_HEALING_FAILED_TO_START, numSelfHealingFailedToStart(), ONGOING_ANOMALY_DURATION_MS, ongoingAnomalyDurationMs());
  }

  @Override
  public String toString() {
    Map<AnomalyType, Double> meanTimeBetweenAnomalies = _meanTimeBetweenAnomaliesMs.getJsonStructure();
    return String.format("{meanTimeBetweenAnomalies:{%s:%s, %s:%s, %s:%s, %s:%s, %s:%s, %s:%s}, meanTimeToStartFix:%s, "
                         + "numSelfHealingStarted:%d, numSelfHealingFailedToStart:%d, ongoingAnomalyDuration=%s}",
                         GOAL_VIOLATION, toPrettyDuration(meanTimeBetweenAnomalies.get(GOAL_VIOLATION)),
                         INTRA_BROKER_GOAL_VIOLATION, toPrettyDuration(meanTimeBetweenAnomalies.get(INTRA_BROKER_GOAL_VIOLATION)),
                         BROKER_FAILURE, toPrettyDuration(meanTimeBetweenAnomalies.get(BROKER_FAILURE)),
                         METRIC_ANOMALY, toPrettyDuration(meanTimeBetweenAnomalies.get(METRIC_ANOMALY)),
                         DISK_FAILURE, toPrettyDuration(meanTimeBetweenAnomalies.get(DISK_FAILURE)),
                         TOPIC_ANOMALY, toPrettyDuration(meanTimeBetweenAnomalies.get(TOPIC_ANOMALY)),
                         toPrettyDuration(_meanTimeToStartFixMs), _numSelfHealingStarted, _numSelfHealingFailedToStart,
                         toPrettyDuration(_ongoingAnomalyDurationMs));
  }
}
