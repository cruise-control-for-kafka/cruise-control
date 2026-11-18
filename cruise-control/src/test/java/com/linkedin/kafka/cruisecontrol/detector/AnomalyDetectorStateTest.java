/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import org.apache.kafka.common.utils.MockTime;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ANOMALY_DETECTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState.NUM_SELF_HEALING_ENDED_FOR_ANOMALY_TYPE;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState.NUM_SELF_HEALING_STARTED_FOR_ANOMALY_TYPE;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState.ONGOING_SELF_HEALING_DURATION_MS_FOR_ANOMALY_TYPE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.BROKER_FAILURE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.DISK_FAILURE;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;
import static org.junit.Assert.assertEquals;


public class AnomalyDetectorStateTest {
  private static final int NUM_CACHED_RECENT_ANOMALY_STATES = 10;

  @Test
  public void testSelfHealingSensorsByAnomalyType() {
    MetricRegistry metricRegistry = new MetricRegistry();
    MockTime time = new MockTime();
    AnomalyDetectorState anomalyDetectorState = new AnomalyDetectorState(time, null, NUM_CACHED_RECENT_ANOMALY_STATES, metricRegistry);
    for (KafkaAnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      assertSelfHealingSensors(metricRegistry, anomalyType, 0L, 0L, 0L);
    }

    // Self-healing of a goal violation starts, and completes successfully.
    startSelfHealing(anomalyDetectorState, anomaly("goalViolation", GOAL_VIOLATION, time.milliseconds()));
    time.sleep(1000L);
    assertSelfHealingSensors(metricRegistry, GOAL_VIOLATION, 1L, 0L, 1000L);
    assertSelfHealingSensors(metricRegistry, BROKER_FAILURE, 0L, 0L, 0L);
    anomalyDetectorState.markSelfHealingFinished("goalViolation", false);
    assertSelfHealingSensors(metricRegistry, GOAL_VIOLATION, 1L, 1L, 0L);
    assertEquals(1L, detectToFixCompleteCount(metricRegistry, GOAL_VIOLATION));

    // Self-healing of a broker failure starts, and completes with error: it ends, but is not timed as a completed fix.
    startSelfHealing(anomalyDetectorState, anomaly("brokerFailure", BROKER_FAILURE, time.milliseconds()));
    time.sleep(500L);
    assertSelfHealingSensors(metricRegistry, BROKER_FAILURE, 1L, 0L, 500L);
    assertSelfHealingSensors(metricRegistry, GOAL_VIOLATION, 1L, 1L, 0L);
    anomalyDetectorState.markSelfHealingFinished("brokerFailure", true);
    assertSelfHealingSensors(metricRegistry, BROKER_FAILURE, 1L, 1L, 0L);
    assertEquals(0L, detectToFixCompleteCount(metricRegistry, BROKER_FAILURE));

    assertEquals(2L, anomalyDetectorState.numSelfHealingStarted());
  }

  @Test
  public void testSelfHealingCountsWithoutMetricRegistry() {
    MockTime time = new MockTime();
    AnomalyDetectorState anomalyDetectorState = new AnomalyDetectorState(time, null, NUM_CACHED_RECENT_ANOMALY_STATES, null);

    startSelfHealing(anomalyDetectorState, anomaly("diskFailure", DISK_FAILURE, time.milliseconds()));
    assertEquals(1L, anomalyDetectorState.numSelfHealingStarted(DISK_FAILURE));
    assertEquals(0L, anomalyDetectorState.numSelfHealingEnded(DISK_FAILURE));
    anomalyDetectorState.markSelfHealingFinished("diskFailure", false);
    assertEquals(1L, anomalyDetectorState.numSelfHealingEnded(DISK_FAILURE));
    assertEquals(0L, anomalyDetectorState.numSelfHealingStarted(GOAL_VIOLATION));
  }

  // Starts self-healing of the given anomaly in the same order as AnomalyDetectorManager does.
  private static void startSelfHealing(AnomalyDetectorState anomalyDetectorState, Anomaly anomaly) {
    anomalyDetectorState.onAnomalyHandle(anomaly, AnomalyState.Status.FIX_STARTED);
    anomalyDetectorState.incrementNumSelfHealingStarted(anomaly.anomalyType());
  }

  private static Anomaly anomaly(String anomalyId, AnomalyType anomalyType, long detectionTimeMs) {
    Anomaly anomaly = EasyMock.mock(Anomaly.class);
    EasyMock.expect(anomaly.anomalyId()).andReturn(anomalyId).anyTimes();
    EasyMock.expect(anomaly.anomalyType()).andReturn(anomalyType).anyTimes();
    EasyMock.expect(anomaly.detectionTimeMs()).andReturn(detectionTimeMs).anyTimes();
    EasyMock.replay(anomaly);
    return anomaly;
  }

  private static void assertSelfHealingSensors(MetricRegistry metricRegistry,
                                               AnomalyType anomalyType,
                                               long expectedStarted,
                                               long expectedEnded,
                                               long expectedOngoingDurationMs) {
    assertEquals(expectedStarted,
                 metricRegistry.getCounters().get(sensorName(NUM_SELF_HEALING_STARTED_FOR_ANOMALY_TYPE, anomalyType)).getCount());
    assertEquals(expectedEnded,
                 metricRegistry.getCounters().get(sensorName(NUM_SELF_HEALING_ENDED_FOR_ANOMALY_TYPE, anomalyType)).getCount());
    assertEquals(expectedOngoingDurationMs,
                 metricRegistry.getGauges().get(sensorName(ONGOING_SELF_HEALING_DURATION_MS_FOR_ANOMALY_TYPE, anomalyType)).getValue());
  }

  private static long detectToFixCompleteCount(MetricRegistry metricRegistry, AnomalyType anomalyType) {
    return metricRegistry.getTimers().get(sensorName("%s-detect-to-fix-complete-timer", anomalyType)).getCount();
  }

  private static String sensorName(String format, AnomalyType anomalyType) {
    return MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, String.format(format, anomalyType.toString().toLowerCase()));
  }
}
