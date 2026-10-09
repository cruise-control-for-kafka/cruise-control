/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyState.Status.DETECTED;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;


public class AnomalyDetailsTest {
  private static final String MOCK_ANOMALY_ID = UUID.randomUUID().toString();
  private static final long MOCK_DETECTION_MS = 100L;
  private static final long MOCK_STATUS_UPDATE_MS = 200L;
  private static final Map<Boolean, List<String>> VIOLATED_GOALS_BY_FIXABILITY;
  static {
    VIOLATED_GOALS_BY_FIXABILITY = Map.of(true, Collections.singletonList("Goal-1"), false, Collections.singletonList("Goal-2"));
  }
  private static final Map<Integer, Long> FAILED_BROKERS = Collections.singletonMap(1, 100L);
  private static final Map<Integer, Map<String, Long>> FAILED_DISKS = Collections.singletonMap(1, Collections.singletonMap("logDir", 100L));
  private static final String MOCK_DESCRIPTION = "MockDescription";

  @Test
  public void testPopulateAnomalyDetails() {
    AnomalyState mockAnomalyState = EasyMock.mock(AnomalyState.class);

    for (KafkaAnomalyType anomalyType : KafkaAnomalyType.cachedValues()) {
      AnomalyDetails details = new AnomalyDetails(mockAnomalyState, anomalyType, false, false);
      EasyMock.expect(mockAnomalyState.detectionMs()).andReturn(MOCK_DETECTION_MS).once();
      EasyMock.expect(mockAnomalyState.status()).andReturn(DETECTED).once();
      EasyMock.expect(mockAnomalyState.anomalyId()).andReturn(MOCK_ANOMALY_ID).once();
      EasyMock.expect(mockAnomalyState.statusUpdateMs()).andReturn(MOCK_STATUS_UPDATE_MS).once();

      switch (anomalyType) {
        case GOAL_VIOLATION:
          GoalViolations goalViolations = EasyMock.mock(GoalViolations.class);
          EasyMock.expect(goalViolations.violatedGoalsByFixability()).andReturn(VIOLATED_GOALS_BY_FIXABILITY).once();
          replayAndVerify(mockAnomalyState, details, goalViolations);
          break;
        case INTRA_BROKER_GOAL_VIOLATION:
          IntraBrokerGoalViolations intraBrokerGoalViolations = EasyMock.mock(IntraBrokerGoalViolations.class);
          EasyMock.expect(intraBrokerGoalViolations.violatedGoalsByFixability()).andReturn(VIOLATED_GOALS_BY_FIXABILITY).once();
          replayAndVerify(mockAnomalyState, details, intraBrokerGoalViolations);
          break;
        case BROKER_FAILURE:
          BrokerFailures brokerFailures = EasyMock.mock(BrokerFailures.class);
          EasyMock.expect(brokerFailures.failedBrokers()).andReturn(FAILED_BROKERS).once();
          replayAndVerify(mockAnomalyState, details, brokerFailures);
          break;
        case DISK_FAILURE:
          DiskFailures diskFailures = EasyMock.mock(DiskFailures.class);
          EasyMock.expect(diskFailures.failedDisks()).andReturn(FAILED_DISKS).once();
          replayAndVerify(mockAnomalyState, details, diskFailures);
          break;
        case METRIC_ANOMALY:
          KafkaMetricAnomaly kafkaMetricAnomaly = EasyMock.mock(KafkaMetricAnomaly.class);
          EasyMock.expect(kafkaMetricAnomaly.description()).andReturn(MOCK_DESCRIPTION).once();
          replayAndVerify(mockAnomalyState, details, kafkaMetricAnomaly);
          break;
        case TOPIC_ANOMALY:
          TopicAnomaly topicAnomaly = EasyMock.mock(TopicAnomaly.class);
          // Note: EasyMock provides a built-in behavior for equals(), toString(), hashCode() and finalize() even for class mocking.
          // Hence, we cannot record our own behavior for topicAnomaly.toString().
          replayAndVerify(mockAnomalyState, details, topicAnomaly);
          break;
        case MAINTENANCE_EVENT:
          MaintenanceEvent maintenanceEvent = EasyMock.mock(MaintenanceEvent.class);
          // Note: EasyMock provides a built-in behavior for equals(), toString(), hashCode() and finalize() even for class mocking.
          // Hence, we cannot record our own behavior for maintenanceEvent.toString().
          replayAndVerify(mockAnomalyState, details, maintenanceEvent);
          break;
        default:
          throw new IllegalStateException("Unrecognized anomaly type " + anomalyType);
      }
    }
  }

  private static void replayAndVerify(AnomalyState mockAnomalyState, AnomalyDetails details, Anomaly anomaly) {
    EasyMock.expect(mockAnomalyState.anomaly()).andReturn(anomaly).once();
    EasyMock.replay(mockAnomalyState, anomaly);
    Map<String, Object> populatedAnomalyDetails = details.populateAnomalyDetails();
    assertNotNull(populatedAnomalyDetails);
    assertFalse(populatedAnomalyDetails.isEmpty());
    EasyMock.verify(mockAnomalyState, anomaly);
    EasyMock.reset(mockAnomalyState, anomaly);
  }
}
