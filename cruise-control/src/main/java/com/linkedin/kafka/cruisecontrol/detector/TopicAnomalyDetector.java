/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;

/**
 * This class will be scheduled to periodically check if {@link TopicAnomalyFinder} identifies a topic anomaly.
 * An alert will be triggered if one of the desired topic property is not met.
 */
public class TopicAnomalyDetector extends AbstractAnomalyDetector implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicAnomalyDetector.class);
    private final List<TopicAnomalyFinder> _topicAnomalyFinders;

    TopicAnomalyDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
        super(anomalies, kafkaCruiseControl);
        Map<String, Object> configWithCruiseControlObject = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG,
                kafkaCruiseControl);
        _topicAnomalyFinders = kafkaCruiseControl.config().getConfiguredInstances(AnomalyDetectorConfig.TOPIC_ANOMALY_FINDER_CLASSES_CONFIG,
                TopicAnomalyFinder.class,
                configWithCruiseControlObject);
    }

    @Override
    public void run() {
        try {
            if (getAnomalyDetectionStatus(_kafkaCruiseControl, true, true) != AnomalyDetectionStatus.READY) {
                return;
            }
            for (TopicAnomalyFinder topicAnomalyFinder : _topicAnomalyFinders) {
                _anomalies.addAll(topicAnomalyFinder.topicAnomalies());
            }
        } catch (Exception e) {
            LOG.warn("Topic anomaly detector encountered exception: ", e);
        } finally {
            LOG.debug("Topic anomaly detection finished.");
        }
    }
}
