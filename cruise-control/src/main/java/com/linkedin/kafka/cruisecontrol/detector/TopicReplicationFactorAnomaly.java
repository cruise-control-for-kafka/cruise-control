/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.UpdateTopicConfigurationRunnable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.buildTopicRegex;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder.BAD_TOPICS_BY_DESIRED_RF_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.TOPIC_ANOMALY;


/**
 * Topics, which have at least one partition that violates replication factor requirements. For more detail about detection
 * process and criteria, check {@link com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder}.
 */
public class TopicReplicationFactorAnomaly extends TopicAnomaly {
    // Bad topic here refers to the topic having at least one partition, which violates replication factor requirements.
    protected Map<Short, Set<TopicReplicationFactorAnomalyEntry>> _badTopicsByDesiredRF;
    protected UpdateTopicConfigurationRunnable _updateTopicConfigurationRunnable;

    /**
     * @return An unmodifiable version of the actual bad topic map
     */
    public Map<Short, Set<TopicReplicationFactorAnomalyEntry>> badTopicsByDesiredRF() {
        return Collections.unmodifiableMap(_badTopicsByDesiredRF);
    }

    @Override
    public boolean fix() throws Exception {
        if (_updateTopicConfigurationRunnable == null) {
            return false;
        }
        _optimizationResult = _updateTopicConfigurationRunnable.getResult();
        boolean hasProposalsToFix = hasProposalsToFix();
        // Ensure that only the relevant response is cached to avoid memory pressure.
        _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
        return hasProposalsToFix;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, KafkaAnomalyType.TOPIC_ANOMALY);
        KafkaCruiseControlConfig config = kafkaCruiseControl.config();
        _badTopicsByDesiredRF = (Map<Short, Set<TopicReplicationFactorAnomalyEntry>>) configs.get(BAD_TOPICS_BY_DESIRED_RF_CONFIG);
        if (_badTopicsByDesiredRF == null || _badTopicsByDesiredRF.isEmpty()) {
            throw new IllegalArgumentException(String.format("Missing %s for topic replication factor anomaly.", BAD_TOPICS_BY_DESIRED_RF_CONFIG));
        }
        boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
        boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
        boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
        boolean skipRackAwarenessCheck = config.getBoolean(RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG);
        Map<Short, Pattern> topicPatternByReplicationFactor = populateTopicPatternByReplicationFactor();
        _updateTopicConfigurationRunnable = new UpdateTopicConfigurationRunnable(kafkaCruiseControl,
                topicPatternByReplicationFactor,
                getSelfHealingGoalNames(config),
                allowCapacityEstimation,
                excludeRecentlyDemotedBrokers,
                excludeRecentlyRemovedBrokers,
                _anomalyId.toString(),
                reasonSupplier(),
                stopOngoingExecution(),
                skipRackAwarenessCheck);
    }

    protected Map<Short, Pattern> populateTopicPatternByReplicationFactor() {
        Map<Short, Pattern> topicPatternByReplicationFactor = new HashMap<>();
        for (Map.Entry<Short, Set<TopicReplicationFactorAnomalyEntry>> entry : _badTopicsByDesiredRF.entrySet()) {
            Set<String> topics = new HashSet<>();
            entry.getValue().forEach(anomaly -> topics.add(anomaly.topicName()));
            topicPatternByReplicationFactor.put(entry.getKey(), buildTopicRegex(topics));
        }
        return topicPatternByReplicationFactor;
    }

    @Override
    public Supplier<String> reasonSupplier() {
        return () -> String.format("Self healing for %s: %s", TOPIC_ANOMALY, this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{Topics with replication factor violations: [");
        _badTopicsByDesiredRF.forEach(
                (key, value) -> sb.append(String.format("{With desired RF %d: %s}, ", key, value)));
        sb.setLength(sb.length() - 2);
        sb.append("]}");
        return sb.toString();
    }

    public static class TopicReplicationFactorAnomalyEntry {
        private final String _topicName;
        private final double _violationRatio;

        public TopicReplicationFactorAnomalyEntry(String topicName, double violationRatio) {
            _topicName = topicName;
            _violationRatio = violationRatio;
        }

        public String topicName() {
            return _topicName;
        }

        public double violationRatio() {
            return _violationRatio;
        }

        @Override
        public String toString() {
            return String.format("{%s(%.2f)}", _topicName, _violationRatio * 100.0);
        }
    }
}
