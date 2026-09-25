/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigDef;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Type.CLASS;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfigUtils.getConfiguredInstance;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomaly.TopicReplicationFactorAnomalyEntry;


/**
 * The class will check whether there are topics having partition(s) with replication factor different than the desired value
 * which is configured by {@link #SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG}.
 * Note for topics having special minISR config, if its minISR plus value of {@link #TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG}
 * is larger than the value of {@link #SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG} and equals to its replication
 * factor, the topic will not be taken as an anomaly.
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the target replication factor of topics.</li>
 *   <li>{@link #TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK}: The config to specify topics excluded from the anomaly checking.
 *   The value is treated as a regular expression, default value is set to
 *   {@link #DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK}.</li>
 *   <li>{@link #TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG}: The config for the topic anomaly class name,
 *   default value is set to {@link #DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS}.</li>
 *   <li>{@link #TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG}: The config for the topic replication factor margin over minISR. For topics
 *   whose minISR plus this margin is larger than the target replication factor, their replication factor should not be decreased to
 *   target replication factor. Default value is set to {@link #DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN}.</li>
 *   <li>{@link #TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG}: The config for TTL time in millisecond of cached topic minISR
 *   records, default value is set to {@link #DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS}.</li>
 * </ul>
 */
public class TopicReplicationFactorAnomalyFinder implements TopicAnomalyFinder {
    private static final Logger LOG = LoggerFactory.getLogger(TopicAnomalyFinder.class);
    public static final String SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG = "self.healing.target.topic.replication.factor";
    public static final String TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK = "topic.excluded.from.replication.factor.check";
    public static final String DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK = "";
    public static final String TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG = "topic.replication.topic.anomaly.class";
    public static final Class<?> DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS = TopicReplicationFactorAnomaly.class;
    public static final String BAD_TOPICS_BY_DESIRED_RF_CONFIG = "bad.topics.by.desired.rf";
    public static final String TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG = "topic.replication.factor.margin";
    public static final short DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN = 1;
    public static final String TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG = "topic.min.isr.record.retention.time.ms";
    public static final long DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(12);
    public static final long DESCRIBE_TOPIC_CONFIG_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(100);
    private KafkaCruiseControl _kafkaCruiseControl;
    private short _targetReplicationFactor;
    private Pattern _topicExcludedFromCheck;
    private Class<?> _topicReplicationTopicAnomalyClass;
    private AdminClient _adminClient;
    private short _topicReplicationFactorMargin;
    private long _topicMinISRRecordRetentionTimeMs;
    private Map<String, TopicMinISREntry> _cachedTopicMinISR;

    public TopicReplicationFactorAnomalyFinder() {
    }

    /**
     * Package private for unit test.
     *
     * @param kafkaCruiseControl           Cruise control to query cluster metadata.
     * @param targetReplicationFactor      The target replication factor.
     * @param topicReplicationFactorMargin The config for the topic replication factor margin over minISR.
     * @param adminClient                  Admin client to query topic metadata.
     */
    TopicReplicationFactorAnomalyFinder(KafkaCruiseControl kafkaCruiseControl,
                                        short targetReplicationFactor,
                                        short topicReplicationFactorMargin,
                                        AdminClient adminClient) {
        _kafkaCruiseControl = kafkaCruiseControl;
        _targetReplicationFactor = targetReplicationFactor;
        _topicExcludedFromCheck = Pattern.compile(DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK);
        _topicReplicationTopicAnomalyClass = DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS;
        _topicReplicationFactorMargin = topicReplicationFactorMargin;
        _topicMinISRRecordRetentionTimeMs = DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS;
        _adminClient = adminClient;
        _cachedTopicMinISR = new LinkedHashMap<>();
    }

    @Override
    public Set<TopicAnomaly> topicAnomalies() {
        LOG.info("Start to detect topic replication factor anomaly.");
        Cluster cluster = _kafkaCruiseControl.kafkaCluster();
        Set<String> topicsToCheck;
        if (_topicExcludedFromCheck.pattern().isEmpty()) {
            topicsToCheck = new HashSet<>(cluster.topics());
        } else {
            topicsToCheck = new HashSet<>();
            cluster.topics().stream().filter(topic -> !_topicExcludedFromCheck.matcher(topic).matches()).forEach(topicsToCheck::add);
        }
        refreshTopicMinISRCache();
        if (!topicsToCheck.isEmpty()) {
            maybeRetrieveAndCacheTopicMinISR(topicsToCheck);
            Map<Short, Set<TopicReplicationFactorAnomalyEntry>> badTopicsByDesiredRF = populateBadTopicsByDesiredRF(topicsToCheck, cluster);
            if (!badTopicsByDesiredRF.isEmpty()) {
                return Collections.singleton(createTopicReplicationFactorAnomaly(badTopicsByDesiredRF, _targetReplicationFactor));
            }
        }
        return Collections.emptySet();
    }

    /**
     * Retrieve topic minISR config information if it is not cached locally.
     *
     * @param topicsToCheck Set of topics to check.
     */
    private void maybeRetrieveAndCacheTopicMinISR(Set<String> topicsToCheck) {
        Set<ConfigResource> topicResourcesToCheck = new HashSet<>();
        topicsToCheck.stream().filter(t -> !_cachedTopicMinISR.containsKey(t))
                .forEach(t -> topicResourcesToCheck.add(new ConfigResource(ConfigResource.Type.TOPIC, t)));
        if (topicResourcesToCheck.isEmpty()) {
            return;
        }
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : _adminClient.describeConfigs(topicResourcesToCheck).values().entrySet()) {
            try {
                short topicMinISR = Short.parseShort(entry.getValue().get(DESCRIBE_TOPIC_CONFIG_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value());
                _cachedTopicMinISR.put(entry.getKey().name(), new TopicMinISREntry(topicMinISR, System.currentTimeMillis()));
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                LOG.warn("Skip attempt to fix replication factor of topic {} due to unable to retrieve its minISR config.",
                        entry.getKey().name());
            }
        }
    }

    /**
     * Scan through topics to check whether the topic having partition(s) with bad replication factor. For each topic, the
     * target replication factor to check against is the maximum value of {@link #SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG}
     * and topic's minISR plus value of {@link #TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG}.
     *
     * @param topicsToCheck Set of topics to check.
     * @param cluster       Kafka cluster.
     * @return Map of detected topic replication factor anomaly entries by target (i.e. desired) replication factor.
     */
    private Map<Short, Set<TopicReplicationFactorAnomalyEntry>> populateBadTopicsByDesiredRF(Set<String> topicsToCheck, Cluster cluster) {
        Map<Short, Set<TopicReplicationFactorAnomalyEntry>> badTopicsByDesiredRF = new HashMap<>();
        for (String topic : topicsToCheck) {
            if (_cachedTopicMinISR.containsKey(topic)) {
                short topicMinISR = _cachedTopicMinISR.get(topic).minISR();
                short targetReplicationFactor = (short) Math.max(_targetReplicationFactor, topicMinISR + _topicReplicationFactorMargin);
                int violatedPartitionCount = 0;
                for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
                    if (partitionInfo.replicas().length != targetReplicationFactor) {
                        violatedPartitionCount++;
                    }
                }
                if (violatedPartitionCount > 0) {
                    badTopicsByDesiredRF.putIfAbsent(targetReplicationFactor, new HashSet<>());
                    badTopicsByDesiredRF.get(targetReplicationFactor).add(
                            new TopicReplicationFactorAnomalyEntry(topic, (double) violatedPartitionCount / cluster.partitionCountForTopic(topic)));
                }
            }
        }
        return badTopicsByDesiredRF;
    }

    /**
     * Invalidate stale topic minISR record from local cache.
     */
    private void refreshTopicMinISRCache() {
        long currentTimeMs = System.currentTimeMillis();
        Iterator<Map.Entry<String, TopicMinISREntry>> cacheIterator = _cachedTopicMinISR.entrySet().iterator();
        while (cacheIterator.hasNext()) {
            Map.Entry<String, TopicMinISREntry> entry = cacheIterator.next();
            if (entry.getValue().createTimeMs() + _topicMinISRRecordRetentionTimeMs < currentTimeMs) {
                cacheIterator.remove();
            } else {
                break;
            }
        }
    }

    private TopicAnomaly createTopicReplicationFactorAnomaly(Map<Short, Set<TopicReplicationFactorAnomalyEntry>> badTopicsByDesiredRF,
                                                             short targetReplicationFactor) {
        Map<String, Object> configs = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl,
                BAD_TOPICS_BY_DESIRED_RF_CONFIG, badTopicsByDesiredRF,
                SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, targetReplicationFactor,
                ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
        return getConfiguredInstance(_topicReplicationTopicAnomalyClass, TopicAnomaly.class, configs);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        if (_kafkaCruiseControl == null) {
            throw new IllegalArgumentException("Topic replication factor anomaly finder is missing " + KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        }
        try {
            _targetReplicationFactor = Short.parseShort((String) configs.get(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG));
            if (_targetReplicationFactor <= 0) {
                throw new IllegalArgumentException(String.format("%s config of replication factor anomaly finder should be set to positive,"
                        + " provided %d.", SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, _targetReplicationFactor));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG
                    + " is missing or misconfigured for topic replication factor anomaly finder.");
        }
        String topicExcludedFromCheck = (String) configs.get(TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK);
        _topicExcludedFromCheck = Pattern.compile(topicExcludedFromCheck == null ? DEFAULT_TOPIC_EXCLUDED_FROM_REPLICATION_FACTOR_CHECK
                : topicExcludedFromCheck);
        String topicReplicationTopicAnomalyClass = (String) configs.get(TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG);
        if (topicReplicationTopicAnomalyClass == null) {
            _topicReplicationTopicAnomalyClass = DEFAULT_TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS;
        } else {
            _topicReplicationTopicAnomalyClass = (Class<?>) ConfigDef.parseType(TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG,
                    topicReplicationTopicAnomalyClass,
                    CLASS);
            if (_topicReplicationTopicAnomalyClass == null || !TopicAnomaly.class.isAssignableFrom(_topicReplicationTopicAnomalyClass)) {
                throw new IllegalArgumentException(String.format("Invalid %s is provided to replication factor anomaly finder, provided %s",
                        TOPIC_REPLICATION_FACTOR_ANOMALY_CLASS_CONFIG, _topicReplicationTopicAnomalyClass));
            }
        }
        try {
            String topicReplicationFactorMargin = (String) configs.get(TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG);
            _topicReplicationFactorMargin = topicReplicationFactorMargin == null ? DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN
                    : Short.parseShort(topicReplicationFactorMargin);
            if (_topicReplicationFactorMargin < 0) {
                throw new IllegalArgumentException(String.format("%s config of replication factor anomaly finder should not be set to negative,"
                        + " provided %d.", TOPIC_REPLICATION_FACTOR_MARGIN_CONFIG, _topicReplicationFactorMargin));
            }
        } catch (NumberFormatException e) {
            _topicReplicationFactorMargin = DEFAULT_TOPIC_REPLICATION_FACTOR_MARGIN;
        }
        try {
            String topicMinISRRecordRetentionTimeMs = (String) configs.get(TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG);
            _topicMinISRRecordRetentionTimeMs = topicMinISRRecordRetentionTimeMs == null ? DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS
                    : Long.parseLong(topicMinISRRecordRetentionTimeMs);
            if (_topicMinISRRecordRetentionTimeMs <= 0) {
                throw new IllegalArgumentException(String.format("%s config of replication factor anomaly finder should be set to positive,"
                        + " provided %d.", TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS_CONFIG, _topicMinISRRecordRetentionTimeMs));
            }
        } catch (NumberFormatException e) {
            _topicMinISRRecordRetentionTimeMs = DEFAULT_TOPIC_MIN_ISR_RECORD_RETENTION_TIME_MS;
        }

        _adminClient = _kafkaCruiseControl.adminClient();
        _cachedTopicMinISR = new LinkedHashMap<>();
    }

    /**
     * A class to encapsulate the retrieved topic minISR config information.
     */
    private static class TopicMinISREntry {
        private final short _minISR;
        private final long _createTimeMs;

        TopicMinISREntry(short minISR, long createTimeMs) {
            _minISR = minISR;
            _createTimeMs = createTimeMs;
        }

        short minISR() {
            return _minISR;
        }

        long createTimeMs() {
            return _createTimeMs;
        }
    }
}
