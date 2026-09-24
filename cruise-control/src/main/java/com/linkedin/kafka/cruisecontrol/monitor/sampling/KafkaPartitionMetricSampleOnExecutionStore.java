/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.wrapTopic;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.LOADING_PROGRESS;


/**
 * The sample store that implements the {@link SampleStore}. It stores the partition metric samples back to Kafka during ongoing execution.
 * <p>
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_CONFIG}: The config for Kafka topic name to store partition samples.</li>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the replication factor of Kafka
 *   sample store topics, default value is set to {@link #DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR}.</li>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka
 *   partition sample store topic, default value is set to {@link #DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for
 *   Kafka partition sample store topic, default value is set to {@link #DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 * </ul>
 */
public class KafkaPartitionMetricSampleOnExecutionStore extends AbstractKafkaSampleStore {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionMetricSampleOnExecutionStore.class);

    protected static final String PRODUCER_CLIENT_ID = "KafkaCruiseControlPartitionMetricSampleOnExecutionStoreProducer";
    protected static final long DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(1);

    protected String _partitionMetricSampleStoreTopic;

    public static final String PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_CONFIG = "partition.metric.sample.store.on.execution.topic";
    public static final String PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_REPLICATION_FACTOR_CONFIG =
            "partition.metric.sample.store.on.execution.topic.replication.factor";
    public static final String PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_PARTITION_COUNT_CONFIG =
            "partition.metric.sample.store.on.execution.topic.partition.count";
    public static final String PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_RETENTION_TIME_MS_CONFIG =
            "partition.metric.sample.store.on.execution.topic.retention.time.ms";

    @Override
    public void configure(Map<String, ?> config) {
        _partitionMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_CONFIG);
        String metricSampleStoreTopicReplicationFactorString = (String) config.get(
                PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_REPLICATION_FACTOR_CONFIG);
        _sampleStoreTopicReplicationFactor = metricSampleStoreTopicReplicationFactorString == null
                || metricSampleStoreTopicReplicationFactorString.isEmpty()
                ? null : Short.parseShort(metricSampleStoreTopicReplicationFactorString);
        String partitionSampleStoreTopicPartitionCountString = (String) config.get(
                PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_PARTITION_COUNT_CONFIG);
        int partitionSampleStoreTopicPartitionCount = partitionSampleStoreTopicPartitionCountString == null
                || partitionSampleStoreTopicPartitionCountString.isEmpty()
                ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT
                : Integer.parseInt(partitionSampleStoreTopicPartitionCountString);
        String sampleStoreTopicRetentionTimeMsString = (String) config.get(PARTITION_METRIC_SAMPLE_STORE_ON_EXECUTION_TOPIC_RETENTION_TIME_MS_CONFIG);
        long partitionSampleStoreTopicRetentionTimeMs = sampleStoreTopicRetentionTimeMsString == null
                || sampleStoreTopicRetentionTimeMsString.isEmpty()
                ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS
                : Long.parseLong(sampleStoreTopicRetentionTimeMsString);

        createProducer(config, PRODUCER_CLIENT_ID);
        ensureTopicCreated(config, partitionSampleStoreTopicPartitionCount, partitionSampleStoreTopicRetentionTimeMs);
    }

    @SuppressWarnings("unchecked")
    protected void ensureTopicCreated(Map<String, ?> config, int topicPartitionCount, long topicRetentionTimeMs) {
        AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient((Map<String, Object>) config);
        try {
            short replicationFactor = sampleStoreTopicReplicationFactor(config, adminClient);

            // New topics
            NewTopic partitionSampleStoreNewTopic = wrapTopic(_partitionMetricSampleStoreTopic, topicPartitionCount,
                    replicationFactor, topicRetentionTimeMs);
            ensureTopicCreated(adminClient, partitionSampleStoreNewTopic);
        } finally {
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    @Override
    public void storeSamples(MetricSampler.Samples samples) {
        AtomicInteger metricSampleCount = storePartitionMetricSamples(samples, _producer, _partitionMetricSampleStoreTopic, LOG);
        _producer.flush();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stored {} partition metric samples to Kafka", metricSampleCount.get());
        }
    }

    @Override
    public void loadSamples(SampleLoader sampleLoader) {
    }

    @Override
    public double sampleLoadingProgress() {
        return LOADING_PROGRESS;
    }
}
