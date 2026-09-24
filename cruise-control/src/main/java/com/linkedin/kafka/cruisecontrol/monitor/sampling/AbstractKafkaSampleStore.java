/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.*;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.bootstrapServers;

public abstract class AbstractKafkaSampleStore implements SampleStore {
    protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofMinutes(3);
    protected static final short DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR = 2;
    protected static final int DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;

    protected volatile boolean _shutdown = false;
    protected Short _sampleStoreTopicReplicationFactor;
    protected Producer<byte[], byte[]> _producer;

    protected void createProducer(Map<String, ?> config, String producerClientId) {
        Properties producerProps = new Properties();
        producerProps.putAll(config);
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(config));
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId);
        // Set batch.size and linger.ms to a big number to have better batching.
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30000");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "500000");
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, config.get(MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG).toString());
        _producer = new KafkaProducer<>(producerProps);
    }

    /**
     * Retrieve the desired replication factor of sample store topics.
     *
     * @param config      The configurations for Cruise Control.
     * @param adminClient The adminClient to send describeCluster request.
     * @return Desired replication factor of sample store topics, or {@code null} if failed to resolve replication factor.
     */
    protected short sampleStoreTopicReplicationFactor(Map<String, ?> config, AdminClient adminClient) {
        if (_sampleStoreTopicReplicationFactor != null) {
            return _sampleStoreTopicReplicationFactor;
        }

        int maxRetryCount = Integer.parseInt(config.get(MonitorConfig.FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_CONFIG).toString());
        AtomicInteger numberOfBrokersInCluster = new AtomicInteger(0);
        AtomicReference<String> errorMsg = new AtomicReference<>("");

        boolean success = CruiseControlMetricsUtils.retry(() -> {
            try {
                numberOfBrokersInCluster.set(adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS).size());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                errorMsg.set("Auto creation of sample store topics failed due to failure to describe cluster. " + e);
                return true;
            }

            if (numberOfBrokersInCluster.get() <= 1) {
                errorMsg.set(String.format("Kafka cluster has less than 2 brokers (brokers in cluster=%d)",
                        numberOfBrokersInCluster.get()));
                return true;
            }

            numberOfBrokersInCluster.set(Math.min(DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR, numberOfBrokersInCluster.get()));
            return false;
        }, maxRetryCount);

        if (success) {
            return (short) numberOfBrokersInCluster.get();
        } else {
            throw new IllegalStateException(errorMsg.get());
        }
    }

    protected void ensureTopicCreated(AdminClient adminClient, NewTopic sampleStoreTopic) {
        if (!createTopic(adminClient, sampleStoreTopic)) {
            // Update topic config and partition count to ensure desired properties.
            maybeUpdateTopicConfig(adminClient, sampleStoreTopic);
            maybeIncreasePartitionCount(adminClient, sampleStoreTopic);
        }
    }

    static AtomicInteger storePartitionMetricSamples(MetricSampler.Samples samples, Producer<byte[], byte[]> producer,
                                                     String partitionMetricSampleStoreTopic, Logger log) {
        final AtomicInteger metricSampleCount = new AtomicInteger(0);
        for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
            producer.send(new ProducerRecord<>(partitionMetricSampleStoreTopic, null, sample.sampleTime(), null, sample.toBytes()),
                    (recordMetadata, e) -> {
                        if (e == null) {
                            metricSampleCount.incrementAndGet();
                        } else {
                            log.error("Failed to produce partition metric sample for {} of timestamp {} due to exception",
                                    sample.entity().tp(), sample.sampleTime(), e);
                        }
                    });
        }
        return metricSampleCount;
    }

    @Override
    public void evictSamplesBefore(long timestamp) {
        //TODO: use the deleteMessageBefore method to delete old samples.
    }

    @Override
    public void close() {
        _shutdown = true;
        _producer.close(PRODUCER_CLOSE_TIMEOUT);
    }
}
