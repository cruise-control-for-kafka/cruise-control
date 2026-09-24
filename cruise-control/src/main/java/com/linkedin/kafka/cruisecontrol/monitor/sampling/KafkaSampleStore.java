/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.wrapTopic;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.LOADING_PROGRESS;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.createSampleStoreConsumer;


/**
 * The sample store that implements the {@link SampleStore}. It stores the partition metric samples and broker metric
 * samples back to Kafka and load from Kafka at startup.
 * <p>
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG}: The config for the topic name of Kafka topic to store partition samples.</li>
 *   <li>{@link #BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG}: The config for the topic name of Kafka topic to store broker samples.</li>
 *   <li>{@link #NUM_SAMPLE_LOADING_THREADS_CONFIG}: The config for the number of Kafka sample store consumer threads, default value is
 *   set to {@link #DEFAULT_NUM_SAMPLE_LOADING_THREADS}.</li>
 *   <li>{@link #SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the replication factor of Kafka sample store topics,
 *   default value is set to {@link #DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR}.</li>
 *   <li>{@link #PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka partition sample store
 *    topic, default value is set to {@link #DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka broker sample store topic,
 *   default value is set to {@link #DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for Kafka partition sample
 *   store topic, default value is set to {@link #DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 *   <li>{@link #MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for Kafka broker sample store
 *   topic, default value is set to {@link #DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 * </ul>
 */
public class KafkaSampleStore extends AbstractKafkaSampleStore {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSampleStore.class);
    protected static final Duration CONSUMER_CLOSE_TIMEOUT = Duration.ofSeconds(10);
    // Keep additional windows in case some of the windows do not have enough samples.
    protected static final int ADDITIONAL_WINDOW_TO_RETAIN_FACTOR = 2;
    protected static final ConsumerRecords<byte[], byte[]> SHUTDOWN_RECORDS = ConsumerRecords.empty();
    protected static final Duration SAMPLE_POLL_TIMEOUT = Duration.ofMillis(1000L);

    protected static final int DEFAULT_NUM_SAMPLE_LOADING_THREADS = 8;
    protected static final int DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;
    protected static final long DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(1);
    protected static final long DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(1);
    protected static final String PRODUCER_CLIENT_ID = "KafkaCruiseControlSampleStoreProducer";
    protected static final String CONSUMER_CLIENT_ID_PREFIX = "KafkaCruiseControlSampleStore";

    protected List<Consumer<byte[], byte[]>> _consumers;
    protected ExecutorService _metricProcessorExecutor;
    protected String _partitionMetricSampleStoreTopic;
    protected String _brokerMetricSampleStoreTopic;
    protected int _partitionSampleStoreTopicPartitionCount;
    protected int _brokerSampleStoreTopicPartitionCount;
    protected long _minPartitionSampleStoreTopicRetentionTimeMs;
    protected long _minBrokerSampleStoreTopicRetentionTimeMs;
    protected volatile double _loadingProgress;

    public static final String PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "partition.metric.sample.store.topic";
    public static final String BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "broker.metric.sample.store.topic";
    public static final String NUM_SAMPLE_LOADING_THREADS_CONFIG = "num.sample.loading.threads";
    public static final String SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG = "sample.store.topic.replication.factor";
    public static final String PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG = "partition.sample.store.topic.partition.count";
    public static final String BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG = "broker.sample.store.topic.partition.count";
    public static final String MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG = "min.partition.sample.store.topic.retention.time.ms";
    public static final String MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG = "min.broker.sample.store.topic.retention.time.ms";

    @Override
    public void configure(Map<String, ?> config) {
        _partitionMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
        _brokerMetricSampleStoreTopic = KafkaCruiseControlUtils.getRequiredConfig(config, BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
        String metricSampleStoreTopicReplicationFactorString = (String) config.get(SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG);
        _sampleStoreTopicReplicationFactor = metricSampleStoreTopicReplicationFactorString == null
                || metricSampleStoreTopicReplicationFactorString.isEmpty()
                ? null : Short.parseShort(metricSampleStoreTopicReplicationFactorString);
        String partitionSampleStoreTopicPartitionCountString = (String) config.get(PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG);
        _partitionSampleStoreTopicPartitionCount = partitionSampleStoreTopicPartitionCountString == null
                || partitionSampleStoreTopicPartitionCountString.isEmpty()
                ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT
                : Integer.parseInt(partitionSampleStoreTopicPartitionCountString);
        String brokerSampleStoreTopicPartitionCountString = (String) config.get(BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG);
        _brokerSampleStoreTopicPartitionCount = brokerSampleStoreTopicPartitionCountString == null
                || brokerSampleStoreTopicPartitionCountString.isEmpty()
                ? DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT
                : Integer.parseInt(brokerSampleStoreTopicPartitionCountString);
        String minPartitionSampleStoreTopicRetentionTimeMsString = (String) config.get(MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG);
        _minPartitionSampleStoreTopicRetentionTimeMs = minPartitionSampleStoreTopicRetentionTimeMsString == null
                || minPartitionSampleStoreTopicRetentionTimeMsString.isEmpty()
                ? DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS
                : Long.parseLong(minPartitionSampleStoreTopicRetentionTimeMsString);
        String minBrokerSampleStoreTopicRetentionTimeMsString = (String) config.get(MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG);
        _minBrokerSampleStoreTopicRetentionTimeMs = minBrokerSampleStoreTopicRetentionTimeMsString == null
                || minBrokerSampleStoreTopicRetentionTimeMsString.isEmpty()
                ? DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS
                : Long.parseLong(minBrokerSampleStoreTopicRetentionTimeMsString);
        String numProcessingThreadsString = (String) config.get(NUM_SAMPLE_LOADING_THREADS_CONFIG);
        int numProcessingThreads = numProcessingThreadsString == null || numProcessingThreadsString.isEmpty()
                ? DEFAULT_NUM_SAMPLE_LOADING_THREADS : Integer.parseInt(numProcessingThreadsString);

        _metricProcessorExecutor = Executors.newFixedThreadPool(numProcessingThreads);
        _consumers = new ArrayList<>(numProcessingThreads);
        for (int i = 0; i < numProcessingThreads; i++) {
            _consumers.add(createSampleStoreConsumer(config, CONSUMER_CLIENT_ID_PREFIX));
        }

        createProducer(config, PRODUCER_CLIENT_ID);
        _loadingProgress = LOADING_PROGRESS;

        ensureTopicsCreated(config);
    }

    @SuppressWarnings("unchecked")
    protected void ensureTopicsCreated(Map<String, ?> config) {
        AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient((Map<String, Object>) config);
        try {
            short replicationFactor = sampleStoreTopicReplicationFactor(config, adminClient);

            // Retention
            long partitionSampleWindowMs = (Long) config.get(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
            long brokerSampleWindowMs = (Long) config.get(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG);

            int numPartitionSampleWindows = (Integer) config.get(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
            long partitionSampleRetentionMs = ((long) numPartitionSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * partitionSampleWindowMs;
            partitionSampleRetentionMs = Math.max(_minPartitionSampleStoreTopicRetentionTimeMs, partitionSampleRetentionMs);

            int numBrokerSampleWindows = (Integer) config.get(MonitorConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG);
            long brokerSampleRetentionMs = ((long) numBrokerSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * brokerSampleWindowMs;
            brokerSampleRetentionMs = Math.max(_minBrokerSampleStoreTopicRetentionTimeMs, brokerSampleRetentionMs);

            // New topics
            NewTopic partitionSampleStoreNewTopic = wrapTopic(_partitionMetricSampleStoreTopic, _partitionSampleStoreTopicPartitionCount,
                    replicationFactor, partitionSampleRetentionMs);
            NewTopic brokerSampleStoreNewTopic = wrapTopic(_brokerMetricSampleStoreTopic, _brokerSampleStoreTopicPartitionCount,
                    replicationFactor, brokerSampleRetentionMs);

            ensureTopicCreated(adminClient, partitionSampleStoreNewTopic);
            ensureTopicCreated(adminClient, brokerSampleStoreNewTopic);
        } finally {
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    @Override
    public void storeSamples(MetricSampler.Samples samples) {
        AtomicInteger metricSampleCount = storePartitionMetricSamples(samples, _producer, _partitionMetricSampleStoreTopic, LOG);

        final AtomicInteger brokerMetricSampleCount = new AtomicInteger(0);
        for (BrokerMetricSample sample : samples.brokerMetricSamples()) {
            _producer.send(new ProducerRecord<>(_brokerMetricSampleStoreTopic, sample.toBytes()),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                brokerMetricSampleCount.incrementAndGet();
                            } else {
                                LOG.error("Failed to produce model training sample due to exception", e);
                            }
                        }
                    });
        }
        _producer.flush();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stored {} partition metric samples and {} broker metric samples to Kafka",
                    metricSampleCount.get(), brokerMetricSampleCount.get());
        }
    }

    @Override
    public void loadSamples(SampleLoader sampleLoader) {
        LOG.info("Starting loading samples.");
        long startMs = System.currentTimeMillis();
        AtomicLong numPartitionMetricSamples = new AtomicLong(0L);
        AtomicLong numBrokerMetricSamples = new AtomicLong(0L);
        AtomicLong totalSamples = new AtomicLong(0L);
        AtomicLong numLoadedSamples = new AtomicLong(0L);
        try {
            prepareConsumers();

            for (Consumer<byte[], byte[]> consumer : _consumers) {
                _metricProcessorExecutor.execute(
                        new MetricLoader(consumer, sampleLoader, numLoadedSamples, numPartitionMetricSamples, numBrokerMetricSamples,
                                totalSamples));
            }
            // Blocking waiting for the metric loading to finish.
            _metricProcessorExecutor.shutdown();
            if (!_metricProcessorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                LOG.warn("Timed out waiting for metric loading to finish.");
            }
        } catch (Exception e) {
            LOG.error("Received exception when loading samples", e);
        } finally {
            _consumers.forEach(Consumer::close);
            try {
                if (!_metricProcessorExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS)) {
                    LOG.warn("Timed out waiting for metrics processor to shutdown.");
                    _metricProcessorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted during waiting for metrics processor to shutdown.", e);
            }
        }
        long endMs = System.currentTimeMillis();
        long addedPartitionSampleCount = sampleLoader.partitionSampleCount();
        long addedBrokerSampleCount = sampleLoader.brokerSampleCount();
        long discardedPartitionMetricSamples = numPartitionMetricSamples.get() - addedPartitionSampleCount;
        long discardedBrokerMetricSamples = numBrokerMetricSamples.get() - addedBrokerSampleCount;
        LOG.info("Sample loading finished. Loaded {}{} partition metrics samples and {}{} broker metric samples in {} ms.",
                addedPartitionSampleCount,
                discardedPartitionMetricSamples > 0 ? String.format("(%d discarded)", discardedPartitionMetricSamples) : "",
                sampleLoader.brokerSampleCount(),
                discardedBrokerMetricSamples > 0 ? String.format("(%d discarded)", discardedBrokerMetricSamples) : "",
                endMs - startMs);
    }

    @Override
    public double sampleLoadingProgress() {
        return _loadingProgress;
    }

    @Override
    public void close() {
        super.close();
        // Close consumers.
        for (Consumer<byte[], byte[]> consumer : _consumers) {
            consumer.close(CloseOptions.timeout(CONSUMER_CLOSE_TIMEOUT));
        }
    }

    protected void prepareConsumers() {
        int numConsumers = _consumers.size();
        List<List<TopicPartition>> assignments = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            assignments.add(new ArrayList<>());
        }
        int j = 0;
        for (String topic : Arrays.asList(_partitionMetricSampleStoreTopic, _brokerMetricSampleStoreTopic)) {
            for (PartitionInfo partInfo : _consumers.get(0).partitionsFor(topic)) {
                assignments.get(j++ % numConsumers).add(new TopicPartition(partInfo.topic(), partInfo.partition()));
            }
        }
        for (int i = 0; i < numConsumers; i++) {
            _consumers.get(i).assign(assignments.get(i));
        }
    }

    protected class MetricLoader implements Runnable {
        protected final SampleLoader _sampleLoader;
        protected final AtomicLong _numLoadedSamples;
        protected final AtomicLong _numPartitionMetricSamples;
        protected final AtomicLong _numBrokerMetricSamples;
        protected final AtomicLong _totalSamples;
        protected final Consumer<byte[], byte[]> _consumer;

        MetricLoader(Consumer<byte[], byte[]> consumer,
                     SampleLoader sampleLoader,
                     AtomicLong numLoadedSamples,
                     AtomicLong numPartitionMetricSamples,
                     AtomicLong numBrokerMetricSamples,
                     AtomicLong totalSamples) {
            _consumer = consumer;
            _sampleLoader = sampleLoader;
            _numLoadedSamples = numLoadedSamples;
            _numPartitionMetricSamples = numPartitionMetricSamples;
            _numBrokerMetricSamples = numBrokerMetricSamples;
            _totalSamples = totalSamples;
        }

        @Override
        public void run() {
            try {
                prepareConsumerOffset();
                Map<TopicPartition, Long> beginningOffsets = _consumer.beginningOffsets(_consumer.assignment());
                Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(_consumer.assignment());
                LOG.debug("Loading beginning offsets: {}, loading end offsets: {}", beginningOffsets, endOffsets);
                for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
                    _totalSamples.addAndGet(endOffsets.get(entry.getKey()) - entry.getValue());
                    _loadingProgress = (double) _numLoadedSamples.get() / _totalSamples.get();
                }
                while (!sampleLoadingFinished(endOffsets)) {
                    try {
                        ConsumerRecords<byte[], byte[]> consumerRecords = _consumer.poll(SAMPLE_POLL_TIMEOUT);
                        if (consumerRecords == SHUTDOWN_RECORDS) {
                            LOG.trace("Metric loader received empty records");
                            return;
                        }
                        Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
                        Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();
                        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                            try {
                                if (record.topic().equals(_partitionMetricSampleStoreTopic)) {
                                    PartitionMetricSample sample = PartitionMetricSample.fromBytes(record.value());
                                    partitionMetricSamples.add(sample);
                                    LOG.trace("Loaded partition metric sample {}", sample);
                                } else if (record.topic().equals(_brokerMetricSampleStoreTopic)) {
                                    BrokerMetricSample sample = BrokerMetricSample.fromBytes(record.value());
                                    // For some legacy BrokerMetricSample, there is no timestamp in the broker samples. In this case
                                    // we use the record timestamp as the broker metric timestamp.
                                    sample.close(record.timestamp());
                                    brokerMetricSamples.add(sample);
                                    LOG.trace("Loaded broker metric sample {}", sample);
                                }
                            } catch (UnknownVersionException e) {
                                LOG.warn("Ignoring sample due to", e);
                            }
                        }
                        if (!partitionMetricSamples.isEmpty() || !brokerMetricSamples.isEmpty()) {
                            _sampleLoader.loadSamples(new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples));
                            _numPartitionMetricSamples.getAndAdd(partitionMetricSamples.size());
                            _numBrokerMetricSamples.getAndAdd(brokerMetricSamples.size());
                            _loadingProgress = (double) _numLoadedSamples.addAndGet(consumerRecords.count()) / _totalSamples.get();
                        }
                    } catch (KafkaException ke) {
                        if (ke.getMessage().toLowerCase().contains("record is corrupt")) {
                            for (TopicPartition tp : _consumer.assignment()) {
                                long position = _consumer.position(tp);
                                if (position < endOffsets.get(tp)) {
                                    _consumer.seek(tp, position + 1);
                                }
                            }
                        } else {
                            LOG.error("Metric loader received exception:", ke);
                        }
                    } catch (Exception e) {
                        if (_shutdown) {
                            return;
                        } else {
                            LOG.error("Metric loader received exception:", e);
                        }
                    }
                }
                LOG.info("Metric loader finished loading samples.");
            } catch (Throwable t) {
                LOG.warn("Encountered error when loading sample from Kafka.", t);
            }
        }

        protected boolean sampleLoadingFinished(Map<TopicPartition, Long> endOffsets) {
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                long position = _consumer.position(entry.getKey());
                if (position < entry.getValue()) {
                    LOG.debug("Partition {} is still lagging. Current position: {}, LEO: {}", entry.getKey(),
                            position, entry.getValue());
                    return false;
                }
            }
            return true;
        }

        /**
         * Config the sample loading consumers to consume from proper starting offsets. The sample store Kafka topic may contain data
         * which are too old for {@link com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator} to keep in memory,
         * to prevent loading these stale data, manually seek the consumers' staring offset to the offset at proper timestamp.
         */
        protected void prepareConsumerOffset() {
            Map<TopicPartition, Long> beginningTimestamp = new HashMap<>();
            long currentTimeMs = System.currentTimeMillis();
            for (TopicPartition tp : _consumer.assignment()) {
                if (tp.topic().equals(_brokerMetricSampleStoreTopic)) {
                    beginningTimestamp.put(tp, currentTimeMs - _sampleLoader.brokerMonitoringPeriodMs());
                } else {
                    beginningTimestamp.put(tp, currentTimeMs - _sampleLoader.partitionMonitoringPeriodMs());
                }
            }

            Set<TopicPartition> partitionWithNoRecentMessage = new HashSet<>();
            Map<TopicPartition, OffsetAndTimestamp> beginningOffsetAndTimestamp = _consumer.offsetsForTimes(beginningTimestamp);
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : beginningOffsetAndTimestamp.entrySet()) {
                if (entry.getValue() == null) {
                    // If this sample store topic partition does not have data available after beginning timestamp, then seek to the
                    // beginning of this topic partition.
                    partitionWithNoRecentMessage.add(entry.getKey());
                } else {
                    _consumer.seek(entry.getKey(), entry.getValue().offset());
                }
            }
            if (!partitionWithNoRecentMessage.isEmpty()) {
                _consumer.seekToBeginning(partitionWithNoRecentMessage);
            }
        }
    }
}
