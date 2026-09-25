/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.consumptionDone;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckOffsetFetch;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.createMetricConsumer;


public class CruiseControlMetricsReporterSampler extends AbstractMetricSampler {
    private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporterSampler.class);
    // Configurations
    public static final String METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS = "metric.reporter.sampler.bootstrap.servers";
    public static final String METRIC_REPORTER_TOPIC = "metric.reporter.topic";
    public static final String METRIC_REPORTER_TOPIC_ASSERT_ATTEMPTS = "metric.reporter.sampler.topic.assert.attempts";

    public static final int METRIC_REPORTER_TOPIC_ASSERT_ATTEMPTS_DEFAULT = 5;

    @Deprecated
    public static final String METRIC_REPORTER_SAMPLER_GROUP_ID = "metric.reporter.sampler.group.id";
    public static final Duration METRIC_REPORTER_CONSUMER_POLL_TIMEOUT = Duration.ofMillis(5000L);
    // Default configs
    public static final String CONSUMER_CLIENT_ID_PREFIX = "CruiseControlMetricsReporterSampler";
    public static final long ACCEPTABLE_NETWORK_DELAY_MS = 100L;

    protected Consumer<String, CruiseControlMetric> _metricConsumer;
    protected String _metricReporterTopic;
    protected Set<TopicPartition> _currentPartitionAssignment;
    // Due to delay introduced by KafkaProducer and network, the metric record's event time is smaller than append
    // time at broker side, sampler should take this delay into consideration when collecting metric records into samples.
    // _acceptableMetricRecordProduceDelayMs is a conservative estimate of this delay, if one record's event time not earlier
    // than starting_time_of_sampling_period minus _acceptableMetricRecordProduceDelayMs, it is included in the sample;
    // otherwise it is discarded.
    protected long _acceptableMetricRecordProduceDelayMs;

    @Override

    protected int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
        if (refreshPartitionAssignment()) {
            return 0;
        }
        // Now seek to the startTimeMs.
        Map<TopicPartition, Long> timestampToSeek = new HashMap<>();
        for (TopicPartition tp : _currentPartitionAssignment) {
            timestampToSeek.put(tp, metricSamplerOptions.startTimeMs());
        }
        Set<TopicPartition> assignment = new HashSet<>(_currentPartitionAssignment);
        Map<TopicPartition, Long> endOffsets = _metricConsumer.endOffsets(assignment);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = _metricConsumer.offsetsForTimes(timestampToSeek);
        sanityCheckOffsetFetch(endOffsets, offsetsForTimes);
        // If some partitions do not have data, we simply seek to the end offset. To avoid losing metrics, we use the end
        // offsets before the timestamp query.
        assignment.removeAll(offsetsForTimes.keySet());
        assignment.forEach(tp -> _metricConsumer.seek(tp, endOffsets.get(tp)));
        // For the partition that returned an offset, seek to the returned offsets.
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            _metricConsumer.seek(tp, offsetAndTimestamp != null ? offsetAndTimestamp.offset() : endOffsets.get(tp));
        }
        SortedSet<Integer> partitionIds
                = _currentPartitionAssignment.stream().map(TopicPartition::partition).collect(Collectors.toCollection(TreeSet::new));
        LOG.debug("Starting consuming from metrics reporter topic {} for partitions {}.", _metricReporterTopic, partitionIds);
        _metricConsumer.resume(_metricConsumer.paused());
        int totalMetricsAdded = 0;
        Set<TopicPartition> partitionsToPause = new HashSet<>();
        do {
            ConsumerRecords<String, CruiseControlMetric> records = _metricConsumer.poll(METRIC_REPORTER_CONSUMER_POLL_TIMEOUT);
            for (ConsumerRecord<String, CruiseControlMetric> record : records) {
                if (record == null) {
                    // This means we cannot parse the metrics. It might happen when a newer type of metrics has been added and
                    // the current code is still old. We simply ignore that metric in this case.
                    LOG.warn("Cannot parse record, please update your Cruise Control version.");
                    continue;
                }
                long recordTime = record.value().time();
                if (recordTime + _acceptableMetricRecordProduceDelayMs < metricSamplerOptions.startTimeMs()) {
                    LOG.debug("Discarding metric {} because its timestamp is more than {} ms earlier than the start time of sampling period {}.",
                            record.value(), _acceptableMetricRecordProduceDelayMs, metricSamplerOptions.startTimeMs());
                } else if (recordTime >= metricSamplerOptions.endTimeMs()) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    LOG.debug("Saw metric {} whose timestamp is larger than the end time of sampling period {}. Pausing "
                                    + "partition {} at offset {}.", record.value(), metricSamplerOptions.endTimeMs(),
                            tp, record.offset());
                    partitionsToPause.add(tp);
                } else {
                    addMetricForProcessing(record.value());
                    totalMetricsAdded++;
                }
            }
            if (!partitionsToPause.isEmpty()) {
                _metricConsumer.pause(partitionsToPause);
                partitionsToPause.clear();
            }
        } while (!consumptionDone(_metricConsumer, endOffsets) && System.currentTimeMillis() < metricSamplerOptions.timeoutMs());
        LOG.info("Finished sampling from topic {} for partitions {} in time range [{},{}]. Collected {} metrics.",
                _metricReporterTopic, partitionIds, metricSamplerOptions.startTimeMs(), metricSamplerOptions.endTimeMs(), totalMetricsAdded);

        return totalMetricsAdded;
    }

    /**
     * Ensure that the {@link #_metricConsumer} is assigned to the latest partitions of the {@link #_metricReporterTopic}.
     * This enables metrics reporter sampler to handle dynamic partition size increases in {@link #_metricReporterTopic}.
     *
     * @return {@code true} if the set of partitions currently assigned to this consumer is empty, {@code false} otherwise.
     */
    protected boolean refreshPartitionAssignment() {
        List<PartitionInfo> remotePartitionInfo = _metricConsumer.partitionsFor(_metricReporterTopic);
        if (remotePartitionInfo == null) {
            LOG.error("_metricConsumer returned null for _metricReporterTopic {}", _metricReporterTopic);
            return true;
        }
        if (remotePartitionInfo.isEmpty()) {
            _currentPartitionAssignment = Collections.emptySet();
            LOG.error("The set of partitions currently assigned to the metric consumer is empty.");
            return true;
        }

        // Ensure that reassignment overhead is avoided if partition set of the topic has not changed.
        if (remotePartitionInfo.size() == _currentPartitionAssignment.size()) {
            return false;
        }

        _currentPartitionAssignment = new HashSet<>();
        for (PartitionInfo partitionInfo : remotePartitionInfo) {
            _currentPartitionAssignment.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }

        _metricConsumer.assign(_currentPartitionAssignment);
        return false;
    }

    private boolean isMetricsTopicExists() {
        Map<String, List<PartitionInfo>> topics = _metricConsumer.listTopics();
        if (!topics.containsKey(_metricReporterTopic)) {
            return false;
        }
        return true;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _metricReporterTopic = (String) configs.get(METRIC_REPORTER_TOPIC);
        if (_metricReporterTopic == null) {
            _metricReporterTopic = CruiseControlMetricsReporterConfig.DEFAULT_CRUISE_CONTROL_METRICS_TOPIC;
        }
        String metricTopicAssertAttemptsStr = (String) configs.get(METRIC_REPORTER_TOPIC_ASSERT_ATTEMPTS);
        int metricTopicAssertAttempts = METRIC_REPORTER_TOPIC_ASSERT_ATTEMPTS_DEFAULT;
        if (metricTopicAssertAttemptsStr != null) {
            metricTopicAssertAttempts = Integer.parseInt(metricTopicAssertAttemptsStr);
        }
        CruiseControlMetricsReporterConfig reporterConfig = new CruiseControlMetricsReporterConfig(configs, false);
        _acceptableMetricRecordProduceDelayMs = ACCEPTABLE_NETWORK_DELAY_MS
                + Math.max(reporterConfig.getLong(CruiseControlMetricsReporterConfig
                        .CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_CONFIG),
                reporterConfig.getLong(CruiseControlMetricsReporterConfig
                        .CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG));
        _metricConsumer = createMetricConsumer(configs, CONSUMER_CLIENT_ID_PREFIX);
        _currentPartitionAssignment = Collections.emptySet();

        LOG.info("Waiting for metrics reporter topic [{}] to be available in the Kafka cluster.", _metricReporterTopic);
        if (!CruiseControlMetricsUtils.retry(() -> !this.isMetricsTopicExists(), 2000, 2, metricTopicAssertAttempts, 30_000)) {
            throw new IllegalStateException("Cruise Control cannot find the metrics reporter topic that matches [" + _metricReporterTopic
                    + "] in the Kafka cluster.");
        }

        if (refreshPartitionAssignment()) {
            throw new IllegalStateException("Cruise Control cannot find partitions for the metrics reporter that topic matches ["
                    + _metricReporterTopic + "] in the Kafka cluster.");
        }
    }

    @Override
    public void close() {
        _metricConsumer.close();
    }
}
