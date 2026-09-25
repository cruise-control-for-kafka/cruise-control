/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * A class that is responsible for fetching the metrics from the cluster.
 * The metrics may be used for either load model training purpose or just for cluster workload monitoring.
 */
abstract class MetricFetcher implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);
    protected final MetricSampler _metricSampler;
    protected final Cluster _cluster;
    protected final SampleStore _sampleStore;
    protected final SampleStore _sampleStoreForPartitionMetricOnExecution;
    protected final Set<TopicPartition> _assignedPartitions;
    protected final long _startTimeMs;
    protected final long _endTimeMs;
    protected final MetricDef _metricDef;
    protected final Timer _fetchTimer;
    protected final Meter _fetchFailureRate;
    protected final long _timeout;
    protected final MetricSampler.SamplingMode _samplingMode;

    /**
     * @param metricSampler      The sampler used to retrieve metrics.
     * @param cluster            The Kafka cluster.
     * @param sampleStore        Sample store to persist the fetched samples, or skip storing samples if {@code null}.
     * @param assignedPartitions Partitions to fetch samples from.
     * @param startTimeMs        The start time of the sampling period.
     * @param endTimeMs          The end time of the sampling period.
     * @param metricDef          The metric definitions.
     * @param fetchTimer         The timer to keep track of metric fetch time.
     * @param fetchFailureRate   The meter to keep track of failure rate while fetching metrics.
     * @param samplingMode       The mode of sampling to indicate the sample type of interest.
     */
    MetricFetcher(MetricSampler metricSampler,
                  Cluster cluster,
                  SampleStore sampleStore,
                  Set<TopicPartition> assignedPartitions,
                  long startTimeMs,
                  long endTimeMs,
                  MetricDef metricDef,
                  Timer fetchTimer,
                  Meter fetchFailureRate,
                  MetricSampler.SamplingMode samplingMode) {
        this(metricSampler, cluster, sampleStore, null, assignedPartitions, startTimeMs, endTimeMs,
                metricDef, fetchTimer, fetchFailureRate, samplingMode);
    }

    /**
     * @param metricSampler                            The sampler used to retrieve metrics.
     * @param cluster                                  The Kafka cluster.
     * @param sampleStore                              Sample store to persist the fetched samples, or skip storing samples if {@code null}.
     * @param sampleStoreForPartitionMetricOnExecution Sample store to persist the fetched partition samples during execution, or skip
     *                                                 storing samples if {@code null}.
     * @param assignedPartitions                       Partitions to fetch samples from.
     * @param startTimeMs                              The start time of the sampling period.
     * @param endTimeMs                                The end time of the sampling period.
     * @param metricDef                                The metric definitions.
     * @param fetchTimer                               The timer to keep track of metric fetch time.
     * @param fetchFailureRate                         The meter to keep track of failure rate while fetching metrics.
     * @param samplingMode                             The mode of sampling to indicate the sample type of interest.
     */
    MetricFetcher(MetricSampler metricSampler,
                  Cluster cluster,
                  SampleStore sampleStore,
                  SampleStore sampleStoreForPartitionMetricOnExecution,
                  Set<TopicPartition> assignedPartitions,
                  long startTimeMs,
                  long endTimeMs,
                  MetricDef metricDef,
                  Timer fetchTimer,
                  Meter fetchFailureRate,
                  MetricSampler.SamplingMode samplingMode) {
        _metricSampler = metricSampler;
        _cluster = cluster;
        _sampleStore = sampleStore;
        _sampleStoreForPartitionMetricOnExecution = sampleStoreForPartitionMetricOnExecution;
        _assignedPartitions = assignedPartitions;
        _startTimeMs = startTimeMs;
        _endTimeMs = endTimeMs;
        _metricDef = metricDef;
        _fetchTimer = fetchTimer;
        _fetchFailureRate = fetchFailureRate;
        _samplingMode = samplingMode;
        _timeout = System.currentTimeMillis() + (endTimeMs - startTimeMs) / 2;
    }

    @Override
    public Boolean call() {
        boolean hasSamplingError = false;

        try {
            fetchMetricsForAssignedPartitions();
        } catch (SamplingException mse) {
            LOG.warn("Received sampling error.", mse);
            hasSamplingError = true;
        } catch (Throwable t) {
            LOG.error("Received exception.", t);
            hasSamplingError = true;
        }

        return hasSamplingError;
    }

    /**
     * Execute one iteration of metric sampling for all the assigned partitions.
     */
    protected void fetchMetricsForAssignedPartitions() throws SamplingException {
        final Timer.Context ctx = _fetchTimer.time();

        try {
            MetricSampler.Samples samples = fetchSamples();
            if (_sampleStore != null) {
                if (_samplingMode == MetricSampler.SamplingMode.ONGOING_EXECUTION) {
                    _sampleStore.storeSamples(new MetricSampler.Samples(Collections.emptySet(), samples.brokerMetricSamples()));
                } else {
                    _sampleStore.storeSamples(samples);
                }
            }

            if (_samplingMode == MetricSampler.SamplingMode.ONGOING_EXECUTION && _sampleStoreForPartitionMetricOnExecution != null) {
                _sampleStoreForPartitionMetricOnExecution.storeSamples(new MetricSampler.Samples(samples.partitionMetricSamples(),
                        Collections.emptySet()));
            }
            // TODO: evolve sample store interface to allow independent eviction time for different type of metric samples.
            // We are not calling sampleStore.evictSamplesBefore() because the broker metric samples and partition metric
            // samples may have different number of windows so they can not be evicted using the same timestamp.
        } catch (Exception e) {
            _fetchFailureRate.mark();
            throw e;
        } finally {
            ctx.stop();
        }
    }

    /**
     * Fetch the metric samples indicated by {@link #_samplingMode}.
     *
     * @return The accepted partition and broker metric samples.
     */
    protected MetricSampler.Samples fetchSamples() throws SamplingException {
        MetricSamplerOptions metricSamplerOptions = new MetricSamplerOptions(
                _cluster, _assignedPartitions, _startTimeMs, _endTimeMs, _samplingMode, _metricDef, _timeout);
        MetricSampler.Samples samples = _metricSampler.getSamples(metricSamplerOptions);
        if (samples == null) {
            samples = MetricSampler.EMPTY_SAMPLES;
        }

        if (_samplingMode == MetricSampler.SamplingMode.ALL || _samplingMode == MetricSampler.SamplingMode.PARTITION_METRICS_ONLY) {
            usePartitionMetricSamples(samples.partitionMetricSamples());
        }
        if (_samplingMode != MetricSampler.SamplingMode.PARTITION_METRICS_ONLY) {
            useBrokerMetricSamples(samples.brokerMetricSamples());
        }

        return samples;
    }

    protected abstract void usePartitionMetricSamples(Set<PartitionMetricSample> partitionMetricSamples);

    protected abstract void useBrokerMetricSamples(Set<BrokerMetricSample> brokerMetricSamples);
}
