/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * This class encapsulates the options that need to be passed to the
 * {@link MetricSampler#getSamples(MetricSamplerOptions)} method.
 * Future use-cases may choose to extend this class and add more options.
 */
public class MetricSamplerOptions {
    private final Cluster _cluster;
    private final Set<TopicPartition> _assignedPartitions;
    private final long _startTimeMs;
    private final long _endTimeMs;
    private final MetricSampler.SamplingMode _mode;
    private final MetricDef _metricDef;
    private final long _timeoutMs;

    /**
     * @param cluster            the metadata of the cluster.
     * @param assignedPartitions the topic partition
     * @param startTimeMs        the start time of the sampling period.
     * @param endTimeMs          the end time of the sampling period.
     * @param mode               The sampling mode.
     * @param metricDef          the metric definitions.
     * @param timeoutMs          The sampling timeout in milliseconds to stop sampling even if there is more data to get.
     */
    public MetricSamplerOptions(Cluster cluster,
                                Set<TopicPartition> assignedPartitions,
                                long startTimeMs,
                                long endTimeMs,
                                MetricSampler.SamplingMode mode,
                                MetricDef metricDef,
                                long timeoutMs) {
        _cluster = cluster;
        _assignedPartitions = assignedPartitions;
        _startTimeMs = startTimeMs;
        _endTimeMs = endTimeMs;
        _mode = mode;
        _metricDef = metricDef;
        _timeoutMs = timeoutMs;
    }

    public Cluster cluster() {
        return _cluster;
    }

    public Set<TopicPartition> assignedPartitions() {
        return _assignedPartitions;
    }

    public long startTimeMs() {
        return _startTimeMs;
    }

    public long endTimeMs() {
        return _endTimeMs;
    }

    public MetricSampler.SamplingMode mode() {
        return _mode;
    }

    public MetricDef metricDef() {
        return _metricDef;
    }

    public long timeoutMs() {
        return _timeoutMs;
    }
}
