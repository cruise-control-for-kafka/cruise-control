/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;


/**
 * A metric fetcher that is responsible for fetching the broker metric samples for model training.
 */
class TrainingFetcher extends MetricFetcher {
    private static final MetricSampler.SamplingMode SAMPLING_MODE = MetricSampler.SamplingMode.BROKER_METRICS_ONLY;

    TrainingFetcher(MetricSampler metricSampler,
                    Cluster cluster,
                    SampleStore sampleStore,
                    Set<TopicPartition> assignedPartitions,
                    long startTimeMs,
                    long endTimeMs,
                    MetricDef metricDef,
                    Timer fetchTimer,
                    Meter fetchFailureRate) {
        super(metricSampler, cluster, sampleStore, assignedPartitions, startTimeMs, endTimeMs, metricDef, fetchTimer, fetchFailureRate, SAMPLING_MODE);
    }

    @Override
    protected void usePartitionMetricSamples(Set<PartitionMetricSample> partitionMetricSamples) {
        // Training fetcher does not use partition metric samples.
    }

    @Override
    protected void useBrokerMetricSamples(Set<BrokerMetricSample> brokerMetricSamples) {
        // Add the broker metric samples to the observation.
        ModelParameters.addMetricObservation(brokerMetricSamples);
    }
}
