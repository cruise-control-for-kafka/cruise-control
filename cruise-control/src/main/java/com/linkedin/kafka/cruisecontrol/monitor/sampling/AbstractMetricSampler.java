/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager.BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG;

/**
 * This is a base implementation of a MetricSampler that can be overridden by concrete Metric Sampler
 * implementations. It takes care of the common logic of initializing a {@link CruiseControlMetricsProcessor},
 * and then using it to record every individual {@link CruiseControlMetric}, and finally convert all
 * of these into {@link com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler.Samples}.
 */
public abstract class AbstractMetricSampler implements MetricSampler {
    private CruiseControlMetricsProcessor _metricsProcessor;

    @Override
    public void configure(Map<String, ?> configs) {
        BrokerCapacityConfigResolver capacityResolver =
                (BrokerCapacityConfigResolver) validateNotNull(configs.get(BROKER_CAPACITY_CONFIG_RESOLVER_OBJECT_CONFIG),
                        "Metrics reporter sampler configuration is missing broker capacity config resolver object.");
        boolean allowCpuCapacityEstimation = (Boolean) configs.get(
                MonitorConfig.SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG);
        _metricsProcessor = new CruiseControlMetricsProcessor(capacityResolver, allowCpuCapacityEstimation);
    }

    @Override
    public Samples getSamples(Cluster cluster, Set<TopicPartition> assignedPartitions, long startTimeMs,
                              long endTimeMs, SamplingMode mode, MetricDef metricDef, long timeoutMs) throws SamplingException {
        MetricSamplerOptions metricSamplerOptions = new MetricSamplerOptions(
                cluster, assignedPartitions, startTimeMs, endTimeMs, mode, metricDef, timeoutMs);
        return getSamples(metricSamplerOptions);
    }

    @Override
    public Samples getSamples(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
        int totalMetricsAdded = retrieveMetricsForProcessing(metricSamplerOptions);

        try {
            if (totalMetricsAdded > 0) {
                return _metricsProcessor.process(metricSamplerOptions.cluster(),
                        metricSamplerOptions.assignedPartitions(), metricSamplerOptions.mode());
            } else {
                return MetricSampler.EMPTY_SAMPLES;
            }
        } finally {
            _metricsProcessor.clear();
        }
    }

    /**
     * This method will be called to retrieve all the {@link CruiseControlMetric}s
     * for a cluster in one sampling period for processing by the {@link CruiseControlMetricsProcessor}.
     * <p>
     * Concrete metric sampler implementations can implement this method according to
     * their corresponding business logic of fetching metrics for the cluster.
     *
     * @param metricSamplerOptions Object that encapsulates all the options to be used for sampling metrics.
     * @return Total number of metrics retrieved.
     */
    protected abstract int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions)
            throws SamplingException;

    /**
     * This method adds a metric obtained from the cluster to the list of metrics being
     * retrieved for processing during a single sampling period.
     * The {@link #retrieveMetricsForProcessing(MetricSamplerOptions)} method implemented in the concrete metric
     * sampler class will call this method for every metric it obtains for a single sampling period for the cluster.
     *
     * @param metric Individual {@link CruiseControlMetric} being recorded by the Metric Sampler.
     */
    protected void addMetricForProcessing(CruiseControlMetric metric) {
        this._metricsProcessor.addMetric(metric);
    }
}
