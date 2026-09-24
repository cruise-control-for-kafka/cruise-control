/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.estimateLeaderCpuUtilUsingLinearRegressionModel;


/**
 * A metric fetcher that is responsible for fetching the metric samples to monitor the cluster load.
 */
class SamplingFetcher extends MetricFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(SamplingFetcher.class);
    private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
    private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
    private final boolean _leaderValidation;
    private final boolean _useLinearRegressionModel;

    SamplingFetcher(MetricSampler metricSampler,
                    Cluster cluster,
                    KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                    KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                    SampleStore sampleStore,
                    SampleStore sampleStoreForPartitionMetricOnExecution,
                    Set<TopicPartition> assignedPartitions,
                    long startTimeMs,
                    long endTimeMs,
                    boolean leaderValidation,
                    boolean useLinearRegressionModel,
                    MetricDef metricDef,
                    Timer fetchTimer,
                    Meter fetchFailureRate,
                    MetricSampler.SamplingMode samplingMode) {
        super(metricSampler, cluster, sampleStore, sampleStoreForPartitionMetricOnExecution, assignedPartitions, startTimeMs, endTimeMs,
                metricDef, fetchTimer, fetchFailureRate, samplingMode);
        _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
        _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
        _leaderValidation = leaderValidation;
        _useLinearRegressionModel = useLinearRegressionModel;
    }

    @Override
    protected void usePartitionMetricSamples(Set<PartitionMetricSample> partitionMetricSamples) {
        // Give an initial capacity to avoid resizing.
        Set<TopicPartition> returnedPartitions = new HashSet<>();
        // Ignore the null value if the metric sampler did not return a sample
        if (partitionMetricSamples != null) {
            int discarded = 0;
            Iterator<PartitionMetricSample> iter = partitionMetricSamples.iterator();
            while (iter.hasNext()) {
                PartitionMetricSample partitionMetricSample = iter.next();
                TopicPartition tp = partitionMetricSample.entity().tp();
                if (_assignedPartitions.contains(tp)) {
                    // we fill in the cpu utilization based on the model in case user did not fill it in.
                    if (_useLinearRegressionModel && ModelParameters.trainingCompleted()) {
                        partitionMetricSample.record(KafkaMetricDef.commonMetricDef().metricInfo(CPU_USAGE.name()),
                                estimateLeaderCpuUtilUsingLinearRegressionModel(partitionMetricSample));
                    }
                    // we close the metric sample in case the implementation forgot to do so.
                    partitionMetricSample.close(_endTimeMs);
                    // We remove the sample from the returning set if it is not accepted.
                    if (_partitionMetricSampleAggregator.addSample(partitionMetricSample, _leaderValidation)) {
                        LOG.trace("Enqueued partition metric sample {}", partitionMetricSample);
                    } else {
                        iter.remove();
                        discarded++;
                        LOG.trace("Failed to add partition metric sample {}", partitionMetricSample);
                    }
                    returnedPartitions.add(tp);
                } else {
                    LOG.warn("Collected partition metric sample for partition {} which is not an assigned partition. "
                            + "The metric sample will be ignored.", tp);
                }
            }
            LOG.info("Collected {}{} partition metric samples for {} partitions. Total partition assigned: {}.",
                    partitionMetricSamples.size(), discarded > 0 ? String.format("(%d discarded)", discarded) : "",
                    returnedPartitions.size(), _assignedPartitions.size());
        } else {
            LOG.warn("Failed to collect partition metric samples for {} assigned partitions", _assignedPartitions.size());
        }
    }

    @Override
    protected void useBrokerMetricSamples(Set<BrokerMetricSample> brokerMetricSamples) {
        Set<Integer> returnedBrokerIds = new HashSet<>();
        if (brokerMetricSamples != null) {
            int discarded = 0;
            Iterator<BrokerMetricSample> iter = brokerMetricSamples.iterator();
            while (iter.hasNext()) {
                BrokerMetricSample brokerMetricSample = iter.next();
                // Close the broker metric sample in case user forgot to close it.
                brokerMetricSample.close(_endTimeMs);
                if (_brokerMetricSampleAggregator.addSample(brokerMetricSample)) {
                    LOG.trace("Enqueued broker metric sample {}", brokerMetricSample);
                } else {
                    iter.remove();
                    discarded++;
                    LOG.trace("Failed to add broker metric sample {}", brokerMetricSample);
                }
                returnedBrokerIds.add(brokerMetricSample.brokerId());
            }
            LOG.info("Collected {}{} broker metric samples for {} brokers.",
                    brokerMetricSamples.size(), discarded > 0 ? String.format("(%d discarded)", discarded) : "",
                    returnedBrokerIds.size());
            // Add the broker metric samples to the observation.
            ModelParameters.addMetricObservation(brokerMetricSamples);
        } else {
            LOG.warn("Failed to collect broker metrics samples.");
        }
    }
}
