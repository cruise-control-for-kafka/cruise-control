/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerLoad;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.BROKER_CAPACITY_FETCH_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getRackHandleNull;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.*;

/**
 * Process the raw metrics collected by {@link CruiseControlMetricsReporterSampler} from the Kafka cluster.
 */
public class CruiseControlMetricsProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsProcessor.class);
    private static final long INIT_METRIC_TIMESTAMP = -1L;
    private final Map<Integer, BrokerLoad> _brokerLoad;
    // TODO: Use the cached number of cores in estimation of partition CPU utilization.
    private final Map<Integer, Double> _cachedNumCoresByBroker;
    private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;
    private final boolean _allowCpuCapacityEstimation;
    private long _maxMetricTimestamp;

    /**
     * @param brokerCapacityConfigResolver The resolver for retrieving broker capacities.
     * @param allowCpuCapacityEstimation   {@code true} to allow CPU capacity estimation of brokers used for CPU utilization estimation.
     */
    CruiseControlMetricsProcessor(BrokerCapacityConfigResolver brokerCapacityConfigResolver, boolean allowCpuCapacityEstimation) {
        _brokerLoad = new HashMap<>();
        _cachedNumCoresByBroker = new HashMap<>();
        _brokerCapacityConfigResolver = brokerCapacityConfigResolver;
        _allowCpuCapacityEstimation = allowCpuCapacityEstimation;
        _maxMetricTimestamp = INIT_METRIC_TIMESTAMP;
    }

    void addMetric(CruiseControlMetric metric) {
        int brokerId = metric.brokerId();
        LOG.trace("Adding cruise control metric {}", metric);
        _maxMetricTimestamp = Math.max(metric.time(), _maxMetricTimestamp);
        _brokerLoad.compute(brokerId, (bid, load) -> {
            BrokerLoad brokerLoad = load == null ? new BrokerLoad() : load;
            brokerLoad.recordMetric(metric);
            return brokerLoad;
        });
    }

    /**
     * Update the cached number of cores by broker id. The cache is refreshed only for brokers with missing number of cores.
     * Note that if the broker capacity resolver is unable to resolve or can only estimate certain broker's capacity, the core
     * number for that broker will not be updated.
     *
     * @param cluster Kafka cluster.
     */
    private void updateCachedNumCoresByBroker(Cluster cluster) {
        for (int brokerId : _brokerLoad.keySet()) {
            // Compute cached number of cores by broker id if they have not been cached already.
            _cachedNumCoresByBroker.computeIfAbsent(brokerId, bid -> {
                Node node = cluster.nodeById(bid);
                if (node == null) {
                    LOG.warn("Received metrics from unrecognized broker {}.", bid);
                    return null;
                }
                try {
                    BrokerCapacityInfo capacity =
                            _brokerCapacityConfigResolver.capacityForBroker(getRackHandleNull(node), node.host(), bid, BROKER_CAPACITY_FETCH_TIMEOUT_MS,
                                    _allowCpuCapacityEstimation);
                    return capacity == null ? null : capacity.numCpuCores();
                } catch (TimeoutException | BrokerCapacityResolutionException e) {
                    LOG.warn("Unable to get number of CPU cores for broker {}.", node.id(), e);
                    return null;
                }
            });
        }
    }

    /**
     * Package private for unit tests.
     *
     * @return The cached number of cores by broker.
     */
    Map<Integer, Double> cachedNumCoresByBroker() {
        return _cachedNumCoresByBroker;
    }

    /**
     * Process all the added {@link CruiseControlMetric} to get the {@link MetricSampler.Samples}
     *
     * @param cluster                 Kafka cluster.
     * @param partitionsDotNotHandled Partitions to construct samples for. The topic partition name may have dots.
     * @param samplingMode            The sampling mode to indicate which type of samples are needed.
     * @return The constructed metric samples.
     */
    MetricSampler.Samples process(Cluster cluster,
                                  Set<TopicPartition> partitionsDotNotHandled,
                                  MetricSampler.SamplingMode samplingMode) {
        updateCachedNumCoresByBroker(cluster);
        // Theoretically we should not move forward at all if a broker reported a different all topic bytes in from all
        // its resident replicas. However, it is not clear how often this would happen yet. At this point we still
        // continue process the other brokers. Later on if in practice all topic bytes in and the aggregation value is
        // rarely inconsistent we can just stop the sample generation when the this happens.
        _brokerLoad.forEach((broker, load) -> load.prepareBrokerMetrics(cluster, broker, _maxMetricTimestamp));

        // Get partition metric samples.
        Map<Integer, Integer> skippedPartitionByBroker = null;
        Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
        if (samplingMode != MetricSampler.SamplingMode.BROKER_METRICS_ONLY) {
            skippedPartitionByBroker = addPartitionMetricSamples(cluster, partitionsDotNotHandled, partitionMetricSamples);
        }

        // Get broker metric samples.
        int skippedBroker = 0;
        Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();
        if (samplingMode != MetricSampler.SamplingMode.PARTITION_METRICS_ONLY) {
            skippedBroker = addBrokerMetricSamples(cluster, brokerMetricSamples);
        }

        logProcess(samplingMode, skippedPartitionByBroker, skippedBroker, partitionMetricSamples, brokerMetricSamples);
        return new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples);
    }

    private void logProcess(MetricSampler.SamplingMode samplingMode,
                            Map<Integer, Integer> skippedPartitionByBroker,
                            int skippedBroker,
                            Set<PartitionMetricSample> partitionMetricSamples,
                            Set<BrokerMetricSample> brokerMetricSamples) {
        switch (samplingMode) {
            case ONGOING_EXECUTION:
            case ALL:
                LOG.info("Generated {}{} partition metric samples and {}{} broker metric samples for timestamp {}.",
                        partitionMetricSamples.size(),
                        !skippedPartitionByBroker.isEmpty() ? String.format("(%d skipped by broker %s)",
                                skippedPartitionByBroker.values()
                                        .stream()
                                        .mapToInt(v -> v)
                                        .sum(),
                                skippedPartitionByBroker) : "",
                        brokerMetricSamples.size(), skippedBroker > 0 ? "(" + skippedBroker + " skipped)" : "", _maxMetricTimestamp);
                break;
            case PARTITION_METRICS_ONLY:
                LOG.info("Generated {}{} partition metric samples for timestamp {}.", partitionMetricSamples.size(),
                        !skippedPartitionByBroker.isEmpty() ? String.format("(%d skipped by broker %s)",
                                skippedPartitionByBroker.values()
                                        .stream()
                                        .mapToInt(v -> v)
                                        .sum(),
                                skippedPartitionByBroker) : "", _maxMetricTimestamp);
                break;
            case BROKER_METRICS_ONLY:
                LOG.info("Generated {}{} broker metric samples for timestamp {}.", brokerMetricSamples.size(),
                        skippedBroker > 0 ? "(" + skippedBroker + " skipped)" : "", _maxMetricTimestamp);
                break;
            default:
                throw new IllegalStateException("Unknown sampling mode " + samplingMode);
        }
    }

    void clear() {
        _brokerLoad.clear();
        _maxMetricTimestamp = INIT_METRIC_TIMESTAMP;
    }

    /**
     * Add the partition metric samples to the provided set.
     *
     * @param cluster                 Kafka cluster
     * @param partitionsDotNotHandled The partitions to get samples. The topic partition name may have dots.
     * @param partitionMetricSamples  The set to add the partition samples to.
     * @return The number of skipped partitions by broker ids. A broker id of {@link SamplingUtils#UNRECOGNIZED_BROKER_ID}
     * indicates unrecognized broker.
     */
    private Map<Integer, Integer> addPartitionMetricSamples(Cluster cluster,
                                                            Set<TopicPartition> partitionsDotNotHandled,
                                                            Set<PartitionMetricSample> partitionMetricSamples) {
        Map<Integer, Integer> skippedPartitionByBroker = new HashMap<>();
        Map<Integer, Map<String, Integer>> leaderDistribution = leaderDistribution(cluster);
        for (TopicPartition tpDotNotHandled : partitionsDotNotHandled) {
            try {
                PartitionMetricSample sample = buildPartitionMetricSample(cluster, leaderDistribution, tpDotNotHandled, _brokerLoad,
                        _maxMetricTimestamp, _cachedNumCoresByBroker, skippedPartitionByBroker);
                if (sample != null) {
                    LOG.trace("Added partition metrics sample for {}.", tpDotNotHandled);
                    partitionMetricSamples.add(sample);
                }
            } catch (Exception e) {
                LOG.error("Error building partition metric sample for {}.", tpDotNotHandled, e);
                skippedPartitionByBroker.merge(UNRECOGNIZED_BROKER_ID, 1, Integer::sum);
            }
        }
        return skippedPartitionByBroker;
    }

    /**
     * Add the broker metric samples to the provided set.
     *
     * @param cluster             The Kafka cluster
     * @param brokerMetricSamples The set to add the broker samples to.
     * @return The number of skipped brokers.
     */
    private int addBrokerMetricSamples(Cluster cluster, Set<BrokerMetricSample> brokerMetricSamples) {
        int skippedBroker = 0;
        for (Node node : cluster.nodes()) {
            try {
                BrokerMetricSample sample = buildBrokerMetricSample(node, _brokerLoad, _maxMetricTimestamp);
                if (sample != null) {
                    LOG.trace("Added broker metric sample for broker {}.", node.id());
                    brokerMetricSamples.add(sample);
                } else {
                    skippedBroker++;
                }
            } catch (UnknownVersionException e) {
                LOG.error("Unrecognized serde version detected during broker metric sampling.", e);
                skippedBroker++;
            } catch (Exception e) {
                LOG.error("Error building broker metric sample for {}.", node.id(), e);
                skippedBroker++;
            }
        }
        return skippedBroker;
    }
}
