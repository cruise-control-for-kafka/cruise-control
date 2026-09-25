/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Set;

/**
 * The interface to get metric samples of given topic partitions.
 * <p>
 * Kafka Cruise Control periodically collects the metrics of topics, brokers and partitions in the cluster.
 * The {@link #getSamples(MetricSamplerOptions)} is called periodically to collect metrics
 * of all these types from a cluster in one sampling period.
 * The MetricSampler may be used by multiple threads at the same time, so the implementation need to be thread safe.
 *
 */
public interface MetricSampler extends CruiseControlConfigurable, AutoCloseable {
    Samples EMPTY_SAMPLES = new Samples(Collections.emptySet(), Collections.emptySet());

    /**
     * Get the metric samples from the Kafka cluster with the options passed as arguments.
     * The samples include PartitionMetricSamples and BrokerMetricSamples.
     * <p>
     * Due to the lack of direct metrics at partition level, Kafka Cruise Control needs to estimate the CPU
     * utilization for each partition by using the following formula:
     * <p>
     * BROKER_CPU_UTIL = a * ALL_TOPIC_BYTES_IN_RATE + b * ALL_TOPIC_BYTES_OUT_RATE + c * ALL_FOLLOWER_BYTES_IN_RATE
     * <p>
     * LEADER_PARTITION_CPU_UTIL = a * LEADER_PARTITION_BYTES_IN + b * LEADER_PARTITION_BYTES_OUT
     * <p>
     * FOLLOWER_PARTITION_CPU_UTIL = c * LEADER_PARTITION_BYTES_IN
     * <p>
     * Kafka Cruise Control needs to know the parameters of a, b and c for cost evaluation of leader and
     * partition movement.
     *
     * @param cluster            the metadata of the cluster.
     * @param assignedPartitions the topic partition
     * @param startTimeMs        the start time of the sampling period.
     * @param endTimeMs          the end time of the sampling period.
     * @param mode               The sampling mode.
     * @param metricDef          the metric definitions.
     * @param timeoutMs          The sampling timeout in milliseconds to stop sampling even if there is more data to get.
     * @return Samples collected from the Kafka cluster.
     * @deprecated Will be removed in a future release -- please use {@link #getSamples(MetricSamplerOptions)}
     */
    @Deprecated
    Samples getSamples(Cluster cluster,
                       Set<TopicPartition> assignedPartitions,
                       long startTimeMs,
                       long endTimeMs,
                       SamplingMode mode,
                       MetricDef metricDef,
                       long timeoutMs)
            throws SamplingException;

    /**
     * Get the metric samples from the Kafka cluster with the options passed in {@link MetricSamplerOptions}.
     * The samples include PartitionMetricSamples and BrokerMetricSamples.
     * <p>
     * Due to the lack of direct metrics at partition level, Kafka Cruise Control needs to estimate the CPU
     * utilization for each partition by using the following formula:
     * <p>
     * BROKER_CPU_UTIL = a * ALL_TOPIC_BYTES_IN_RATE + b * ALL_TOPIC_BYTES_OUT_RATE + c * ALL_FOLLOWER_BYTES_IN_RATE
     * <p>
     * LEADER_PARTITION_CPU_UTIL = a * LEADER_PARTITION_BYTES_IN + b * LEADER_PARTITION_BYTES_OUT
     * <p>
     * FOLLOWER_PARTITION_CPU_UTIL = c * LEADER_PARTITION_BYTES_IN
     * <p>
     * Kafka Cruise Control needs to know the parameters of a, b and c for cost evaluation of leader and
     * partition movement.
     *
     * @param metricSamplerOptions This class encapsulates all the arguments needed by MetricSampler to get samples.
     * @return Samples collected from the Kafka cluster.
     */
    Samples getSamples(MetricSamplerOptions metricSamplerOptions) throws SamplingException;

    /**
     * The sampling mode to indicate which type of samples is interested in.
     *
     * <ul>
     *   <li>{@link #PARTITION_METRICS_ONLY}: Sample partition metrics only</li>
     *   <li>{@link #BROKER_METRICS_ONLY}: Sample broker metrics only</li>
     *   <li>{@link #ONGOING_EXECUTION}: Sample broker metrics and partition metrics (in a separate store) during ongoing execution</li>
     *   <li>{@link #ALL}: Sample partition and broker metrics</li>
     * </ul>
     */
    enum SamplingMode {
        PARTITION_METRICS_ONLY, BROKER_METRICS_ONLY, ONGOING_EXECUTION, ALL
    }

    /**
     * A container class that wraps both the partition metric samples and broker metric samples.
     */
    class Samples {
        private final Set<PartitionMetricSample> _partitionMetricSamples;
        private final Set<BrokerMetricSample> _brokerMetricSamples;

        public Samples(Set<PartitionMetricSample> partitionMetricSamples,
                       Set<BrokerMetricSample> brokerMetricSamples) {
            _partitionMetricSamples = partitionMetricSamples;
            _brokerMetricSamples = brokerMetricSamples;
        }

        public Set<PartitionMetricSample> partitionMetricSamples() {
            return _partitionMetricSamples;
        }

        public Set<BrokerMetricSample> brokerMetricSamples() {
            return _brokerMetricSamples;
        }
    }
}
