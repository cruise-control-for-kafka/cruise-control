/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerLoad;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createConsumer;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler.METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS;


public final class SamplingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SamplingUtils.class);
    private static final String SKIP_BUILDING_SAMPLE_PREFIX = "Skip generating metric sample for ";
    public static final int UNRECOGNIZED_BROKER_ID = -1;
    public static final int LOADING_PROGRESS = -1;

    private SamplingUtils() {
    }

    /**
     * Get the number of leader partitions for each topic by each broker. It is useful to derive the partition level IO
     * from the topic level IO on a broker.
     * TODO: Create open source KIP to provide per partition IO metrics.
     *
     * @param cluster Kafka cluster
     * @return The number of leader partitions for each topic by each broker.
     */
    static Map<Integer, Map<String, Integer>> leaderDistribution(Cluster cluster) {
        List<Node> clusterNodes = cluster.nodes();
        Map<Integer, Map<String, Integer>> stats = new HashMap<>();
        for (Node node : clusterNodes) {
            Map<String, Integer> numLeadersByTopic = new HashMap<>();
            stats.put(node.id(), numLeadersByTopic);
            cluster.partitionsForNode(node.id()).forEach(partitionInfo -> numLeadersByTopic.merge(partitionInfo.topic(), 1, Integer::sum));
        }
        return stats;
    }

    /**
     * Estimate the leader CPU utilization of the partition using its metric sample based on the static model defined via
     * {@link ModelUtils#estimateLeaderCpuUtilPerCore(double, double, double, double, double, double)} and the given number of
     * CPU cores.
     *
     * @param pms             Metric sample of partition.
     * @param brokerLoad      Load information for the broker that the leader of the partition resides.
     * @param commonMetricDef Definitions to look up the metric info.
     * @param numCpuCores     Number of CPU cores.
     * @return The estimated CPU utilization of the leader for the partition based on the static model, or {@code null}
     * if estimation is not possible.
     */
    private static Double estimateLeaderCpuUtil(PartitionMetricSample pms, BrokerLoad brokerLoad, MetricDef commonMetricDef, double numCpuCores) {
        double partitionBytesInRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id());
        double partitionBytesOutRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.LEADER_BYTES_OUT.name()).id());
        double partitionReplicationBytesOutRate = pms.metricValue(commonMetricDef.metricInfo(KafkaMetricDef.REPLICATION_BYTES_OUT_RATE.name()).id());
        double brokerTotalBytesOut = brokerLoad.brokerMetric(ALL_TOPIC_BYTES_OUT) + brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_OUT);
        Double estimatedLeaderCpuUtilPerCore = ModelUtils.estimateLeaderCpuUtilPerCore(brokerLoad.brokerMetric(BROKER_CPU_UTIL),
                brokerLoad.brokerMetric(ALL_TOPIC_BYTES_IN),
                brokerTotalBytesOut,
                brokerLoad.brokerMetric(ALL_TOPIC_REPLICATION_BYTES_IN),
                partitionBytesInRate,
                partitionBytesOutRate + partitionReplicationBytesOutRate);
        return estimatedLeaderCpuUtilPerCore != null ? numCpuCores * estimatedLeaderCpuUtilPerCore : null;
    }

    /**
     * Estimate the leader CPU utilization of the partition using its metric sample based on linear regression model
     * defined via {@link ModelUtils#estimateLeaderCpuUtilUsingLinearRegressionModel(double, double)}
     *
     * @param metricSample Metric sample of partition.
     * @return The estimated CPU utilization of the leader for the partition based on linear regression model.
     */
    static double estimateLeaderCpuUtilUsingLinearRegressionModel(PartitionMetricSample metricSample) {
        List<Short> cpuId = KafkaMetricDef.resourceToMetricIds(Resource.CPU);
        List<Short> networkOutId = KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT);
        // TODO: Verify whether the linear regression model intends to use cpu utilization or not (i.e. NW_IN).
        Double cpuUtilization = sumOfMetrics(metricSample, cpuId);
        Double partitionBytesOutRate = sumOfMetrics(metricSample, networkOutId);
        return ModelUtils.estimateLeaderCpuUtilUsingLinearRegressionModel(cpuUtilization, partitionBytesOutRate);
    }

    /**
     * Add all the values of the given metric ids up.
     * TODO: Remove this once we completely move to metric def.
     *
     * @param metricSample Metric sample of partition.
     * @param metricIds    Metric Ids.
     * @return Sum of all the values of the given metric ids.
     */
    private static Double sumOfMetrics(PartitionMetricSample metricSample, Collection<Short> metricIds) {
        double result = 0;
        for (short id : metricIds) {
            result += metricSample.metricValue(id);
        }
        return result;
    }

    /**
     * Removes any dots that potentially exist in the given parameter.
     *
     * @param tp TopicPartition that may contain dots.
     * @return TopicPartition whose dots have been removed from the given topic name.
     */
    private static TopicPartition partitionHandleDotInTopicName(TopicPartition tp) {
        // In the reported metrics, the "." in the topic name will be replaced by "_".
        return !tp.topic().contains(".") ? tp : new TopicPartition(replaceDotsWithUnderscores(tp.topic()), tp.partition());
    }

    /**
     * Removes any dots that potentially exist in the given string. This method useful for making topic names reported by
     * Kafka metadata consistent with the topic names reported by the metrics reporter.
     * <p>
     * Note that the reported metrics implicitly replaces the "." in topic names with "_".
     *
     * @param stringWithDots String that may contain dots.
     * @return String whose dots have been removed from the given string.
     */
    public static String replaceDotsWithUnderscores(String stringWithDots) {
        return !stringWithDots.contains(".") ? stringWithDots : stringWithDots.replace('.', '_');
    }

    /**
     * Create a {@link PartitionMetricSample}, record the relevant metrics for the given partition from the given topic on
     * broker that hosts the given number of leaders, and return the sample.
     *
     * @param cluster                  Kafka cluster.
     * @param leaderDistribution       The leader count per topic/broker
     * @param tpDotNotHandled          The original topic name that may contain dots.
     * @param brokerLoadById           Load information for brokers by the broker id.
     * @param maxMetricTimestamp       Maximum timestamp of the sampled metric during the sampling process.
     * @param cachedNumCoresByBroker   Cached number of cores by broker.
     * @param skippedPartitionByBroker Number of skipped partition samples by broker ids.
     * @return Metric sample populated with topic and partition metrics, or {@code null} if sample generation is skipped.
     */
    static PartitionMetricSample buildPartitionMetricSample(Cluster cluster,
                                                            Map<Integer, Map<String, Integer>> leaderDistribution,
                                                            TopicPartition tpDotNotHandled,
                                                            Map<Integer, BrokerLoad> brokerLoadById,
                                                            long maxMetricTimestamp,
                                                            Map<Integer, Double> cachedNumCoresByBroker,
                                                            Map<Integer, Integer> skippedPartitionByBroker) {
        Node leaderNode = cluster.leaderFor(tpDotNotHandled);
        if (leaderNode == null) {
            LOG.trace("Partition {} has no current leader.", tpDotNotHandled);
            skippedPartitionByBroker.merge(UNRECOGNIZED_BROKER_ID, 1, Integer::sum);
            return null;
        }
        int leaderId = leaderNode.id();
        //TODO: switch to linear regression model without computing partition level CPU usage.
        BrokerLoad brokerLoad = brokerLoadById.get(leaderId);
        TopicPartition tpWithDotHandled = partitionHandleDotInTopicName(tpDotNotHandled);
        if (skipBuildingPartitionMetricSample(tpDotNotHandled, tpWithDotHandled, leaderId, brokerLoad, cachedNumCoresByBroker)) {
            skippedPartitionByBroker.merge(leaderId, 1, Integer::sum);
            return null;
        }

        // Fill in all the common metrics.
        MetricDef commonMetricDef = KafkaMetricDef.commonMetricDef();
        PartitionMetricSample pms = new PartitionMetricSample(leaderId, tpDotNotHandled);
        int numLeaders = leaderDistribution.get(leaderId).get(tpDotNotHandled.topic());
        for (RawMetricType rawMetricType : RawMetricType.topicMetricTypes()) {
            double sampleValue = numLeaders == 0 ? 0 : (brokerLoad.topicMetrics(tpWithDotHandled.topic(), rawMetricType)) / numLeaders;
            MetricInfo metricInfo = commonMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawMetricType).name());
            pms.record(metricInfo, sampleValue);
        }
        // Fill in disk and CPU utilization, which are not topic metric types.
        Double partitionSize = brokerLoad.partitionMetric(tpWithDotHandled.topic(), tpWithDotHandled.partition(), PARTITION_SIZE);
        if (partitionSize == null) {
            skippedPartitionByBroker.merge(leaderId, 1, Integer::sum);
            return null;
        }
        pms.record(commonMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), partitionSize);
        Double estimatedLeaderCpuUtil = estimateLeaderCpuUtil(pms, brokerLoad, commonMetricDef, cachedNumCoresByBroker.get(leaderId));
        if (estimatedLeaderCpuUtil == null) {
            skippedPartitionByBroker.merge(leaderId, 1, Integer::sum);
            return null;
        }
        pms.record(commonMetricDef.metricInfo(KafkaMetricDef.CPU_USAGE.name()), estimatedLeaderCpuUtil);
        pms.close(maxMetricTimestamp);
        return pms;
    }

    /**
     * Create a {@link BrokerMetricSample}, record the relevant metrics for the given broker, and return the sample.
     *
     * @param node               Node hosting the broker.
     * @param brokerLoadById     Load information for brokers by the broker id.
     * @param maxMetricTimestamp Maximum timestamp of the sampled metric during the sampling process.
     * @return Metric sample populated with broker metrics, or {@code null} if sample generation is skipped.
     */
    static BrokerMetricSample buildBrokerMetricSample(Node node,
                                                      Map<Integer, BrokerLoad> brokerLoadById,
                                                      long maxMetricTimestamp) throws UnknownVersionException {
        BrokerLoad brokerLoad = brokerLoadById.get(node.id());
        if (skipBuildingBrokerMetricSample(brokerLoad, node.id())) {
            return null;
        }
        MetricDef brokerMetricDef = KafkaMetricDef.brokerMetricDef();
        BrokerMetricSample bms = new BrokerMetricSample(node.host(), node.id(), brokerLoad.brokerSampleDeserializationVersion());
        for (Map.Entry<Byte, Set<RawMetricType>> entry : RawMetricType.brokerMetricTypesDiffByVersion().entrySet()) {
            for (RawMetricType rawBrokerMetricType : entry.getValue()) {
                // We require the broker to report all the metric types (including nullable values). Otherwise we skip the broker.
                if (!brokerLoad.brokerMetricAvailable(rawBrokerMetricType)) {
                    LOG.warn("{}broker {} because it does not have {} metrics (serde version {}) or the metrics are inconsistent.",
                            SKIP_BUILDING_SAMPLE_PREFIX, node.id(), rawBrokerMetricType, entry.getKey());
                    return null;
                } else {
                    MetricInfo metricInfo = brokerMetricDef.metricInfo(KafkaMetricDef.forRawMetricType(rawBrokerMetricType).name());
                    double metricValue = brokerLoad.brokerMetric(rawBrokerMetricType);
                    bms.record(metricInfo, metricValue);
                }
            }
        }

        // Disk usage is not one of the broker raw metric type.
        bms.record(brokerMetricDef.metricInfo(KafkaMetricDef.DISK_USAGE.name()), brokerLoad.diskUsage());
        bms.close(maxMetricTimestamp);
        return bms;
    }

    /**
     * Check whether the metric sample generation for the partition with the given information should be skipped.
     *
     * @param tpDotNotHandled        The original topic name that may contain dots.
     * @param tpWithDotHandled       Topic partition with dot-handled topic name (see {@link SamplingUtils#partitionHandleDotInTopicName}).
     * @param leaderId               Leader Id of partition.
     * @param brokerLoad             Load information for the broker that the leader of the partition resides.
     * @param cachedNumCoresByBroker Cached number of cores by broker.
     * @return {@code true} to skip generating partition metric sample, {@code false} otherwise.
     */
    private static boolean skipBuildingPartitionMetricSample(TopicPartition tpDotNotHandled,
                                                             TopicPartition tpWithDotHandled,
                                                             int leaderId,
                                                             BrokerLoad brokerLoad,
                                                             Map<Integer, Double> cachedNumCoresByBroker) {
        if (brokerLoad == null || !brokerLoad.brokerMetricAvailable(BROKER_CPU_UTIL)) {
            // Broker load or its BROKER_CPU_UTIL metric is not available.
            LOG.debug("{}partition {} because {} metric for broker {} is unavailable.", SKIP_BUILDING_SAMPLE_PREFIX,
                    tpDotNotHandled, BROKER_CPU_UTIL, leaderId);
            return true;
        } else if (cachedNumCoresByBroker.get(leaderId) == null) {
            // Broker load is available but the corresponding number of cores was not cached.
            LOG.debug("{}partition {} because the number of CPU cores of its leader broker {} is unavailable. Please ensure that "
                            + "either the broker capacity config resolver provides the number of CPU cores without estimation or allow "
                            + "CPU capacity estimation during sampling (i.e. set {} to true).", SKIP_BUILDING_SAMPLE_PREFIX,
                    tpDotNotHandled, leaderId, SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG);
            return true;
        } else if (!brokerLoad.allDotHandledTopicMetricsAvailable(tpWithDotHandled.topic())) {
            // Topic metrics are not available.
            LOG.debug("{}partition {} because broker {} has no metric or topic metrics are not available", SKIP_BUILDING_SAMPLE_PREFIX,
                    tpDotNotHandled, leaderId);
            return true;
        }
        return false;
    }

    /**
     * Check whether the metric sample generation for the broker with the given load and id should be skipped.
     *
     * @param brokerLoad Broker load.
     * @param brokerId   Broker id
     * @return {@code true} to skip generating broker metric sample, {@code false} otherwise.
     */
    private static boolean skipBuildingBrokerMetricSample(BrokerLoad brokerLoad, int brokerId) {
        if (brokerLoad == null) {
            LOG.warn("{}broker {} because all broker metrics are missing.", SKIP_BUILDING_SAMPLE_PREFIX, brokerId);
            return true;
        } else if (!brokerLoad.minRequiredBrokerMetricsAvailable()) {
            if (brokerLoad.missingBrokerMetricsInMinSupportedVersion().size() == 0) {
                LOG.warn("{}broker {} because there are not enough topic metrics to generate broker metrics.",
                        SKIP_BUILDING_SAMPLE_PREFIX, brokerId);
            } else {
                LOG.warn("{}broker {} because the following required metrics are missing {}.", SKIP_BUILDING_SAMPLE_PREFIX,
                        brokerId, brokerLoad.missingBrokerMetricsInMinSupportedVersion());
            }
            return true;
        }

        return false;
    }

    /**
     * Create a Kafka consumer for retrieving reported Cruise Control metrics.
     * The consumer uses {@link String} for keys and {@link CruiseControlMetric} for values.
     * <p>
     * This consumer is not intended to use (1) the group management functionality by using subscribe(topic) or (2) the Kafka-based
     * offset management strategy. Hence, the {@link ConsumerConfig#GROUP_ID_CONFIG} config is irrelevant to it.
     *
     * @param configs        The configurations for Cruise Control.
     * @param clientIdPrefix Client id prefix.
     * @return A new Kafka consumer
     */
    public static Consumer<String, CruiseControlMetric> createMetricConsumer(Map<String, ?> configs, String clientIdPrefix) {
        // Get bootstrap servers
        String bootstrapServers = (String) configs.get(METRIC_REPORTER_SAMPLER_BOOTSTRAP_SERVERS);
        if (bootstrapServers == null) {
            bootstrapServers = bootstrapServers(configs);
        }
        return createConsumer(configs, clientIdPrefix, bootstrapServers, StringDeserializer.class, MetricSerde.class, true);
    }

    /**
     * Retrieve comma separated bootstrap servers from the configurations for Cruise Control for configuring
     * {@link org.apache.kafka.clients.CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG}.
     *
     * @param config The configurations for Cruise Control.
     * @return Comma separated bootstrap servers.
     */
    @SuppressWarnings("unchecked")
    public static String bootstrapServers(Map<String, ?> config) {
        return String.join(",", (List<String>) config.get(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    /**
     * Create a Kafka consumer for retrieving samples from the Sample Store.
     * The consumer uses {@link ByteArrayDeserializer} for both key and values.
     * <p>
     * This consumer is not intended to use (1) the group management functionality by using subscribe(topic) or (2) the Kafka-based
     * offset management strategy. Hence, the {@link ConsumerConfig#GROUP_ID_CONFIG} config is irrelevant to it.
     *
     * @param configs        The configurations for Cruise Control.
     * @param clientIdPrefix Client id prefix.
     * @return A new Kafka consumer
     */
    public static Consumer<byte[], byte[]> createSampleStoreConsumer(Map<String, ?> configs, String clientIdPrefix) {
        return createConsumer(configs, clientIdPrefix, bootstrapServers(configs), ByteArrayDeserializer.class, ByteArrayDeserializer.class, false);
    }
}
