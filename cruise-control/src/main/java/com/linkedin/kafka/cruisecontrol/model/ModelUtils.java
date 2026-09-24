/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static java.lang.Math.max;


/**
 * The util class for model.
 */
public final class ModelUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);
    public static final String BROKER_ID = "brokerid";
    public static final String BROKER_STATE = "brokerstate";
    public static final String REPLICAS = "replicas";
    public static final String IS_LEADER = "isLeader";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String LOAD = "load";
    public static final String METRIC_VALUES = "MetricValues";
    public static final String NAME = "name";
    public static final String BROKERS = "brokers";
    public static final String RACK_ID = "rackid";
    public static final String HOSTS = "hosts";
    // In some extremely low throughput cluster, the partition IO may appear to be higher than the broker IO.
    // We allow such error if it is not too big and just treat the partition IO the same as broker IO.
    // We also ignore all the inaccuracy when the broker total throughput is too low.
    private static final double ALLOWED_METRIC_ERROR_FACTOR = 1.05;
    private static final int UNSTABLE_METRIC_THROUGHPUT_THRESHOLD = 10;
    private static boolean useLinearRegressionModel = false;

    private ModelUtils() {

    }

    public static void init(KafkaCruiseControlConfig config) {
        useLinearRegressionModel = config.getBoolean(MonitorConfig.USE_LINEAR_REGRESSION_MODEL_CONFIG);
    }

    /**
     * Get the CPU utilization of follower using the load of the leader replica.
     *
     * @param leaderBytesInRate  Leader bytes in rate.
     * @param leaderBytesOutRate Leader bytes out rate.
     * @param leaderCpuUtil      CPU utilization of the leader.
     * @return The CPU utilization of follower using the load of the leader replica.
     */
    public static double getFollowerCpuUtilFromLeaderLoad(double leaderBytesInRate,
                                                          double leaderBytesOutRate,
                                                          double leaderCpuUtil) {
        if (useLinearRegressionModel) {
            double followerBytesInCoefficient =
                    ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.FOLLOWER_BYTES_IN);
            return followerBytesInCoefficient * leaderBytesInRate;
        } else {
            if (leaderBytesInRate == 0.0 && leaderBytesOutRate == 0.0) {
                return 0.0;
            } else {
                return leaderCpuUtil * (ModelParameters.cpuWeightOfFollowerBytesInRate() * leaderBytesInRate) / (
                        ModelParameters.cpuWeightOfLeaderBytesInRate() * leaderBytesInRate
                                + ModelParameters.cpuWeightOfLeaderBytesOutRate() * leaderBytesOutRate);
            }
        }
    }

    /**
     * Estimate the leader CPU utilization for the partition with the given information as a Double in [0.0,1.0], or
     * {@code null} if estimation is not possible due to inconsistency between partition and broker-level byte rates.
     *
     * @param brokerCpuUtil             A double in [0.0,1.0], representing the CPU usage of the broker hosting the partition leader.
     * @param brokerLeaderBytesInRate   Leader bytes in rate in the broker.
     * @param brokerLeaderBytesOutRate  Leader bytes out rate in the broker.
     *                                  The leader bytes out rate comes from bytesOutPerSec metrics at the topic level.
     * @param brokerFollowerBytesInRate Follower bytes in rate in the broker.
     * @param partitionBytesInRate      Leader bytes in rate for the partition.
     * @param partitionBytesOutRate     Total bytes out rate (i.e. leader/replication bytes out) for the partition.
     * @return Estimated CPU utilization of the leader replica of the partition as a Double in [0.0,1.0], or {@code null}
     * if estimation is not possible due to inconsistency between partition and broker-level byte rates.
     */
    public static Double estimateLeaderCpuUtilPerCore(double brokerCpuUtil,
                                                      double brokerLeaderBytesInRate,
                                                      double brokerLeaderBytesOutRate,
                                                      double brokerFollowerBytesInRate,
                                                      double partitionBytesInRate,
                                                      double partitionBytesOutRate) {
        if (useLinearRegressionModel) {
            return estimateLeaderCpuUtilUsingLinearRegressionModel(partitionBytesInRate, partitionBytesOutRate);
        } else {
            if (brokerLeaderBytesInRate == 0 && brokerLeaderBytesOutRate == 0) {
                return 0.0;
            } else if (brokerLeaderBytesInRate * ALLOWED_METRIC_ERROR_FACTOR < partitionBytesInRate
                    && brokerLeaderBytesInRate > UNSTABLE_METRIC_THROUGHPUT_THRESHOLD) {
                LOG.error("Partition bytes in rate {} is greater than broker bytes in rate {}.",
                        partitionBytesInRate, brokerLeaderBytesInRate);
                return null;
            } else if (brokerLeaderBytesOutRate * ALLOWED_METRIC_ERROR_FACTOR < partitionBytesOutRate
                    && brokerLeaderBytesOutRate > UNSTABLE_METRIC_THROUGHPUT_THRESHOLD) {
                LOG.error("Partition bytes out rate {} is greater than broker bytes out rate {}.",
                        partitionBytesOutRate, brokerLeaderBytesOutRate);
                return null;
            } else {
                double brokerLeaderBytesInContribution = ModelParameters.cpuWeightOfLeaderBytesInRate() * brokerLeaderBytesInRate;
                double brokerLeaderBytesOutContribution = ModelParameters.cpuWeightOfLeaderBytesOutRate() * brokerLeaderBytesOutRate;
                double brokerFollowerBytesInContribution = ModelParameters.cpuWeightOfFollowerBytesInRate() * brokerFollowerBytesInRate;
                double totalContribution = brokerLeaderBytesInContribution + brokerLeaderBytesOutContribution + brokerFollowerBytesInContribution;
                double brokerLeaderReplicaBytesInContributionFactor =
                        Math.min(1, brokerLeaderBytesInRate == 0 ? 0 : partitionBytesInRate / brokerLeaderBytesInRate);
                double brokerLeaderReplicaBytesOutContributionFactor =
                        Math.min(1, brokerLeaderBytesOutRate == 0 ? 0 : partitionBytesOutRate / brokerLeaderBytesOutRate);
                double leaderReplicaContribution = (brokerLeaderBytesInContribution * brokerLeaderReplicaBytesInContributionFactor)
                        + (brokerLeaderBytesOutContribution * brokerLeaderReplicaBytesOutContributionFactor);

                return (leaderReplicaContribution / totalContribution) * brokerCpuUtil;
            }
        }
    }

    /**
     * Estimate the leader CPU utilization using linear regression model.
     *
     * @param leaderBytesInRate  Leader bytes in rate.
     * @param leaderBytesOutRate Leader bytes out rate.
     * @return Estimated leader CPU utilization.
     */
    public static double estimateLeaderCpuUtilUsingLinearRegressionModel(double leaderBytesInRate,
                                                                         double leaderBytesOutRate) {
        double leaderBytesInCoefficient =
                ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_IN);
        double leaderBytesOutCoefficient =
                ModelParameters.getCoefficient(LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_OUT);
        return leaderBytesInCoefficient * leaderBytesInRate
                + leaderBytesOutCoefficient * leaderBytesOutRate;
    }

    /**
     * Get a single snapshot value that is representative for the given resource. The current algorithm uses
     * <ul>
     *   <li>the mean of the recent utilization for {@link Resource#NW_IN}, {@link Resource#NW_OUT}, and {@link Resource#CPU}</li>
     *   <li>the latest utilization for {@link Resource#DISK}.</li>
     * </ul>
     *
     * @param resource               Resource for which the expected utilization will be provided.
     * @param aggregatedMetricValues the aggregated metric values to calculate the expected utilization.
     * @return A single representative utilization value on a resource, or {@code 0} if the given aggregatedMetricValues is empty.
     */
    public static double expectedUtilizationFor(Resource resource, AggregatedMetricValues aggregatedMetricValues) {
        validateNotNull(aggregatedMetricValues, "AggregatedMetricValues cannot be null.");
        if (aggregatedMetricValues.isEmpty()) {
            return 0.0;
        }
        validateNotNull(resource, "Resource cannot be null.");
        double result = 0;
        for (MetricInfo info : KafkaMetricDef.resourceToMetricInfo(resource)) {
            MetricValues valuesForId = aggregatedMetricValues.valuesFor(info.id());
            validateNotNull(valuesForId, () -> String.format("The aggregated metric values does not contain metric %s for resource %s.",
                    info, resource.name()));
            result += resource == Resource.DISK ? valuesForId.latest() : valuesForId.avg();
        }
        return max(result, 0.0);
    }

    /**
     * Check the consistency between a Partition object and a PartitionInfo object
     *
     * @param partition     the {@link Partition} object that contains partition info
     * @param partitionInfo the {@link PartitionInfo} object that contains partition info
     * @return {@code true} if both objects contains the same partition info
     */
    public static boolean hasSameReplicasFor(Partition partition, PartitionInfo partitionInfo) {
        if (partition == null) {
            return false;
        }

        List<Replica> replicasFromPartition = partition.replicas();
        Node[] replicasFromPartitionInfo = partitionInfo.replicas();
        if (replicasFromPartition.size() != replicasFromPartitionInfo.length) {
            return false;
        }

        for (int i = 0; i < replicasFromPartitionInfo.length; i++) {
            if (replicasFromPartitionInfo[i].id() != replicasFromPartition.get(i).broker().id()) {
                return false;
            }
        }

        return true;
    }
}
