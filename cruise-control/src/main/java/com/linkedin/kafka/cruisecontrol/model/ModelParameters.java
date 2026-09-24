/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;

import javax.annotation.Nonnull;
import java.util.Collection;


public final class ModelParameters {
    // The linear regression model parameters.
    private static final LinearRegressionModelParameters LINEAR_REGRESSION_PARAMETERS = new LinearRegressionModelParameters();

    // The static model
    /**
     * The contribution weight of leader bytes in on the CPU utilization of a broker.
     */
    private static double cpuWeightOfLeaderBytesInRate = 0.7;
    /**
     * The contribution weight of leader bytes out on the CPU utilization of a broker.
     */
    private static double cpuWeightOfLeaderBytesOutRate = 0.15;
    /**
     * The contribution weight of follower bytes in on the CPU utilization of a broker.
     */
    private static double cpuWeightOfFollowerBytesInRate = 0.15;

    private ModelParameters() {

    }

    /**
     * Initialize the model parameters.
     *
     * @param config The configurations for Cruise Control.
     */
    public static void init(KafkaCruiseControlConfig config) {
        cpuWeightOfLeaderBytesInRate = config.getDouble(MonitorConfig.LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
        cpuWeightOfLeaderBytesOutRate = config.getDouble(MonitorConfig.LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
        cpuWeightOfFollowerBytesInRate = config.getDouble(MonitorConfig.FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
        LinearRegressionModelParameters.init(config);
    }

    public static Double getCoefficient(LinearRegressionModelParameters.ModelCoefficient name) {
        return LINEAR_REGRESSION_PARAMETERS.getCoefficient(name);
    }

    public static double cpuWeightOfLeaderBytesInRate() {
        return cpuWeightOfLeaderBytesInRate;
    }

    public static double cpuWeightOfLeaderBytesOutRate() {
        return cpuWeightOfLeaderBytesOutRate;
    }

    public static double cpuWeightOfFollowerBytesInRate() {
        return cpuWeightOfFollowerBytesInRate;
    }

    public static boolean trainingCompleted() {
        return LINEAR_REGRESSION_PARAMETERS.trainingCompleted();
    }

    public static double modelCoefficientTrainingCompleteness() {
        return LINEAR_REGRESSION_PARAMETERS.modelCoefficientTrainingCompleteness();
    }

    /**
     * Trigger the calculation of the model parameters.
     *
     * @return {@code true} if the parameters are generated, otherwise false;
     */
    public static boolean updateModelCoefficient() {
        return LINEAR_REGRESSION_PARAMETERS.updateModelCoefficient();
    }

    public static void addMetricObservation(Collection<BrokerMetricSample> trainingData) {
        LINEAR_REGRESSION_PARAMETERS.addMetricObservation(trainingData);
    }

    @Nonnull
    public static LinearRegressionModelParameters.LinearRegressionModelState linearRegressionModelState() {
        return LINEAR_REGRESSION_PARAMETERS.modelState();
    }

    /**
     * An enumeration holding the different configuration combinations. CURRENT_CLUSTER refers to the current cluster's
     * configuration.
     */
    public enum ConfigSetting {
        CURRENT_CLUSTER,
        PLAINTEXT_NONE, PLAINTEXT_GZIP, PLAINTEXT_SNAPPY, PLAINTEXT_LZ4,
        SSL_NONE, SSL_GZIP, SSL_SNAPPY, SSL_LZ4
    }
}
