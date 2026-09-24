/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigDef;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;

import java.util.Collections;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Type.CLASS;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfigUtils.getConfiguredInstance;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED_WITH_ERROR;


/**
 * A provisioner that honors {@link ProvisionRecommendation provision recommendations} of brokers and partitions, but ignores recommendations
 * for other resources.
 * Configurations for this pluggable class:
 * <ul>
 *   <li>{@link #BROKER_PROVISIONER_CLASS_CONFIG}: A provisioner class for adding / removing brokers to / from the cluster. Users must implement
 *   their custom broker provisioner, which works in their platform (e.g. on Azure). The default value {@link #DEFAULT_BROKER_PROVISIONER_CLASS}
 *   returns a {@link ProvisionerState} that always indicates {@link ProvisionerState.State#COMPLETED} upon rightsizing.</li>
 *   <li>{@link #PARTITION_PROVISIONER_CLASS_CONFIG}: A provisioner class for adding partitions to the cluster. The default value
 *   {@link #DEFAULT_PARTITION_PROVISIONER_CLASS} is platform independent.</li>
 * </ul>
 */
public class BasicProvisioner implements Provisioner {
    public static final String BROKER_PROVISIONER_CLASS_CONFIG = "broker.provisioner.class";
    public static final Class<?> DEFAULT_BROKER_PROVISIONER_CLASS = BasicBrokerProvisioner.class;
    public static final String PARTITION_PROVISIONER_CLASS_CONFIG = "partition.provisioner.class";
    public static final Class<?> DEFAULT_PARTITION_PROVISIONER_CLASS = PartitionProvisioner.class;

    protected KafkaCruiseControl _kafkaCruiseControl;
    protected Provisioner _basicBrokerProvisioner;
    protected Provisioner _partitionProvisioner;

    @Override
    public ProvisionerState rightsize(Map<String, ProvisionRecommendation> recommendationByRecommender, RightsizeOptions rightsizeOptions) {
        validateNotNull(recommendationByRecommender, "Provision recommendations cannot be null.");
        ProvisionerState partitionProvisionerState = _partitionProvisioner.rightsize(recommendationByRecommender, rightsizeOptions);
        ProvisionerState brokerProvisionerState = _basicBrokerProvisioner.rightsize(recommendationByRecommender, rightsizeOptions);

        // Aggregate partition and broker provisioner states.
        return aggregateProvisionerState(partitionProvisionerState, brokerProvisionerState);
    }

    /**
     * Aggregate the summary of actions taken towards rightsizing partitions and brokers in the cluster.
     *
     * @param partitionProvisionerState {@link ProvisionerState} of actions taken on the cluster towards rightsizing partitions, or {@code null}
     *                                  if no actions were taken. It is expected to be in either (1) COMPLETED or (2) COMPLETED_WITH_ERROR state.
     * @param brokerProvisionerState    {@link ProvisionerState} of actions taken on the cluster towards rightsizing brokers, or {@code null}
     *                                  if no actions were taken. It is expected to be in either (1) COMPLETED or (2) COMPLETED_WITH_ERROR state.
     * @return An aggregated {@link ProvisionerState}, showing the summary of actions taken towards rightsizing partitions and brokers in the cluster.
     */
    static ProvisionerState aggregateProvisionerState(ProvisionerState partitionProvisionerState, ProvisionerState brokerProvisionerState) {
        ProvisionerState aggregatedState;
        if (partitionProvisionerState == null) {
            aggregatedState = brokerProvisionerState;
        } else if (brokerProvisionerState == null) {
            aggregatedState = partitionProvisionerState;
        } else {
            // Aggregating provision recommendations for partitions and brokers would end up either (1) COMPLETED or (2) COMPLETED_WITH_ERROR state.
            // The aggregate state would be (1) COMPLETED_WITH_ERROR if any result indicates an error, (2) COMPLETED otherwise.
            // The overall summary indicates the aggregate summary of broker and partition provision states.
            boolean isCompletedWithError = partitionProvisionerState.state() == COMPLETED_WITH_ERROR
                    || brokerProvisionerState.state() == COMPLETED_WITH_ERROR;

            String aggregateSummary = String.format("%s || %s", partitionProvisionerState.summary(), brokerProvisionerState.summary());
            aggregatedState = new ProvisionerState(isCompletedWithError ? COMPLETED_WITH_ERROR : COMPLETED, aggregateSummary);
        }

        return aggregatedState;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _kafkaCruiseControl = (KafkaCruiseControl) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG),
                () -> String.format("Missing %s when creating Provisioner",
                        KAFKA_CRUISE_CONTROL_OBJECT_CONFIG));

        // Retrieve broker provisioner class.
        Class<?> brokerProvisionerClass = retrieveProvisionerClass(configs, BROKER_PROVISIONER_CLASS_CONFIG, DEFAULT_BROKER_PROVISIONER_CLASS);
        // Retrieve partition provisioner class.
        Class<?> partitionProvisionerClass = retrieveProvisionerClass(configs, PARTITION_PROVISIONER_CLASS_CONFIG, DEFAULT_PARTITION_PROVISIONER_CLASS);

        // Configure the broker and partition provisioners.
        Map<String, Object> provisionerConfigs = Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
        _basicBrokerProvisioner = getConfiguredInstance(brokerProvisionerClass, Provisioner.class, provisionerConfigs);
        _partitionProvisioner = getConfiguredInstance(partitionProvisionerClass, Provisioner.class, provisionerConfigs);
    }

    private static Class<?> retrieveProvisionerClass(Map<String, ?> configs, String provisionerClassConfig, Class<?> defaultProvisionerClass) {
        String provisionerClassConfigValue = (String) configs.get(provisionerClassConfig);
        Class<?> provisionerClass;
        if (provisionerClassConfigValue == null) {
            provisionerClass = defaultProvisionerClass;
        } else {
            provisionerClass = (Class<?>) ConfigDef.parseType(provisionerClassConfig, provisionerClassConfigValue, CLASS);

            if (provisionerClass == null || !Provisioner.class.isAssignableFrom(provisionerClass)) {
                throw new IllegalArgumentException(String.format("Invalid %s is provided to basic provisioner, provided %s",
                        provisionerClassConfig, provisionerClass));
            }
        }

        return provisionerClass;
    }

}
