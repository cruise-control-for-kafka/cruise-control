/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;


/**
 * An abstract class to extract the common logic of single resource provisioners -- e.g. broker provisioner, partition provisioner.
 */
public abstract class AbstractSingleResourceProvisioner implements Provisioner {
    protected KafkaCruiseControl _kafkaCruiseControl;

    @Override
    public synchronized ProvisionerState rightsize(Map<String, ProvisionRecommendation> recommendationByRecommender, RightsizeOptions options) {
        validateNotNull(recommendationByRecommender, "Provision recommendations cannot be null.");

        // 1. Retrieve filtered recommendations.
        Map<String, ProvisionRecommendation> recommendations = filteredRecommendations(recommendationByRecommender);
        if (!canRightsize(recommendations)) {
            // No actions will be taken towards rightsizing.
            return null;
        }

        // 2. Execute the relevant recommendations (e.g. aggregated, all, or a subset of them) and return the result of the execution.
        return executeFor(recommendations);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _kafkaCruiseControl = (KafkaCruiseControl) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG),
                () -> String.format("Missing %s when creating Provisioner",
                        KAFKA_CRUISE_CONTROL_OBJECT_CONFIG));
    }

    /**
     * Create a new subset map of the given recommendation-by-recommender map containing only the filtered entries.
     * A single resource provisioner can filter entries based on its own resource of interest -- e.g. broker, partition, rack, disk.
     * If the given map has no matching entries to the filter, then the response would be an empty map.
     *
     * @param recommendationByRecommender Provision recommendations provided by corresponding recommenders.
     * @return A new subset map of the given recommendation-by-recommender map containing entries filtered based on Provisioner's interest.
     */
    protected abstract Map<String, ProvisionRecommendation> filteredRecommendations(Map<String, ProvisionRecommendation> recommendationByRecommender);

    /**
     * Check whether the provisioner can take actions towards rightsizing using the given filtered recommendations.
     *
     * @param filteredRecommendations Filtered recommendations by recommender -- cannot be {@code null}.
     * @return {@code true} if the provisioner can take actions towards rightsizing, {@code false} otherwise.
     */
    protected abstract boolean canRightsize(Map<String, ProvisionRecommendation> filteredRecommendations);

    /**
     * Execute the relevant recommendations to add or remove resources. Depending on the specific provisioner implementations, relevant
     * recommendations could include all, a subset of, or an aggregated set of the filtered recommendations.
     *
     * @param filteredRecommendations Filtered recommendations by recommender -- cannot be {@code null}.
     * @return The aggregated {@link ProvisionerState} of the provisioning actions, or {@code null} if no result is provided to aggregate.
     */
    protected abstract ProvisionerState executeFor(Map<String, ProvisionRecommendation> filteredRecommendations);
}
