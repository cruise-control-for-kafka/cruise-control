/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerUtils.brokerRecommendations;


/**
 * A provisioner that honors {@link ProvisionRecommendation provision recommendations} of brokers and ignores recommendations for other resources.
 * <p>
 * It is basic, because
 * <ul>
 *   <li>It ignores the potential optional constraints in {@link ProvisionRecommendation provision recommendations}, including:
 *   typical broker id, its capacity, and resource, as well as total capacity</li>
 *   <li>It takes no action towards rightsizing if all {@link ProvisionRecommendation provision recommendations} set excluded rack ids</li>
 * </ul>
 * <p>
 * As a result, this provisioner may
 * <ul>
 *   <li>add/remove more/fewer brokers in clusters that consist of brokers not sharing the same capacity for disk, cpu, and network</li>
 *   <li>fail to handle recommendations that require rack knowledge to properly add or remove brokers</li>
 * </ul>
 *
 */
public class BasicBrokerProvisioner extends AbstractSingleResourceProvisioner {
    private static final Logger LOG = LoggerFactory.getLogger(BasicBrokerProvisioner.class);

    protected Map<String, ProvisionRecommendation> filteredRecommendations(Map<String, ProvisionRecommendation> recommendationByRecommender) {
        return brokerRecommendations(recommendationByRecommender);
    }

    /**
     * Check whether the provisioner can take actions towards rightsizing using the given broker recommendations.
     * Provisioner can rightsize only if at least one of the given recommendations does not specify excluded rack ids.
     *
     * @param brokerRecommendations Broker recommendations by recommender -- cannot be {@code null}.
     * @return {@code true} if the provisioner can take actions towards rightsizing, {@code false} otherwise.
     */
    protected boolean canRightsize(Map<String, ProvisionRecommendation> brokerRecommendations) {
        boolean canRightSize = false;
        for (ProvisionRecommendation recommendation : brokerRecommendations.values()) {
            if (recommendation.excludedRackIds() == null) {
                canRightSize = true;
                break;
            }
        }

        return canRightSize;
    }

    /**
     * Determine the broker recommendation to execute using the given broker recommendations. Then execute this recommendation to add/remove brokers.
     * The determination of the broker recommendation to execute is based on picking the recommendation with the
     * <ul>
     *   <li>maximum number of brokers to add for {@link ProvisionStatus#UNDER_PROVISIONED} recommendations.</li>
     *   <li>minimum number of brokers to remove for {@link ProvisionStatus#OVER_PROVISIONED} recommendations.</li>
     * </ul>
     *
     * @param brokerRecommendations Broker recommendations by recommender based on which the broker recommendation to execute will be determined.
     * @return A {@link ProvisionerState}, indicating the result of executing the broker recommendation to add or remove brokers.
     */
    protected ProvisionerState executeFor(Map<String, ProvisionRecommendation> brokerRecommendations) {
        if (brokerRecommendations.isEmpty()) {
            return null;
        }

        // 1. Identify the recommender for the recommendation to execute.
        String recommender = null;
        int numBrokersToAddOrRemove = (brokerRecommendations.values().iterator().next().status() == ProvisionStatus.UNDER_PROVISIONED)
                ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        for (Map.Entry<String, ProvisionRecommendation> recommendationEntry : brokerRecommendations.entrySet()) {
            int recommendedNumBrokers = recommendationEntry.getValue().numBrokers();
            if (((recommendationEntry.getValue().status() == ProvisionStatus.UNDER_PROVISIONED) ? (recommendedNumBrokers > numBrokersToAddOrRemove)
                    : (recommendedNumBrokers < numBrokersToAddOrRemove))) {

                numBrokersToAddOrRemove = recommendedNumBrokers;
                recommender = recommendationEntry.getKey();
            }
        }

        // 2. Execute the recommendation to add or remove brokers.
        ProvisionRecommendation recommendationToExecute = brokerRecommendations.get(recommender);
        LOG.info("Executing the provision recommendation: [{}] {}.", recommender, recommendationToExecute);
        return addOrRemoveBrokers(recommendationToExecute);
    }

    /**
     * Overrides of this function are expected to be non-blocking -- i.e. starts the rightsizing, but does not block until the completion.
     * Add or remove brokers to satisfy the given recommendation. Any override of this function should:
     * <ul>
     *   <li>Add brokers to the cluster if {@link ProvisionRecommendation#status()} of the {@code brokerRecommendationToExecute} equals
     *   {@link ProvisionStatus#UNDER_PROVISIONED}, remove brokers otherwise.</li>
     *   <li>Use {@link ProvisionRecommendation#numBrokers()} to determine the number of brokers to add or remove.</li>
     *   <li>(Optional) If supported by the underlying resource provisioning system and if specified in the given recommendation (i.e. has a value
     *   other than {@link ProvisionRecommendation#DEFAULT_OPTIONAL_INT}, {@link ProvisionRecommendation#DEFAULT_OPTIONAL_DOUBLE}, and {@code null}),
     *   use typical broker id, its capacity, and resource, as well as total capacity in provisioning.</li>
     *   <li>Return a provisioner state with {@link ProvisionerState.State#COMPLETED_WITH_ERROR} if there is already an ongoing provisioning
     *   (i.e. broker addition/removal) in the cluster.</li>
     *   <li>Return a provisioner state with {@link ProvisionerState.State#COMPLETED} if the provisioning has been initiated -- i.e. start the
     *   rightsizing, but does not block until the completion. Hence, the response should never have {@link ProvisionerState.State#IN_PROGRESS}</li>
     * </ul>
     *
     * @param brokerRecommendationToExecute Broker recommendation to execute -- cannot be {@code null}.
     * @return A {@link ProvisionerState}, indicating the result of executing the broker recommendation to add or remove brokers.
     */
    protected ProvisionerState addOrRemoveBrokers(ProvisionRecommendation brokerRecommendationToExecute) {
        // To automatically add / remove brokers to / from the cluster (i.e. ), this method must be overridden.
        return new ProvisionerState(COMPLETED, String.format("Provisioner support is missing. Skip recommendation: %s", brokerRecommendationToExecute));
    }
}
