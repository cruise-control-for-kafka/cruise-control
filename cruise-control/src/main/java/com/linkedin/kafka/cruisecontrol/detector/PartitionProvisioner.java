/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.NewTopic;

import static com.linkedin.kafka.cruisecontrol.common.Utils.getTopicNamesMatchedWithPattern;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED_WITH_ERROR;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerUtils.increasePartitionCount;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerUtils.partitionRecommendations;


/**
 * A provisioner that honors {@link ProvisionRecommendation provision recommendations} of partitions and ignores recommendations for other resources.
 */
public class PartitionProvisioner extends AbstractSingleResourceProvisioner {

    protected Map<String, ProvisionRecommendation> filteredRecommendations(Map<String, ProvisionRecommendation> recommendationByRecommender) {
        return partitionRecommendations(recommendationByRecommender);
    }

    /**
     * Check whether the provisioner can take actions towards rightsizing using the given partition recommendations.
     * Provisioner can rightsize only if there is at least one partition recommendation to rightsize.
     *
     * @param partitionRecommendations Partition recommendations by recommender -- cannot be {@code null}.
     * @return {@code true} if the provisioner can take actions towards rightsizing, {@code false} otherwise.
     */
    protected boolean canRightsize(Map<String, ProvisionRecommendation> partitionRecommendations) {
        return !partitionRecommendations.isEmpty();
    }

    /**
     * Execute the given partition recommendations to add partitions. Then aggregate and return the result of applying partition recommendations.
     *
     * @param partitionRecommendations Partition recommendations by recommender for which the provision actions will be applied.
     * @return The aggregated {@link ProvisionerState} of the provisioning actions, or {@code null} if no result is provided to aggregate.
     */
    protected ProvisionerState executeFor(Map<String, ProvisionRecommendation> partitionRecommendations) {
        // 1. Apply the partition recommendations.
        Map<String, ProvisionerState> provisionerStateByRecommender = applyPartitionRecommendations(partitionRecommendations);

        // 2. Aggregate and return the result of applying partition recommendations.
        return aggregateProvisionerStates(provisionerStateByRecommender);
    }

    /**
     * Applies the provision actions for the given partition recommendations.
     *
     * @param partitionRecommendations Partition recommendations by recommender for which the provision actions will be applied.
     * @return {@link ProvisionerState} by recommender, indicating the result of applying the recommended provision actions.
     */
    protected Map<String, ProvisionerState> applyPartitionRecommendations(Map<String, ProvisionRecommendation> partitionRecommendations) {
        Map<String, ProvisionerState> provisionerStateByRecommender = new HashMap<>();
        for (Map.Entry<String, ProvisionRecommendation> entry : partitionRecommendations.entrySet()) {
            provisionerStateByRecommender.put(entry.getKey(), provisionPartition(entry.getValue()));
        }
        return provisionerStateByRecommender;
    }

    /**
     * Use the information from the given recommendation to provision partitions for each topic matching the given topic pattern to ensure
     * that each topic has the given number of partitions.
     *
     * @param recommendation A provision recommendation of partitions.
     * @return The {@link ProvisionerState} indicating the result of applying the recommended provision actions.
     */
    protected ProvisionerState provisionPartition(ProvisionRecommendation recommendation) {
        Set<String> topicNames = getTopicNamesMatchedWithPattern(recommendation.topicPattern(), () -> _kafkaCruiseControl.kafkaCluster().topics());
        Set<NewTopic> topicsToAddPartitions = new HashSet<>();
        for (String topicName : topicNames) {
            topicsToAddPartitions.add(new NewTopic(topicName, Optional.of(recommendation.numPartitions()), Optional.empty()));
        }

        return increasePartitionCount(_kafkaCruiseControl.adminClient(), topicsToAddPartitions);
    }

    /**
     * Aggregate the given results of applying partition recommendations provided by the corresponding recommender.
     *
     * @param provisionerStateByRecommender Results of applying partition recommendations provided by the corresponding recommender.
     * @return The aggregated {@link ProvisionerState} of the provisioning actions, or {@code null} if no result is provided to aggregate.
     */
    protected static ProvisionerState aggregateProvisionerStates(Map<String, ProvisionerState> provisionerStateByRecommender) {
        if (provisionerStateByRecommender.isEmpty()) {
            return null;
        }

        // Applying a provision recommendation for partitions would end up either (1) COMPLETED or (2) COMPLETED_WITH_ERROR state.
        // The aggregate state would be (1) COMPLETED_WITH_ERROR if any result indicates an error, (2) COMPLETED otherwise.
        // The overall summary indicates the aggregate summary by each recommender.
        boolean hasAnyResultCompletedWithError = false;
        StringBuilder aggregateSummary = new StringBuilder();
        for (Map.Entry<String, ProvisionerState> entry : provisionerStateByRecommender.entrySet()) {
            if (entry.getValue().state() == COMPLETED_WITH_ERROR) {
                hasAnyResultCompletedWithError = true;
            }
            aggregateSummary.append(String.format("[%s] %s ", entry.getKey(), entry.getValue().summary()));
        }

        return new ProvisionerState(hasAnyResultCompletedWithError ? COMPLETED_WITH_ERROR : COMPLETED, aggregateSummary.toString().trim());
    }
}
