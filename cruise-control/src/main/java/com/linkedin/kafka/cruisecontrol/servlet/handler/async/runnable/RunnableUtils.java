/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.WaitingForOngoingExecutionToStop;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.NO_TASK_IN_PROGRESS;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getRackHandleNull;


public final class RunnableUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlUtils.class);
    // Common self healing constants used in runnables.
    public static final boolean SELF_HEALING_DRYRUN = false;
    public static final Set<Integer> SELF_HEALING_DESTINATION_BROKER_IDS = Collections.emptySet();
    public static final ReplicaMovementStrategy SELF_HEALING_REPLICA_MOVEMENT_STRATEGY = null;
    public static final Pattern SELF_HEALING_EXCLUDED_TOPICS = null;
    public static final Integer SELF_HEALING_CONCURRENT_MOVEMENTS = null;
    public static final Long SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS = null;
    public static final boolean SELF_HEALING_SKIP_HARD_GOAL_CHECK = false;
    public static final ModelCompletenessRequirements SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS = null;
    public static final boolean SELF_HEALING_SKIP_URP_DEMOTION = true;
    public static final boolean SELF_HEALING_EXCLUDE_FOLLOWER_DEMOTION = true;
    public static final boolean SELF_HEALING_IS_TRIGGERED_BY_USER_REQUEST = false;
    public static final boolean SELF_HEALING_FAST_MODE = true;
    private static final Set<String> KAFKA_ASSIGNER_GOALS =
            Set.of(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(), KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName());

    private RunnableUtils() {
    }

    /**
     * Populate cluster rack information for topics to change replication factor. In the process this method also conducts a sanity
     * check to ensure that there are enough brokers and racks in the cluster to allocate new replicas to racks which do not host
     * replica of the same partition.
     *
     * @param topicsByReplicationFactor   Topics to change replication factor by target replication factor.
     * @param cluster                     Current cluster state.
     * @param skipTopicRackAwarenessCheck Whether to skip the rack awareness sanity check or not.
     * @param brokersByRack               Mapping from rack to broker.
     * @param rackByBroker                Mapping from broker to rack.
     */
    public static void populateRackInfoForReplicationFactorChange(Map<Short, Set<String>> topicsByReplicationFactor,
                                                                  Cluster cluster,
                                                                  boolean skipTopicRackAwarenessCheck,
                                                                  Map<String, List<Integer>> brokersByRack,
                                                                  Map<Integer, String> rackByBroker) {
        for (Node node : cluster.nodes()) {
            String rack = getRackHandleNull(node);
            brokersByRack.putIfAbsent(rack, new ArrayList<>());
            brokersByRack.get(rack).add(node.id());
            rackByBroker.put(node.id(), rack);
        }

        topicsByReplicationFactor.forEach((replicationFactor, topics) -> {
            if (replicationFactor > rackByBroker.size()) {
                throw new RuntimeException(String.format("Unable to change replication factor (RF) of topics %s to %d since there are only %d "
                                + "alive brokers in the cluster. Requested RF cannot be more than number of alive brokers.",
                        topics, replicationFactor, rackByBroker.size()));
            } else if (replicationFactor > brokersByRack.size()) {
                if (skipTopicRackAwarenessCheck) {
                    LOG.info("Target replication factor for topics {} is {}, which is larger than the number of racks in cluster. Hence, the same rack"
                            + " will contain more than one replicas from the same partition.", topics, replicationFactor);
                } else {
                    throw new RuntimeException(String.format("Unable to change replication factor of topics %s to %d since there are only %d "
                                    + "racks in the cluster, to skip the rack-awareness check, set %s to true in the request.",
                            topics, replicationFactor, brokersByRack.size(), ParameterUtils.SKIP_RACK_AWARENESS_CHECK_PARAM));
                }
            }
        });
    }

    /**
     * Check that only one target replication factor is set for each topic.
     *
     * @param topicsToChangeByReplicationFactor Topics to change replication factor by target replication factor.
     */
    private static void sanityCheckTargetReplicationFactorForTopic(Map<Short, Set<String>> topicsToChangeByReplicationFactor) {
        Set<String> topicsToChange = new HashSet<>();
        Set<String> topicsHavingMultipleTargetReplicationFactors = new HashSet<>();
        for (Set<String> topics : topicsToChangeByReplicationFactor.values()) {
            for (String topic : topics) {
                if (!topicsToChange.add(topic)) {
                    topicsHavingMultipleTargetReplicationFactors.add(topic);
                }
            }
        }
        if (!topicsHavingMultipleTargetReplicationFactors.isEmpty()) {
            throw new IllegalStateException(String.format("Topics %s are requested with more than one target replication factor.",
                    topicsHavingMultipleTargetReplicationFactors));
        }
    }

    /**
     * Populate topics to change replication factor based on the request and current cluster state.
     *
     * @param topicPatternByReplicationFactor Requested topic patterns to change replication factor by target replication factor.
     * @param cluster                         Current cluster state.
     * @return Topics to change replication factor by target replication factor.
     */
    public static Map<Short, Set<String>> topicsForReplicationFactorChange(Map<Short, Pattern> topicPatternByReplicationFactor,
                                                                           Cluster cluster) {
        Map<Short, Set<String>> topicsToChangeByReplicationFactor = new HashMap<>();
        for (Map.Entry<Short, Pattern> entry : topicPatternByReplicationFactor.entrySet()) {
            short replicationFactor = entry.getKey();
            Pattern topicPattern = entry.getValue();
            Set<String> topics = cluster.topics().stream().filter(t -> topicPattern.matcher(t).matches()).collect(Collectors.toSet());
            // Ensure there are topics matching the requested topic pattern.
            if (topics.isEmpty()) {
                throw new IllegalStateException(String.format("There is no topic in cluster matching pattern '%s'.", topicPattern));
            }
            Set<String> topicsToChange = topics.stream()
                    .filter(t -> cluster.partitionsForTopic(t).stream().anyMatch(p -> p.replicas().length != replicationFactor))
                    .collect(Collectors.toSet());
            if (!topicsToChange.isEmpty()) {
                topicsToChangeByReplicationFactor.put(replicationFactor, topicsToChange);
            }
        }

        if (topicsToChangeByReplicationFactor.isEmpty()) {
            throw new IllegalStateException(String.format("All topics matching given pattern already have target replication factor. Requested "
                    + "topic pattern by replication factor: %s.", topicPatternByReplicationFactor));
        }
        // Sanity check that no topic is set with more than one target replication factor.
        sanityCheckTargetReplicationFactorForTopic(topicsToChangeByReplicationFactor);
        return topicsToChangeByReplicationFactor;
    }

    /**
     * Check if the ClusterAndGeneration needs to be refreshed to retrieve the requested substates.
     *
     * @param substates Substates for which the need for refreshing the ClusterAndGeneration will be evaluated.
     * @return {@code true} if substates contain {@link CruiseControlState.SubState#ANALYZER} or
     * {@link CruiseControlState.SubState#MONITOR}, {@code false} otherwise.
     */
    public static boolean shouldRefreshClusterAndGeneration(Set<CruiseControlState.SubState> substates) {
        return substates.stream()
                .anyMatch(substate -> substate == CruiseControlState.SubState.ANALYZER
                        || substate == CruiseControlState.SubState.MONITOR);
    }

    /**
     * Check partitions in the given cluster for existence of offline replicas, and get the first such partition.
     * If there no such partitions are identified, return {@code null}.
     *
     * @param cluster The current cluster state.
     * @return The first identified partition with offline replicas, {@code null} if no such partition exists.
     */
    public static PartitionInfo partitionWithOfflineReplicas(Cluster cluster) {
        for (String topic : cluster.topics()) {
            for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
                if (partitionInfo.offlineReplicas().length > 0) {
                    return partitionInfo;
                }
            }
        }
        return null;
    }

    /**
     * Check whether any of the given goals contain a Kafka Assigner goal.
     *
     * @param goals The goals to check
     * @return {@code true} if the given goals contain a Kafka Assigner goal, {@code false} otherwise.
     */
    public static boolean isKafkaAssignerMode(Collection<String> goals) {
        return goals.stream().anyMatch(KAFKA_ASSIGNER_GOALS::contains);
    }

    /**
     * Sanity check to ensure that the given cluster model has no offline replicas on bad disks in Kafka Assigner mode.
     *
     * @param goals        Goals to check whether it is Kafka Assigner mode or not.
     * @param clusterModel Cluster model for which the existence of an offline replicas on bad disks will be verified.
     */
    public static void sanityCheckBrokersHavingOfflineReplicasOnBadDisks(List<String> goals, ClusterModel clusterModel) {
        if (isKafkaAssignerMode(goals) && !clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
            throw new IllegalStateException("Kafka Assigner mode is not supported when there are offline replicas on bad disks."
                    + " Please run fix_offline_replicas before using Kafka Assigner mode.");
        }
    }

    /**
     * Gracefully stop the ongoing execution (if any) and wait until the execution stops. Exceptional cases:
     * <ul>
     *   <li>If there is no ongoing Cruise Control execution, this call is a noop.</li>
     *   <li>If another request has asked for modifying the ongoing execution, then throw an exception.</li>
     * </ul>
     * <p>
     * The caller of this method should be aware that the following scenario is possible and acceptable:
     * <ol>
     *   <li>A user request calls this function to stop the ongoing execution.</li>
     *   <li>This function stops the ongoing execution -- i.e. the execution state now is
     *   {@link com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State#NO_TASK_IN_PROGRESS}.</li>
     *   <li>Before the caller of this function would start a new execution, another user request (or self-healing)
     *   can start a new execution.</li>
     * </ol>
     *
     * @param kafkaCruiseControl The Kafka Cruise Control instance.
     * @param operationProgress  The progress for the job.
     */
    public static void maybeStopOngoingExecutionToModifyAndWait(KafkaCruiseControl kafkaCruiseControl,
                                                                OperationProgress operationProgress) {
        if (!kafkaCruiseControl.hasOngoingExecution()) {
            LOG.info("There is already no ongoing Cruise Control execution. Skip stopping execution.");
            return;
        } else if (!kafkaCruiseControl.modifyOngoingExecution(true)) {
            throw new IllegalStateException("Another request has asked for modifying the ongoing execution.");
        }

        LOG.info("Gracefully stopping the ongoing execution...");

        WaitingForOngoingExecutionToStop step = new WaitingForOngoingExecutionToStop();
        operationProgress.addStep(step);

        while (kafkaCruiseControl.executionState() != NO_TASK_IN_PROGRESS) {
            // Stop ongoing execution, and wait for the execution to stop.
            try {
                kafkaCruiseControl.userTriggeredStopExecution(false);
                Thread.sleep(kafkaCruiseControl.executionProgressCheckIntervalMs());
            } catch (InterruptedException e) {
                kafkaCruiseControl.modifyOngoingExecution(false);
                throw new IllegalStateException("Interrupted while waiting for gracefully stopping the ongoing execution.");
            }
        }
        step.done();
    }

    /**
     * Compute optimization options, update recently removed and demoted brokers (if not dryRun) and return the computed result.
     *
     * @param clusterModel                  The state of the cluster.
     * @param isTriggeredByGoalViolation    {@code true} if proposals is triggered by goal violation, {@code false} otherwise.
     * @param kafkaCruiseControl            The Kafka Cruise Control instance.
     * @param brokersToDrop                 Brokers to drop from recently removed and demoted brokers. Modifies the actual values if not dryRun.
     * @param dryRun                        {@code true} if dryrun, {@code false} otherwise.
     * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
     * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
     * @param excludedTopicsPattern         The topics that should be excluded from the optimization action.
     * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
     *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
     * @param onlyMoveImmigrantReplicas     {@code true} to move only immigrant replicas, {@code false} otherwise.
     * @param fastMode                      {@code true} to compute proposals in fast mode, {@code false} otherwise.
     * @return Computed optimization options.
     */
    public static OptimizationOptions computeOptimizationOptions(ClusterModel clusterModel,
                                                                 boolean isTriggeredByGoalViolation,
                                                                 KafkaCruiseControl kafkaCruiseControl,
                                                                 Set<Integer> brokersToDrop,
                                                                 boolean dryRun,
                                                                 boolean excludeRecentlyDemotedBrokers,
                                                                 boolean excludeRecentlyRemovedBrokers,
                                                                 Pattern excludedTopicsPattern,
                                                                 Set<Integer> requestedDestinationBrokerIds,
                                                                 boolean onlyMoveImmigrantReplicas,
                                                                 boolean fastMode) {

        // Update recently removed and demoted brokers.
        RecentBrokers recentBrokers = maybeDropFromRecentBrokers(kafkaCruiseControl, brokersToDrop, dryRun);

        Set<Integer> excludedBrokersForLeadership = excludeRecentlyDemotedBrokers ? recentBrokers.recentlyDemotedBrokers()
                : Collections.emptySet();

        Set<Integer> excludedBrokersForReplicaMove = excludeRecentlyRemovedBrokers ? recentBrokers.recentlyRemovedBrokers()
                : Collections.emptySet();

        Set<String> excludedTopics = kafkaCruiseControl.excludedTopics(clusterModel, excludedTopicsPattern);
        LOG.debug("Topics excluded from partition movement: {}", excludedTopics);
        return new OptimizationOptions(excludedTopics, excludedBrokersForLeadership, excludedBrokersForReplicaMove,
                isTriggeredByGoalViolation, requestedDestinationBrokerIds, onlyMoveImmigrantReplicas, fastMode);
    }

    /**
     * Update recently removed and demoted brokers with the given brokers to drop if the given dryrun is false.
     *
     * @param kafkaCruiseControl The Kafka Cruise Control instance.
     * @param brokersToDrop      Brokers to drop from recently removed and demoted brokers (if exist).
     * @param dryRun             {@code true} if dryrun, {@code false} otherwise.
     * @return Recent brokers that are intended to be excluded from relevant replica and/or leadership transfer operations.
     */
    private static RecentBrokers maybeDropFromRecentBrokers(KafkaCruiseControl kafkaCruiseControl,
                                                            Set<Integer> brokersToDrop,
                                                            boolean dryRun) {
        ExecutorState executorState = kafkaCruiseControl.executorState();
        if (!dryRun) {
            kafkaCruiseControl.dropRecentBrokers(brokersToDrop, true);
            kafkaCruiseControl.dropRecentBrokers(brokersToDrop, false);
            return new RecentBrokers(executorState.recentlyRemovedBrokers(), executorState.recentlyDemotedBrokers());
        } else {
            Set<Integer> recentlyRemoved = new HashSet<>(executorState.recentlyRemovedBrokers());
            recentlyRemoved.removeAll(brokersToDrop);
            Set<Integer> recentlyDemoted = new HashSet<>(executorState.recentlyDemotedBrokers());
            recentlyDemoted.removeAll(brokersToDrop);
            return new RecentBrokers(recentlyRemoved, recentlyDemoted);
        }
    }

    /**
     * Sanity check to ensure that the given cluster model contains brokers with offline replicas.
     *
     * @param clusterModel Cluster model for which the existence of an offline replica will be verified.
     */
    public static void sanityCheckOfflineReplicaPresence(ClusterModel clusterModel) {
        if (clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
            for (Broker deadBroker : clusterModel.deadBrokers()) {
                if (!deadBroker.replicas().isEmpty()) {
                    // Has offline replica(s) on a dead broker.
                    return;
                }
            }
            throw new IllegalStateException("Cluster has no offline replica on brokers " + clusterModel.brokers() + " to fix.");
        }
        // Has offline replica(s) on a broken disk.
    }

    /**
     * A helper class to keep recently removed and demoted brokers that are intended to be excluded from relevant replica
     * and/or leadership transfer operations.
     */
    public static class RecentBrokers {
        private final Set<Integer> _recentlyRemovedBrokers;
        private final Set<Integer> _recentlyDemotedBrokers;

        public RecentBrokers(Set<Integer> recentlyRemovedBrokers, Set<Integer> recentlyDemotedBrokers) {
            _recentlyRemovedBrokers = validateNotNull(recentlyRemovedBrokers, "Attempt to set a null value for recent removed brokers.");
            _recentlyDemotedBrokers = validateNotNull(recentlyDemotedBrokers, "Attempt to set a null value for recent demoted brokers.");
        }

        public Set<Integer> recentlyRemovedBrokers() {
            return _recentlyRemovedBrokers;
        }

        public Set<Integer> recentlyDemotedBrokers() {
            return _recentlyDemotedBrokers;
        }
    }
}
