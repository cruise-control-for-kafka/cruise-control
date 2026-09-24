/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;

import java.util.Set;
import java.util.function.Function;

/**
 * A factory class of replica sort functions. It is always preferred to use the functions in this factory instead
 * of writing ad-hoc functions.
 */
public final class ReplicaSortFunctionFactory {
    // Priority functions
    /**
     * Prioritize the immigrant replicas
     */
    private static final Function<Replica, Integer> PRIORITIZE_IMMIGRANTS = r -> r.originalBroker() != r.broker() ? 0 : 1;
    /**
     * Prioritize the offline replicas
     */
    private static final Function<Replica, Integer> PRIORITIZE_OFFLINE_REPLICAS = r -> r.isCurrentOffline() ? 0 : 1;
    /**
     * Prioritize the disk immigrant replicas
     */
    private static final Function<Replica, Integer> PRIORITIZE_DISK_IMMIGRANTS = r -> r.originalDisk() != r.disk() ? 0 : 1;

    // Selection functions
    /**
     * Select leaders only
     */
    private static final Function<Replica, Boolean> SELECT_LEADERS = Replica::isLeader;
    /**
     * Select followers only
     */
    private static final Function<Replica, Boolean> SELECT_FOLLOWERS = r -> !r.isLeader();
    /**
     * Select online replicas only
     */
    private static final Function<Replica, Boolean> SELECT_ONLINE_REPLICAS = r -> !r.isCurrentOffline();
    /**
     * Select offline replicas only
     */
    private static final Function<Replica, Boolean> SELECT_OFFLINE_REPLICAS = Replica::isCurrentOffline;
    /**
     * Select immigrants only
     */
    private static final Function<Replica, Boolean> SELECT_IMMIGRANTS = r -> r.originalBroker() != r.broker();
    /**
     * Select immigrant or offline replicas only
     */
    private static final Function<Replica, Boolean> SELECT_IMMIGRANT_OR_OFFLINE_REPLICAS = r -> r.originalBroker() != r.broker()
            || r.isCurrentOffline();

    private ReplicaSortFunctionFactory() {
    }

    // Score functions

    /**
     * @param metricGroup the metric group to score
     * @return A score function to score by the metric group value of the given metric group in positive way, i.e. the higher
     * the metric group value, the higher the score.
     */
    public static Function<Replica, Double> sortByMetricGroupValue(String metricGroup) {
        return r -> {
            MetricValues metricValues = r.load()
                    .loadByWindows()
                    .valuesForGroup(metricGroup, KafkaMetricDef.commonMetricDef(), true);
            return (double) metricValues.avg();
        };
    }

    /**
     * @param metricGroup the metric group to score
     * @return A score function to score by the metric group value of the given metric group in negative way, i.e. the higher
     * the metric group value, the lower the score.
     */
    public static Function<Replica, Double> reverseSortByMetricGroupValue(String metricGroup) {
        return r -> {
            MetricValues metricValues = r.load()
                    .loadByWindows()
                    .valuesForGroup(metricGroup, KafkaMetricDef.commonMetricDef(), true);
            return -(double) metricValues.avg();
        };
    }

    // Priority functions

    /**
     * @return A priority function that prioritize the immigrants replicas.
     */
    public static Function<Replica, Integer> prioritizeImmigrants() {
        return PRIORITIZE_IMMIGRANTS;
    }

    /**
     * @return A priority function that prioritize the offline replicas.
     */
    public static Function<Replica, Integer> prioritizeOfflineReplicas() {
        return PRIORITIZE_OFFLINE_REPLICAS;
    }

    /**
     * @return A priority function that prioritize the immigrant replicas to the disk.
     */
    public static Function<Replica, Integer> prioritizeDiskImmigrants() {
        return PRIORITIZE_DISK_IMMIGRANTS;
    }

    // Selection functions

    /**
     * @return A selection function that only includes immigrant replicas.
     */
    public static Function<Replica, Boolean> selectImmigrants() {
        return SELECT_IMMIGRANTS;
    }

    /**
     * @return A selection function that only includes immigrant replicas and offline replicas.
     */
    public static Function<Replica, Boolean> selectImmigrantOrOfflineReplicas() {
        return SELECT_IMMIGRANT_OR_OFFLINE_REPLICAS;
    }

    /**
     * @return A selection function that only includes leaders.
     */
    public static Function<Replica, Boolean> selectLeaders() {
        return SELECT_LEADERS;
    }

    /**
     * @return A selection function that only includes followers.
     */
    public static Function<Replica, Boolean> selectFollowers() {
        return SELECT_FOLLOWERS;
    }

    /**
     * @return A selection function that only includes offline replicas.
     */
    public static Function<Replica, Boolean> selectOfflineReplicas() {
        return SELECT_OFFLINE_REPLICAS;
    }

    /**
     * @return A selection function that only includes online replicas.
     */
    public static Function<Replica, Boolean> selectOnlineReplicas() {
        return SELECT_ONLINE_REPLICAS;
    }

    /**
     * @param excludedTopics Topics excluded from partition movements.
     * @return A selection function that filters out replicas which are online and from topics which should be excluded.
     */
    public static Function<Replica, Boolean> selectReplicasBasedOnExcludedTopics(Set<String> excludedTopics) {
        return r -> r.isOriginalOffline() || !excludedTopics.contains(r.topicPartition().topic());
    }

    /**
     * @param includedTopics Topics included for partition movements.
     * @return A selection function that filters out replicas which are not from topics which should be included.
     */
    public static Function<Replica, Boolean> selectReplicasBasedOnIncludedTopics(Set<String> includedTopics) {
        return r -> includedTopics.contains(r.topicPartition().topic());
    }

    /**
     * @param resource The resource to check.
     * @param limit    The resource limit used to filter replicas.
     * @return A selection function that only includes replicas whose metric value for certain resource is above limit.
     */
    public static Function<Replica, Boolean> selectReplicasAboveLimit(Resource resource, double limit) {
        return r -> r.load().expectedUtilizationFor(resource) > limit;
    }

    /**
     * @param resource The resource to check.
     * @param limit    The resource limit used to filter replicas.
     * @return A selection function that only includes replicas whose metric value for certain resource is below limit.
     */
    public static Function<Replica, Boolean> selectReplicasBelowLimit(Resource resource, double limit) {
        return r -> r.load().expectedUtilizationFor(resource) < limit;
    }
}
