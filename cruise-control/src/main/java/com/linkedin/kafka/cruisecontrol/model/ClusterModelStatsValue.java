/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@JsonResponseClass
@JsonResponseExternalFields(Statistic.class)
public class ClusterModelStatsValue {
    protected final Map<Statistic, Map<Resource, Double>> _resourceUtilizationStats;
    protected final Map<Statistic, Double> _potentialNwOutUtilizationStats;
    protected final Map<Statistic, Number> _replicaStats;
    protected final Map<Statistic, Number> _leaderReplicaStats;
    protected final Map<Statistic, Number> _topicReplicaStats;

    public ClusterModelStatsValue(Map<Statistic, Map<Resource, Double>> resourceUtilizationStats,
                                  Map<Statistic, Double> potentialNwOutUtilizationStats,
                                  Map<Statistic, Number> replicaStats,
                                  Map<Statistic, Number> leaderReplicaStats,
                                  Map<Statistic, Number> topicReplicaStats) {
        _resourceUtilizationStats = resourceUtilizationStats;
        _potentialNwOutUtilizationStats = potentialNwOutUtilizationStats;
        _replicaStats = replicaStats;
        _leaderReplicaStats = leaderReplicaStats;
        _topicReplicaStats = topicReplicaStats;
    }

    protected Map<String, Object> getJsonStructure() {
        List<Statistic> cachedStatistic = Statistic.cachedValues();
        // List of all statistics AVG, MAX, MIN, STD
        Map<String, Object> allStatMap = new HashMap<>();
        for (Statistic stat : cachedStatistic) {
            allStatMap.put(stat.stat(), new ClusterModelStatsValueHolder(_resourceUtilizationStats.get(stat),
                    _potentialNwOutUtilizationStats.get(stat),
                    _replicaStats.get(stat),
                    _leaderReplicaStats.get(stat),
                    _topicReplicaStats.get(stat)).getJsonStructure());
        }
        return allStatMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Statistic stat : Statistic.cachedValues()) {
            sb.append(String.format("%s:{", stat));
            for (Resource resource : Resource.cachedValues()) {
                sb.append(String.format("%s:%12.3f ", resource, _resourceUtilizationStats.get(stat).get(resource)));
            }
            sb.append(new ClusterModelStatsValueHolder(_resourceUtilizationStats.get(stat),
                    _potentialNwOutUtilizationStats.get(stat),
                    _replicaStats.get(stat),
                    _leaderReplicaStats.get(stat),
                    _topicReplicaStats.get(stat)));
        }
        return sb.substring(0, sb.length() - 2);
    }
}
