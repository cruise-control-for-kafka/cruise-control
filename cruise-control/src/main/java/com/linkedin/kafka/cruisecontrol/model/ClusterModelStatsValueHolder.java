/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@JsonResponseClass
@JsonResponseExternalFields(Resource.class)
public class ClusterModelStatsValueHolder {
    @JsonResponseField
    protected static final String POTENTIAL_NW_OUT = "potentialNwOut";
    @JsonResponseField
    protected static final String LEADER_REPLICAS = "leaderReplicas";
    @JsonResponseField
    protected static final String TOPIC_REPLICAS = "topicReplicas";
    @JsonResponseField
    protected static final String REPLICAS = "replicas";
    protected final Map<Resource, Double> _resourceUtilizationStats;
    protected final Double _potentialNwOutUtilizationStats;
    protected final Number _replicaStats;
    protected final Number _leaderReplicaStats;
    protected final Number _topicReplicaStats;

    public ClusterModelStatsValueHolder(Map<Resource, Double> resourceUtilizationStats,
                                        Double potentialNwOutUtilizationStats,
                                        Number replicaStats,
                                        Number leaderReplicaStats,
                                        Number topicReplicaStats) {
        _resourceUtilizationStats = resourceUtilizationStats;
        _potentialNwOutUtilizationStats = potentialNwOutUtilizationStats;
        _replicaStats = replicaStats;
        _leaderReplicaStats = leaderReplicaStats;
        _topicReplicaStats = topicReplicaStats;
    }

    protected Map<String, Object> getJsonStructure() {
        List<Resource> cachedResources = Resource.cachedValues();
        Map<String, Object> resourceMap = new HashMap<>();
        for (Resource resource : cachedResources) {
            resourceMap.put(resource.resource(), _resourceUtilizationStats.get(resource));
        }
        resourceMap.put(POTENTIAL_NW_OUT, _potentialNwOutUtilizationStats);
        resourceMap.put(REPLICAS, _replicaStats);
        resourceMap.put(LEADER_REPLICAS, _leaderReplicaStats);
        resourceMap.put(TOPIC_REPLICAS, _topicReplicaStats);
        return resourceMap;
    }

    @Override
    public String toString() {
        return String.format("%s:%12.3f %s:%s %s:%s %s:%s}%n",
                POTENTIAL_NW_OUT, _potentialNwOutUtilizationStats,
                REPLICAS, _replicaStats,
                LEADER_REPLICAS, _leaderReplicaStats,
                TOPIC_REPLICAS, _topicReplicaStats);
    }
}
