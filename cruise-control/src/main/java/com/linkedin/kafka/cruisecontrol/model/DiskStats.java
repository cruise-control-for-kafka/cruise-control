/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.Map;


/**
 * A helper class to store statistics about the {@link Disk}.
 */
@JsonResponseClass
public class DiskStats {
    private final int _numLeaderReplicas;
    private final int _numReplicas;
    // For dead disk, its utilization will be null.
    private final Double _utilization;
    private final double _capacity;
    @JsonResponseField
    private static final String DISK_MB = "DiskMB";
    @JsonResponseField
    private static final String DISK_PCT = "DiskPct";
    @JsonResponseField
    private static final String NUM_LEADER_REPLICAS = "NumLeaderReplicas";
    @JsonResponseField
    private static final String NUM_REPLICAS = "NumReplicas";
    private static final String DEAD_STATE = "DEAD";

    DiskStats(int numLeaderReplicas, int numReplicas, double utilization, double capacity) {
        _numLeaderReplicas = numLeaderReplicas;
        _numReplicas = numReplicas;
        _utilization = capacity > 0 ? utilization : null;
        _capacity = capacity;
    }

    public int numLeaderReplicas() {
        return _numLeaderReplicas;
    }

    public int numReplicas() {
        return _numReplicas;
    }

    public Double utilization() {
        return _utilization;
    }

    public double capacity() {
        return _capacity;
    }

    public Double utilizationPercentage() {
        return _utilization == null ? null : _utilization * 100.0 / _capacity;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        return Map.of(DISK_MB, _utilization == null ? DEAD_STATE : _utilization, DISK_PCT, _utilization == null ? DEAD_STATE : utilizationPercentage(),
                NUM_LEADER_REPLICAS, _numLeaderReplicas, NUM_REPLICAS, _numReplicas);
    }
}
