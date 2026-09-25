/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A container class to hold generation information.
 */
public class ModelGeneration implements Serializable {
    @Serial
    private static final long serialVersionUID = 1494955635123L;
    // TODO: Make acceptable staleness lags configurable.
    public static final int CLUSTER_GENERATION_ACCEPTABLE_STALENESS_LAG = 1;
    public static final int LOAD_GENERATION_ACCEPTABLE_STALENESS_LAG = 4;

    private final int _clusterGeneration;
    private final long _loadGeneration;
    private int _hash = 0;

    public ModelGeneration(int clusterGeneration, long loadGeneration) {
        _clusterGeneration = clusterGeneration;
        _loadGeneration = loadGeneration;
    }

    public int clusterGeneration() {
        return _clusterGeneration;
    }

    public long loadGeneration() {
        return _loadGeneration;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ModelGeneration)) {
            return false;
        }

        AtomicReference<ModelGeneration> other = new AtomicReference<>((ModelGeneration) o);
        return _clusterGeneration == other.get().clusterGeneration() && _loadGeneration == other.get().loadGeneration();
    }

    /**
     * Check whether this model generation is stale. A model generation is stale if it is behind the latest model
     * generation more than the acceptable staleness limit.
     *
     * @param latestModelGeneration The model generation to compare against this model generation for staleness check.
     * @return {@code true} if this model generation is beyond the acceptable staleness limits, {@code false} otherwise.
     */
    public boolean isStale(ModelGeneration latestModelGeneration) {
        return latestModelGeneration.clusterGeneration() - _clusterGeneration > CLUSTER_GENERATION_ACCEPTABLE_STALENESS_LAG
                || latestModelGeneration.loadGeneration() - _loadGeneration > LOAD_GENERATION_ACCEPTABLE_STALENESS_LAG;
    }

    @Override
    public int hashCode() {
        if (_hash != 0) {
            return _hash;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + _clusterGeneration;
        result = prime * result + (int) _loadGeneration;
        this._hash = result;
        return result;
    }

    @Override
    public String toString() {
        return String.format("[ClusterGeneration=%d,LoadGeneration=%d]", _clusterGeneration, _loadGeneration);
    }
}
