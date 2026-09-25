/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate if an anomaly detector is ready for initiating a detection.
 *
 * <ul>
 *   <li>{@link #READY}: Ready to initiate anomaly detection.</li>
 *   <li>{@link #SKIP_HAS_OFFLINE_REPLICAS}: Offline replicas in the cluster prevent anomaly detection.</li>
 *   <li>{@link #SKIP_LOAD_MONITOR_NOT_READY}: Load monitor is not ready.</li>
 *   <li>{@link #SKIP_EXECUTOR_NOT_READY}: An execution is in progress, starting, or stopping.</li>
 *   <li>{@link #SKIP_MODEL_GENERATION_NOT_CHANGED}: Cluster model generation has not changed since the last check.</li>
 *   <li>{@link #SKIP_HAS_DEAD_BROKERS}: Dead brokers in the cluster prevent anomaly detection.</li>
 * </ul>
 */
public enum AnomalyDetectionStatus {
    READY, SKIP_HAS_OFFLINE_REPLICAS, SKIP_LOAD_MONITOR_NOT_READY, SKIP_EXECUTOR_NOT_READY, SKIP_MODEL_GENERATION_NOT_CHANGED,
    SKIP_HAS_DEAD_BROKERS;

    private static final List<AnomalyDetectionStatus> CACHED_VALUES = List.of(values());

    /**
     * Use this instead of values() because values() creates a new array each time.
     *
     * @return enumerated values in the same order as values()
     */
    public static List<AnomalyDetectionStatus> cachedValues() {
        return CACHED_VALUES;
    }
}
