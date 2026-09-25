/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;


/**
 * A plan to fix offline replicas in the cluster.
 */
public class FixOfflineReplicasPlan extends MaintenancePlan {
    public static final byte LATEST_SUPPORTED_VERSION = 0;

    public FixOfflineReplicasPlan(long timeMs, int brokerId) {
        super(MaintenanceEventType.FIX_OFFLINE_REPLICAS, timeMs, brokerId, LATEST_SUPPORTED_VERSION);
    }
}
