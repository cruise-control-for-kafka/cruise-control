/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;


/**
 * A plan to rebalance the cluster.
 */
public class RebalancePlan extends MaintenancePlan {
    public static final byte LATEST_SUPPORTED_VERSION = 0;

    public RebalancePlan(long timeMs, int brokerId) {
        super(MaintenanceEventType.REBALANCE, timeMs, brokerId, LATEST_SUPPORTED_VERSION);
    }
}
