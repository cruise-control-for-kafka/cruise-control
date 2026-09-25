/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.SortedSet;


/**
 * A plan to add brokers.
 * <p>
 * The desired brokers to add are indicated using a set of broker ids.
 */
public class AddBrokerPlan extends MaintenancePlanWithBrokers {
    public static final byte LATEST_SUPPORTED_VERSION = 0;

    public AddBrokerPlan(long timeMs, int brokerId, SortedSet<Integer> brokers) {
        super(MaintenanceEventType.ADD_BROKER, timeMs, brokerId, LATEST_SUPPORTED_VERSION, brokers);
    }
}
