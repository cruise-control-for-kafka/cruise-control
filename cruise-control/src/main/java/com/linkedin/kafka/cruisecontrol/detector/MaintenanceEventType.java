/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate the type of a maintenance event.
 * <p>
 * Currently supported maintenance types are as follows:
 * <ul>
 *  <li>{@link #ADD_BROKER}: Move replicas to added brokers for load balancing.</li>
 *  <li>{@link #REMOVE_BROKER}: Remove all replicas from the specified brokers.</li>
 *  <li>{@link #FIX_OFFLINE_REPLICAS}: Fix offline replicas by moving them to alive disks on healthy brokers.</li>
 *  <li>{@link #REBALANCE}: Rebalance the cluster load.</li>
 *  <li>{@link #DEMOTE_BROKER}: Demote the specified brokers.</li>
 *  <li>{@link #TOPIC_REPLICATION_FACTOR}: Update replication factor of topics.</li>
 * </ul>
 */
public enum MaintenanceEventType {
    // Do not change the order of enums. Append new ones to the end.
    ADD_BROKER, REMOVE_BROKER, FIX_OFFLINE_REPLICAS, REBALANCE, DEMOTE_BROKER, TOPIC_REPLICATION_FACTOR;

    private static final List<MaintenanceEventType> CACHED_VALUES = List.of(values());

    // This id helps with serialization and deserialization of event types
    byte id() {
        return (byte) ordinal();
    }

    /**
     * Retrieve the {@link MaintenanceEvent} that corresponds to the given id.
     *
     * @param id ID that corresponds to the maintenance event type.
     * @return Maintenance Event type with the given id.
     */
    public static MaintenanceEventType forId(byte id) {
        if (id < cachedValues().size()) {
            return cachedValues().get(id);
        }

        throw new IllegalArgumentException("MaintenanceEventType " + id + " does not exist.");
    }

    /**
     * Use this instead of values() because values() creates a new array each time.
     *
     * @return enumerated values in the same order as values()
     */
    public static List<MaintenanceEventType> cachedValues() {
        return CACHED_VALUES;
    }
}
