/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.nio.ByteBuffer;

import org.apache.kafka.common.utils.Crc32C;


/**
 * An abstract class for all maintenance plans.
 */
public abstract class MaintenancePlan {
    protected final MaintenanceEventType _maintenanceEventType;
    protected final long _timeMs;
    protected final int _brokerId;
    protected final byte _planVersion;

    public MaintenancePlan(MaintenanceEventType maintenanceEventType, long timeMs, int brokerId, byte planVersion) {
        _maintenanceEventType = maintenanceEventType;
        _timeMs = timeMs;
        _brokerId = brokerId;
        _planVersion = planVersion;
    }

    /**
     * Retrieve the maintenance event type of this maintenance plan.
     *
     * @return the maintenance event type of this maintenance plan, which is stored in the serialized metrics so
     * that the deserializer will know which class should be used to deserialize the data.
     */
    public MaintenanceEventType maintenanceEventType() {
        return _maintenanceEventType;
    }

    /**
     * @return The timestamp in ms that corresponds to the generation of this plan.
     */
    public long timeMs() {
        return _timeMs;
    }

    /**
     * @return The id of the broker that reported this maintenance event.
     */
    public int brokerId() {
        return _brokerId;
    }

    /**
     * @return The plan version.
     */
    public byte planVersion() {
        return _planVersion;
    }

    /**
     * The content size for buffer is calculated as follows:
     * <ul>
     *   <li>{@link Byte#BYTES} - maintenance event type id.</li>
     *   <li>{@link Byte#BYTES} - plan version.</li>
     *   <li>{@link Long#BYTES} - timeMs.</li>
     *   <li>{@link Integer#BYTES} - broker id.</li>
     *   <li></li>
     * </ul>
     *
     * @return CRC of the content
     */
    protected long getCrc() {
        int contentSize = (Byte.BYTES
                + Byte.BYTES
                + Long.BYTES
                + Integer.BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(contentSize);
        buffer.put(maintenanceEventType().id());
        buffer.put(planVersion());
        buffer.putLong(timeMs());
        buffer.putInt(brokerId());
        // The CRC covers all data to the end of the buffer.
        return Crc32C.compute(buffer, -buffer.position(), contentSize);
    }

    @Override
    public String toString() {
        return String.format("[%s] Source [timeMs: %d, broker: %d]", _maintenanceEventType, _timeMs, _brokerId);
    }
}
