/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kafka.common.utils.Crc32C;


/**
 * A plan to update the replication factor of specified topics.
 * <p>
 * The desired replication factor for topics are indicated using topic regex per desired replication factor.
 */
public class TopicReplicationFactorPlan extends MaintenancePlan {
    public static final byte LATEST_SUPPORTED_VERSION = 0;
    // A map containing the regex of topics by the corresponding desired replication factor
    private final SortedMap<Short, String> _topicRegexWithRFUpdate;

    public TopicReplicationFactorPlan(long timeMs, int brokerId, SortedMap<Short, String> topicRegexWithRFUpdate) {
        super(MaintenanceEventType.TOPIC_REPLICATION_FACTOR, timeMs, brokerId, LATEST_SUPPORTED_VERSION);
        if (topicRegexWithRFUpdate == null || topicRegexWithRFUpdate.isEmpty()) {
            throw new IllegalArgumentException("Missing replication factor updates for the plan.");
        }
        // Sanity check: the number of bulk updates for replication factor of topics.
        if (topicRegexWithRFUpdate.size() > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("Cannot update more than %d different replication factor (attempt: %d).",
                    Byte.MAX_VALUE, topicRegexWithRFUpdate.size()));
        }
        // Sanity check: Each regex must have some value.
        for (String regex : topicRegexWithRFUpdate.values()) {
            if (regex == null || regex.isEmpty()) {
                throw new IllegalArgumentException("Missing topics of the replication factor update for the plan.");
            }
        }
        _topicRegexWithRFUpdate = new TreeMap<>(topicRegexWithRFUpdate);
    }

    public Map<Short, String> topicRegexWithRFUpdate() {
        return _topicRegexWithRFUpdate;
    }

    /**
     * The content size for buffer is calculated as follows:
     * <ul>
     *   <li>{@link Byte#BYTES} - maintenance event type id.</li>
     *   <li>{@link Byte#BYTES} - plan version.</li>
     *   <li>{@link Long#BYTES} - timeMs.</li>
     *   <li>{@link Integer#BYTES} - broker id.</li>
     *   <li>{@link Byte#BYTES} - number of replication factor update entries.</li>
     *   <li>The required capacity for RF entries (see below) - /* total capacity for all entries.</li>
     * </ul>
     * <p>
     * The required capacity for each RF entry is calculated as follows:
     * <ul>
     *   <li>{@link Short#BYTES} - replication factor.</li>
     *   <li>{@link Integer#BYTES} - regex length.</li>
     *   <li>The byte length of the actual regex - regex.</li>
     * </ul>
     *
     * @return CRC of the content
     */
    protected long getCrc() {
        byte numRFUpdateEntries = (byte) _topicRegexWithRFUpdate.size();
        int requiredCapacityForRFEntries = 0;
        for (Map.Entry<Short, String> entry : _topicRegexWithRFUpdate.entrySet()) {
            requiredCapacityForRFEntries += (Short.BYTES
                    + Integer.BYTES
                    + entry.getValue().getBytes(StandardCharsets.UTF_8).length);
        }
        int contentSize = (Byte.BYTES
                + Byte.BYTES
                + Long.BYTES
                + Integer.BYTES
                + Byte.BYTES
                + requiredCapacityForRFEntries);
        ByteBuffer buffer = ByteBuffer.allocate(contentSize);
        buffer.put(maintenanceEventType().id());
        buffer.put(planVersion());
        buffer.putLong(timeMs());
        buffer.putInt(brokerId());
        buffer.put(numRFUpdateEntries);
        for (Map.Entry<Short, String> entry : _topicRegexWithRFUpdate.entrySet()) {
            buffer.putShort(entry.getKey());
            byte[] regex = entry.getValue().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(regex.length);
            buffer.put(regex);
        }
        // The CRC covers all data to the end of the buffer.
        return Crc32C.compute(buffer, -buffer.position(), contentSize);
    }

    @Override
    public String toString() {
        return String.format("[%s] TopicRegexWithRFUpdate: %s, Source [timeMs: %d, broker: %d]", _maintenanceEventType,
                _topicRegexWithRFUpdate, _timeMs, _brokerId);
    }
}
