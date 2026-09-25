/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;

import java.util.Collections;
import java.util.List;


/**
 * A class to indicate how an anomaly is handled.
 */
public class AnomalyState {
    private final Anomaly _anomaly;
    private final long _detectionMs;
    private long _statusUpdateMs;
    private Status _status;

    public AnomalyState(Anomaly anomaly) {
        _anomaly = anomaly;
        _detectionMs = System.currentTimeMillis();
        _statusUpdateMs = _detectionMs;
        _status = Status.DETECTED;
    }

    /**
     * @return The status of the anomaly.
     */
    public Status status() {
        return _status;
    }

    /**
     * @return The id of the anomaly.
     */
    public String anomalyId() {
        return _anomaly.anomalyId();
    }

    /**
     * @return The anomaly.
     */
    public Anomaly anomaly() {
        return _anomaly;
    }

    /**
     * @return The detection time of the anomaly in milliseconds.
     */
    public long detectionMs() {
        return _detectionMs;
    }

    /**
     * @return The status update time of the anomaly in milliseconds.
     */
    public long statusUpdateMs() {
        return _statusUpdateMs;
    }

    /**
     * Set the status of the anomaly.
     *
     * @param status The new status of the anomaly.
     */
    public void setStatus(Status status) {
        _status = status;
        _statusUpdateMs = System.currentTimeMillis();
    }

    public enum Status {
        DETECTED, IGNORED, FIX_STARTED, FIX_FAILED_TO_START, CHECK_WITH_DELAY, LOAD_MONITOR_NOT_READY, COMPLETENESS_NOT_READY;

        private static final List<Status> CACHED_VALUES = List.of(values());

        /**
         * Use this instead of values() because values() creates a new array each time.
         *
         * @return enumerated values in the same order as values()
         */
        public static List<Status> cachedValues() {
            return Collections.unmodifiableList(CACHED_VALUES);
        }
    }
}
