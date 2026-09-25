/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

/**
 * The result of an anomaly notification.
 */
public final class AnomalyNotificationResult {
    public enum Action {
        IGNORE, FIX, CHECK
    }

    private final Action _action;
    private final long _delay;

    private AnomalyNotificationResult(Action action, long delay) {
        if (action == Action.IGNORE && delay > 0) {
            throw new IllegalArgumentException("The ignore action should not have a delay.");
        }
        _action = action;
        _delay = delay;
    }

    /**
     * @return A notification result to indicate ignoring the anomaly.
     */
    public static AnomalyNotificationResult ignore() {
        return new AnomalyNotificationResult(Action.IGNORE, -1L);
    }

    /**
     * @return A notification result to indicate fixing the anomaly.
     */
    public static AnomalyNotificationResult fix() {
        return new AnomalyNotificationResult(Action.FIX, -1L);
    }

    /**
     * @param delayMs Delay in milliseconds.
     * @return A notification result to indicate checking the anomaly at a later time.
     */
    public static AnomalyNotificationResult check(long delayMs) {
        return new AnomalyNotificationResult(Action.CHECK, delayMs);
    }

    /**
     * @return The action associated with the anomaly.
     */
    public Action action() {
        return _action;
    }

    /**
     * @return The delay associated with the anomaly. Valid only if the action is {@link Action#CHECK}.
     */
    public long delay() {
        return _delay;
    }

    @Override
    public String toString() {
        return "{" + _action + "," + _delay + "}";
    }
}
