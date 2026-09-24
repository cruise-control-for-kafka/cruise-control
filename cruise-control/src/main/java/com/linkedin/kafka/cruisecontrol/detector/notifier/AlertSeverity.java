/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

/**
 * Alert severity representation based on standard wording
 */
public enum AlertSeverity {
    CRITICAL("critical"), MAJOR("major"), MINOR("minor"), WARNING("warning");

    private final String _value;

    AlertSeverity(String value) {
        this._value = value;
    }

    @Override
    public String toString() {
        return _value;
    }
}
