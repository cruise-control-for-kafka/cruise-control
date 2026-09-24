/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.*;


/**
 * A no-op maintenance event reader, which produces no maintenance events.
 */
public class NoopMaintenanceEventReader implements MaintenanceEventReader {
    private KafkaCruiseControl _kafkaCruiseControl;

    @Override
    public Set<MaintenanceEvent> readEvents(Duration timeout) {
        _kafkaCruiseControl.sleep(timeout.toMillis());
        return Collections.emptySet();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        if (_kafkaCruiseControl == null) {
            throw new IllegalArgumentException("Topic replication factor anomaly finder is missing " + KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        }
    }

    @Override
    public void close() {
    }
}
