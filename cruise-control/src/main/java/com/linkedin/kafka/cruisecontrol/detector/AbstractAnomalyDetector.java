/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;

import java.util.Queue;


/**
 * An abstract class to extract the common logic of anomaly detectors.
 */
public abstract class AbstractAnomalyDetector {
    protected final Queue<Anomaly> _anomalies;
    protected final KafkaCruiseControl _kafkaCruiseControl;

    public AbstractAnomalyDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
        _anomalies = anomalies;
        _kafkaCruiseControl = kafkaCruiseControl;
    }
}
