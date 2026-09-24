/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;

import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.TOPIC_ANOMALY;

/**
 * The interface for a topic anomaly.
 * Topic anomaly refers to one or more topics are under an undesired state which may jeopardize data completeness or
 * cluster balanceness (e.g. topic having small replication factor or gigantic replicas).
 */
public abstract class TopicAnomaly extends KafkaAnomaly {
    @Override
    public AnomalyType anomalyType() {
        return TOPIC_ANOMALY;
    }
}
