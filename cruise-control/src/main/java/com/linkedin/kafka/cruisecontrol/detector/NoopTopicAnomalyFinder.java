/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A no-op topic anomaly analyzer.
 */
public class NoopTopicAnomalyFinder implements TopicAnomalyFinder {
    @Override
    public Set<TopicAnomaly> topicAnomalies() {
        return Collections.emptySet();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
