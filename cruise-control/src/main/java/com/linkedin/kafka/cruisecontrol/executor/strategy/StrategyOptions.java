/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.Cluster;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * A class to indicate options intended to be used during application of a {@link ReplicaMovementStrategy}.
 */
public final class StrategyOptions {
    private final Cluster _cluster;
    private final Map<String, MinIsrWithTime> _minIsrWithTimeByTopic;

    public static class Builder {
        // Required parameters
        private final Cluster _cluster;
        // Optional parameters - initialized to default values
        private Map<String, MinIsrWithTime> _minIsrWithTimeByTopic = Collections.emptyMap();

        public Builder(Cluster cluster) {
            validateNotNull(cluster, "The cluster cannot be null.");
            _cluster = cluster;
        }

        /**
         * (Optional) Set value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
         *
         * @param minIsrWithTimeByTopic Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} / topic.
         * @return this builder.
         */
        public Builder minIsrWithTimeByTopic(Map<String, MinIsrWithTime> minIsrWithTimeByTopic) {
            validateNotNull(minIsrWithTimeByTopic, "The minIsrWithTimeByTopic cannot be null.");
            _minIsrWithTimeByTopic = minIsrWithTimeByTopic;
            return this;
        }

        public StrategyOptions build() {
            return new StrategyOptions(this);
        }
    }

    private StrategyOptions(Builder builder) {
        _cluster = builder._cluster;
        _minIsrWithTimeByTopic = builder._minIsrWithTimeByTopic;
    }

    public Cluster cluster() {
        return _cluster;
    }

    public Map<String, MinIsrWithTime> minIsrWithTimeByTopic() {
        return Collections.unmodifiableMap(_minIsrWithTimeByTopic);
    }
}
