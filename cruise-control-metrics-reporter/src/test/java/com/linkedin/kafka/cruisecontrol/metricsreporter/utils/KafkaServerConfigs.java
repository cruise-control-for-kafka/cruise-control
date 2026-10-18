/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

/**
 * Defines Kafka broker configuration keys to avoid relying on internal Kafka classes.
 * This reduces coupling to Kafka's internal APIs and improves long-term maintainability.
 */
public final class KafkaServerConfigs {
    public static final String AUTO_CREATE_TOPICS_ENABLE_CONFIG = "auto.create.topics.enable";
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG = "offsets.topic.replication.factor";
    public static final String DEFAULT_REPLICATION_FACTOR_CONFIG = "default.replication.factor";
    public static final String NUM_PARTITIONS_CONFIG = "num.partitions";
    public static final String METADATA_LOG_DIR_CONFIG = "metadata.log.dir";
    public static final String LOG_FLUSH_INTERVAL_MESSAGES_CONFIG = "log.flush.interval.messages";

    private KafkaServerConfigs() { }
}

