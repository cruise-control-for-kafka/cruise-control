/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * The interface for getting the topic configs of Kafka. Users should implement this interface so Cruise Control can
 * get relevant cluster configurations for presenting to user -- e.g. "min.insync.replicas".
 */
@InterfaceStability.Evolving
public interface TopicConfigProvider extends CruiseControlConfigurable, AutoCloseable {

    /**
     * @return Cluster-level configs that applies to a topic if no topic-level config exist for it.
     */
    Properties clusterConfigs();

    /**
     * Get topic-level configurations for the requested topic.
     *
     * @param topic Topic name for which the topic-level configurations are required.
     * @return A {@link Properties} instance containing the topic-level configuration for the requested topic.
     */
    Properties topicConfigs(String topic);

    /**
     * Get the topic-level configurations for the requested topics.
     *
     * @param topics The set of topic names for which the topic-level configurations are required.
     * @return A map from the topic name to a {@link Properties} instance containing that topic's configuration.
     */
    Map<String, Properties> topicConfigs(Set<String> topics);

    /**
     * @return Topic-level configs for all topics.
     */
    Map<String, Properties> allTopicConfigs();
}
