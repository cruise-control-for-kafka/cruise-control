/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.describeClusterConfigs;


/**
 * The Kafka topic config provider implementation based on using the Kafka Admin Client for topic- and cluster-level configurations.
 */
public class KafkaAdminTopicConfigProvider implements TopicConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminTopicConfigProvider.class);
    // TODO: Make this configurable.
    public static final Duration DESCRIBE_CLUSTER_CONFIGS_TIMEOUT = Duration.ofSeconds(90);
    protected Properties _clusterConfigs;
    protected AdminClient _adminClient;

    /**
     * To reduce admin client requests, this method provides the cluster-level configs cached during the configuration time. So, if cluster-level
     * configs change over time, which is a rare event, Cruise Control instance should be bounced to retrieve the latest cluster-level configs.
     *
     * @return Cluster-level configs that applies to a topic if no topic-level config exist for it, or {@code null} if retrieval timed out.
     */
    @Override
    public Properties clusterConfigs() {
        return _clusterConfigs;
    }

    /**
     * Fetches the configuration for the requested topic. If an error is encountered the details will be logged and an
     * empty Properties instance will be returned.
     *
     * @param topic Topic name for which the topic-level configurations are required.
     * @return Properties instance containing the topic configuration.
     */
    @Override
    public Properties topicConfigs(String topic) {
        Config topicConfig = null;
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try {
            LOG.debug("Requesting details for topic '{}'", topic);
            topicConfig = _adminClient
                    .describeConfigs(Collections.singletonList(topicResource))
                    .all()
                    .get()
                    .get(topicResource);
        } catch (ExecutionException ee) {
            if (Errors.REQUEST_TIMED_OUT.exception().getClass() == ee.getCause().getClass()) {
                LOG.warn("Failed to retrieve configuration for topic '{}' due to describeConfigs request time out. Check for Kafka-side issues"
                        + " and consider increasing the configured timeout.", topic);
            } else {
                // e.g. could be UnknownTopicOrPartitionException due to topic deletion or InvalidTopicException
                LOG.warn("Cannot retrieve configuration for topic '{}'.", topic, ee);
            }
        } catch (InterruptedException ie) {
            LOG.debug("Interrupted while getting configuration for topic '{}'.", topic, ie);
        }

        if (topicConfig != null) {
            return convertConfigToProperties(topicConfig);
        } else {
            LOG.warn("The configuration for topic '{}' could not be retrieved, returning empty Properties instance.", topic);
            return new Properties();
        }
    }

    /**
     * Fetches the configuration for the requested topics. If an error is encountered, for each topic, the details will be
     * logged and the entry for that topic will be omitted from the returned map.
     *
     * @param topics The set of topic names for which the topic-level configurations are required.
     * @return A Map from topic name string to Properties instance containing that topic's configuration.
     */
    @Override
    public Map<String, Properties> topicConfigs(Set<String> topics) {

        Map<ConfigResource, KafkaFuture<Config>> topicConfigs;
        topicConfigs = _adminClient.describeConfigs(
                topics.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toList())
        ).values();

        Map<String, Properties> propsMap = new HashMap<>();
        if (topicConfigs != null) {
            for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : topicConfigs.entrySet()) {
                try {
                    Config config = entry.getValue().get();
                    propsMap.put(entry.getKey().name(), convertConfigToProperties(config));
                } catch (ExecutionException ee) {
                    if (Errors.REQUEST_TIMED_OUT.exception().getClass() == ee.getCause().getClass()) {
                        LOG.warn("Failed to retrieve config for topics due to describeConfigs request timing out. "
                                + "Check for Kafka-side issues and consider increasing the configured timeout.");
                        // If one has timed out then they all will so abort the loop.
                        break;
                    } else {
                        // e.g. could be UnknownTopicOrPartitionException due to topic deletion or InvalidTopicException
                        LOG.debug("Cannot retrieve config for topic {}.", entry.getKey().name(), ee);
                    }
                } catch (InterruptedException ie) {
                    LOG.debug("Interrupted while getting config for topic {}.", entry.getKey().name(), ie);
                }
            }
        }

        return propsMap;
    }

    /**
     * Fetches the configuration for all the topics on the Kafka cluster. If an error is encountered when retrieving the
     * topic names then the error details will be logged and an empty Map instance will be returned.
     *
     * @return A Map from topic name string to Properties instance containing that topic's configuration.
     */
    @Override
    public Map<String, Properties> allTopicConfigs() {

        // Request a map of futures for the config of each topic on the Kafka cluster
        LOG.debug("Requesting configurations for all topics");
        Set<String> topicNames = null;
        try {
            topicNames = _adminClient.listTopics().names().get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Unable to obtain list of all topic names from the Kafka Cluster");
        }

        if (topicNames == null) {
            return Collections.emptyMap();
        } else {
            return topicConfigs(topicNames);
        }
    }

    protected static Properties convertConfigToProperties(Config config) {
        Properties props = new Properties();
        for (ConfigEntry entry : config.entries()) {
            if (entry.name() == null || entry.value() == null) {
                continue;
            }
            props.put(entry.name(), entry.value());
        }
        return props;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _adminClient = (AdminClient) validateNotNull(
                configs.get(LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG),
                () -> String.format("Missing %s when creating Kafka Admin Client based Topic Config Provider",
                        LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG));
        Config clusterConfigs;
        try {
            clusterConfigs = describeClusterConfigs(_adminClient, DESCRIBE_CLUSTER_CONFIGS_TIMEOUT);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to describe Kafka cluster configs.", e);
        }

        if (clusterConfigs != null) {
            _clusterConfigs = convertConfigToProperties(clusterConfigs);
        } else {
            LOG.warn("Cluster configuration could not be retrieved, using empty Properties instance.");
            _clusterConfigs = new Properties();
        }
    }

    @Override
    public void close() {
        //no-op
    }
}
