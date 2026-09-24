/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class KafkaClusterState extends AbstractCruiseControlResponse {
  @JsonResponseField
  public static final String KAFKA_BROKER_STATE = "KafkaBrokerState";
  @JsonResponseField
  public static final String KAFKA_PARTITION_STATE = "KafkaPartitionState";
  protected final Map<String, Properties> _allTopicConfigs;
  protected final Properties _clusterConfigs;
  protected final AdminClient _adminClient;
  protected final BrokerSetResolver _brokerSetResolver;
  protected Cluster _kafkaCluster;

  public KafkaClusterState(Cluster kafkaCluster,
                           TopicConfigProvider topicConfigProvider,
                           AdminClient adminClient,
                           KafkaCruiseControlConfig config,
                           BrokerSetResolver brokerSetResolver) {
    super(config);
    _kafkaCluster = kafkaCluster;
    _allTopicConfigs = topicConfigProvider.topicConfigs(_kafkaCluster.topics());
    _clusterConfigs = topicConfigProvider.clusterConfigs();
    _adminClient = adminClient;
    _brokerSetResolver = brokerSetResolver;
  }

  protected String getJsonString(CruiseControlParameters parameters) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure;
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;
    boolean isVerbose = kafkaClusterStateParams.isVerbose();
    Pattern topic = kafkaClusterStateParams.topic();
    try {
      jsonStructure = getJsonStructure(isVerbose, topic);
      jsonStructure.put(VERSION, JSON_VERSION);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to populate broker logDir state.", e);
    }
    return gson.toJson(jsonStructure);
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @param verbose {@code true} if verbose, {@code false} otherwise.
   * @param topic Regex of topic to filter partition states by, is null if no filter is to be applied
   * @return An object that can be further used to encode into JSON.
   */
  protected Map<String, Object> getJsonStructure(boolean verbose, Pattern topic)
      throws ExecutionException, InterruptedException {
    Map<String, Object> cruiseControlState = new HashMap<>();
    cruiseControlState.put(KAFKA_BROKER_STATE,
                           new ClusterBrokerState(_kafkaCluster, _adminClient, _config, _brokerSetResolver).getJsonStructure());
    cruiseControlState.put(KAFKA_PARTITION_STATE,
                           new ClusterPartitionState(verbose, topic, _kafkaCluster, _allTopicConfigs, _clusterConfigs).getJsonStructure());
    return cruiseControlState;
  }

  protected String getPlaintext(CruiseControlParameters parameters) {
    KafkaClusterStateParameters kafkaClusterStateParams = (KafkaClusterStateParameters) parameters;
    boolean isVerbose = kafkaClusterStateParams.isVerbose();
    Pattern topic = kafkaClusterStateParams.topic();
    StringBuilder sb = new StringBuilder();
    try {
      // Broker summary.
      new ClusterBrokerState(_kafkaCluster, _adminClient, _config, _brokerSetResolver).writeBrokerSummary(sb);
      // Partition summary.
      new ClusterPartitionState(isVerbose, topic, _kafkaCluster, _allTopicConfigs, _clusterConfigs).writePartitionSummary(sb, isVerbose);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to populate broker logDir state.", e);
    }
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString(parameters) : getPlaintext(parameters);
    // Discard irrelevant response.
    _kafkaCluster = null;
  }
}
