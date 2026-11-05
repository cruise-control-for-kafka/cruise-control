/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaClusterState;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Cluster;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class KafkaClusterStateRequest extends AbstractSyncRequest {
  protected Cluster _kafkaCluster;
  protected KafkaCruiseControlConfig _config;
  protected KafkaClusterStateParameters _parameters;
  protected TopicConfigProvider _topicConfigProvider;
  protected Admin _adminClient;
  protected BrokerSetResolver _brokerSetResolver;

  public KafkaClusterStateRequest() {
    super();
  }

  @Operation(operationId = "kafkaClusterState",
      summary = "Retrive the current Kafka cluster state.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Current Kafka cluster state.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/KafkaClusterState")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected KafkaClusterState handle() {
    return new KafkaClusterState(_kafkaCluster, _topicConfigProvider, _adminClient, _config, _brokerSetResolver);
  }

  @Override
  public KafkaClusterStateParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return KafkaClusterStateRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControlEndPoints cruiseControlEndPoints = getCruiseControlEndpoints();
    _kafkaCluster = cruiseControlEndPoints.asyncKafkaCruiseControl().kafkaCluster();
    _topicConfigProvider = cruiseControlEndPoints.asyncKafkaCruiseControl().topicConfigProvider();
    _config = cruiseControlEndPoints.asyncKafkaCruiseControl().config();
    _adminClient = cruiseControlEndPoints.asyncKafkaCruiseControl().adminClient();
    _brokerSetResolver = cruiseControlEndPoints.asyncKafkaCruiseControl().brokerSetResolver();
    _parameters = (KafkaClusterStateParameters) validateNotNull(configs.get(KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
