/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaClusterStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.KafkaClusterState;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG;


public class KafkaClusterStateRequest extends AbstractSyncRequest {
    protected Cluster _kafkaCluster;
    protected KafkaCruiseControlConfig _config;
    protected KafkaClusterStateParameters _parameters;
    protected TopicConfigProvider _topicConfigProvider;
    protected AdminClient _adminClient;
    protected BrokerSetResolver _brokerSetResolver;

    public KafkaClusterStateRequest() {
        super();
    }

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
