/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.ProvisionerState;
import com.linkedin.kafka.cruisecontrol.detector.RightsizeOptions;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RightsizeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.RightsizeResult;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RIGHTSIZE_PARAMETER_OBJECT_CONFIG;


public class RightsizeRequest extends AbstractSyncRequest {
    public static final String RECOMMENDER_UP = "Recommender-Under-Provisioned";
    protected KafkaCruiseControl _kafkaCruiseControl;
    protected RightsizeParameters _parameters;

    public RightsizeRequest() {
        super();
    }

    @Override
    protected RightsizeResult handle() {
        KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
        ProvisionRecommendation recommendation = createProvisionRecommendation();
        Map<String, ProvisionRecommendation> provisionRecommendation;
        provisionRecommendation = Collections.singletonMap(RECOMMENDER_UP, recommendation);

        ProvisionerState provisionerState = _kafkaCruiseControl.provisioner().rightsize(provisionRecommendation, new RightsizeOptions());

        return new RightsizeResult(recommendation, provisionerState, config);
    }

    /**
     * Create a provision recommendation and ensure that
     * <ul>
     *   <li>exactly one resource type is set</li>
     *   <li>if the resource type is partition, then the corresponding topic must be specified; otherwise, the topic cannot be specified</li>
     *   <li>if the resource type is partition, then only one topic must be specified</li>
     * </ul>
     *
     * @return The {@link ProvisionRecommendation} to recommend for rightsizing
     * @throws UserRequestException when the sanity check fails.
     */
    private ProvisionRecommendation createProvisionRecommendation() throws UserRequestException {
        ProvisionRecommendation recommendation;

        // Validate multiple resources are not set
        if (_parameters.numBrokersToAdd() != ProvisionRecommendation.DEFAULT_OPTIONAL_INT
                && _parameters.partitionCount() == ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
            // Validate the topic cannot be specified when the resource type is not partition.
            if (_parameters.topic() != null) {
                throw new UserRequestException("When the resource type is not partition, topic cannot be specified.");
            }
            recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(_parameters.numBrokersToAdd())
                    .build();
        } else if (_parameters.numBrokersToAdd() == ProvisionRecommendation.DEFAULT_OPTIONAL_INT
                && _parameters.partitionCount() != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
            // Validate the topic pattern is not null or empty.
            Pattern topic = _parameters.topic();
            if (topic == null || topic.pattern().isEmpty()) {
                throw new UserRequestException("When the resource type is partition, a corresponding non-empty topic regex must be specified.");
            }
            recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numPartitions(_parameters.partitionCount())
                    .topicPattern(topic)
                    .build();
        } else {
            throw new UserRequestException(String.format("Exactly one resource type must be set (Brokers:%d Partitions:%d))",
                    _parameters.numBrokersToAdd(),
                    _parameters.partitionCount()));
        }
        return recommendation;
    }

    @Override
    public RightsizeParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return RightsizeRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
        _parameters = (RightsizeParameters) validateNotNull(configs.get(RIGHTSIZE_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
