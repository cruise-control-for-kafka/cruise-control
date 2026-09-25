/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.ProvisionerState;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus.UNDER_PROVISIONED;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


@JsonResponseClass
public class RightsizeResult extends AbstractCruiseControlResponse {
  @JsonResponseField(required = false)
  protected static final String NUM_BROKERS_TO_ADD = "numBrokersToAdd";
  @JsonResponseField(required = false)
  protected static final String PARTITION_COUNT = "partitionCount";
  @JsonResponseField(required = false)
  protected static final String TOPIC = "topic";
  @JsonResponseField
  protected static final String PROVISIONER_STATE = "provisionerState";
  private static final String NO_ACTION = "No actions were taken on the cluster towards rightsizing it";

  protected final int _numBrokersToAdd;
  protected final int _partitionCount;
  protected Pattern _topic;
  protected String _provisionerState;

  /**
   * Note: {@link RightsizeResult} can only be constructed with a {@link ProvisionRecommendation}, whose status is
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus#UNDER_PROVISIONED under-provisioned}.
   *
   * @param recommendation Provision recommendation that was used in rightsizing.
   * @param provisionerState {@link ProvisionerState} of actions taken on the cluster towards rightsizing or {@code null} if no actions were taken.
   * @param config The configurations for Cruise Control.
   */
  public RightsizeResult(ProvisionRecommendation recommendation, ProvisionerState provisionerState, KafkaCruiseControlConfig config) {
    super(config);

    if (validateNotNull(recommendation, "Provision recommendation cannot be null.").status() != UNDER_PROVISIONED) {
      throw new IllegalArgumentException(String.format("Cannot construct a RightsizeResult with the provision recommendation [%s], because "
                                                       + "its status is not %s.", recommendation, UNDER_PROVISIONED));
    }
    _numBrokersToAdd = recommendation.numBrokers();
    _partitionCount = recommendation.numPartitions();
    _topic = recommendation.topicPattern();
    _provisionerState = provisionerState == null ? NO_ACTION : provisionerState.toString();
  }

  protected String getJsonString() {
    Map<String, Object> jsonStructure = new HashMap<>();
    if (_numBrokersToAdd != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      jsonStructure.put(NUM_BROKERS_TO_ADD, _numBrokersToAdd);
    }
    if (_partitionCount != ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      jsonStructure.put(PARTITION_COUNT, _partitionCount);
    }
    if (_topic != null && !_topic.pattern().isEmpty()) {
      jsonStructure.put(TOPIC, _topic.pattern());
    }
    jsonStructure.put(PROVISIONER_STATE, _provisionerState);
    jsonStructure.put(VERSION, JSON_VERSION);
    Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    return gson.toJson(jsonStructure);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    _cachedResponse = getJsonString();
    // Discard irrelevant response.
    _topic = null;
    _provisionerState = null;
  }
}
