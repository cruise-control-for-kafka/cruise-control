/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.getBaseJsonString;

@Schema(requiredProperties = {"version"})
@JsonResponseClass(versioned = true)
@JsonResponseExternalFields(ResponseUtils.class)
public class TrainResult extends AbstractCruiseControlResponse {

  public TrainResult(KafkaCruiseControlConfig config) {
    super(config);
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    String message = String.format("Load model training started. Check status through the %s endpoint", CruiseControlEndPoint.STATE);
    _cachedResponse = parameters.json() ? getBaseJsonString(message) : message;
  }
}
