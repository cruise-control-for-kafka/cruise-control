/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TrainParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.TrainResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TRAIN_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class TrainRequest extends AbstractSyncRequest {
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected TrainParameters _parameters;

  public TrainRequest() {
    super();
  }

  @Operation(operationId = "train",
      summary = "Train Cruise Control to better model broker cpu usage.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful train response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/TrainResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected TrainResult handle() {
    _kafkaCruiseControl.train(_parameters.startMs(), _parameters.endMs());
    return new TrainResult(_kafkaCruiseControl.config());
  }

  @Override
  public TrainParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return TrainRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
    _parameters = (TrainParameters) validateNotNull(configs.get(TRAIN_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
