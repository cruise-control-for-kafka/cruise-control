/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.GetStateRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STATE_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class CruiseControlStateRequest extends AbstractAsyncRequest {
  protected CruiseControlStateParameters _parameters;

  public CruiseControlStateRequest() {
    super();
  }

  @Operation(operationId = "state",
      summary = "Get the state of cruise control.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful state response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/CruiseControlState")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))}),
      @ApiResponse(responseCode = "202",
      description = "State querying in progress.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ProgressResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Get state");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new GetStateRunnable(_asyncKafkaCruiseControl, future, _parameters));
    return future;
  }

  @Override
  public CruiseControlStateParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return CruiseControlStateRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (CruiseControlStateParameters) validateNotNull(configs.get(STATE_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
