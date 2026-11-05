/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.DemoteBrokerRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class DemoteRequest extends AbstractAsyncRequest {
  protected DemoteBrokerParameters _parameters;

  public DemoteRequest() {
    super();
  }

  @Operation(operationId = "demoteBroker",
      summary = "Demote specified brokers.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful demote brokers response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/OptimizationResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))}),
      @ApiResponse(responseCode = "202",
      description = "Demote brokers in progress.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ProgressResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Demote");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new DemoteBrokerRunnable(_asyncKafkaCruiseControl, future, uuid, _parameters));
    return future;
  }

  @Override
  public DemoteBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return DemoteRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (DemoteBrokerParameters) validateNotNull(configs.get(DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
