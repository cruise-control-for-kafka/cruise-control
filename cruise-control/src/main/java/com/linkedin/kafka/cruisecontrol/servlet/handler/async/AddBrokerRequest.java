/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.AddBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ADD_BROKER_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class AddBrokerRequest extends AbstractAsyncRequest {
  protected AddBrokerParameters _parameters;

  public AddBrokerRequest() {
    super();
  }

  @Operation(operationId = "addBroker",
      summary = "Move partitions to the specified brokers, according to the specified goals.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful add brokers response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/OptimizationResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))}),
      @ApiResponse(responseCode = "202",
      description = "Add brokers in progress.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ProgressResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Add brokers");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new AddBrokersRunnable(_asyncKafkaCruiseControl, future, _parameters, uuid));
    return future;
  }

  @Override
  public AddBrokerParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return AddBrokerRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (AddBrokerParameters) validateNotNull(configs.get(ADD_BROKER_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
