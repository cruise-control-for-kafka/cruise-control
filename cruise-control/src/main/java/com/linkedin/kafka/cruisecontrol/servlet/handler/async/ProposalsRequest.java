/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.ProposalsRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PROPOSALS_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class ProposalsRequest extends AbstractAsyncRequest {
  protected ProposalsParameters _parameters;

  public ProposalsRequest() {
    super();
  }

  @Operation(operationId = "proposals",
      summary = "Get current proposals.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful optimization proposals response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/OptimizationResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))}),
      @ApiResponse(responseCode = "202",
      description = "Optimization proposals generation in progress.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ProgressResult"))})})
  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Get customized proposals");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new ProposalsRunnable(_asyncKafkaCruiseControl, future, _parameters));
    return future;
  }

  @Override
  public ProposalsParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ProposalsRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (ProposalsParameters) validateNotNull(configs.get(PROPOSALS_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
