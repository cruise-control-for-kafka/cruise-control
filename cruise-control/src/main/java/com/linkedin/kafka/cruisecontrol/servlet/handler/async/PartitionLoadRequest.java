/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.PartitionLoadRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_LOAD_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class PartitionLoadRequest extends AbstractAsyncRequest {
  protected PartitionLoadParameters _parameters;

  public PartitionLoadRequest() {
    super();
  }

  @Operation(operationId = "partitionload",
      summary = "Get the resource load for each partition.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful partition load state response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/PartitionLoadState")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))}),
      @ApiResponse(responseCode = "202",
      description = "Partition load state fetch in progress.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ProgressResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture(String.format("Get partition load from %d to %d", _parameters.startMs(), _parameters.endMs()));
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new PartitionLoadRunnable(_asyncKafkaCruiseControl, future, _parameters));
    return future;
  }

  @Override
  public PartitionLoadParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return PartitionLoadRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (PartitionLoadParameters) validateNotNull(configs.get(PARTITION_LOAD_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
