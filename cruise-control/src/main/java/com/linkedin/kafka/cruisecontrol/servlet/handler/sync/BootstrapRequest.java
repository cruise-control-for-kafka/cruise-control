/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.BootstrapResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BOOTSTRAP_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class BootstrapRequest extends AbstractSyncRequest {
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected BootstrapParameters _parameters;

  public BootstrapRequest() {
    super();
  }

  @Operation(operationId = "bootstrap",
      summary = "Bootstrap Cruise Control.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful admin response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/BootstrapResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected BootstrapResult handle() {
    Long startMs = _parameters.startMs();
    Long endMs = _parameters.endMs();
    boolean clearMetrics = _parameters.clearMetrics();
    if (_parameters.developerMode()) {
      // This endpoint is used only for development purposes in developer_mode=true.
      _kafkaCruiseControl.bootstrap(startMs, endMs, clearMetrics);
    }

    return new BootstrapResult(_kafkaCruiseControl.config());
  }

  @Override
  public BootstrapParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return BootstrapRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
    _parameters = (BootstrapParameters) validateNotNull(configs.get(BOOTSTRAP_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
