/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResumeSamplingResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PAUSE_RESUME_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class ResumeRequest extends AbstractSyncRequest {
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected PauseResumeParameters _parameters;

  public ResumeRequest() {
    super();
  }

  @Operation(operationId = "resumeSampling",
      summary = "Resume metric sampling.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful execution response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ResumeSamplingResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected ResumeSamplingResult handle() {
    _kafkaCruiseControl.resumeMetricSampling(_parameters.reason());
    return new ResumeSamplingResult(_kafkaCruiseControl.config());
  }

  @Override
  public PauseResumeParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ResumeRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
    _parameters = (PauseResumeParameters) validateNotNull(configs.get(PAUSE_RESUME_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
