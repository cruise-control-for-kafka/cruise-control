/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.StopProposalResult;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FORCE_STOP_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class StopProposalRequest extends AbstractSyncRequest {
  private static final Logger LOG = LoggerFactory.getLogger(StopProposalRequest.class);
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected StopProposalParameters _parameters;

  public StopProposalRequest() {
    super();
  }

  @Operation(operationId = "stopProposalExecution",
      summary = "Stop ongoing execution in Cruise Control.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful admin response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/StopProposalResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected StopProposalResult handle() {
    if (_parameters.forceExecutionStop()) {
      LOG.info("{} is a deprecated parameter. Force stopping an execution is not supported.", FORCE_STOP_PARAM);
    }
    _kafkaCruiseControl.userTriggeredStopExecution(_parameters.stopExternalAgent());
    return new StopProposalResult(_kafkaCruiseControl.config());
  }

  @Override
  public StopProposalParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return StopProposalRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
    _parameters = (StopProposalParameters) validateNotNull(configs.get(STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
