/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewBoardParameters;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_BOARD_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class ReviewBoardRequest extends AbstractSyncRequest {
  protected Purgatory _purgatory;
  protected ReviewBoardParameters _parameters;

  public ReviewBoardRequest() {
    super();
  }

  @Operation(operationId = "reviewBoard",
      summary = "Check review status of Cruise Control requests.",
      responses = {@ApiResponse(responseCode = "200",
      description = "Successful review response.",
      content = {@Content(mediaType = "application/json",
      schema = @Schema(ref = "#/components/schemas/ReviewResult")),
      @Content(mediaType = "text/plain",
      schema = @Schema(type = "string"))})})
  @Override
  protected ReviewResult handle() {
    return _purgatory.reviewBoard(_parameters.reviewIds());
  }

  @Override
  public ReviewBoardParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ReviewBoardRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _purgatory = getCruiseControlEndpoints().purgatory();
    _parameters = (ReviewBoardParameters) validateNotNull(configs.get(REVIEW_BOARD_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
