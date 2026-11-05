/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler;

import com.linkedin.cruisecontrol.servlet.handler.Request;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlRequestHandler;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager.USER_TASK_HEADER_NAME;


public abstract class AbstractRequest implements Request {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRequest.class);
  protected KafkaCruiseControlRequestHandler _requestHandler;

  /**
   * Handle the request and populate the response.
   *
   */
  @Override
  public void handle(CruiseControlRequestContext requestContext)
          throws Exception {

    if (parameters().parseParameters(requestContext)) {
      LOG.warn("Failed to parse parameters: {} for request: {}.", requestContext.getParameterMap(), requestContext.getPathInfo());
      return;
    }

    CruiseControlResponse ccResponse = getResponse(requestContext);
    ccResponse.writeSuccessResponse(parameters(), requestContext);
  }

  /**
   * Get the response of the request
   * @param requestContext the request context.
   * <ul>
   *   <li>Asynchronous requests return either the final response or the progress of the async request.</li>
   *   <li>Synchronous requests return the final response of the sync request.</li>
   * </ul>
   *
   * @return Response of the requests.
   */
  @Operation(
      parameters = @Parameter(name = USER_TASK_HEADER_NAME, in = ParameterIn.HEADER,
          description = "UUID returned by an earlier request. Send it when polling or retrieving the same "
              + "operation with the same HTTP method, endpoint, and query parameters. Unknown, "
              + "expired, or mismatched task identifiers cause an HTTP 400 response.",
          schema = @Schema(type = "string", format = "uuid")),
      responses = {
          @ApiResponse(responseCode = "2XX", headers = @Header(name = USER_TASK_HEADER_NAME,
              description = "UUID assigned to this operation. Reuse it as the User-Task-ID request header "
                  + "when polling an HTTP 202 response or retrieving the same task. This header "
                  + "is also returned for completed synchronous requests. It can be absent when "
                  + "a request is placed in the two-step verification review queue.",
              schema = @Schema(type = "string", format = "uuid"))),
          @ApiResponse(responseCode = "default", description = "Error response.",
              content = {
                  @Content(mediaType = "application/json", schema = @Schema(ref = "#/components/schemas/ErrorResponse")),
                  @Content(mediaType = "text/plain", schema = @Schema(type = "string"))
              })
      })
  protected abstract CruiseControlResponse getResponse(CruiseControlRequestContext requestContext)
          throws Exception;

  public abstract CruiseControlParameters parameters();

  @Override
  public void configure(Map<String, ?> configs) {
    _requestHandler = (KafkaCruiseControlRequestHandler) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG),
            "Cruise Control request handler configuration is missing from the request.");
  }
}
