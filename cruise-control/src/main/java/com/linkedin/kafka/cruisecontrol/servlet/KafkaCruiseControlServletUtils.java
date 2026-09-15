/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.RequestParameterWrapper;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import static com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlParametersConfig.*;
import static com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlRequestConfig.*;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.writeErrorResponse;
import static jakarta.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;


/**
 * The util class for Kafka Cruise Control servlet.
 */
public final class KafkaCruiseControlServletUtils {
  public static final String GET_METHOD = "GET";
  public static final String POST_METHOD = "POST";
  public static final String KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG = "kafka.cruise.control.request.handler.object";
  public static final String KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG = "kafka.cruise.control.http.servlet.request.object";
  public static final String KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG = "kafka.cruise.control.config.object";
  public static final String ROUTING_CONTEXT_OBJECT_CONFIG = "routing.context.object";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
  private static final String ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";
  private static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";
  private static final String ACCESS_CONTROL_MAX_AGE_IN_SEC = "1728000";
  private static final Map<EndPoint, RequestParameterWrapper> REQUEST_PARAMETER_CONFIGS;

  static {
    Map<EndPoint, RequestParameterWrapper> requestParameterConfigs = new HashMap<>();

    RequestParameterWrapper bootstrap = new RequestParameterWrapper(BOOTSTRAP_PARAMETERS_CLASS_CONFIG,
                                                                    BOOTSTRAP_PARAMETER_OBJECT_CONFIG,
                                                                    BOOTSTRAP_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper train = new RequestParameterWrapper(TRAIN_PARAMETERS_CLASS_CONFIG,
                                                                TRAIN_PARAMETER_OBJECT_CONFIG,
                                                                TRAIN_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper load = new RequestParameterWrapper(LOAD_PARAMETERS_CLASS_CONFIG,
                                                               LOAD_PARAMETER_OBJECT_CONFIG,
                                                               LOAD_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper partitionLoad = new RequestParameterWrapper(PARTITION_LOAD_PARAMETERS_CLASS_CONFIG,
                                                                        PARTITION_LOAD_PARAMETER_OBJECT_CONFIG,
                                                                        PARTITION_LOAD_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper proposals = new RequestParameterWrapper(PROPOSALS_PARAMETERS_CLASS_CONFIG,
                                                                    PROPOSALS_PARAMETER_OBJECT_CONFIG,
                                                                    PROPOSALS_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper state = new RequestParameterWrapper(STATE_PARAMETERS_CLASS_CONFIG,
                                                                STATE_PARAMETER_OBJECT_CONFIG,
                                                                STATE_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper addBroker = new RequestParameterWrapper(ADD_BROKER_PARAMETERS_CLASS_CONFIG,
                                                                    ADD_BROKER_PARAMETER_OBJECT_CONFIG,
                                                                    ADD_BROKER_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper removeBroker = new RequestParameterWrapper(REMOVE_BROKER_PARAMETERS_CLASS_CONFIG,
                                                                       REMOVE_BROKER_PARAMETER_OBJECT_CONFIG,
                                                                       REMOVE_BROKER_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper fixOfflineReplicas = new RequestParameterWrapper(FIX_OFFLINE_REPLICAS_PARAMETERS_CLASS_CONFIG,
                                                                             FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG,
                                                                             FIX_OFFLINE_REPLICAS_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper demoteBroker = new RequestParameterWrapper(DEMOTE_BROKER_PARAMETERS_CLASS_CONFIG,
                                                                       DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG,
                                                                       DEMOTE_BROKER_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper rebalance = new RequestParameterWrapper(REBALANCE_PARAMETERS_CLASS_CONFIG,
                                                                    REBALANCE_PARAMETER_OBJECT_CONFIG,
                                                                    REBALANCE_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper stopProposalExecution = new RequestParameterWrapper(STOP_PROPOSAL_PARAMETERS_CLASS_CONFIG,
                                                                                STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG,
                                                                                STOP_PROPOSAL_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper pauseSampling = new RequestParameterWrapper(PAUSE_SAMPLING_PARAMETERS_CLASS_CONFIG,
                                                                        PAUSE_RESUME_PARAMETER_OBJECT_CONFIG,
                                                                        PAUSE_SAMPLING_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper permissions = new RequestParameterWrapper(PERMISSIONS_PARAMETERS_CLASS_CONFIG,
                                                                       PERMISSIONS_PARAMETER_OBJECT_CONFIG,
                                                                        PERMISSIONS_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper resumeSampling = new RequestParameterWrapper(RESUME_SAMPLING_PARAMETERS_CLASS_CONFIG,
                                                                         PAUSE_RESUME_PARAMETER_OBJECT_CONFIG,
                                                                         RESUME_SAMPLING_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper kafkaClusterState = new RequestParameterWrapper(KAFKA_CLUSTER_STATE_PARAMETERS_CLASS_CONFIG,
                                                                            KAFKA_CLUSTER_STATE_PARAMETER_OBJECT_CONFIG,
                                                                            KAFKA_CLUSTER_STATE_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper userTasks = new RequestParameterWrapper(USER_TASKS_PARAMETERS_CLASS_CONFIG,
                                                                    USER_TASKS_PARAMETER_OBJECT_CONFIG,
                                                                    USER_TASKS_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper admin = new RequestParameterWrapper(ADMIN_PARAMETERS_CLASS_CONFIG,
                                                                ADMIN_PARAMETER_OBJECT_CONFIG,
                                                                ADMIN_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper review = new RequestParameterWrapper(REVIEW_PARAMETERS_CLASS_CONFIG,
                                                                 REVIEW_PARAMETER_OBJECT_CONFIG,
                                                                 REVIEW_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper reviewBoard = new RequestParameterWrapper(REVIEW_BOARD_PARAMETERS_CLASS_CONFIG,
                                                                      REVIEW_BOARD_PARAMETER_OBJECT_CONFIG,
                                                                      REVIEW_BOARD_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper topicConfiguration = new RequestParameterWrapper(TOPIC_CONFIGURATION_PARAMETERS_CLASS_CONFIG,
                                                                             TOPIC_CONFIGURATION_PARAMETER_OBJECT_CONFIG,
                                                                             TOPIC_CONFIGURATION_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper rightsize = new RequestParameterWrapper(RIGHTSIZE_PARAMETERS_CLASS_CONFIG,
                                                                    RIGHTSIZE_PARAMETER_OBJECT_CONFIG,
                                                                    RIGHTSIZE_REQUEST_CLASS_CONFIG);
    RequestParameterWrapper removeDisks = new RequestParameterWrapper(REMOVE_DISKS_PARAMETERS_CLASS_CONFIG,
                                                                      REMOVE_DISKS_PARAMETER_OBJECT_CONFIG,
                                                                      REMOVE_DISKS_REQUEST_CLASS_CONFIG);

    requestParameterConfigs.put(BOOTSTRAP, bootstrap);
    requestParameterConfigs.put(TRAIN, train);
    requestParameterConfigs.put(LOAD, load);
    requestParameterConfigs.put(PARTITION_LOAD, partitionLoad);
    requestParameterConfigs.put(PROPOSALS, proposals);
    requestParameterConfigs.put(STATE, state);
    requestParameterConfigs.put(ADD_BROKER, addBroker);
    requestParameterConfigs.put(REMOVE_BROKER, removeBroker);
    requestParameterConfigs.put(FIX_OFFLINE_REPLICAS, fixOfflineReplicas);
    requestParameterConfigs.put(DEMOTE_BROKER, demoteBroker);
    requestParameterConfigs.put(REBALANCE, rebalance);
    requestParameterConfigs.put(STOP_PROPOSAL_EXECUTION, stopProposalExecution);
    requestParameterConfigs.put(PAUSE_SAMPLING, pauseSampling);
    requestParameterConfigs.put(RESUME_SAMPLING, resumeSampling);
    requestParameterConfigs.put(KAFKA_CLUSTER_STATE, kafkaClusterState);
    requestParameterConfigs.put(USER_TASKS, userTasks);
    requestParameterConfigs.put(ADMIN, admin);
    requestParameterConfigs.put(REVIEW, review);
    requestParameterConfigs.put(REVIEW_BOARD, reviewBoard);
    requestParameterConfigs.put(TOPIC_CONFIGURATION, topicConfiguration);
    requestParameterConfigs.put(RIGHTSIZE, rightsize);
    requestParameterConfigs.put(PERMISSIONS, permissions);
    requestParameterConfigs.put(REMOVE_DISKS, removeDisks);

    REQUEST_PARAMETER_CONFIGS = Collections.unmodifiableMap(requestParameterConfigs);
  }

  static final String[] HEADERS_TO_TRY = {
      "X-Forwarded-For",
      "Proxy-Client-IP",
      "WL-Proxy-Client-IP",
      "HTTP_X_FORWARDED_FOR",
      "HTTP_X_FORWARDED",
      "HTTP_X_CLUSTER_CLIENT_IP",
      "HTTP_CLIENT_IP",
      "HTTP_FORWARDED_FOR",
      "HTTP_FORWARDED",
      "HTTP_VIA",
      "REMOTE_ADDR"
  };

  private KafkaCruiseControlServletUtils() {

  }

  public static RequestParameterWrapper requestParameterFor(EndPoint endpoint) {
    return REQUEST_PARAMETER_CONFIGS.get(endpoint);
  }

  /**
   * Get the ip address of the client sending the request.
   * @param requestContext the request context.
   * @return The ip address of the client sending the request.
   */
  public static String getClientIpAddress(CruiseControlRequestContext requestContext) {
    for (String header : HEADERS_TO_TRY) {
      String ip = requestContext.getHeader(header);
      if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
        return ip;
      }
    }
    return requestContext.getRemoteAddr();
  }

  static String urlEncode(String s) throws UnsupportedEncodingException {
    return s == null ? null : URLEncoder.encode(s, StandardCharsets.UTF_8.name());
  }

  /**
   * Returns the GET or POST endpoint if the request contains a valid one, otherwise (1) writes the error response to
   * the given HTTP response and (2) returns null.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @return The endpoint if the request contains a valid one, otherwise (1) writes the error response to the given HTTP
   * response and (2) returns null.
   */
  public static CruiseControlEndPoint getValidEndpoint(CruiseControlRequestContext requestContext)
      throws IOException {
    CruiseControlEndPoint endPoint = endPoint(requestContext);
    if (endPoint == null) {
      String method = requestContext.getMethod();
      String errorMessage = String.format("Unrecognized endpoint in request '%s'%nSupported %s endpoints: %s",
                                          requestContext.getPathInfo(), method, method.equals(GET_METHOD)
                                                                         ? CruiseControlEndPoint.getEndpoints()
                                                                         : CruiseControlEndPoint.postEndpoints());
      writeErrorResponse(requestContext, null, errorMessage, SC_NOT_FOUND, wantJSON(requestContext), wantResponseSchema(requestContext));
      return null;
    }
    return endPoint;
  }

  /**
   * Creates a {@link HttpServletResponse#SC_BAD_REQUEST} Http servlet response.
   * @param ure User request exception to be handled.
   * @param requestContext HTTP request received by Cruise Control.
   * @return The error message.
   */
  public static String handleUserRequestException(UserRequestException ure,
                                                  CruiseControlRequestContext requestContext)
      throws IOException {
    String errorMessage = String.format("Bad %s request '%s' due to '%s'.", requestContext.getMethod(),
            requestContext.getPathInfo(), ure.getMessage());
    writeErrorResponse(requestContext, ure, errorMessage, SC_BAD_REQUEST, wantJSON(requestContext), wantResponseSchema(requestContext));
    return errorMessage;
  }
  /**
   * Creates a {@link HttpServletResponse#SC_FORBIDDEN} Http servlet response.
   * @param ce Config exception to be handled.
   * @param requestContext HTTP request received by Cruise Control.
   * @return The error message.
   */
  public static String handleConfigException(ConfigException ce,
                                             CruiseControlRequestContext requestContext)
      throws IOException {
    String errorMessage = String.format("Cannot process %s request '%s' due to: '%s'.",
                                        requestContext.getMethod(), requestContext.getPathInfo(), ce.getMessage());
    writeErrorResponse(requestContext, ce, errorMessage, SC_FORBIDDEN, wantJSON(requestContext), wantResponseSchema(requestContext));
    return errorMessage;
  }

  /**
   * Creates a {@link HttpServletResponse#SC_INTERNAL_SERVER_ERROR} Http servlet response.
   * @param e Exception to be handled
   * @param requestContext HTTP request received by Cruise Control.
   * @return The error message.
   */
  public static String handleException(Exception e,
                                       CruiseControlRequestContext requestContext)
      throws IOException {
    String errorMessage = String.format("Error processing %s request '%s' due to: '%s'.",
                                        requestContext.getMethod(), requestContext.getPathInfo(), e.getMessage());
    writeErrorResponse(requestContext, e, errorMessage, SC_INTERNAL_SERVER_ERROR, wantJSON(requestContext), wantResponseSchema(requestContext));
    return errorMessage;
  }

  /**
   * Handle OPTIONS request for CORS applications
   *
   * https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS#Examples_of_access_control_scenarios
   *
   * @param response HTTP response to return to user.
   * @param config The configurations for Cruise Control.
   */
  static void handleOptions(HttpServletResponse response, KafkaCruiseControlConfig config) {
    response.setStatus(SC_OK);
    if (config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG)) {
      response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_ORIGIN_CONFIG));
      // This is required only as part of pre-flight response
      response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG));
      response.setHeader(ACCESS_CONTROL_ALLOW_HEADERS, config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));
      response.setHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
      response.setHeader(ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_MAX_AGE_IN_SEC);
    }
  }

  /**
   * Ensure that the given headerName does not exist in the given request header.
   *
   * @param requestContext HTTP request received by Cruise Control.
   * @param headerName a <code>String</code> specifying the header name
   */
  static void ensureHeaderNotPresent(CruiseControlRequestContext requestContext, String headerName) {
    String value = requestContext.getHeaders().get(headerName);
    if (value != null) {
      throw new IllegalArgumentException(String.format("Unexpected header %s (value: %s) in the request.", value, headerName));
    }
  }

  static void ensureHeaderNotPresent(HttpServletRequest request, String headerName) {
    String value = request.getHeader(headerName);
    if (value != null) {
      throw new IllegalArgumentException(String.format("Unexpected header %s (value: %s) in the request.", value, headerName));
    }
  }

  public static String httpServletRequestToString(CruiseControlRequestContext requestContext) {
    return String.format("%s %s", requestContext.getMethod(), requestContext.getRequestURI());
  }

  /**
   * Combines the given base with the query parameters retrieved from the given parameter map.
   *
   * @param base The base of the query without parameters
   * @param parameterMap Parameter map.
   * @return Query with parameters.
   */
  public static String queryWithParameters(String base, Map<String, String[]> parameterMap) {
    StringBuilder sb = new StringBuilder(base);
    String queryParamDelimiter = "?";
    for (Map.Entry<String, String[]> paramSet : parameterMap.entrySet()) {
      for (String paramValue : paramSet.getValue()) {
        sb.append(queryParamDelimiter).append(paramSet.getKey()).append("=").append(paramValue);
        if ("?".equals(queryParamDelimiter)) {
          queryParamDelimiter = "&";
        }
      }
    }
    return sb.toString();
  }
}
