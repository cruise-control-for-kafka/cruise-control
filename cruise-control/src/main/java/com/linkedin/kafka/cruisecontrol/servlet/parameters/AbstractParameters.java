/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.ServletRequestContext;
import com.linkedin.kafka.cruisecontrol.vertx.VertxRequestContext;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.ROUTING_CONTEXT_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.GET_RESPONSE_SCHEMA;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.JSON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.handleParameterParseException;


/**
 * An abstract class for Cruise Control parameters. This class will be extended to crete custom parameters for different
 * endpoints.
 */
public abstract class AbstractParameters implements CruiseControlParameters {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractParameters.class);
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(JSON_PARAM);
    validParameterNames.add(GET_RESPONSE_SCHEMA);
    validParameterNames.add(DO_AS);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);

  }
  protected CruiseControlRequestContext _requestContext;
  protected boolean _initialized = false;
  protected KafkaCruiseControlConfig _config;
  // Common to all parameters, expected to be populated via initParameters.
  protected boolean _json = false;
  protected boolean _wantResponseSchema = false;
  protected EndPoint _endPoint = null;

  public AbstractParameters() {

  }

  protected void initParameters() throws UnsupportedEncodingException {
    _initialized = true;
    _endPoint = ParameterUtils.endPoint(_requestContext);
    _json = ParameterUtils.wantJSON(_requestContext);
    _wantResponseSchema = ParameterUtils.wantResponseSchema(_requestContext);
  }

  @Override
  public boolean parseParameters(CruiseControlRequestContext requestContext) {
    if (_initialized) {
      LOG.trace("Attempt to parse an already parsed request {}.", _requestContext);
      return false;
    }
    try {
      initParameters();
      return false;
    } catch (Exception e) {
      try {
        handleParameterParseException(e, requestContext, e.getMessage(), _json, _wantResponseSchema);
      } catch (IOException ioe) {
        LOG.error(String.format("Failed to write parse parameter exception to output stream. Endpoint: %s.", _endPoint), ioe);
      }
      return true;
    }
  }

  @Override
  public boolean json() {
    return _json;
  }

  @Override
  public boolean wantResponseSchema() {
    return _wantResponseSchema;
  }

  @Override
  public void setReviewId(int reviewId) {
    // Relevant to parameters with review process.
  }

  @Override
  public EndPoint endPoint() {
    return _endPoint;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.get(ROUTING_CONTEXT_OBJECT_CONFIG) == null) {
      _requestContext = new ServletRequestContext(
              (HttpServletRequest) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG),
                      "HttpServletRequest configuration is missing from the request."), null, _config);
    } else {
      _requestContext = new VertxRequestContext((RoutingContext) validateNotNull(configs.get(ROUTING_CONTEXT_OBJECT_CONFIG),
              "HttpServletRequest configuration is missing from the request."), _config);
    }
    _config = (KafkaCruiseControlConfig) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG),
                                                         "KafkaCruiseControlConfig configuration is missing from the request.");
  }
}
