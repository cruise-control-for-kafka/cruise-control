/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlRequestHandler;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.handleOptions;

/**
 * The servlet for Kafka Cruise Control.
 */
public class ServletRequestHandler extends HttpServlet {

  private final KafkaCruiseControlRequestHandler _requestHandler;

  public ServletRequestHandler(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    _requestHandler = new KafkaCruiseControlRequestHandler(asynckafkaCruiseControl, dropwizardMetricRegistry);
  }

  @Override
  public void destroy() {
    super.destroy();
    _requestHandler.destroy();
  }

  protected void doOptions(HttpServletRequest request, HttpServletResponse response) {
    handleOptions(response, _requestHandler.cruiseControlEndPoints().config());
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    _requestHandler.doGetOrPost(new ServletRequestContext(request, response, _requestHandler.cruiseControlEndPoints().config()));
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    _requestHandler.doGetOrPost(new ServletRequestContext(request, response, _requestHandler.cruiseControlEndPoints().config()));
  }

  public KafkaCruiseControlEndPoints cruiseControlEndPoints() {
    return _requestHandler.cruiseControlEndPoints();
  }
}
