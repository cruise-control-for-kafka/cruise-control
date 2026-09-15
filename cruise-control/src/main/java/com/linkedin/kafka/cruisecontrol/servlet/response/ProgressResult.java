/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;
import static jakarta.servlet.http.HttpServletResponse.SC_ACCEPTED;

@JsonResponseClass
public class ProgressResult extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String PROGRESS = "progress";
  protected List<OperationFuture> _futures;

  public ProgressResult(List<OperationFuture> futures, KafkaCruiseControlConfig config) {
    super(config);
    _futures = futures;
  }

  protected String getJsonString() {
    Map<String, Object> jsonResponse = new HashMap<>();
    jsonResponse.put(VERSION, JSON_VERSION);
    List<Object> progress = new ArrayList<>(_futures.size());
    for (OperationFuture future: _futures) {
      progress.add(future.getJsonStructure());
    }
    jsonResponse.put(PROGRESS, progress);
    Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

    return gson.toJson(jsonResponse);
  }

  @Override
  public void writeSuccessResponse(CruiseControlParameters parameters, CruiseControlRequestContext requestContext) throws IOException {
    boolean json = parameters.json();
    boolean wantResponseSchema = parameters.wantResponseSchema();
    discardIrrelevantResponse(parameters);
    requestContext.writeResponseToOutputStream(SC_ACCEPTED, json, wantResponseSchema, _cachedResponse);
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    for (OperationFuture operationFuture: _futures) {
      sb.append(String.format("%s:%n%s", operationFuture.operation(), operationFuture.progressString()));
    }
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
    // Discard irrelevant response.
    _futures = null;
  }
}
