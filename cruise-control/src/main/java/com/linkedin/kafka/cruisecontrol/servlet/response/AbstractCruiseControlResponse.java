/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import java.io.IOException;

import static jakarta.servlet.http.HttpServletResponse.SC_OK;


public abstract class AbstractCruiseControlResponse implements CruiseControlResponse {
  protected String _cachedResponse;
  protected KafkaCruiseControlConfig _config;

  /**
   * Note if the response corresponds to a request which is initiated by Cruise Control, the config passed in will be null.
   */
  public AbstractCruiseControlResponse(KafkaCruiseControlConfig config) {
    _cachedResponse = null;
    _config = config;
  }

  protected abstract void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters);

  @Override
  public void writeSuccessResponse(CruiseControlParameters parameters, CruiseControlRequestContext requestContext) throws IOException {
    boolean json = parameters.json();
    boolean wantResponseSchema = parameters.wantResponseSchema();
    discardIrrelevantResponse(parameters);
    requestContext.writeResponseToOutputStream(SC_OK, json, wantResponseSchema, _cachedResponse);
  }

  @Override
  public synchronized void discardIrrelevantResponse(CruiseControlParameters parameters) {
    if (_cachedResponse == null) {
      discardIrrelevantAndCacheRelevant(parameters);
      if (_cachedResponse == null) {
        throw new IllegalStateException("Failed to cache the relevant response.");
      }
    }
  }

  @Override
  public String cachedResponse() {
    return _cachedResponse;
  }
}
