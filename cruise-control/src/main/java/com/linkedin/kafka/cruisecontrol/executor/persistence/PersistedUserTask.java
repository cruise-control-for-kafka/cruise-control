/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import java.util.Objects;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * The user task that started an execution, as {@code user_tasks} reports it, so that an execution resumed after a restart is
 * reported under the same user task id.
 */
public final class PersistedUserTask {
  private final String _endPoint;
  private final String _request;
  private final String _clientIdentity;
  private final long _startMs;

  /**
   * @param endPoint Name of the endpoint of the request (e.g. REBALANCE).
   * @param request The request with its parameters.
   * @param clientIdentity The address of the client that sent the request.
   * @param startMs The time the request was received.
   */
  public PersistedUserTask(String endPoint, String request, String clientIdentity, long startMs) {
    _endPoint = validateNotNull(endPoint, "Endpoint cannot be null.");
    _request = validateNotNull(request, "Request cannot be null.");
    _clientIdentity = clientIdentity == null ? "" : clientIdentity;
    _startMs = startMs;
  }

  public String endPoint() {
    return _endPoint;
  }

  public String request() {
    return _request;
  }

  public String clientIdentity() {
    return _clientIdentity;
  }

  public long startMs() {
    return _startMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistedUserTask)) {
      return false;
    }
    PersistedUserTask that = (PersistedUserTask) o;
    return _startMs == that._startMs && _endPoint.equals(that._endPoint) && _request.equals(that._request)
           && _clientIdentity.equals(that._clientIdentity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_endPoint, _request, _clientIdentity, _startMs);
  }

  @Override
  public String toString() {
    return String.format("{endPoint: %s, request: %s, client: %s, startMs: %d}", _endPoint, _request, _clientIdentity, _startMs);
  }
}
