/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.handler.AbstractRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.AbstractAsyncRequest;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public abstract class AbstractSyncRequest extends AbstractRequest {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncRequest.class);
    private UserTaskManager _userTaskManager;
    private Map<EndPoint, Timer> _successfulRequestExecutionTimer;

    public AbstractSyncRequest() {

    }

    /**
     * @return Handle the request and return the response.
     */
    protected abstract CruiseControlResponse handle();

    @Override
    public CruiseControlResponse getResponse(CruiseControlRequestContext requestContext)
            throws Exception {
        LOG.info("Processing sync request {}.", name());
        long requestExecutionStartTime = System.nanoTime();
        int step = 0;
        OperationFuture resultFuture = _userTaskManager.getOrCreateUserTask(requestContext, uuid -> {
            OperationFuture future = new OperationFuture(String.format("%s request", parameters().endPoint().toString()));
            future.complete(handle());
            return future;
        }, step, false, parameters()).get(step);

        CruiseControlResponse ccResponse = resultFuture.get();
        _successfulRequestExecutionTimer.get(parameters().endPoint()).update(System.nanoTime() - requestExecutionStartTime, TimeUnit.NANOSECONDS);
        return ccResponse;
    }

    @Override
    public abstract CruiseControlParameters parameters();

    public abstract String name();

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControlEndPoints cruiseControlEndPoints = getCruiseControlEndpoints();
        _userTaskManager = cruiseControlEndPoints.userTaskManager();
        _successfulRequestExecutionTimer = cruiseControlEndPoints.successfulRequestExecutionTimer();
    }

    protected KafkaCruiseControlEndPoints getCruiseControlEndpoints() {
        return _requestHandler.cruiseControlEndPoints();
    }
}
