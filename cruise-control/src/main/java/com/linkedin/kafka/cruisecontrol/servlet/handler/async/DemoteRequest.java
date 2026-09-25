/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.DemoteBrokerRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG;


public class DemoteRequest extends AbstractAsyncRequest {
    protected DemoteBrokerParameters _parameters;

    public DemoteRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Demote");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new DemoteBrokerRunnable(_asyncKafkaCruiseControl, future, uuid, _parameters));
        return future;
    }

    @Override
    public DemoteBrokerParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return DemoteRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (DemoteBrokerParameters) validateNotNull(configs.get(DEMOTE_BROKER_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
