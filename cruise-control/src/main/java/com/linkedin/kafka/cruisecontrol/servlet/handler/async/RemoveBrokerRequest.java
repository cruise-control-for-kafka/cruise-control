/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REMOVE_BROKER_PARAMETER_OBJECT_CONFIG;


public class RemoveBrokerRequest extends AbstractAsyncRequest {
    protected RemoveBrokerParameters _parameters;

    public RemoveBrokerRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Remove brokers");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new RemoveBrokersRunnable(_asyncKafkaCruiseControl, future, _parameters, uuid));
        return future;
    }

    @Override
    public RemoveBrokerParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return RemoveBrokerRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (RemoveBrokerParameters) validateNotNull(configs.get(REMOVE_BROKER_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
