/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.LoadRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.LOAD_PARAMETER_OBJECT_CONFIG;


public class ClusterLoadRequest extends AbstractAsyncRequest {
    protected ClusterLoadParameters _parameters;

    public ClusterLoadRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Get broker stats");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new LoadRunnable(_asyncKafkaCruiseControl, future, _parameters));
        return future;
    }

    @Override
    public ClusterLoadParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return ClusterLoadRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (ClusterLoadParameters) validateNotNull(configs.get(LOAD_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
