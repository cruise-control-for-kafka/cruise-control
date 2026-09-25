/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.FixOfflineReplicasRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG;


public class FixOfflineReplicasRequest extends AbstractAsyncRequest {
    protected FixOfflineReplicasParameters _parameters;

    public FixOfflineReplicasRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Fix offline replicas");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new FixOfflineReplicasRunnable(_asyncKafkaCruiseControl, future, _parameters, uuid));
        return future;
    }

    @Override
    public FixOfflineReplicasParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return FixOfflineReplicasRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (FixOfflineReplicasParameters) validateNotNull(configs.get(FIX_OFFLINE_REPLICAS_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
