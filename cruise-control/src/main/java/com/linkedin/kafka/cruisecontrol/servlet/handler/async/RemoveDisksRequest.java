/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveDisksRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REMOVE_DISKS_PARAMETER_OBJECT_CONFIG;

public class RemoveDisksRequest extends AbstractAsyncRequest {
    protected RemoveDisksParameters _parameters;

    public RemoveDisksRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Remove disks");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().submit(new RemoveDisksRunnable(_asyncKafkaCruiseControl, future, _parameters, uuid));
        return future;
    }

    @Override
    public RemoveDisksParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return RemoveDisksRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (RemoveDisksParameters) validateNotNull(configs.get(REMOVE_DISKS_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
