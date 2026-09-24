/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.PartitionLoadRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_LOAD_PARAMETER_OBJECT_CONFIG;


public class PartitionLoadRequest extends AbstractAsyncRequest {
    protected PartitionLoadParameters _parameters;

    public PartitionLoadRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture(String.format("Get partition load from %d to %d", _parameters.startMs(), _parameters.endMs()));
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new PartitionLoadRunnable(_asyncKafkaCruiseControl, future, _parameters));
        return future;
    }

    @Override
    public PartitionLoadParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return PartitionLoadRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (PartitionLoadParameters) validateNotNull(configs.get(PARTITION_LOAD_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
