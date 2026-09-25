/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.ProposalsRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ProposalsParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PROPOSALS_PARAMETER_OBJECT_CONFIG;


public class ProposalsRequest extends AbstractAsyncRequest {
    protected ProposalsParameters _parameters;

    public ProposalsRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Get customized proposals");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new ProposalsRunnable(_asyncKafkaCruiseControl, future, _parameters));
        return future;
    }

    @Override
    public ProposalsParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return ProposalsRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (ProposalsParameters) validateNotNull(configs.get(PROPOSALS_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
