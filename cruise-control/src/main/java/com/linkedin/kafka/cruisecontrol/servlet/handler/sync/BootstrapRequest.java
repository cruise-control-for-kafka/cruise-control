/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.BootstrapParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.BootstrapResult;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BOOTSTRAP_PARAMETER_OBJECT_CONFIG;


public class BootstrapRequest extends AbstractSyncRequest {
    protected KafkaCruiseControl _kafkaCruiseControl;
    protected BootstrapParameters _parameters;

    public BootstrapRequest() {
        super();
    }

    @Override
    protected BootstrapResult handle() {
        Long startMs = _parameters.startMs();
        Long endMs = _parameters.endMs();
        boolean clearMetrics = _parameters.clearMetrics();
        if (_parameters.developerMode()) {
            // This endpoint is used only for development purposes in developer_mode=true.
            _kafkaCruiseControl.bootstrap(startMs, endMs, clearMetrics);
        }

        return new BootstrapResult(_kafkaCruiseControl.config());
    }

    @Override
    public BootstrapParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return BootstrapRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
        _parameters = (BootstrapParameters) validateNotNull(configs.get(BOOTSTRAP_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
