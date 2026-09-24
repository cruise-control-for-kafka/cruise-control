/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PauseResumeParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.PauseSamplingResult;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PAUSE_RESUME_PARAMETER_OBJECT_CONFIG;


public class PauseRequest extends AbstractSyncRequest {
    protected KafkaCruiseControl _kafkaCruiseControl;
    protected PauseResumeParameters _parameters;

    public PauseRequest() {
        super();
    }

    @Override
    protected PauseSamplingResult handle() {
        _kafkaCruiseControl.pauseMetricSampling(_parameters.reason());
        return new PauseSamplingResult(_kafkaCruiseControl.config());
    }

    @Override
    public PauseResumeParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return PauseRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
        _parameters = (PauseResumeParameters) validateNotNull(configs.get(PAUSE_RESUME_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
