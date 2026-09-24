/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.StopProposalParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.StopProposalResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FORCE_STOP_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG;


public class StopProposalRequest extends AbstractSyncRequest {
    private static final Logger LOG = LoggerFactory.getLogger(StopProposalRequest.class);
    protected KafkaCruiseControl _kafkaCruiseControl;
    protected StopProposalParameters _parameters;

    public StopProposalRequest() {
        super();
    }

    @Override
    protected StopProposalResult handle() {
        if (_parameters.forceExecutionStop()) {
            LOG.info("{} is a deprecated parameter. Force stopping an execution is not supported.", FORCE_STOP_PARAM);
        }
        _kafkaCruiseControl.userTriggeredStopExecution(_parameters.stopExternalAgent());
        return new StopProposalResult(_kafkaCruiseControl.config());
    }

    @Override
    public StopProposalParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return StopProposalRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
        _parameters = (StopProposalParameters) validateNotNull(configs.get(STOP_PROPOSAL_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
