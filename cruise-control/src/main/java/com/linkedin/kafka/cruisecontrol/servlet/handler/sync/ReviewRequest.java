/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReviewParameters;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReviewResult;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_PARAMETER_OBJECT_CONFIG;


public class ReviewRequest extends AbstractSyncRequest {
    protected Purgatory _purgatory;
    protected ReviewParameters _parameters;

    public ReviewRequest() {
        super();
    }

    @Override
    protected ReviewResult handle() {
        return _purgatory.applyReview(_parameters.reviewRequests(), _parameters.reason());
    }

    @Override
    public ReviewParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return ReviewRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _purgatory = getCruiseControlEndpoints().purgatory();
        _parameters = (ReviewParameters) validateNotNull(configs.get(REVIEW_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
