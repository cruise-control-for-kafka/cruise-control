/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.END_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.START_MS_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#TRAIN}
 *
 * <pre>
 * Train the Kafka Cruise Control linear regression model. The trained model will only be used if
 * {@link com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig#USE_LINEAR_REGRESSION_MODEL_CONFIG} is true.
 *
 *    GET /kafkacruisecontrol/train?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]&amp;json=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class TrainParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(START_MS_PARAM);
        validParameterNames.add(END_MS_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Long _startMs;
    protected Long _endMs;

    public TrainParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _startMs = ParameterUtils.startMsOrDefault(_requestContext, null);
        _endMs = ParameterUtils.endMsOrDefault(_requestContext, null);
        if (_startMs == null || _endMs == null) {
            throw new UserRequestException("Missing start or end parameter.");
        }
        ParameterUtils.validateTimeRange(_startMs, _endMs);
    }

    public Long startMs() {
        return _startMs;
    }

    public Long endMs() {
        return _endMs;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
