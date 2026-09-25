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

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#BOOTSTRAP}
 *
 * <pre>
 * 1. RANGE MODE:
 *    GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]&amp;clearmetrics=[true/false]
 *    &amp;json=[true/false]&amp;get_response_schema=[true/false]&amp;developer_mode=[true/false]&amp;doAs=[user]
 * 2. SINCE MODE:
 *    GET /kafkacruisecontrol/bootstrap?start=[START_TIMESTAMP]&amp;clearmetrics=[true/false]&amp;json=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;developer_mode=[true/false]&amp;doAs=[user]
 * 3. RECENT MODE:
 *    GET /kafkacruisecontrol/bootstrap?clearmetrics=[true/false]&amp;json=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;developer_mode=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class BootstrapParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(START_MS_PARAM);
        validParameterNames.add(END_MS_PARAM);
        validParameterNames.add(CLEAR_METRICS_PARAM);
        validParameterNames.add(DEVELOPER_MODE_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Long _startMs;
    protected Long _endMs;
    protected boolean _clearMetrics;
    protected boolean _developerMode;

    public BootstrapParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _startMs = ParameterUtils.startMsOrDefault(_requestContext, null);
        _endMs = ParameterUtils.endMsOrDefault(_requestContext, null);
        _clearMetrics = ParameterUtils.clearMetrics(_requestContext);
        _developerMode = ParameterUtils.developerMode(_requestContext);
        if (_startMs == null && _endMs != null) {
            throw new UserRequestException("The start time cannot be empty when end time is specified.");
        }
        if (_startMs != null && _endMs != null) {
            ParameterUtils.validateTimeRange(_startMs, _endMs);
        }
    }

    public Long startMs() {
        return _startMs;
    }

    public Long endMs() {
        return _endMs;
    }

    public boolean clearMetrics() {
        return _clearMetrics;
    }

    public boolean developerMode() {
        return _developerMode;
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
