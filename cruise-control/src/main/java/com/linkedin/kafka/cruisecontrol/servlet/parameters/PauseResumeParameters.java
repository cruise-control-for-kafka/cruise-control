/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#PAUSE_SAMPLING} and {@link CruiseControlEndPoint#RESUME_SAMPLING}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 * 1. Pause metrics sampling. (RUNNING -&gt; PAUSED).
 *    POST /kafkacruisecontrol/pause_sampling?json=[true/false]&amp;reason=[reason-for-pause]&amp;review_id=[id]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]
 *
 * 2. Resume metrics sampling. (PAUSED -&gt; RUNNING).
 *    POST /kafkacruisecontrol/resume_sampling?json=[true/false]&amp;reason=[reason-for-resume]&amp;review_id=[id]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class PauseResumeParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(REVIEW_ID_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected String _reason;
    protected Integer _reviewId;

    public PauseResumeParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _reason = ParameterUtils.reason(_requestContext, false);
        boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
    }

    public String reason() {
        return _reason;
    }

    @Override
    public void setReviewId(int reviewId) {
        _reviewId = reviewId;
    }

    public Integer reviewId() {
        return _reviewId;
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
