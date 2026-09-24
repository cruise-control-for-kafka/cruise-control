/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#REVIEW}.
 *
 * <pre>
 *    POST /kafkacruisecontrol/review?json=[true/false]&amp;approve=[id1,id2,...]&amp;discard=[id1,id2,...]
 *    &amp;reason=[reason-for-review]&amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class ReviewParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(APPROVE_PARAM);
        validParameterNames.add(DISCARD_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected String _reason;
    protected Map<ReviewStatus, Set<Integer>> _reviewRequests;

    public ReviewParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _reason = ParameterUtils.reason(_requestContext, false);
        _reviewRequests = ParameterUtils.reviewRequests(_requestContext);
    }

    public String reason() {
        return _reason;
    }

    public Map<ReviewStatus, Set<Integer>> reviewRequests() {
        return _reviewRequests;
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
