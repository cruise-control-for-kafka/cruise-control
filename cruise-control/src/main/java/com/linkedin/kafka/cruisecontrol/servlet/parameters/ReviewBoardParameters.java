/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_IDS_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#REVIEW_BOARD}.
 *
 * <pre>
 *    GET /kafkacruisecontrol/review_board?json=[true/false]&amp;review_ids=[id1,id2,...]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class ReviewBoardParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(REVIEW_IDS_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Set<Integer> _reviewIds;

    public ReviewBoardParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _reviewIds = ParameterUtils.reviewIds(_requestContext);
    }

    public Set<Integer> reviewIds() {
        return _reviewIds;
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
