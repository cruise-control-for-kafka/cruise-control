/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#RIGHTSIZE}.
 *
 * <ul>
 *   <li>Note that "num_brokers_to_add" is mutually exclusive to the "partition_count" parameter -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/rightsize?json=[true/false]&amp;get_response_schema=[true/false]
 *    &amp;doAs=[user]&amp;num_brokers_to_add=[POSITIVE-INTEGER]&amp;partition_count=[POSITIVE-INTEGER]&amp;topic=[topic]
 *    &amp;reason=[reason-for-request]
 * </pre>
 */
public class RightsizeParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(NUM_BROKERS_TO_ADD);
        validParameterNames.add(PARTITION_COUNT);
        validParameterNames.add(TOPIC_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected int _numBrokersToAdd;
    protected int _partitionCount;
    protected Pattern _topic;

    public RightsizeParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _numBrokersToAdd = ParameterUtils.numBrokersToAdd(_requestContext);
        _partitionCount = ParameterUtils.partitionCount(_requestContext);
        _topic = ParameterUtils.topic(_requestContext);
    }

    public int numBrokersToAdd() {
        return _numBrokersToAdd;
    }

    public int partitionCount() {
        return _partitionCount;
    }

    public Pattern topic() {
        return _topic;
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
