/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


public abstract class KafkaOptimizationParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
        validParameterNames.add(VERBOSE_PARAM);
        validParameterNames.add(EXCLUDE_RECENTLY_DEMOTED_BROKERS_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected boolean _allowCapacityEstimation;
    protected boolean _isVerbose;
    protected boolean _excludeRecentlyDemotedBrokers;

    KafkaOptimizationParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_requestContext);
        _isVerbose = ParameterUtils.isVerbose(_requestContext);
        _excludeRecentlyDemotedBrokers = ParameterUtils.excludeRecentlyDemotedBrokers(_requestContext);
    }

    public boolean allowCapacityEstimation() {
        return _allowCapacityEstimation;
    }

    public boolean isVerbose() {
        return _isVerbose;
    }

    public boolean excludeRecentlyDemotedBrokers() {
        return _excludeRecentlyDemotedBrokers;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
    }
}
