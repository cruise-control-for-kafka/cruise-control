/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#UPDATE_CONCURRENCY_ADJUSTER}.
 */
public class UpdateConcurrencyAdjusterParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(DISABLE_CONCURRENCY_ADJUSTER_FOR_PARAM);
        validParameterNames.add(ENABLE_CONCURRENCY_ADJUSTER_FOR_PARAM);
        validParameterNames.add(MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Set<ConcurrencyType> _disableConcurrencyAdjusterFor;
    protected Set<ConcurrencyType> _enableConcurrencyAdjusterFor;
    protected Boolean _minIsrBasedConcurrencyAdjustment;

    protected UpdateConcurrencyAdjusterParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        Map<Boolean, Set<ConcurrencyType>> concurrencyAdjusterFor = ParameterUtils.concurrencyAdjusterFor(_requestContext);
        _enableConcurrencyAdjusterFor = concurrencyAdjusterFor.get(true);
        _disableConcurrencyAdjusterFor = concurrencyAdjusterFor.get(false);
        _minIsrBasedConcurrencyAdjustment = ParameterUtils.minIsrBasedConcurrencyAdjustment(_requestContext);
    }

    /**
     * Create a {@link UpdateConcurrencyAdjusterParameters} object from the request.
     *
     * @param configs Information collected from request and Cruise Control configs.
     * @return A {@link UpdateConcurrencyAdjusterParameters} object; or {@code null} if any required parameter is not
     * specified in the request.
     */
    public static UpdateConcurrencyAdjusterParameters maybeBuildUpdateConcurrencyAdjusterParameters(Map<String, ?> configs)
            throws UnsupportedEncodingException {
        UpdateConcurrencyAdjusterParameters updateConcurrencyAdjusterParameters = new UpdateConcurrencyAdjusterParameters();
        updateConcurrencyAdjusterParameters.configure(configs);
        updateConcurrencyAdjusterParameters.initParameters();
        // If no concurrency adjuster is enabled/disabled in the request and there is no MinISR-based concurrency adjustment, return null.
        if (updateConcurrencyAdjusterParameters.enableConcurrencyAdjusterFor().isEmpty()
                && updateConcurrencyAdjusterParameters.disableConcurrencyAdjusterFor().isEmpty()
                && updateConcurrencyAdjusterParameters.minIsrBasedConcurrencyAdjustment() == null) {
            return null;
        }
        return updateConcurrencyAdjusterParameters;
    }

    public Set<ConcurrencyType> disableConcurrencyAdjusterFor() {
        return Collections.unmodifiableSet(_disableConcurrencyAdjusterFor);
    }

    public Set<ConcurrencyType> enableConcurrencyAdjusterFor() {
        return Collections.unmodifiableSet(_enableConcurrencyAdjusterFor);
    }

    public Boolean minIsrBasedConcurrencyAdjustment() {
        return _minIsrBasedConcurrencyAdjustment;
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
