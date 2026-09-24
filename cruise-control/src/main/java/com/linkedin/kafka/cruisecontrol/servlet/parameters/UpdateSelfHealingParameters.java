/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DISABLE_SELF_HEALING_FOR_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENABLE_SELF_HEALING_FOR_PARAM;


/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#UPDATE_SELF_HEALING}.
 */
public class UpdateSelfHealingParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(DISABLE_SELF_HEALING_FOR_PARAM);
        validParameterNames.add(ENABLE_SELF_HEALING_FOR_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Set<AnomalyType> _disableSelfHealingFor;
    protected Set<AnomalyType> _enableSelfHealingFor;

    protected UpdateSelfHealingParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        Map<Boolean, Set<AnomalyType>> selfHealingFor = ParameterUtils.selfHealingFor(_requestContext);
        _enableSelfHealingFor = selfHealingFor.get(true);
        _disableSelfHealingFor = selfHealingFor.get(false);
    }

    /**
     * Create a {@link UpdateSelfHealingParameters} object from the request.
     *
     * @param configs Information collected from request and Cruise Control configs.
     * @return A UpdateSelfHealingParameters object; or null if any required parameter is not specified in the request.
     */
    public static UpdateSelfHealingParameters maybeBuildUpdateSelfHealingParameters(Map<String, ?> configs)
            throws UnsupportedEncodingException {
        UpdateSelfHealingParameters selfHealingUpdateParameters = new UpdateSelfHealingParameters();
        selfHealingUpdateParameters.configure(configs);
        selfHealingUpdateParameters.initParameters();
        // At least self-healing for one anomaly type should requested to enable/disable; otherwise, return null.
        if (selfHealingUpdateParameters.enableSelfHealingFor().isEmpty()
                && selfHealingUpdateParameters.disableSelfHealingFor().isEmpty()) {
            return null;
        }
        return selfHealingUpdateParameters;
    }

    public Set<AnomalyType> disableSelfHealingFor() {
        return Collections.unmodifiableSet(_disableSelfHealingFor);
    }

    public Set<AnomalyType> enableSelfHealingFor() {
        return Collections.unmodifiableSet(_enableSelfHealingFor);
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
