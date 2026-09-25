/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DROP_RECENTLY_DEMOTED_BROKERS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DROP_RECENTLY_REMOVED_BROKERS_PARAM;


/**
 * Optional Parameters for {@link CruiseControlEndPoint#ADMIN}.
 * This class holds all the request parameters for {@link AdminParameters.AdminType#DROP_RECENT_BROKERS}.
 */
public class DropRecentBrokersParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(DROP_RECENTLY_REMOVED_BROKERS_PARAM);
        validParameterNames.add(DROP_RECENTLY_DEMOTED_BROKERS_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Set<Integer> _dropRecentlyRemovedBrokers;
    protected Set<Integer> _dropRecentlyDemotedBrokers;

    protected DropRecentBrokersParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _dropRecentlyRemovedBrokers = ParameterUtils.dropRecentlyRemovedBrokers(_requestContext);
        _dropRecentlyDemotedBrokers = ParameterUtils.dropRecentlyDemotedBrokers(_requestContext);
    }

    /**
     * Create a {@link DropRecentBrokersParameters} object from the request.
     *
     * @param configs Information collected from request and Cruise Control configs.
     * @return A DropRecentBrokersParameters object; or null if any required parameter is not specified in the request.
     */
    public static DropRecentBrokersParameters maybeBuildDropRecentBrokersParameters(Map<String, ?> configs)
            throws UnsupportedEncodingException {
        DropRecentBrokersParameters dropRecentBrokersParameters = new DropRecentBrokersParameters();
        dropRecentBrokersParameters.configure(configs);
        dropRecentBrokersParameters.initParameters();
        // At least one recently removed/demoted broker should be specified to drop; otherwise, return null.
        if (dropRecentBrokersParameters.dropRecentlyDemotedBrokers().isEmpty()
                && dropRecentBrokersParameters.dropRecentlyRemovedBrokers().isEmpty()) {
            return null;
        }
        return dropRecentBrokersParameters;
    }

    public Set<Integer> dropRecentlyRemovedBrokers() {
        return _dropRecentlyRemovedBrokers;
    }

    public Set<Integer> dropRecentlyDemotedBrokers() {
        return _dropRecentlyDemotedBrokers;
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
