/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#PROPOSALS}
 *
 * <pre>
 *    GET /kafkacruisecontrol/proposals?verbose=[ENABLE_VERBOSE]&amp;ignore_proposal_cache=[true/false]
 *    &amp;goals=[goal1,goal2...]&amp;data_from=[valid_windows/valid_partitions]&amp;excluded_topics=[pattern]
 *    &amp;use_ready_default_goals=[true/false]&amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;exclude_recently_removed_brokers=[true/false]
 *    &amp;destination_broker_ids=[id1,id2...]&amp;kafka_assigner=[true/false]&amp;rebalance_disk=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;fast_mode=[true/false]&amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 */
public class ProposalsParameters extends GoalBasedOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(KAFKA_ASSIGNER_MODE_PARAM);
        validParameterNames.add(DESTINATION_BROKER_IDS_PARAM);
        validParameterNames.add(IGNORE_PROPOSAL_CACHE_PARAM);
        validParameterNames.add(REBALANCE_DISK_MODE_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.addAll(GoalBasedOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Set<Integer> _destinationBrokerIds;
    protected boolean _ignoreProposalCache;
    protected boolean _isRebalanceDiskMode;

    public ProposalsParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _destinationBrokerIds = ParameterUtils.destinationBrokerIds(_requestContext);
        _ignoreProposalCache = ParameterUtils.ignoreProposalCache(_requestContext);
        _isRebalanceDiskMode = ParameterUtils.isRebalanceDiskMode(_requestContext);
    }

    public Set<Integer> destinationBrokerIds() {
        return _destinationBrokerIds;
    }

    public boolean ignoreProposalCache() {
        return _ignoreProposalCache;
    }

    public boolean isRebalanceDiskMode() {
        return _isRebalanceDiskMode;
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
