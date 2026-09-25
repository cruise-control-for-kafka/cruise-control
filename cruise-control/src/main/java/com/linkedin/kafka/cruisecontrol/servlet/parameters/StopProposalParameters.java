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

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#STOP_PROPOSAL_EXECUTION}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>Note that setting "force_stop" to true will make Cruise Control forcefully delete zNode /admin/reassign_partitions,
 *   /admin/preferred_replica_election and /controller in order to cancel ongoing the replica/leader movements. It may result
 *   the subject topics having inconsistent replication factor across partitions. </li>
 * </ul>
 *
 * <pre>
 * Stop the proposal execution.
 *    POST /kafkacruisecontrol/stop_proposal_execution?json=[true/false]&amp;review_id=[id]&amp;force_stop=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;stop_external_agent=[true/false]&amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 */
public class StopProposalParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(REVIEW_ID_PARAM);
        validParameterNames.add(FORCE_STOP_PARAM);
        validParameterNames.add(STOP_EXTERNAL_AGENT_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Integer _reviewId;
    protected boolean _forceExecutionStop;
    protected boolean _stopExternalAgent;

    public StopProposalParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
        _forceExecutionStop = ParameterUtils.forceExecutionStop(_requestContext);
        _stopExternalAgent = ParameterUtils.stopExternalAgent(_requestContext);
    }

    @Override
    public void setReviewId(int reviewId) {
        _reviewId = reviewId;
    }

    public Integer reviewId() {
        return _reviewId;
    }

    public boolean forceExecutionStop() {
        return _forceExecutionStop;
    }

    public boolean stopExternalAgent() {
        return _stopExternalAgent;
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
