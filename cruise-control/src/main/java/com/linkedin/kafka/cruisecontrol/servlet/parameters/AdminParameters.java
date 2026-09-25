/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ChangeExecutionConcurrencyParameters.maybeBuildChangeExecutionConcurrencyParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.DropRecentBrokersParameters.maybeBuildDropRecentBrokersParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.UpdateConcurrencyAdjusterParameters.maybeBuildUpdateConcurrencyAdjusterParameters;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.UpdateSelfHealingParameters.maybeBuildUpdateSelfHealingParameters;


/**
 * Parameters for {@link CruiseControlEndPoint#ADMIN}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/admin?json=[true/false]&amp;disable_self_healing_for=[Set-of-{@link KafkaAnomalyType}]
 *    &amp;enable_self_healing_for=[Set-of-{@link KafkaAnomalyType}]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_intra_broker_partition_movements=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]
 *    &amp;review_id=[id]&amp;drop_recently_demoted_brokers=[id1,id2...]&amp;drop_recently_removed_brokers=[id1,id2...]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;get_response_schema=[true/false]
 *    &amp;disable_concurrency_adjuster_for=[Set-of-{@link ConcurrencyType}]
 *    &amp;enable_concurrency_adjuster_for=[Set-of-{@link ConcurrencyType}]
 *    &amp;min_isr_based_concurrency_adjustment=[true/false]&amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 */
public class AdminParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(REVIEW_ID_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.addAll(DropRecentBrokersParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        validParameterNames.addAll(UpdateSelfHealingParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        validParameterNames.addAll(ChangeExecutionConcurrencyParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        validParameterNames.addAll(UpdateConcurrencyAdjusterParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Integer _reviewId;
    protected Map<String, ?> _configs;
    protected DropRecentBrokersParameters _dropBrokersParameters;
    protected UpdateSelfHealingParameters _updateSelfHealingParameters;
    protected ChangeExecutionConcurrencyParameters _changeExecutionConcurrencyParameters;
    protected UpdateConcurrencyAdjusterParameters _updateConcurrencyAdjusterParameters;

    public AdminParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
        _dropBrokersParameters = maybeBuildDropRecentBrokersParameters(_configs);
        _updateSelfHealingParameters = maybeBuildUpdateSelfHealingParameters(_configs);
        _changeExecutionConcurrencyParameters = maybeBuildChangeExecutionConcurrencyParameters(_configs);
        _updateConcurrencyAdjusterParameters = maybeBuildUpdateConcurrencyAdjusterParameters(_configs);
        if (areAllParametersNull(_dropBrokersParameters, _updateSelfHealingParameters, _changeExecutionConcurrencyParameters,
                _updateConcurrencyAdjusterParameters)) {
            throw new UserRequestException("Nothing executable found in request.");
        }
    }

    @Override
    public void setReviewId(int reviewId) {
        _reviewId = reviewId;
    }

    public Integer reviewId() {
        return _reviewId;
    }

    public DropRecentBrokersParameters dropRecentBrokersParameters() {
        return _dropBrokersParameters;
    }

    public UpdateSelfHealingParameters updateSelfHealingParameters() {
        return _updateSelfHealingParameters;
    }

    public ChangeExecutionConcurrencyParameters changeExecutionConcurrencyParameters() {
        return _changeExecutionConcurrencyParameters;
    }

    public UpdateConcurrencyAdjusterParameters updateConcurrencyAdjusterParameters() {
        return _updateConcurrencyAdjusterParameters;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _configs = configs;
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }

    /**
     * Supported admin operation type to be handled via {@link CruiseControlEndPoint#ADMIN} endpoint.
     */
    public enum AdminType {
        UPDATE_SELF_HEALING, CHANGE_CONCURRENCY, DROP_RECENT_BROKERS, UPDATE_CONCURRENCY_ADJUSTER
    }
}
