/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Collections;
import java.util.Set;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.VERBOSE_PARAM;

/**
 * Parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#REMOVE_DISKS}
 *
 * <ul>
 *   <li>Note that "brokerid_and_logdirs" takes comma as delimiter between two broker id and logdir pairs -- i.e. we assume
 *   a valid logdir name contains no comma.</li>
 * </ul>
 *
 * <pre>
 * Remove disks
 *    POST /kafkacruisecontrol/remove_disks?brokerid_and_logdirs=[broker_id1-logdir1,broker_id2-logdir2]
 *    &amp;dryrun=[true/false]&amp;stop_ongoing_execution=[true/false]&amp;reason=[reason-for-request]
 *    &amp;allow_capacity_estimation=[true/false]&amp;verbose=[true/false]&amp;json=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class RemoveDisksParameters extends GoalBasedOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(BROKER_ID_AND_LOGDIRS_PARAM);
        validParameterNames.add(DRY_RUN_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
        validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
        validParameterNames.add(VERBOSE_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }
    private boolean _dryRun;
    private String _reason;
    private boolean _stopOngoingExecution;
    private Map<Integer, Set<String>> _logdirByBrokerId;

    public RemoveDisksParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _logdirByBrokerId = ParameterUtils.brokerIdAndLogdirs(_requestContext);
        _dryRun = ParameterUtils.getDryRun(_requestContext);
        boolean requestReasonRequired = _config.getBoolean(ExecutorConfig.REQUEST_REASON_REQUIRED_CONFIG);
        _reason = ParameterUtils.reason(_requestContext, requestReasonRequired && !_dryRun);
        _stopOngoingExecution = ParameterUtils.stopOngoingExecution(_requestContext);
        if (_stopOngoingExecution && _dryRun) {
            throw new UserRequestException(String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
        }
    }

    public Map<Integer, Set<String>> brokerIdAndLogdirs() {
        return _logdirByBrokerId;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
    public String reason() {
        return _reason;
    }
    public boolean dryRun() {
        return _dryRun;
    }
    public boolean stopOngoingExecution() {
        return _stopOngoingExecution;
    }
}
