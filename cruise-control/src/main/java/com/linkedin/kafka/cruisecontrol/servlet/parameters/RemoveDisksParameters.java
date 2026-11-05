/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.Explode;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.enums.ParameterStyle;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Collections;
import java.util.Set;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.JSON_PARAM;

public class RemoveDisksParameters extends GoalBasedOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(BROKER_ID_AND_LOGDIRS_PARAM);
        validParameterNames.add(DRY_RUN_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
        validParameterNames.add(JSON_PARAM);
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

    @Parameters(value = {@Parameter(name = "dryrun",
        in = ParameterIn.QUERY,
        description = "Whether to dry-run the request or not.",
        schema = @Schema(type = "boolean",
        defaultValue = "true")),
        @Parameter(name = "stop_ongoing_execution",
        in = ParameterIn.QUERY,
        description = "Whether to stop the ongoing execution (if any) and start executing the given request.",
        schema = @Schema(type = "boolean",
        defaultValue = "false")),
        @Parameter(name = "reason",
        in = ParameterIn.QUERY,
        description = "Reason for request.",
        example = "Balance disk utilization across all brokers in the cluster.",
        schema = @Schema(type = "string")),
        @Parameter(name = "brokerid_and_logdirs",
        in = ParameterIn.QUERY,
        description = "Comma-separated broker-id and log-directory pairs. Each pair uses the first hyphen"
        + " as its separator.",
        example = "101-/tmp/kafka-logs-1,101-/tmp/kafka-logs-2",
        required = true,
        style = ParameterStyle.FORM,
        explode = Explode.FALSE,
        array = @ArraySchema(schema = @Schema(type = "string",
        pattern = "^\\d+-.+$")))})
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
