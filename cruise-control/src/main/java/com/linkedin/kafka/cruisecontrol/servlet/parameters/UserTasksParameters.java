/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.USER_TASK_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CLIENT_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENTRIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENDPOINTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TYPES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.FETCH_COMPLETED_TASK_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#USER_TASKS}
 *
 * <pre>
 * Retrieve the recent user tasks.
 *    GET /kafkacruisecontrol/user_tasks?json=[true/false]&amp;user_task_ids=[Set-of-USER-TASK-IDS]&amp;client_ids=[Set-of-ClientIdentity]&amp;
 *    endpoints=[Set-of-{@link CruiseControlEndPoint}]&amp;types=[Set-of-{@link UserTaskManager.TaskState}]&amp;entries=[POSITIVE-INTEGER]
 *    &amp;fetch_completed_task=[true/false]&amp;get_response_schema=[true/false]&amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 */
public class UserTasksParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(USER_TASK_IDS_PARAM);
    validParameterNames.add(CLIENT_IDS_PARAM);
    validParameterNames.add(ENDPOINTS_PARAM);
    validParameterNames.add(TYPES_PARAM);
    validParameterNames.add(ENTRIES_PARAM);
    validParameterNames.add(FETCH_COMPLETED_TASK_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Set<UUID> _userTaskIds;
  protected Set<String> _clientIds;
  protected Set<CruiseControlEndPoint> _endPoints;
  protected Set<UserTaskManager.TaskState> _types;
  protected int _entries;
  protected boolean _fetchCompletedTask;

  public UserTasksParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _userTaskIds = ParameterUtils.userTaskIds(_requestContext);
    _clientIds = ParameterUtils.clientIds(_requestContext);
    _endPoints = ParameterUtils.endPoints(_requestContext);
    _types = ParameterUtils.types(_requestContext);
    _entries = ParameterUtils.entries(_requestContext);
    _fetchCompletedTask = ParameterUtils.fetchCompletedTask(_requestContext);
  }

  public Set<UUID> userTaskIds() {
    return Collections.unmodifiableSet(_userTaskIds);
  }

  public Set<String> clientIds() {
    return Collections.unmodifiableSet(_clientIds);
  }

  public Set<CruiseControlEndPoint> endPoints() {
    return Collections.unmodifiableSet(_endPoints);
  }

  public Set<UserTaskManager.TaskState> types() {
    return Collections.unmodifiableSet(_types);
  }

  public int entries() {
    return _entries;
  }

  public boolean fetchCompletedTask() {
    return _fetchCompletedTask;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Parameters(value = {@Parameter(name = "client_ids",
      in = ParameterIn.QUERY,
      description = "Comma separated IP addresses to filter the task results Cruise Control report.",
      schema = @Schema(type = "string",
      example = "[0.0.0.1]")),
      @Parameter(name = "endpoints",
      in = ParameterIn.QUERY,
      description = "Comma separated endpoints to filter the task results Cruise Control report.",
      schema = @Schema(type = "string",
      example = "[PROPOSALS,REBALANCE]")),
      @Parameter(name = "entries",
      in = ParameterIn.QUERY,
      description = "The number of entries to show in the response.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "0")),
      @Parameter(name = "types",
      in = ParameterIn.QUERY,
      description = "Comma separated HTTP request types to filter the task results Cruise Control report.",
      schema = @Schema(type = "string",
      example = "[COMPLETED, ACTIVE]")),
      @Parameter(name = "user_task_ids",
      in = ParameterIn.QUERY,
      description = "Comma separated UUIDs to filter the task results Cruise Control report.",
      schema = @Schema(type = "string")),
      @Parameter(name = "fetch_completed_task",
      in = ParameterIn.QUERY,
      description = "Whether return the original request's final response.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "reason",
      in = ParameterIn.QUERY,
      description = "Reason for request.",
      schema = @Schema(type = "string"))})
  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
