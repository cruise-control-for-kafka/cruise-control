/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TIME_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.END_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.START_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.POPULATE_DISK_INFO_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CAPACITY_ONLY_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#LOAD}
 *
 * <ul>
 *   <li>Note that both parameter "time" and "end" are used to specify the end time for cluster model, thus they are mutually exclusive.</li>
 *</ul>
 *
 * <pre>
 * Get the cluster load
 *    GET /kafkacruisecontrol/load?start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]&amp;time=[END_TIMESTAMP]&amp;allow_capacity_estimation=[true/false]
 *    &amp;json=[true/false]&amp;populate_disk_info=[true/false]&amp;get_response_schema=[true/false]&amp;capacity_only=[true/false]
 *    &amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 */
public class ClusterLoadParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(TIME_PARAM);
    validParameterNames.add(END_MS_PARAM);
    validParameterNames.add(START_MS_PARAM);
    validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    validParameterNames.add(POPULATE_DISK_INFO_PARAM);
    validParameterNames.add(CAPACITY_ONLY_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected long _endMs;
  protected long _startMs;
  protected ModelCompletenessRequirements _requirements;
  protected boolean _allowCapacityEstimation;
  protected boolean _populateDiskInfo;
  protected boolean _capacityOnly;

  public ClusterLoadParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    Long time = ParameterUtils.time(_requestContext);
    _endMs = time == null ? ParameterUtils.endMsOrDefault(_requestContext, System.currentTimeMillis()) : time;
    _startMs = ParameterUtils.startMsOrDefault(_requestContext, ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL);
    ParameterUtils.validateTimeRange(_startMs, _endMs);
    _requirements = new ModelCompletenessRequirements(1, 0.0, true);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_requestContext);
    _populateDiskInfo = ParameterUtils.populateDiskInfo(_requestContext);
    _capacityOnly = ParameterUtils.capacityOnly(_requestContext);
  }

  public long startMs() {
    return _startMs;
  }

  public long endMs() {
    return _endMs;
  }

  public ModelCompletenessRequirements requirements() {
    return _requirements;
  }

  public boolean allowCapacityEstimation() {
    return _allowCapacityEstimation;
  }

  public boolean populateDiskInfo() {
    return _populateDiskInfo;
  }

  public boolean capacityOnly() {
    return _capacityOnly;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Parameters(value = {@Parameter(name = "start",
      in = ParameterIn.QUERY,
      description = "Start time of the cluster load. Default is time of earliest valid window.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "-1",
      defaultValue = "-1")),
      @Parameter(name = "end",
      in = ParameterIn.QUERY,
      description = "End time of the cluster load. Default is current system time.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "0")),
      @Parameter(name = "time",
      in = ParameterIn.QUERY,
      description = "End time of the clutser load. Default is current system time, mutually exclusive "
      + "with end parameter.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "0")),
      @Parameter(name = "allow_capacity_estimation",
      in = ParameterIn.QUERY,
      description = "Whether to allow capacity estimation when cruise-control is unable to obtain all "
      + "per-broker capacity information.",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
      @Parameter(name = "populate_disk_info",
      in = ParameterIn.QUERY,
      description = "Whether show the load of each disk of broker.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "capacity_only",
      in = ParameterIn.QUERY,
      description = "Whether show only the cluster capacity or the utilization, as well.",
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
