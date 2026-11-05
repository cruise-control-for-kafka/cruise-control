/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.AVG_LOAD_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_ID_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.END_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ENTRIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MAX_LOAD_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MIN_VALID_PARTITION_RATIO_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RESOURCE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.START_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TOPIC_PARAM;

/**
 * Parameters for {@link CruiseControlEndPoint#PARTITION_LOAD}
 *
 * <pre>
 * Get the partition load sorted by the utilization of a given resource and filtered by given topic regular expression
 *    and partition number/range
 *    GET /kafkacruisecontrol/partition_load?resource=[RESOURCE]&amp;start=[START_TIMESTAMP]&amp;end=[END_TIMESTAMP]
 *    &amp;entries=[number-of-entries-to-show]&amp;topic=[topic]&amp;partition=[partition/start_partition-end_partition]
 *    &amp;min_valid_partition_ratio=[min_valid_partition_ratio]&amp;allow_capacity_estimation=[true/false]
 *    &amp;max_load=[true/false]&amp;avg_load=[true/false]&amp;json=[true/false]&amp;brokerid=[brokerid]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 */
public class PartitionLoadParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(RESOURCE_PARAM);
    validParameterNames.add(START_MS_PARAM);
    validParameterNames.add(END_MS_PARAM);
    validParameterNames.add(ENTRIES_PARAM);
    validParameterNames.add(TOPIC_PARAM);
    validParameterNames.add(PARTITION_PARAM);
    validParameterNames.add(MIN_VALID_PARTITION_RATIO_PARAM);
    validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    validParameterNames.add(MAX_LOAD_PARAM);
    validParameterNames.add(AVG_LOAD_PARAM);
    validParameterNames.add(BROKER_ID_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Resource _resource;
  protected long _startMs;
  protected long _endMs;
  protected int _entries;
  protected Pattern _topic;
  protected int _partitionUpperBoundary;
  protected int _partitionLowerBoundary;
  protected Double _minValidPartitionRatio;
  protected boolean _allowCapacityEstimation;
  protected boolean _wantMaxLoad;
  protected boolean _wantAvgLoad;
  protected Set<Integer> _brokerIds;

  public PartitionLoadParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    String resourceString = ParameterUtils.resourceString(_requestContext);
    try {
      _resource = Resource.valueOf(resourceString.toUpperCase());
    } catch (IllegalArgumentException iae) {
      throw new UserRequestException(String.format("Invalid resource type %s. The resource type must be one of the "
                                                   + "following: CPU, DISK, NW_IN, NW_OUT", resourceString));
    }

    _wantMaxLoad = ParameterUtils.wantMaxLoad(_requestContext);
    _wantAvgLoad = ParameterUtils.wantAvgLoad(_requestContext);
    if (_wantMaxLoad && _wantAvgLoad) {
      throw new UserRequestException("Parameters to ask for max and avg load are mutually exclusive to each other.");
    }
    _topic = ParameterUtils.topic(_requestContext);
    _partitionLowerBoundary = ParameterUtils.partitionBoundary(_requestContext, false);
    _partitionUpperBoundary = ParameterUtils.partitionBoundary(_requestContext, true);
    _entries = ParameterUtils.entries(_requestContext);
    _minValidPartitionRatio = ParameterUtils.minValidPartitionRatio(_requestContext);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_requestContext);
    _brokerIds = ParameterUtils.brokerIds(_requestContext, true);
    _startMs = ParameterUtils.startMsOrDefault(_requestContext, ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL);
    _endMs = ParameterUtils.endMsOrDefault(_requestContext, System.currentTimeMillis());
    ParameterUtils.validateTimeRange(_startMs, _endMs);
  }

  public Resource resource() {
    return _resource;
  }

  public long startMs() {
    return _startMs;
  }

  public long endMs() {
    return _endMs;
  }

  public int entries() {
    return _entries;
  }

  public Pattern topic() {
    return _topic;
  }

  public int partitionUpperBoundary() {
    return _partitionUpperBoundary;
  }

  public int partitionLowerBoundary() {
    return _partitionLowerBoundary;
  }

  public Double minValidPartitionRatio() {
    return _minValidPartitionRatio;
  }

  public boolean allowCapacityEstimation() {
    return _allowCapacityEstimation;
  }

  public boolean wantMaxLoad() {
    return _wantMaxLoad;
  }

  public boolean wantAvgLoad() {
    return _wantAvgLoad;
  }

  public Set<Integer> brokerIds() {
    return Collections.unmodifiableSet(_brokerIds);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Parameters(value = {@Parameter(name = "resource",
      in = ParameterIn.QUERY,
      description = "The host and broker-level resource by which to sort the cruise-control response.",
      schema = @Schema(type = "string",
      defaultValue = "disk",
      implementation = Resource.class)),
      @Parameter(name = "start",
      in = ParameterIn.QUERY,
      description = "The timestamp in millisecond of the earliest metric sample use to generate load.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "-1",
      defaultValue = "-1")),
      @Parameter(name = "end",
      in = ParameterIn.QUERY,
      description = "The timestamp in millisecond of the latest metric sample use to generate load, "
      + "current time will be used if this parameter is not specified.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "0")),
      @Parameter(name = "entries",
      in = ParameterIn.QUERY,
      description = "The number of entries to show in the response.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1",
      defaultValue = "2147483647")),
      @Parameter(name = "topic",
      in = ParameterIn.QUERY,
      description = "A regular expression used to filter the partition load returned based on topic.",
      schema = @Schema(type = "string",
      example = "myTopic.*")),
      @Parameter(name = "partition",
      in = ParameterIn.QUERY,
      description = "The partition or partition range to filter partition load returned.",
      schema = @Schema(oneOf = {Integer.class,
      com.linkedin.kafka.cruisecontrol.servlet.response.OpenApiSchemaTypes.PartitionRange.class})),
      @Parameter(name = "min_valid_partition_ratio",
      in = ParameterIn.QUERY,
      description = "The minimum required ratio of partition load data completeness.",
      schema = @Schema(type = "number",
      format = "double",
      minimum = "0.0",
      maximum = "1.0",
      defaultValue = "0.98")),
      @Parameter(name = "allow_capacity_estimation",
      in = ParameterIn.QUERY,
      description = "Whether to allow capacity estimation when cruise-control is unable to obtain all "
      + "per-broker capacity information.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "max_load",
      in = ParameterIn.QUERY,
      description = "If true, the maximum load is returned.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "avg_load",
      in = ParameterIn.QUERY,
      description = "If true, the average load is returned.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "brokerid",
      in = ParameterIn.QUERY,
      description = "Set of broker ids used to filter partition load returned.",
      array = @ArraySchema(schema = @Schema(type = "integer",
      format = "int32"),
      arraySchema = @Schema(example = "[1,2,3]"))),
      @Parameter(name = "reason",
      in = ParameterIn.QUERY,
      description = "Reason for request.",
      schema = @Schema(type = "string"))})
  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
