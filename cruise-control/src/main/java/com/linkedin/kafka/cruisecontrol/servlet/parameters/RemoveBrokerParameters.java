/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.THROTTLE_REMOVED_BROKER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DESTINATION_BROKER_IDS_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#REMOVE_BROKER}
 *<ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *</ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;throttle_removed_broker=[true/false]&amp;goals=[goal1,goal2...]&amp;allow_capacity_estimation=[true/false]
 *    &amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;kafka_assigner=[true/false]
 *    &amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;destination_broker_ids=[id1,id2...]&amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;reason=[reason-for-request]
 *    &amp;stop_ongoing_execution=[true/false]&amp;get_response_schema=[true/false]&amp;fast_mode=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class RemoveBrokerParameters extends AddedOrRemovedBrokerParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(THROTTLE_REMOVED_BROKER_PARAM);
    validParameterNames.add(DESTINATION_BROKER_IDS_PARAM);
    validParameterNames.addAll(AddedOrRemovedBrokerParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected boolean _throttleRemovedBrokers;
  protected Set<Integer> _destinationBrokerIds;

  public RemoveBrokerParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _throttleRemovedBrokers = ParameterUtils.throttleAddedOrRemovedBrokers(_requestContext, _endPoint);
    _destinationBrokerIds = ParameterUtils.destinationBrokerIds(_requestContext);
  }

  public boolean throttleRemovedBrokers() {
    return _throttleRemovedBrokers;
  }

  public Set<Integer> destinationBrokerIds() {
    return _destinationBrokerIds;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Parameters(value = {@Parameter(name = "brokerid",
      in = ParameterIn.QUERY,
      description = "List of target broker ids.",
      required = true,
      array = @ArraySchema(schema = @Schema(type = "integer"))),
      @Parameter(name = "dryrun",
      in = ParameterIn.QUERY,
      description = "Whether to dry-run the request or not.",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
      @Parameter(name = "throttle_removed_broker",
      in = ParameterIn.QUERY,
      description = "Whether to throttle the added broker.",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
      @Parameter(name = "goals",
      in = ParameterIn.QUERY,
      description = "List of goals used to generate proposal, the default goals will be used if this "
      + "parameter is not specified.",
      array = @ArraySchema(schema = @Schema(type = "string"),
      arraySchema = @Schema(example = "[\"RackAwareGoal\",\"ReplicaCapacityGoal\",\"ReplicaDistributionGoal\"]"))),
      @Parameter(name = "allow_capacity_estimation",
      in = ParameterIn.QUERY,
      description = "Whether to allow capacity estimation when cruise-control is unable to obtain all "
      + "per-broker capacity information.",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
      @Parameter(name = "concurrent_partition_movements_per_broker",
      in = ParameterIn.QUERY,
      description = "The upper bound of ongoing replica movements going into/out of each broker.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1",
      defaultValue = "5")),
      @Parameter(name = "max_partition_movements_in_cluster",
      in = ParameterIn.QUERY,
      description = "Change upper bound of ongoing inter broker partition movements in cluster.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1")),
      @Parameter(name = "concurrent_leader_movements",
      in = ParameterIn.QUERY,
      description = "The upper bound of cluster ongoing leadership movements.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1",
      defaultValue = "1000")),
      @Parameter(name = "broker_concurrent_leader_movements",
      in = ParameterIn.QUERY,
      description = "The upper bound of per broker ongoing leadership movements.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1",
      defaultValue = "250")),
      @Parameter(name = "skip_hard_goal_check",
      in = ParameterIn.QUERY,
      description = "Whether to allow hard goals be skipped in proposal generation.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "excluded_topics",
      in = ParameterIn.QUERY,
      description = "Specify topic whose partition is excluded from replica movement.",
      schema = @Schema(type = "string",
      example = "__CruiseControl.%2A")),
      @Parameter(name = "kafka_assigner",
      in = ParameterIn.QUERY,
      description = "Whether to use Kafka assigner mode to generate proposals.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "use_ready_default_goals",
      in = ParameterIn.QUERY,
      description = "Whether to only use ready goals to generate proposal.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "verbose",
      in = ParameterIn.QUERY,
      description = "Return detailed state information.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "exclude_recently_demoted_brokers",
      in = ParameterIn.QUERY,
      description = "Whether to allow leader replicas to be moved to recently demoted brokers.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "exclude_recently_removed_brokers",
      in = ParameterIn.QUERY,
      description = "Whether to allow replicas to be moved to recently removed broker.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "replica_movement_strategies",
      in = ParameterIn.QUERY,
      description = "Replica movement strategies to use.",
      array = @ArraySchema(schema = @Schema(type = "string"),
      arraySchema = @Schema(example = "[\"PrioritizeLargeReplicaMovementStrategy\",\"PostponeUrpReplicaMovementStrategy\"]"))),
      @Parameter(name = "destination_broker_ids",
      in = ParameterIn.QUERY,
      description = "Comma-separated and/or space-separated list of broker IDs.",
      array = @ArraySchema(schema = @Schema(type = "integer"))),
      @Parameter(name = "execution_progress_check_interval_ms",
      in = ParameterIn.QUERY,
      description = "Execution progress check interval in milliseconds.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "5000")),
      @Parameter(name = "stop_ongoing_execution",
      in = ParameterIn.QUERY,
      description = "Whether to stop the ongoing execution (if any) and start executing the given request.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "reason",
      in = ParameterIn.QUERY,
      description = "Reason for request.",
      schema = @Schema(type = "string")),
      @Parameter(name = "review_id",
      in = ParameterIn.QUERY,
      description = "Review id for 2-step verification.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "0")),
      @Parameter(name = "data_from",
      in = ParameterIn.QUERY,
      description = "Whether to calculate proposal from available valid partitions or valid windows.",
      schema = @Schema(type = "string",
      defaultValue = "VALID_WINDOWS")),
      @Parameter(name = "replication_throttle",
      in = ParameterIn.QUERY,
      description = "Upper bound on the bandwidth in bytes per second used to move replicas.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "1")),
      @Parameter(name = "fast_mode",
      in = ParameterIn.QUERY,
      description = "True to compute proposals in fast mode, false otherwise",
      schema = @Schema(type = "boolean",
      defaultValue = "true"))})
  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
