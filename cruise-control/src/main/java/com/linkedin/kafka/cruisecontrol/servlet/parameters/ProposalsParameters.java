/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.IGNORE_PROPOSAL_CACHE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.KAFKA_ASSIGNER_MODE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DESTINATION_BROKER_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REBALANCE_DISK_MODE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;


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

  @Parameters(value = {@Parameter(name = "kafka_assigner",
      in = ParameterIn.QUERY,
      description = "Whether to use Kafka assigner mode to generate proposals.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "destination_broker_ids",
      in = ParameterIn.QUERY,
      description = "Specify brokers to move replicas to.",
      array = @ArraySchema(schema = @Schema(type = "integer",
      format = "int32"),
      arraySchema = @Schema(example = "[1,2,3]"))),
      @Parameter(name = "ignore_proposal_cache",
      in = ParameterIn.QUERY,
      description = "Whether to ignore the cached proposal or not.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "rebalance_disk",
      in = ParameterIn.QUERY,
      description = "whether to balance load between disks within brokers (requires JBOD Kafka deployment).",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "data_from",
      in = ParameterIn.QUERY,
      description = "Whether to calculate proposal from available valid partitions or valid windows.",
      schema = @Schema(implementation = ParameterUtils.DataFrom.class,
      defaultValue = "VALID_WINDOWS")),
      @Parameter(name = "use_ready_default_goals",
      in = ParameterIn.QUERY,
      description = "Whether to use only ready goals to generate proposals.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "excluded_topics",
      in = ParameterIn.QUERY,
      description = "Specify topic whose partition is excluded from replica movement.",
      example = "__CruiseControl.%2A",
      schema = @Schema(type = "string")),
      @Parameter(name = "exclude_recently_removed_brokers",
      in = ParameterIn.QUERY,
      description = "Whether allow replicas to be moved to recently removed broker.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "goals",
      in = ParameterIn.QUERY,
      description = "List of goals used to generate proposal, the default goals will be used if this "
      + "parameter is not specified.",
      array = @ArraySchema(schema = @Schema(type = "string"),
      arraySchema = @Schema(example = "[\"RackAwareGoal\",\"ReplicaCapacityGoal\",\"ReplicaDistributionGoal\"]"))),
      @Parameter(name = "allow_capacity_estimation",
      in = ParameterIn.QUERY,
      description = "Whether to allow broker capacity to be estimated.",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
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
      @Parameter(name = "fast_mode",
      in = ParameterIn.QUERY,
      description = "True to compute proposals in fast mode, false otherwise",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
      @Parameter(name = "reason",
      in = ParameterIn.QUERY,
      description = "Reason for request.",
      schema = @Schema(type = "string"))})
  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
