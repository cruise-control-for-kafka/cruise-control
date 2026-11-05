/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.areAllParametersNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicReplicationFactorChangeParameters.maybeBuildTopicReplicationFactorChangeParameters;


/**
 * Parameters for {@link CruiseControlEndPoint#TOPIC_CONFIGURATION}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>If topics to change replication factor and target replication factor is not specified in URL, they can also be
 *   specified in request body. The body format is expected to be valid JSON format. e.g.
 *    <pre><code>
 *   {
 *       replication_factor: {
 *           topic_by_replication_factor : {
 *               target_replication_factor_1 : topic_regex_1,
 *               target_replication_factor_2 : topic_regex_2,
 *               ...
 *       }
 *   }
 *   </code></pre>
 *   If user specifies new replication factor in both URL (via combination of `topic` and `replication_factor` parameter)
 *   and body, an exception will be thrown.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/topic_configuration?json=[true/false]&amp;verbose=[true/false]&amp;topic=[topic]
 *    &amp;replication_factor=[target_replication_factor]&amp;skip_rack_awareness_check=[true/false]
 *    &amp;dryRun=[true/false]&amp;goals=[goal1,goal2...]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;reason=[reason-for-request]
 *    &amp;stop_ongoing_execution=[true/false]&amp;get_response_schema=[true/false]&amp;fast_mode=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class TopicConfigurationParameters extends GoalBasedOptimizationParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(REVIEW_ID_PARAM);
    validParameterNames.add(DRY_RUN_PARAM);
    validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.addAll(TopicReplicationFactorChangeParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    validParameterNames.addAll(GoalBasedOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Integer _reviewId;
  protected boolean _dryRun;
  protected boolean _stopOngoingExecution;
  protected String _reason;
  protected TopicReplicationFactorChangeParameters _topicReplicationFactorChangeParameters;
  protected Map<String, ?> _configs;

  public TopicConfigurationParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
    _dryRun = ParameterUtils.getDryRun(_requestContext);
    boolean requestReasonRequired = _config.getBoolean(ExecutorConfig.REQUEST_REASON_REQUIRED_CONFIG);
    _reason = ParameterUtils.reason(_requestContext, requestReasonRequired && !_dryRun);
    _stopOngoingExecution = ParameterUtils.stopOngoingExecution(_requestContext);
    if (_stopOngoingExecution && _dryRun) {
      throw new UserRequestException(String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
    }
    _topicReplicationFactorChangeParameters = maybeBuildTopicReplicationFactorChangeParameters(_configs);
    if (areAllParametersNull(_topicReplicationFactorChangeParameters)) {
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

  public boolean dryRun() {
    return _dryRun;
  }

  public boolean stopOngoingExecution() {
    return _stopOngoingExecution;
  }

  public String reason() {
    return _reason;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _configs = configs;
  }

  public TopicReplicationFactorChangeParameters topicReplicationFactorChangeParameters() {
    return _topicReplicationFactorChangeParameters;
  }

  /**
   * Supported topic configuration type to be changed via {@link CruiseControlEndPoint#TOPIC_CONFIGURATION} endpoint.
   */
  public enum TopicConfigurationType {
    REPLICATION_FACTOR
  }

  @Parameters(value = {@Parameter(name = "dryrun",
      in = ParameterIn.QUERY,
      description = "Whether to dry-run the request or not.",
      schema = @Schema(type = "boolean",
      defaultValue = "true")),
      @Parameter(name = "topic",
      in = ParameterIn.QUERY,
      description = "Regular expression to specify subject topics.",
      schema = @Schema(type = "string",
      example = "topic_%5B0-9%5D.%2A")),
      @Parameter(name = "replication_factor",
      in = ParameterIn.QUERY,
      description = "Target replication factor.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1")),
      @Parameter(name = "data_from",
      in = ParameterIn.QUERY,
      description = "Whether to calculate proposal from available valid partitions or valid windows.",
      schema = @Schema(type = "string",
      defaultValue = "VALID_WINDOWS")),
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
      defaultValue = "false")),
      @Parameter(name = "concurrent_partition_movements_per_broker",
      in = ParameterIn.QUERY,
      description = "The upper bound of ongoing replica movements going into/out of each broker.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1")),
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
      minimum = "1")),
      @Parameter(name = "broker_concurrent_leader_movements",
      in = ParameterIn.QUERY,
      description = "The upper bound of per broker ongoing leadership movements.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "1")),
      @Parameter(name = "skip_hard_goal_check",
      in = ParameterIn.QUERY,
      description = "Whether to allow hard goals be skipped in proposal generation.",
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
      @Parameter(name = "excluded_topics",
      in = ParameterIn.QUERY,
      description = "Specify topic whose partition is excluded from replica movement.",
      schema = @Schema(type = "string",
      example = "__CruiseControl.%2A")),
      @Parameter(name = "execution_progress_check_interval_ms",
      in = ParameterIn.QUERY,
      description = "Execution progress check interval in milliseconds.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "5000")),
      @Parameter(name = "reason",
      in = ParameterIn.QUERY,
      description = "Reason for request.",
      schema = @Schema(type = "string")),
      @Parameter(name = "replica_movement_strategies",
      in = ParameterIn.QUERY,
      description = "Replica movement strategies to use.",
      array = @ArraySchema(schema = @Schema(type = "string"),
      arraySchema = @Schema(example = "[\"PrioritizeLargeReplicaMovementStrategy\",\"PostponeUrpReplicaMovementStrategy\"]"))),
      @Parameter(name = "replication_throttle",
      in = ParameterIn.QUERY,
      description = "Upper bound on the bandwidth in bytes per second used to move replicas.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "1")),
      @Parameter(name = "review_id",
      in = ParameterIn.QUERY,
      description = "Review id for 2-step verification.",
      schema = @Schema(type = "string")),
      @Parameter(name = "skip_rack_awareness_check",
      in = ParameterIn.QUERY,
      description = "Whether to allow rack awareness check to be skipped.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "stop_ongoing_execution",
      in = ParameterIn.QUERY,
      description = "Whether to stop the ongoing execution (if any) and start executing the given request.",
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
