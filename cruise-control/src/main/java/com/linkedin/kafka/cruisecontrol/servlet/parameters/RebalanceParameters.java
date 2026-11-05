/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_LEADER_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICA_MOVEMENT_STRATEGIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICATION_THROTTLE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#REBALANCE}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 * Trigger a workload balance.
 *    POST /kafkacruisecontrol/rebalance?dryRun=[true/false]&amp;goals=[goal1,goal2...]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_intra_broker_partition_movements=[POSITIVE-INTEGER]&amp;concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]
 *    &amp;use_ready_default_goals=[true/false]&amp;verbose=[true/false]&amp;exclude_recently_demoted_brokers=[true/false]
 *    &amp;exclude_recently_removed_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;ignore_proposal_cache=[true/false]&amp;destination_broker_ids=[id1,id2...]&amp;kafka_assigner=[true/false]
 *    &amp;rebalance_disk=[true/false]&amp;review_id=[id]&amp;get_response_schema=[true/false]
 *    &amp;replication_throttle=[bytes_per_second]&amp;reason=[reason-for-request]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;stop_ongoing_execution=[true/false]&amp;fast_mode=[true/false]
 *    &amp;doAs=[user]
 * </pre>
 */
public class RebalanceParameters extends ProposalsParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(DRY_RUN_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    validParameterNames.add(MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
    validParameterNames.add(CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM);
    validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.add(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
    validParameterNames.add(SKIP_HARD_GOAL_CHECK_PARAM);
    validParameterNames.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    validParameterNames.add(REPLICATION_THROTTLE_PARAM);
    validParameterNames.add(REVIEW_ID_PARAM);
    validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
    validParameterNames.addAll(ProposalsParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected boolean _dryRun;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _maxInterBrokerPartitionMovements;
  protected Integer _concurrentIntraBrokerPartitionMovements;
  protected Integer _clusterLeaderMovementConcurrency;
  protected Integer _brokerLeaderMovementConcurrency;
  protected Long _executionProgressCheckIntervalMs;
  protected boolean _skipHardGoalCheck;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Long _replicationThrottle;
  protected Integer _reviewId;
  protected String _reason;
  protected boolean _stopOngoingExecution;

  public RebalanceParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _dryRun = ParameterUtils.getDryRun(_requestContext);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTER_BROKER_REPLICA);
    _maxInterBrokerPartitionMovements = ParameterUtils.maxPartitionMovements(_requestContext);
    _concurrentIntraBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTRA_BROKER_REPLICA);
    _clusterLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_CLUSTER);
    _brokerLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_BROKER);
    _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_requestContext);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_requestContext);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_requestContext, _config);
    _ignoreProposalCache = ParameterUtils.ignoreProposalCache(_requestContext);
    _destinationBrokerIds = ParameterUtils.destinationBrokerIds(_requestContext);
    boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _replicationThrottle = ParameterUtils.replicationThrottle(_requestContext, _config);
    _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
    _isRebalanceDiskMode = ParameterUtils.isRebalanceDiskMode(_requestContext);
    boolean requestReasonRequired = _config.getBoolean(ExecutorConfig.REQUEST_REASON_REQUIRED_CONFIG);
    _reason = ParameterUtils.reason(_requestContext, requestReasonRequired && !_dryRun);
    _stopOngoingExecution = ParameterUtils.stopOngoingExecution(_requestContext);
    if (_stopOngoingExecution && _dryRun) {
      throw new UserRequestException(String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
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

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer concurrentIntraBrokerPartitionMovements() {
    return _concurrentIntraBrokerPartitionMovements;
  }

  public Integer maxInterBrokerPartitionMovements() {
    return _maxInterBrokerPartitionMovements;
  }

  public Long executionProgressCheckIntervalMs() {
    return _executionProgressCheckIntervalMs;
  }

  public Integer clusterConcurrentLeaderMovements() {
    return _clusterLeaderMovementConcurrency;
  }

  public Integer brokerConcurrentLeaderMovements() {
    return _brokerLeaderMovementConcurrency;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }

  public String reason() {
    return _reason;
  }

  public boolean stopOngoingExecution() {
    return _stopOngoingExecution;
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
      @Parameter(name = "stop_ongoing_execution",
      in = ParameterIn.QUERY,
      description = "Whether to stop the ongoing execution (if any) and start executing the given request.",
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
      @Parameter(name = "concurrent_intra_broker_partition_movements",
      in = ParameterIn.QUERY,
      description = "The upper bound of ongoing replica movements between disks within each broker.",
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
      @Parameter(name = "excluded_topics",
      in = ParameterIn.QUERY,
      description = "Specify topic whose partition is excluded from replica movement.",
      schema = @Schema(type = "string",
      example = "__CruiseControl.%2A")),
      @Parameter(name = "use_ready_default_goals",
      in = ParameterIn.QUERY,
      description = "Whether to only use ready goals to generate proposal.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "data_from",
      in = ParameterIn.QUERY,
      description = "Whether to calculate proposal from available valid partitions or valid windows.",
      schema = @Schema(type = "string",
      defaultValue = "VALID_WINDOWS")),
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
      defaultValue = "true")),
      @Parameter(name = "replica_movement_strategies",
      in = ParameterIn.QUERY,
      description = "Replica movement strategies to use.",
      array = @ArraySchema(schema = @Schema(type = "string"),
      arraySchema = @Schema(example = "[\"PrioritizeLargeReplicaMovementStrategy\",\"PostponeUrpReplicaMovementStrategy\"]"))),
      @Parameter(name = "ignore_proposal_cache",
      in = ParameterIn.QUERY,
      description = "Whether to ignore the cached proposal or not.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "destination_broker_ids",
      in = ParameterIn.QUERY,
      description = "Specify the destination brokers for partitions to move replicas to.",
      array = @ArraySchema(schema = @Schema(type = "string"),
      arraySchema = @Schema(example = "[1,2,3]"))),
      @Parameter(name = "kafka_assigner",
      in = ParameterIn.QUERY,
      description = "Whether to use Kafka assigner mode to generate proposals.",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "rebalance_disk",
      in = ParameterIn.QUERY,
      description = "Whether to balance load between disks within brokers (requires JBOD Kafka deployment).",
      schema = @Schema(type = "boolean",
      defaultValue = "false")),
      @Parameter(name = "review_id",
      in = ParameterIn.QUERY,
      description = "Review id for 2-step verification.",
      schema = @Schema(type = "integer",
      format = "int32",
      minimum = "0")),
      @Parameter(name = "replication_throttle",
      in = ParameterIn.QUERY,
      description = "Upper bound on the bandwidth in bytes per second used to move replicas.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "1")),
      @Parameter(name = "execution_progress_check_interval_ms",
      in = ParameterIn.QUERY,
      description = "Execution progress check interval in milliseconds.",
      schema = @Schema(type = "integer",
      format = "int64",
      minimum = "5000")),
      @Parameter(name = "reason",
      in = ParameterIn.QUERY,
      description = "Reason for request.",
      example = "Balance disk utilization across all brokers in the cluster.",
      schema = @Schema(type = "string")),
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
