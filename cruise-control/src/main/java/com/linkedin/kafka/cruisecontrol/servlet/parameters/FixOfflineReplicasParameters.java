/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

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

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#FIX_OFFLINE_REPLICAS}
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
 *
 * <pre>
 * Fix offline replicas
 *    POST /kafkacruisecontrol/fix_offline_replicas?dryrun=[true/false]&amp;goals=[goal1,goal2...]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;json=[true/false]&amp;skip_hard_goal_check=[true/false]
 *    &amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]
 *    &amp;excluded_topics=[pattern]&amp;use_ready_default_goals=[true/false]&amp;data_from=[valid_windows/valid_partitions]
 *    &amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;replication_throttle=[bytes_per_second]
 *    &amp;review_id=[id]&amp;reason=[reason-for-request]&amp;get_response_schema=[true/false]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;stop_ongoing_execution=[true/false]&amp;fast_mode=[true/false]
 *    &amp;doAs=[user]
 * </pre>
 */
public class FixOfflineReplicasParameters extends GoalBasedOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(DRY_RUN_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
        validParameterNames.add(MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
        validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
        validParameterNames.add(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM);
        validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
        validParameterNames.add(SKIP_HARD_GOAL_CHECK_PARAM);
        validParameterNames.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
        validParameterNames.add(REPLICATION_THROTTLE_PARAM);
        validParameterNames.add(REVIEW_ID_PARAM);
        validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
        validParameterNames.addAll(GoalBasedOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected boolean _dryRun;
    protected Integer _concurrentInterBrokerPartitionMovements;
    protected Integer _maxInterBrokerPartitionMovements;
    protected Integer _clusterLeaderMovementConcurrency;
    protected Integer _brokerLeaderMovementConcurrency;
    protected Long _executionProgressCheckIntervalMs;
    protected boolean _skipHardGoalCheck;
    protected ReplicaMovementStrategy _replicaMovementStrategy;
    protected Long _replicationThrottle;
    protected Integer _reviewId;
    protected String _reason;
    protected boolean _stopOngoingExecution;

    public FixOfflineReplicasParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _dryRun = ParameterUtils.getDryRun(_requestContext);
        _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTER_BROKER_REPLICA);
        _maxInterBrokerPartitionMovements = ParameterUtils.maxPartitionMovements(_requestContext);
        _clusterLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_CLUSTER);
        _brokerLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_BROKER);
        _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_requestContext);
        _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_requestContext);
        _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_requestContext, _config);
        _replicationThrottle = ParameterUtils.replicationThrottle(_requestContext, _config);
        boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
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

    public Integer maxInterBrokerPartitionMovements() {
        return _maxInterBrokerPartitionMovements;
    }

    public Long executionProgressCheckIntervalMs() {
        return _executionProgressCheckIntervalMs;
    }

    public Integer clusterLeaderMovementConcurrency() {
        return _clusterLeaderMovementConcurrency;
    }

    public Integer brokerLeaderMovementConcurrency() {
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

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
