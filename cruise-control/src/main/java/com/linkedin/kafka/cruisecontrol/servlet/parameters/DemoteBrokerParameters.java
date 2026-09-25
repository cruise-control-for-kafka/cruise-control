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
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Parameters for {@link CruiseControlEndPoint#DEMOTE_BROKER}
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>Note that "brokerid_and_logdirs" takes comma as delimiter between two broker id and logdir pairs -- i.e. we assume
 *   a valid logdir name contains no comma.</li>
 * </ul>
 *
 * <pre>
 * Demote a broker
 *    POST /kafkacruisecontrol/demote_broker?brokerid=[id1,id2...]&amp;dryRun=[true/false]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;allow_capacity_estimation=[true/false]&amp;json=[true/false]
 *    &amp;skip_urp_demotion=[true/false]&amp;exclude_follower_demotion=[true/false]&amp;verbose=[true/false]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;brokerid_and_logdirs=[broker_id1-logdir1,broker_id2-logdir2]&amp;review_id=[id]
 *    &amp;replication_throttle=[bytes_per_second]&amp;reason=[reason-for-request]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;stop_ongoing_execution=[true/false]
 *    &amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class DemoteBrokerParameters extends KafkaOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(DRY_RUN_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(BROKER_ID_PARAM);
        validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
        validParameterNames.add(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM);
        validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
        validParameterNames.add(SKIP_URP_DEMOTION_PARAM);
        validParameterNames.add(EXCLUDE_FOLLOWER_DEMOTION_PARAM);
        validParameterNames.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
        validParameterNames.add(REPLICATION_THROTTLE_PARAM);
        validParameterNames.add(REVIEW_ID_PARAM);
        validParameterNames.add(BROKER_ID_AND_LOGDIRS_PARAM);
        validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
        validParameterNames.addAll(KafkaOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected boolean _dryRun;
    protected Set<Integer> _brokerIds;
    protected Integer _clusterLeaderMovementConcurrency;
    protected Integer _brokerLeaderMovementConcurrency;
    protected Long _executionProgressCheckIntervalMs;
    protected boolean _skipUrpDemotion;
    protected boolean _excludeFollowerDemotion;
    protected ReplicaMovementStrategy _replicaMovementStrategy;
    protected Long _replicationThrottle;
    protected Integer _reviewId;
    protected Map<Integer, Set<String>> _logdirByBrokerId;
    protected String _reason;
    protected boolean _stopOngoingExecution;

    public DemoteBrokerParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _brokerIds = ParameterUtils.brokerIds(_requestContext, false);
        _dryRun = ParameterUtils.getDryRun(_requestContext);
        _clusterLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_CLUSTER);
        _brokerLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_BROKER);
        _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_requestContext);
        _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_requestContext);
        _skipUrpDemotion = ParameterUtils.skipUrpDemotion(_requestContext);
        _excludeFollowerDemotion = ParameterUtils.excludeFollowerDemotion(_requestContext);
        _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_requestContext, _config);
        _replicationThrottle = ParameterUtils.replicationThrottle(_requestContext, _config);
        boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
        _logdirByBrokerId = ParameterUtils.brokerIdAndLogdirs(_requestContext);
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

    public Set<Integer> brokerIds() {
        return _brokerIds;
    }

    public Integer clusterLeaderMovementConcurrency() {
        return _clusterLeaderMovementConcurrency;
    }

    public Integer brokerLeaderMovementConcurrency() {
        return _brokerLeaderMovementConcurrency;
    }

    public Long executionProgressCheckIntervalMs() {
        return _executionProgressCheckIntervalMs;
    }

    public boolean skipUrpDemotion() {
        return _skipUrpDemotion;
    }

    public boolean excludeFollowerDemotion() {
        return _excludeFollowerDemotion;
    }

    public Map<Integer, Set<String>> brokerIdAndLogdirs() {
        return _logdirByBrokerId;
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
