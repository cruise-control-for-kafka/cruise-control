/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


/**
 * Optional parameters for {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#TOPIC_CONFIGURATION}.
 * This class holds all the request parameters for {@link TopicConfigurationParameters.TopicConfigurationType#REPLICATION_FACTOR}.
 */
public class TopicReplicationFactorChangeParameters extends AbstractParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(TOPIC_PARAM);
        validParameterNames.add(REPLICATION_FACTOR_PARAM);
        validParameterNames.add(SKIP_RACK_AWARENESS_CHECK_PARAM);
        validParameterNames.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
        validParameterNames.add(MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
        validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
        validParameterNames.add(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM);
        validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
        validParameterNames.add(SKIP_HARD_GOAL_CHECK_PARAM);
        validParameterNames.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
        validParameterNames.add(REPLICATION_THROTTLE_PARAM);
        validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    protected Map<Short, Pattern> _topicPatternByReplicationFactor;
    protected boolean _skipRackAwarenessCheck;
    protected Integer _concurrentInterBrokerPartitionMovements;
    protected Integer _maxInterBrokerPartitionMovements;
    protected Integer _clusterLeaderMovementConcurrency;
    protected Integer _brokerLeaderMovementConcurrency;
    protected Long _executionProgressCheckIntervalMs;
    protected boolean _skipHardGoalCheck;
    protected ReplicaMovementStrategy _replicaMovementStrategy;
    protected Long _replicationThrottle;

    protected TopicReplicationFactorChangeParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _topicPatternByReplicationFactor = ParameterUtils.topicPatternByReplicationFactor(_requestContext);
        if (_topicPatternByReplicationFactor.keySet().stream().anyMatch(rf -> rf < 1)) {
            throw new UserRequestException("Target replication factor cannot be set to smaller than 1.");
        }
        _skipRackAwarenessCheck = ParameterUtils.skipRackAwarenessCheck(_requestContext);
        _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTER_BROKER_REPLICA);
        _maxInterBrokerPartitionMovements = ParameterUtils.maxPartitionMovements(_requestContext);
        _clusterLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_CLUSTER);
        _brokerLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_BROKER);
        _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_requestContext);
        _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_requestContext);
        _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_requestContext, _config);
        _replicationThrottle = ParameterUtils.replicationThrottle(_requestContext, _config);
    }

    /**
     * Create a {@link TopicReplicationFactorChangeParameters} object from the request.
     *
     * @param configs Information collected from request and Cruise Control configs.
     * @return A TopicReplicationFactorChangeParameters object; or null if any required parameter is not specified in the request.
     */
    static TopicReplicationFactorChangeParameters maybeBuildTopicReplicationFactorChangeParameters(Map<String, ?> configs)
            throws UnsupportedEncodingException {
        TopicReplicationFactorChangeParameters topicReplicationFactorChangeParameters = new TopicReplicationFactorChangeParameters();
        topicReplicationFactorChangeParameters.configure(configs);
        topicReplicationFactorChangeParameters.initParameters();
        // At least one pair of target topic pattern and target replication factor should be explicitly specified in the request;
        // otherwise, return null.
        if (topicReplicationFactorChangeParameters.topicPatternByReplicationFactor().isEmpty()) {
            return null;
        }
        return topicReplicationFactorChangeParameters;
    }

    public Map<Short, Pattern> topicPatternByReplicationFactor() {
        return Collections.unmodifiableMap(_topicPatternByReplicationFactor);
    }

    public boolean skipRackAwarenessCheck() {
        return _skipRackAwarenessCheck;
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

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
