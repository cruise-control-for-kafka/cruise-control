/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DESTINATION_BROKER_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.THROTTLE_REMOVED_BROKER_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#REMOVE_BROKER}
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 * </ul>
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

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
}
