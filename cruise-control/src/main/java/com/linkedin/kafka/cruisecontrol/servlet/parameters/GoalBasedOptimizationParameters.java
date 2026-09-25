/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;


public abstract class GoalBasedOptimizationParameters extends KafkaOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;

    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(DATA_FROM_PARAM);
        validParameterNames.add(USE_READY_DEFAULT_GOALS_PARAM);
        validParameterNames.add(EXCLUDED_TOPICS_PARAM);
        validParameterNames.add(EXCLUDE_RECENTLY_REMOVED_BROKERS_PARAM);
        validParameterNames.add(GOALS_PARAM);
        validParameterNames.add(FAST_MODE_PARAM);
        validParameterNames.addAll(KafkaOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }

    private static final boolean INCLUDE_ALL_TOPICS = true;
    private static final Map<ParameterUtils.DataFrom, Integer> MIN_NUM_VALID_WINDOWS =
            Map.of(ParameterUtils.DataFrom.VALID_PARTITIONS, Integer.MAX_VALUE, ParameterUtils.DataFrom.VALID_WINDOWS, 1);
    private static final Map<ParameterUtils.DataFrom, Double> MIN_VALID_PARTITIONS_RATIO =
            Map.of(ParameterUtils.DataFrom.VALID_PARTITIONS, 0.0, ParameterUtils.DataFrom.VALID_WINDOWS, 1.0);
    protected ParameterUtils.DataFrom _dataFrom;
    protected boolean _useReadyDefaultGoals;
    protected Pattern _excludedTopics;
    protected boolean _excludeRecentlyRemovedBrokers;
    protected GoalsAndRequirements _goalsAndRequirements;
    protected boolean _fastMode;

    GoalBasedOptimizationParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _dataFrom = ParameterUtils.getDataFrom(_requestContext);
        _useReadyDefaultGoals = ParameterUtils.useReadyDefaultGoals(_requestContext);
        _excludedTopics = ParameterUtils.excludedTopics(_requestContext);
        _excludeRecentlyRemovedBrokers = ParameterUtils.excludeRecentlyRemovedBrokers(_requestContext);
        List<String> goals = ParameterUtils.getGoals(_requestContext);
        _goalsAndRequirements = new GoalsAndRequirements(goals, getRequirements(_dataFrom));
        _fastMode = ParameterUtils.fastMode(_requestContext);
    }

    public ParameterUtils.DataFrom dataFrom() {
        return _dataFrom;
    }

    public boolean useReadyDefaultGoals() {
        return _useReadyDefaultGoals;
    }

    public List<String> goals() {
        return _goalsAndRequirements.goals();
    }

    public ModelCompletenessRequirements modelCompletenessRequirements() {
        return _goalsAndRequirements.requirements();
    }

    public Pattern excludedTopics() {
        return _excludedTopics;
    }

    public boolean excludeRecentlyRemovedBrokers() {
        return _excludeRecentlyRemovedBrokers;
    }

    public boolean fastMode() {
        return _fastMode;
    }

    protected static ModelCompletenessRequirements getRequirements(ParameterUtils.DataFrom dataFrom) {
        return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS.get(dataFrom),
                MIN_VALID_PARTITIONS_RATIO.get(dataFrom),
                INCLUDE_ALL_TOPICS);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
    }
}
