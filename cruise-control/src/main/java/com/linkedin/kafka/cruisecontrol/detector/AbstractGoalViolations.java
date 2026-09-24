/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.*;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;


/**
 * An abstract class that should be extended for Goal violations
 */
abstract class AbstractGoalViolations extends KafkaAnomaly {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGoalViolations.class);
    public static final String UNFIXABLE_GOAL_VIOLATIONS = "Unfixable goal violations";
    public static final String FIXABLE_GOAL_VIOLATIONS = "Fixable goal violations";
    // The priority order of goals is maintained here.
    protected Map<Boolean, List<String>> _violatedGoalsByFixability;
    protected boolean _excludeRecentlyDemotedBrokers;
    protected boolean _excludeRecentlyRemovedBrokers;
    protected RebalanceRunnable _rebalanceRunnable;
    protected ProvisionResponse _provisionResponse;

    /**
     * An anomaly to indicate goal violation(s).
     */
    AbstractGoalViolations() {
    }

    /**
     * Add detected goal violation.
     *
     * @param goalName The name of the goal.
     * @param fixable Whether the violated goal is fixable or not.
     */
    void addViolation(String goalName, boolean fixable) {
        _violatedGoalsByFixability.computeIfAbsent(fixable, k -> new ArrayList<>()).add(goalName);
    }

    /**
     * @param provisionResponse Aggregated provision response corresponding to this goal violation.
     */
    public void setProvisionResponse(ProvisionResponse provisionResponse) {
        _provisionResponse = provisionResponse;
    }

    /**
     * @return Aggregated provision response corresponding to this goal violation.
     */
    public ProvisionResponse provisionResponse() {
        return _provisionResponse;
    }

    /**
     * @return All the goal violations.
     */
    public Map<Boolean, List<String>> violatedGoalsByFixability() {
        return Collections.unmodifiableMap(_violatedGoalsByFixability);
    }

    @Override
    public boolean fix() throws KafkaCruiseControlException {
        boolean hasProposalsToFix = false;
        if (_violatedGoalsByFixability.get(false) == null) {
            try {
                // Fix the fixable goal violations with rebalance operation.
                _optimizationResult = new OptimizationResult(_rebalanceRunnable.computeResult(), null);
                hasProposalsToFix = hasProposalsToFix();
                // Ensure that only the relevant response is cached to avoid memory pressure.
                _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
            } catch (IllegalStateException e) {
                LOG.warn("Got exception when trying to fix the cluster for violated goals {}: {}",
                        _violatedGoalsByFixability.get(true), e.getMessage());
            }
        } else {
            LOG.info("Skip fixing goal violations due to unfixable goal violations {} detected.",
                    _violatedGoalsByFixability.get(false));
        }
        return hasProposalsToFix;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{%s: {", UNFIXABLE_GOAL_VIOLATIONS));
        StringJoiner joiner = new StringJoiner(",");
        _violatedGoalsByFixability.getOrDefault(false, Collections.emptyList()).forEach(joiner::add);
        sb.append(joiner);
        sb.append(String.format("}, %s: {", FIXABLE_GOAL_VIOLATIONS));
        joiner = new StringJoiner(",");
        _violatedGoalsByFixability.getOrDefault(true, Collections.emptyList()).forEach(joiner::add);
        sb.append(joiner);
        sb.append(String.format("}, Exclude brokers recently (removed: %s demoted: %s)%s}",
                _excludeRecentlyRemovedBrokers, _excludeRecentlyDemotedBrokers,
                _provisionResponse == null ? "" : String.format(", Provision: %s", _provisionResponse)));
        return sb.toString();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _violatedGoalsByFixability = new HashMap<>();
        _optimizationResult = null;
        KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, GOAL_VIOLATION);
        KafkaCruiseControlConfig config = kafkaCruiseControl.config();
        boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
        _excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
        _excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
        _rebalanceRunnable = new RebalanceRunnable(kafkaCruiseControl,
                getSelfHealingGoalNames(config),
                allowCapacityEstimation,
                _excludeRecentlyDemotedBrokers,
                _excludeRecentlyRemovedBrokers,
                _anomalyId.toString(),
                reasonSupplier(),
                stopOngoingExecution());
    }
}
