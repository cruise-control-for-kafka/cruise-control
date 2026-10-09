/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingIntraBrokerGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION;


/**
 * A class that holds the intra broker goal violations.
 */
public class IntraBrokerGoalViolations extends AbstractGoalViolations {
    private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerGoalViolations.class);

    /**
     * An anomaly to indicate goal violation(s).
     */
    public IntraBrokerGoalViolations() {
    }

    @Override
    public AnomalyType anomalyType() {
        return INTRA_BROKER_GOAL_VIOLATION;
    }

    @Override
    public Supplier<String> reasonSupplier() {
        return () -> String.format("Self healing for %s: %s", INTRA_BROKER_GOAL_VIOLATION, this);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _violatedGoalsByFixability = new HashMap<>();
        _optimizationResult = null;
        KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, INTRA_BROKER_GOAL_VIOLATION);
        KafkaCruiseControlConfig config = kafkaCruiseControl.config();
        boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
        _excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
        _excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
        _rebalanceRunnable = new RebalanceRunnable(kafkaCruiseControl,
                getSelfHealingIntraBrokerGoalNames(config),
                allowCapacityEstimation,
                _excludeRecentlyDemotedBrokers,
                _excludeRecentlyRemovedBrokers,
                _anomalyId.toString(),
                reasonSupplier(),
                stopOngoingExecution(),
                true,
                true,
                true);
    }
}
