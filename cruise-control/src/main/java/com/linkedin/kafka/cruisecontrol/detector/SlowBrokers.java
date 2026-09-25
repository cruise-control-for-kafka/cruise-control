/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.DemoteBrokerRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;


/**
 * The slow brokers that have been detected.
 */
public class SlowBrokers extends KafkaMetricAnomaly {
    protected RemoveBrokersRunnable _removeBrokersRunnable;
    protected DemoteBrokerRunnable _demoteBrokerRunnable;

    /**
     * An anomaly to indicate slow broker(s).
     */
    public SlowBrokers() {
    }

    @Override
    public boolean fix() throws KafkaCruiseControlException {
        boolean hasProposalsToFix = false;
        if (!_fixable) {
            return hasProposalsToFix;
        }

        if (_removeBrokersRunnable != null) {
            // Fix the cluster by removing the slow brokers.
            _optimizationResult = new OptimizationResult(_removeBrokersRunnable.computeResult(), null);
            hasProposalsToFix = hasProposalsToFix();
            // Ensure that only the relevant response is cached to avoid memory pressure.
            _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
        } else if (_demoteBrokerRunnable != null) {
            // Fix the cluster by demoting the slow brokers.
            _optimizationResult = new OptimizationResult(_demoteBrokerRunnable.computeResult(), null);
            hasProposalsToFix = hasProposalsToFix();
            // Ensure that only the relevant response is cached to avoid memory pressure.
            _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
        }
        return hasProposalsToFix;
    }

    @Override
    public Supplier<String> reasonSupplier() {
        return () -> String.format("Self healing for slow brokers: %s", this);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, KafkaAnomalyType.METRIC_ANOMALY);
        if (_fixable) {
            Boolean removeSlowBroker = (Boolean) configs.get(SlowBrokerFinder.REMOVE_SLOW_BROKER_CONFIG);
            if (removeSlowBroker == null) {
                throw new IllegalArgumentException(String.format("Missing %s for slow broker anomaly.", SlowBrokerFinder.REMOVE_SLOW_BROKER_CONFIG));
            }
            KafkaCruiseControlConfig config = kafkaCruiseControl.config();
            boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
            boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
            boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
            if (removeSlowBroker) {
                _removeBrokersRunnable =
                        new RemoveBrokersRunnable(kafkaCruiseControl,
                                _brokerEntitiesWithDetectionTimeMs.keySet().stream().mapToInt(BrokerEntity::brokerId).boxed()
                                        .collect(Collectors.toSet()),
                                getSelfHealingGoalNames(config),
                                allowCapacityEstimation,
                                excludeRecentlyDemotedBrokers,
                                excludeRecentlyRemovedBrokers,
                                _anomalyId.toString(),
                                reasonSupplier(),
                                stopOngoingExecution());
            } else {
                _demoteBrokerRunnable =
                        new DemoteBrokerRunnable(kafkaCruiseControl,
                                _brokerEntitiesWithDetectionTimeMs.keySet().stream().mapToInt(BrokerEntity::brokerId).boxed()
                                        .collect(Collectors.toSet()),
                                allowCapacityEstimation,
                                excludeRecentlyDemotedBrokers,
                                _anomalyId.toString(),
                                reasonSupplier(),
                                stopOngoingExecution());
            }
        }
    }
}
