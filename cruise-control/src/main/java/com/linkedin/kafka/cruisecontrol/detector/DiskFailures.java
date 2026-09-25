/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.FixOfflineReplicasRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.DiskFailureDetector.FAILED_DISKS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.DISK_FAILURE;

/**
 * The disk failures that have been detected.
 */
public class DiskFailures extends KafkaAnomaly {
    protected Map<Integer, Map<String, Long>> _failedDisksByBroker;
    protected FixOfflineReplicasRunnable _fixOfflineReplicasRunnable;

    public DiskFailures() {
    }

    /**
     * @return The failed disks and their failure time in millisecond grouped by broker.
     */
    public Map<Integer, Map<String, Long>> failedDisks() {
        return Collections.unmodifiableMap(_failedDisksByBroker);
    }

    @Override
    public boolean fix() throws KafkaCruiseControlException {
        // Fix the cluster by moving replicas off the dead disks.
        _optimizationResult = new OptimizationResult(_fixOfflineReplicasRunnable.computeResult(), null);
        boolean hasProposalsToFix = hasProposalsToFix();
        // Ensure that only the relevant response is cached to avoid memory pressure.
        _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
        return hasProposalsToFix;
    }

    @Override
    public AnomalyType anomalyType() {
        return DISK_FAILURE;
    }

    @Override
    public Supplier<String> reasonSupplier() {
        return () -> String.format("Self healing for %s: %s", DISK_FAILURE, this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append("{\n");
        _failedDisksByBroker.forEach((brokerId, failures) ->
                failures.forEach((logDir, eventTime) ->
                        sb.append(String.format("\tDisk %s on broker %d failed at %s%n ", logDir, brokerId, utcDateFor(eventTime)))
                )
        );
        sb.append("}");
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, DISK_FAILURE);
        _failedDisksByBroker = (Map<Integer, Map<String, Long>>) configs.get(FAILED_DISKS_OBJECT_CONFIG);
        if (_failedDisksByBroker == null || _failedDisksByBroker.isEmpty()) {
            throw new IllegalArgumentException("Unable to create disk failure anomaly with no failed disk specified.");
        }
        _optimizationResult = null;
        KafkaCruiseControlConfig config = kafkaCruiseControl.config();
        boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
        boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
        boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
        _fixOfflineReplicasRunnable = new FixOfflineReplicasRunnable(kafkaCruiseControl,
                getSelfHealingGoalNames(config),
                allowCapacityEstimation,
                excludeRecentlyDemotedBrokers,
                excludeRecentlyRemovedBrokers,
                _anomalyId.toString(),
                reasonSupplier(),
                stopOngoingExecution());
    }
}
