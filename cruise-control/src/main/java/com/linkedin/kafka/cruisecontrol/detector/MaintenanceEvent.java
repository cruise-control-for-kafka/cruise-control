/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.AddBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.DemoteBrokerRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.FixOfflineReplicasRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.GoalBasedOperationRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RemoveBrokersRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.UpdateTopicConfigurationRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.MAINTENANCE_EVENT;


/**
 * Creator of a maintenance event is expected to populate configs for:
 * <ul>
 *   <li>{@link AnomalyDetectorUtils#KAFKA_CRUISE_CONTROL_OBJECT_CONFIG}: Kafka Cruise Control Object (required for all
 *   {@link MaintenanceEventType})</li>
 *   <li>{@link AnomalyDetectorUtils#ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG}: Anomaly detection time (required for all
 *   {@link MaintenanceEventType})</li>
 *   <li>{@link #MAINTENANCE_EVENT_TYPE_CONFIG}: The type of the event (required for all {@link MaintenanceEventType})</li>
 *   <li>{@link #BROKERS_OBJECT_CONFIG}: A set of broker ids to add, remove, or demote (required for
 *   {@link MaintenanceEventType#ADD_BROKER}, {@link MaintenanceEventType#REMOVE_BROKER}, and
 *   {@link MaintenanceEventType#DEMOTE_BROKER})</li>
 *   <li>{@link #TOPICS_WITH_RF_UPDATE_CONFIG}: Topics (specified as a regex) for replication factor update by the desired
 *   replication factor (required for {@link MaintenanceEventType#TOPIC_REPLICATION_FACTOR})</li>
 * </ul>
 */
public class MaintenanceEvent extends KafkaAnomaly {
    public static final String MAINTENANCE_EVENT_TYPE_CONFIG = "maintenance.event.type";
    public static final String BROKERS_OBJECT_CONFIG = "brokers.object";
    public static final String TOPICS_WITH_RF_UPDATE_CONFIG = "topics.with.rf.update";
    // Runnable for add/remove/demote broker, fix offline replicas, rebalance, or update topic replication factor.
    protected GoalBasedOperationRunnable _goalBasedOperationRunnable;
    protected MaintenanceEventType _maintenanceEventType;
    protected Set<Integer> _brokers;
    // Topics (specified as a regex) having at least one partition, which are requested to go through the specified
    // replication factor update.
    protected Map<Short, String> _topicsWithRFUpdate;

    @Override
    public Supplier<String> reasonSupplier() {
        return () -> String.format("Self healing for %s: %s", MAINTENANCE_EVENT, this);
    }

    @Override
    public AnomalyType anomalyType() {
        return MAINTENANCE_EVENT;
    }

    public MaintenanceEventType maintenanceEventType() {
        return _maintenanceEventType;
    }

    @Override
    public boolean fix() throws KafkaCruiseControlException {
        // Start the relevant fix for the maintenance event.
        _optimizationResult = new OptimizationResult(_goalBasedOperationRunnable.computeResult(), null);
        boolean hasProposalsToFix = hasProposalsToFix();
        // Ensure that only the relevant response is cached to avoid memory pressure.
        _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
        return hasProposalsToFix;
    }

    @Override
    public String toString() {
        // Add details on maintenance event.
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{Handling %s", _maintenanceEventType));
        if (_topicsWithRFUpdate != null) {
            // Add summary for TOPIC_REPLICATION_FACTOR
            sb.append(String.format(" by desired RF: [%s]", _topicsWithRFUpdate));
        } else if (_brokers != null) {
            // Add summary for ADD_BROKER / REMOVE_BROKER / DEMOTE_BROKER
            sb.append(String.format(" for brokers: [%s]", _brokers));
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MaintenanceEvent)) {
            return false;
        }
        AtomicReference<MaintenanceEvent> that = new AtomicReference<>((MaintenanceEvent) o);
        // Equality check excludes _goalBasedOperationRunnable.
        return _maintenanceEventType == that.get()._maintenanceEventType && Objects.equals(_brokers, that.get()._brokers)
                && Objects.equals(_topicsWithRFUpdate, that.get()._topicsWithRFUpdate);
    }

    @Override
    public int hashCode() {
        // Hash code excludes _goalBasedOperationRunnable.
        return Objects.hash(_maintenanceEventType, _brokers, _topicsWithRFUpdate);
    }

    @SuppressWarnings("unchecked")
    protected void initBrokers(Map<String, ?> configs) {
        _brokers = (Set<Integer>) configs.get(BROKERS_OBJECT_CONFIG);
        if (_brokers == null || _brokers.isEmpty()) {
            throw new IllegalArgumentException(String.format("Missing brokers for maintenance event of type %s.", _maintenanceEventType));
        }
    }

    @SuppressWarnings("unchecked")
    protected void initTopicsWithRFUpdate(Map<String, ?> configs) {
        _topicsWithRFUpdate = (Map<Short, String>) configs.get(TOPICS_WITH_RF_UPDATE_CONFIG);
        if (_topicsWithRFUpdate == null || _topicsWithRFUpdate.isEmpty()) {
            throw new IllegalArgumentException(String.format("Missing %s to identify topics (specified as a regex) for replication "
                    + "factor update.", TOPICS_WITH_RF_UPDATE_CONFIG));
        }
    }

    protected Map<Short, Pattern> topicPatternByReplicationFactor(Map<String, ?> configs) {
        initTopicsWithRFUpdate(configs);
        Map<Short, Pattern> topicPatternByReplicationFactor = new HashMap<>();
        _topicsWithRFUpdate.forEach((key, value) -> topicPatternByReplicationFactor.put(key, Pattern.compile(value)));
        return topicPatternByReplicationFactor;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, MAINTENANCE_EVENT);
        KafkaCruiseControlConfig config = kafkaCruiseControl.config();
        boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
        boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
        boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
        boolean skipRackAwarenessCheck = config.getBoolean(RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG);
        _optimizationResult = null;
        _maintenanceEventType = (MaintenanceEventType) configs.get(MAINTENANCE_EVENT_TYPE_CONFIG);
        _stopOngoingExecution = (Boolean) configs.get(MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION_CONFIG);
        switch (_maintenanceEventType) {
            case ADD_BROKER:
                initBrokers(configs);
                _goalBasedOperationRunnable = new AddBrokersRunnable(kafkaCruiseControl,
                        _brokers,
                        getSelfHealingGoalNames(config),
                        allowCapacityEstimation,
                        excludeRecentlyDemotedBrokers,
                        excludeRecentlyRemovedBrokers,
                        _anomalyId.toString(),
                        reasonSupplier(),
                        stopOngoingExecution());
                break;
            case REMOVE_BROKER:
                initBrokers(configs);
                _goalBasedOperationRunnable = new RemoveBrokersRunnable(kafkaCruiseControl,
                        _brokers,
                        getSelfHealingGoalNames(config),
                        allowCapacityEstimation,
                        excludeRecentlyDemotedBrokers,
                        excludeRecentlyRemovedBrokers,
                        _anomalyId.toString(),
                        reasonSupplier(),
                        stopOngoingExecution());
                break;
            case FIX_OFFLINE_REPLICAS:
                _goalBasedOperationRunnable = new FixOfflineReplicasRunnable(kafkaCruiseControl,
                        getSelfHealingGoalNames(config),
                        allowCapacityEstimation,
                        excludeRecentlyDemotedBrokers,
                        excludeRecentlyRemovedBrokers,
                        _anomalyId.toString(),
                        reasonSupplier(),
                        stopOngoingExecution());
                break;
            case REBALANCE:
                _goalBasedOperationRunnable = new RebalanceRunnable(kafkaCruiseControl,
                        getSelfHealingGoalNames(config),
                        allowCapacityEstimation,
                        excludeRecentlyDemotedBrokers,
                        excludeRecentlyRemovedBrokers,
                        _anomalyId.toString(),
                        reasonSupplier(),
                        stopOngoingExecution());
                break;
            case DEMOTE_BROKER:
                initBrokers(configs);
                _goalBasedOperationRunnable = new DemoteBrokerRunnable(kafkaCruiseControl,
                        _brokers,
                        allowCapacityEstimation,
                        excludeRecentlyDemotedBrokers,
                        _anomalyId.toString(),
                        reasonSupplier(),
                        stopOngoingExecution());
                break;
            case TOPIC_REPLICATION_FACTOR:
                Map<Short, Pattern> topicPatternByReplicationFactor = topicPatternByReplicationFactor(configs);
                _goalBasedOperationRunnable = new UpdateTopicConfigurationRunnable(kafkaCruiseControl,
                        topicPatternByReplicationFactor,
                        getSelfHealingGoalNames(config),
                        allowCapacityEstimation,
                        excludeRecentlyDemotedBrokers,
                        excludeRecentlyRemovedBrokers,
                        _anomalyId.toString(),
                        reasonSupplier(),
                        stopOngoingExecution(),
                        skipRackAwarenessCheck);
                break;
            default:
                throw new IllegalStateException(String.format("Unsupported maintenance event type %s.", _maintenanceEventType));
        }
    }
}
