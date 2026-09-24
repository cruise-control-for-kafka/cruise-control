/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.consumptionDone;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createTopic;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeIncreasePartitionCount;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.maybeUpdateTopicConfig;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckOffsetFetch;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.wrapTopic;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.createMaintenanceEventConsumer;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.BROKERS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.MAINTENANCE_EVENT_TYPE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent.TOPICS_WITH_RF_UPDATE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.MAINTENANCE_EVENT;


/**
 * A maintenance event reader that retrieves events from the configured Kafka topic.
 * <p>
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #MAINTENANCE_EVENT_TOPIC_CONFIG}: The config for the name of the Kafka topic to consume maintenance events
 *   from (default: {@link #DEFAULT_MAINTENANCE_EVENT_TOPIC}).</li>
 *   <li>{@link #MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the replication factor of the maintenance
 *   event topic (default: min({@link #DEFAULT_MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR}, broker-count-in-the-cluster)).</li>
 *   <li>{@link #MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT_CONFIG}: The config for the partition count of the maintenance
 *   event topic (default: {@link #DEFAULT_MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT}).</li>
 *   <li>{@link #MAINTENANCE_EVENT_TOPIC_RETENTION_MS_CONFIG}: The config for the retention of the maintenance event topic
 *   (default: {@link #DEFAULT_MAINTENANCE_EVENT_TOPIC_RETENTION_TIME_MS}).</li>
 *   <li>{@link #MAINTENANCE_PLAN_EXPIRATION_MS_CONFIG}: The config for the validity period of a maintenance plan
 *   (default: {@link #DEFAULT_MAINTENANCE_PLAN_EXPIRATION_MS}).</li>
 * </ul>
 */
public class MaintenanceEventTopicReader implements MaintenanceEventReader {
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceEventTopicReader.class);
    protected String _maintenanceEventTopic;
    protected Consumer<String, MaintenancePlan> _consumer;
    protected Set<TopicPartition> _currentPartitionAssignment;
    protected volatile boolean _shutdown = false;
    protected long _lastEventReadPeriodEndTimeMs;
    protected KafkaCruiseControl _kafkaCruiseControl;
    // A maintenance event has a certain validity period after which it expires and becomes invalid. A delay in handling
    // could be introduced by the Kafka producer that generates the plan, the consumer that retrieves the plan, or the
    // network that connects these clients to the Kafka cluster. Maintenance event topic reader discards the expired events.
    protected long _maintenancePlanExpirationMs;

    public static final String MAINTENANCE_PLAN_EXPIRATION_MS_CONFIG = "maintenance.plan.expiration.ms";
    public static final long DEFAULT_MAINTENANCE_PLAN_EXPIRATION_MS = Duration.ofMinutes(15).toMillis();
    public static final String MAINTENANCE_EVENT_TOPIC_CONFIG = "maintenance.event.topic";
    public static final String DEFAULT_MAINTENANCE_EVENT_TOPIC = "__MaintenanceEvent";
    public static final String MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG = "maintenance.event.topic.replication.factor";
    public static final short DEFAULT_MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR = 2;
    public static final String MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT_CONFIG = "maintenance.event.topic.partition.count";
    public static final int DEFAULT_MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT = 8;
    public static final String MAINTENANCE_EVENT_TOPIC_RETENTION_MS_CONFIG = "maintenance.event.topic.retention.ms";
    public static final long DEFAULT_MAINTENANCE_EVENT_TOPIC_RETENTION_TIME_MS = Duration.ofHours(6).toMillis();
    public static final Duration CONSUMER_CLOSE_TIMEOUT = Duration.ofSeconds(2);
    public static final String CONSUMER_CLIENT_ID_PREFIX = MaintenanceEventTopicReader.class.getSimpleName();
    // How far should topic reader initially (i.e. upon startup) look back in the history for maintenance events.
    public static final long INIT_MAINTENANCE_HISTORY_MS = TimeUnit.MINUTES.toMillis(1);

    /**
     * Seek to the relevant offsets (i.e. either (1) end time of the last event read period or (2) end offset) that the
     * consumer will use on the next poll from each partition.
     *
     * @return End offsets by the partitions to be consumed.
     */
    protected Map<TopicPartition, Long> seekToRelevantOffsets() throws SamplingException {
        Map<TopicPartition, Long> timestampToSeek = new HashMap<>();
        for (TopicPartition tp : _currentPartitionAssignment) {
            timestampToSeek.put(tp, _lastEventReadPeriodEndTimeMs);
        }
        Set<TopicPartition> assignment = new HashSet<>(_currentPartitionAssignment);
        Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(assignment);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = _consumer.offsetsForTimes(timestampToSeek);
        sanityCheckOffsetFetch(endOffsets, offsetsForTimes);

        // If offsets for times are provided for a partition, seek to the returned offset. Otherwise, i.e. for partitions
        // without a record timestamp greater than or equal to the target timestamp, seek to the end offset.
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            _consumer.seek(tp, offsetAndTimestamp != null ? offsetAndTimestamp.offset() : endOffsets.get(tp));
        }

        return endOffsets;
    }

    protected void addMaintenancePlan(MaintenancePlan maintenancePlan, Set<MaintenanceEvent> maintenanceEvents) {
        LOG.debug("Retrieved maintenance plan {}.", maintenancePlan);
        Map<String, Object> parameterConfigOverrides = new HashMap<>();
        parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
        parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
        parameterConfigOverrides.put(MAINTENANCE_EVENT_TYPE_CONFIG, maintenancePlan.maintenanceEventType());
        switch (maintenancePlan.maintenanceEventType()) {
            case ADD_BROKER:
                parameterConfigOverrides.put(BROKERS_OBJECT_CONFIG, ((AddBrokerPlan) maintenancePlan).brokers());
                break;
            case REMOVE_BROKER:
                parameterConfigOverrides.put(BROKERS_OBJECT_CONFIG, ((RemoveBrokerPlan) maintenancePlan).brokers());
                break;
            case FIX_OFFLINE_REPLICAS:
            case REBALANCE:
                break;
            case DEMOTE_BROKER:
                parameterConfigOverrides.put(BROKERS_OBJECT_CONFIG, ((DemoteBrokerPlan) maintenancePlan).brokers());
                break;
            case TOPIC_REPLICATION_FACTOR:
                parameterConfigOverrides.put(TOPICS_WITH_RF_UPDATE_CONFIG, ((TopicReplicationFactorPlan) maintenancePlan).topicRegexWithRFUpdate());
                break;
            default:
                throw new IllegalStateException(String.format("Unrecognized event type %s", maintenancePlan.maintenanceEventType()));
        }
        maintenanceEvents.add(_kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.MAINTENANCE_EVENT_CLASS_CONFIG,
                MaintenanceEvent.class,
                parameterConfigOverrides));
    }

    /**
     * See {@link MaintenanceEventReader#readEvents(Duration)}
     * This method may block beyond the timeout in order to execute additional logic to create maintenance events from the
     * maintenance plans retrieved from the relevant topic.
     *
     * @param timeout The maximum time to block for retrieving records from the relevant topic.
     * @return Set of maintenance events, or empty set if none is available after the given timeout expires.
     */
    @Override
    public Set<MaintenanceEvent> readEvents(Duration timeout) throws SamplingException {
        LOG.debug("Reading maintenance events.");
        long eventReadPeriodEndMs = _kafkaCruiseControl.timeMs();
        if (refreshPartitionAssignment()) {
            _lastEventReadPeriodEndTimeMs = eventReadPeriodEndMs;
            return Collections.emptySet();
        }

        long timeoutEndMs = eventReadPeriodEndMs + timeout.toMillis();
        Set<MaintenanceEvent> maintenanceEvents = new HashSet<>();
        try {
            Map<TopicPartition, Long> endOffsets = seekToRelevantOffsets();
            LOG.debug("Started to consume from maintenance event topic partitions {}.", _currentPartitionAssignment);
            _consumer.resume(_consumer.paused());
            Set<TopicPartition> partitionsToPause = new HashSet<>();

            do {
                ConsumerRecords<String, MaintenancePlan> records = _consumer.poll(timeout);
                for (ConsumerRecord<String, MaintenancePlan> record : records) {
                    if (record == null) {
                        // This means that the record cannot be parsed because the maintenance plan version is not supported. It might
                        // happen when existing maintenance plans have been updated and the current code is still old. We simply ignore
                        // that plan in this case (see MaintenancePlanSerde.MaintenancePlanTypeAdapter#verifyTypeAndVersion(String, byte).
                        LOG.warn("Cannot parse record, please update your Cruise Control version.");
                        continue;
                    }

                    long planGenerationTimeMs = record.value().timeMs();
                    if (planGenerationTimeMs + _maintenancePlanExpirationMs < eventReadPeriodEndMs) {
                        LOG.warn("Discarding the expired plan {}. (Expired: {} Evaluated: {}).", record.value(),
                                planGenerationTimeMs + _maintenancePlanExpirationMs, eventReadPeriodEndMs);
                    } else if (planGenerationTimeMs >= eventReadPeriodEndMs) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        LOG.debug("Saw plan {} generated after the end time of event read period {}. Pausing {} at offset {}.",
                                record.value(), eventReadPeriodEndMs, tp, record.offset());
                        partitionsToPause.add(tp);
                    } else {
                        addMaintenancePlan(record.value(), maintenanceEvents);
                    }
                }

                if (!partitionsToPause.isEmpty()) {
                    _consumer.pause(partitionsToPause);
                    partitionsToPause.clear();
                }
            } while (!consumptionDone(_consumer, endOffsets) && _kafkaCruiseControl.timeMs() < timeoutEndMs);

            if (!maintenanceEvents.isEmpty()) {
                LOG.info("Retrieved {} maintenance plans from partitions {} (range [{},{}]).", maintenanceEvents.size(),
                        _currentPartitionAssignment, _lastEventReadPeriodEndTimeMs, eventReadPeriodEndMs);
            }
        } finally {
            _lastEventReadPeriodEndTimeMs = eventReadPeriodEndMs;
        }

        return maintenanceEvents;
    }

    /**
     * Ensure that the {@link #_consumer} is assigned to the latest partitions of the {@link #_maintenanceEventTopic}.
     * This enables metrics reporter sampler to handle dynamic partition size increases in {@link #_maintenanceEventTopic}.
     *
     * @return {@code true} if the set of partitions currently assigned to this consumer is empty, {@code false} otherwise.
     */
    protected boolean refreshPartitionAssignment() {
        List<PartitionInfo> remotePartitionInfo = _consumer.partitionsFor(_maintenanceEventTopic);
        if (remotePartitionInfo == null) {
            LOG.error("Consumer returned null for maintenance event topic {}.", _maintenanceEventTopic);
            return true;
        }
        if (remotePartitionInfo.isEmpty()) {
            _currentPartitionAssignment = Collections.emptySet();
            LOG.error("The set of partitions currently assigned to the maintenance event consumer is empty.");
            return true;
        }

        // Ensure that reassignment overhead is avoided if partition set of the topic has not changed.
        if (remotePartitionInfo.size() == _currentPartitionAssignment.size()) {
            return false;
        }

        _currentPartitionAssignment = new HashSet<>();
        for (PartitionInfo partitionInfo : remotePartitionInfo) {
            _currentPartitionAssignment.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }

        _consumer.assign(_currentPartitionAssignment);
        return false;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, MAINTENANCE_EVENT);
        String maintenanceEventTopic = (String) configs.get(MAINTENANCE_EVENT_TOPIC_CONFIG);
        _maintenanceEventTopic = maintenanceEventTopic == null || maintenanceEventTopic.isEmpty()
                ? DEFAULT_MAINTENANCE_EVENT_TOPIC : maintenanceEventTopic;

        String maintenancePlanExpirationMs = (String) configs.get(MAINTENANCE_PLAN_EXPIRATION_MS_CONFIG);
        _maintenancePlanExpirationMs = maintenancePlanExpirationMs == null || maintenancePlanExpirationMs.isEmpty()
                ? DEFAULT_MAINTENANCE_PLAN_EXPIRATION_MS : Long.parseLong(maintenancePlanExpirationMs);

        _consumer = createMaintenanceEventConsumer(configs, CONSUMER_CLIENT_ID_PREFIX);

        ensureTopicCreated(configs);
        _currentPartitionAssignment = Collections.emptySet();
        if (refreshPartitionAssignment()) {
            throw new IllegalStateException("Cannot find the maintenance event topic " + _maintenanceEventTopic + " in the cluster.");
        }

        _lastEventReadPeriodEndTimeMs = _kafkaCruiseControl.timeMs() - INIT_MAINTENANCE_HISTORY_MS;
    }

    /**
     * Retrieve the replication factor of the maintenance event topic.
     * If user did not set a value for {@link #MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG}, it uses the
     * min({@link #DEFAULT_MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR}, {@code broker-count-in-the-cluster}).
     *
     * @param config      The configurations for Cruise Control.
     * @param adminClient The adminClient to send describeCluster request.
     * @return The replication factor of the maintenance event topic.
     */
    protected static short maintenanceEventTopicReplicationFactor(Map<String, ?> config, AdminClient adminClient) {
        String maintenanceEventTopicRF = (String) config.get(MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR_CONFIG);
        if (maintenanceEventTopicRF == null || maintenanceEventTopicRF.isEmpty()) {
            short numberOfBrokersInCluster;
            try {
                numberOfBrokersInCluster = (short) adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS).size();
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new IllegalStateException("Auto creation of maintenance event topic failed due to failure to describe cluster.", e);
            }

            return (short) Math.min(DEFAULT_MAINTENANCE_EVENT_TOPIC_REPLICATION_FACTOR, numberOfBrokersInCluster);
        }

        return Short.parseShort(maintenanceEventTopicRF);
    }

    protected static long maintenanceEventTopicRetentionMs(Map<String, ?> config) {
        String maintenanceEventTopicRetentionMs = (String) config.get(MAINTENANCE_EVENT_TOPIC_RETENTION_MS_CONFIG);
        return maintenanceEventTopicRetentionMs == null || maintenanceEventTopicRetentionMs.isEmpty()
                ? DEFAULT_MAINTENANCE_EVENT_TOPIC_RETENTION_TIME_MS : Long.parseLong(maintenanceEventTopicRetentionMs);
    }

    protected static int maintenanceEventTopicPartitionCount(Map<String, ?> config) {
        String maintenanceEventTopicPartitionCount = (String) config.get(MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT_CONFIG);
        return maintenanceEventTopicPartitionCount == null || maintenanceEventTopicPartitionCount.isEmpty()
                ? DEFAULT_MAINTENANCE_EVENT_TOPIC_PARTITION_COUNT
                : Integer.parseInt(maintenanceEventTopicPartitionCount);
    }

    protected void ensureTopicCreated(Map<String, ?> config) {
        AdminClient adminClient = _kafkaCruiseControl.adminClient();
        short replicationFactor = maintenanceEventTopicReplicationFactor(config, adminClient);
        long retentionMs = maintenanceEventTopicRetentionMs(config);
        int partitionCount = maintenanceEventTopicPartitionCount(config);

        NewTopic maintenanceEventTopic = wrapTopic(_maintenanceEventTopic, partitionCount, replicationFactor, retentionMs);
        maybeCreateOrUpdateTopic(adminClient, maintenanceEventTopic);
    }

    /**
     * If the topic does not exist, create it with the requested partition count, replication factor, retention, and cleanup
     * policy. Otherwise, if the existing topic has
     * <ul>
     *   <li>fewer than the requested number of partitions, increase the count to the requested value</li>
     *   <li>a different retention, then update the retention to the requested value</li>
     *   <li>a different cleanup policy, update the cleanup policy to the requested value</li>
     * </ul>
     * <p>
     * Note that the replication factor is not adjusted if an existing maintenance event topic has a different replication
     * factor. Automated handling of replication factor inconsistencies is the responsibility of {@link TopicAnomalyDetector}.
     *
     * @param adminClient           Admin client to use in topic creation, config checks, and required updates (if any).
     * @param maintenanceEventTopic Maintenance event topic.
     */
    protected void maybeCreateOrUpdateTopic(AdminClient adminClient, NewTopic maintenanceEventTopic) {
        if (!createTopic(adminClient, maintenanceEventTopic)) {
            // Update topic config and partition count to ensure desired properties.
            maybeUpdateTopicConfig(adminClient, maintenanceEventTopic);
            maybeIncreasePartitionCount(adminClient, maintenanceEventTopic);
        }
    }

    @Override
    public void close() {
        _shutdown = true;
        // Close consumer.
        _consumer.close(CloseOptions.timeout(CONSUMER_CLOSE_TIMEOUT));
        _currentPartitionAssignment.clear();
    }
}
