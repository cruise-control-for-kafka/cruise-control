/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ConcurrencyAdjustingRecommendation;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;


public final class ExecutionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionUtils.class);
    public static final int DEFAULT_RETRY_BACKOFF_BASE = 2;
    public static final long METADATA_REFRESH_BACKOFF = 100L;
    public static final long METADATA_REFRESH_BACKOFF_MAX = CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS;
    public static final long METADATA_EXPIRY_MS = Long.MAX_VALUE;
    public static final Duration MIN_ISR_CACHE_CLEANER_PERIOD = Duration.ofMinutes(10);
    public static final Duration MIN_ISR_CACHE_CLEANER_INITIAL_DELAY = Duration.ofMinutes(0);
    // A special timestamp to indicate that a broker is a permanent part of recently removed or demoted broker set.
    public static final long PERMANENT_TIMESTAMP = 0L;
    public static final String EXECUTION_STARTED = "execution-started";
    public static final String KAFKA_ASSIGNER_MODE = "kafka_assigner";
    public static final String EXECUTION_STOPPED = "execution-stopped";
    public static final String GAUGE_EXECUTION_STOPPED = EXECUTION_STOPPED;
    public static final String GAUGE_EXECUTION_STOPPED_BY_USER = EXECUTION_STOPPED + "-by-user";
    public static final String GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-" + KAFKA_ASSIGNER_MODE;
    public static final String GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-non-" + KAFKA_ASSIGNER_MODE;
    // The following 2 per-broker-cap gauges are deprecated since individual broker concurrency is applied.
    public static final String GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP = "inter-broker-partition-movements-per-broker-cap";
    public static final String GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER_CAP = "intra-broker-partition-movements-per-broker-cap";
    public static final String GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_GLOBAL_CAP = "leadership-movements-global-cap";
    public static final String GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_MAX_CONCURRENCY = "inter-broker-partition-movements-max-concurrency";
    public static final String GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_MIN_CONCURRENCY = "inter-broker-partition-movements-min-concurrency";
    public static final String GAUGE_EXECUTION_INTER_BROKER_PARTITION_MOVEMENTS_AVG_CONCURRENCY = "inter-broker-partition-movements-avg-concurrency";
    public static final String GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_MAX_CONCURRENCY = "intra-broker-partition-movements-max-concurrency";
    public static final String GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_MIN_CONCURRENCY = "intra-broker-partition-movements-min-concurrency";
    public static final String GAUGE_EXECUTION_INTRA_BROKER_PARTITION_MOVEMENTS_AVG_CONCURRENCY = "intra-broker-partition-movements-avg-concurrency";
    public static final String GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_MAX_CONCURRENCY = "leadership-movements-max-concurrency";
    public static final String GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_MIN_CONCURRENCY = "leadership-movements-min-concurrency";
    public static final String GAUGE_EXECUTION_LEADERSHIP_MOVEMENTS_AVG_CONCURRENCY = "leadership-movements-avg-concurrency";
    public static final String TIMER_PROPOSAL_EXECUTION_TIME_INVOLVE_BROKER_REMOVAL = "proposal_execution-time-involve-broker-removal";
    public static final String TIMER_PROPOSAL_EXECUTION_TIME_INVOLVE_BROKER_DEMOTION_ONLY = "proposal-execution-time-involve-broker-demotion-only";
    public static final String TIMER_PROPOSAL_EXECUTION_TIME = "proposal-execution-time";

    public static final String PARTITION_MOVEMENT_PER_SEC = "partition-movement-count-per-second";
    public static final String PARTITION_MOVEMENT_MB_PER_SEC = "partition-movement-MB-per-second";
    public static final long EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS = 5;
    public static final long EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS = 0;
    static final Map<ConcurrencyType, Integer> ADDITIVE_INCREASE = new HashMap<>();
    static final Map<ConcurrencyType, Integer> MULTIPLICATIVE_DECREASE = new HashMap<>();
    static final Map<ConcurrencyType, Integer> MAX_CONCURRENCY = new HashMap<>();
    static final Map<ConcurrencyType, Integer> MIN_CONCURRENCY = new HashMap<>();
    static final Map<String, Double> CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME = new HashMap<>();
    // Cluster concurrency would be decreased if the number of brokers with metrics above the limits defined in
    // CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME is greater than this value.
    private static int minNumBrokersViolateMetricLimitToDecreaseClusterConcurrency;
    private static long listPartitionReassignmentsTimeoutMs;
    private static int listPartitionReassignmentsMaxAttempts;

    private ExecutionUtils() {
    }

    /**
     * Initialize the concurrency adjuster limits and timeout-related configs for list partition reassignment requests.
     *
     * @param config The configurations for Cruise Control.
     */
    static void init(KafkaCruiseControlConfig config) {
        CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_LOG_FLUSH_TIME_MS_999TH.name(),
                config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG));
        CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH.name(),
                config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG));
        CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_PRODUCE_LOCAL_TIME_MS_999TH.name(),
                config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG));
        CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH.name(),
                config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG));
        CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.put(BROKER_REQUEST_QUEUE_SIZE.name(),
                config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG));
        ADDITIVE_INCREASE.put(ConcurrencyType.INTER_BROKER_REPLICA,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_CONFIG));
        ADDITIVE_INCREASE.put(ConcurrencyType.LEADERSHIP_CLUSTER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_CONFIG));
        ADDITIVE_INCREASE.put(ConcurrencyType.LEADERSHIP_BROKER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_PER_BROKER_CONFIG));
        MULTIPLICATIVE_DECREASE.put(ConcurrencyType.INTER_BROKER_REPLICA,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_CONFIG));
        MULTIPLICATIVE_DECREASE.put(ConcurrencyType.LEADERSHIP_CLUSTER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_CONFIG));
        MULTIPLICATIVE_DECREASE.put(ConcurrencyType.LEADERSHIP_BROKER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_PER_BROKER_CONFIG));
        MAX_CONCURRENCY.put(ConcurrencyType.INTER_BROKER_REPLICA,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG));
        MAX_CONCURRENCY.put(ConcurrencyType.LEADERSHIP_CLUSTER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG));
        MAX_CONCURRENCY.put(ConcurrencyType.LEADERSHIP_BROKER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG));
        MIN_CONCURRENCY.put(ConcurrencyType.INTER_BROKER_REPLICA,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG));
        MIN_CONCURRENCY.put(ConcurrencyType.LEADERSHIP_CLUSTER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG));
        MIN_CONCURRENCY.put(ConcurrencyType.LEADERSHIP_BROKER,
                config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG));
        listPartitionReassignmentsTimeoutMs = config.getLong(ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG);
        listPartitionReassignmentsMaxAttempts = config.getInt(ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS_CONFIG);
        minNumBrokersViolateMetricLimitToDecreaseClusterConcurrency =
                Math.max(1, config.getInt(ExecutorConfig.MIN_NUM_BROKERS_VIOLATE_METRIC_LIMIT_TO_DECREASE_CLUSTER_CONCURRENCY_CONFIG));
    }

    public static String toMetricName(Short metricId) {
        return KafkaMetricDef.brokerMetricDef().metricInfo(metricId).name();
    }

    /**
     * Check whether the current metrics are within the limit specified by {@link #CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME}.
     * Package private for unit tests.
     *
     * @param brokerId                     the broker id to check if its metrics is in limit value
     * @param currentMetrics               Current metrics of the broker.
     * @param overLimitDetailsByMetricName the string builder to build over limit report
     * @return {@code true} if all brokers are within the limit specified by {@link #CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME},
     * {@code false} otherwise.
     */
    static boolean withinConcurrencyAdjusterLimit(int brokerId, @Nullable ValuesAndExtrapolations currentMetrics,
                                                  Map<String, StringBuilder> overLimitDetailsByMetricName) {
        if (currentMetrics == null) {
            LOG.warn("Concurrency adjuster recommended decreasing concurrency for broker {} as no broker metrics exist to verify.", brokerId);
            return false;
        }

        boolean withinAdjusterLimit = true;
        // Check whether the broker is within the acceptable limit for the relevant metrics. If not, collect details.
        for (Short metricId : currentMetrics.metricValues().metricIds()) {
            String metricName = toMetricName(metricId);
            Double limit = CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.get(metricName);
            if (limit != null) {
                double metricValue = currentMetrics.metricValues().valuesFor(metricId).latest();
                if (metricValue > limit) {
                    overLimitDetailsByMetricName.get(metricName).append(String.format("%d(%.2f) ", brokerId, metricValue));
                    withinAdjusterLimit = false;
                }
            }
        }
        return withinAdjusterLimit;
    }

    /**
     * Provide a recommended concurrency for the ongoing movements of the given concurrency type using (At/Under)MinISR status of partitions.
     * If the cluster has partitions that are
     * <ul>
     *   <li>UnderMinISR without offline replicas, recommend cancelling the execution</li>
     *   <li>AtMinISR without offline replicas, recommend to decrease the concurrency</li>
     * </ul>
     *
     * @param cluster               Kafka cluster.
     * @param minIsrWithTimeByTopic Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
     * @return the concurrency recommendation.
     */
    static ConcurrencyAdjustingRecommendation recommendedConcurrency(Cluster cluster, Map<String, MinIsrWithTime> minIsrWithTimeByTopic) {
        Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
        Set<PartitionInfo> underMinIsr = new TreeSet<>(comparator);
        Set<PartitionInfo> atMinIsr = new TreeSet<>(comparator);
        populateMinIsrState(cluster, minIsrWithTimeByTopic, underMinIsr, atMinIsr, null, false);

        if (!underMinIsr.isEmpty()) {
            LOG.info("Concurrency adjuster recommended cancelling the execution due to UnderMinISR without offline replicas in {}.", underMinIsr);
            return ConcurrencyAdjustingRecommendation.STOP_EXECUTION;
        } else if (!atMinIsr.isEmpty()) {
            ConcurrencyAdjustingRecommendation concurrencyAdjustingRecommendation = new ConcurrencyAdjustingRecommendation();
            for (PartitionInfo partitionInfo : atMinIsr) {
                for (Node replica : partitionInfo.replicas()) {
                    concurrencyAdjustingRecommendation.recommendConcurrencyDecrease(replica.id());
                }
            }
            concurrencyAdjustingRecommendation.recommendDecreaseClusterConcurrency();
            return concurrencyAdjustingRecommendation;
        }
        return ConcurrencyAdjustingRecommendation.NO_CHANGE_RECOMMENDED;
    }

    /**
     * Provide concurrency recommendations for the ongoing movements based on broker metrics. For each broker, it recommends to increase
     * the concurrency if the all metrics values are within the limit, and recommends to decrease the concurrency otherwise.
     * For cluster overall concurrency, it recommends to increase if all metrics of all brokers are within the limit,
     * otherwise, it recommends to decrease.
     *
     * @param currentMetricsByBroker Current metrics by broker.
     * @return the concurrency recommendation.
     */
    static ConcurrencyAdjustingRecommendation recommendedConcurrency(Map<BrokerEntity, ValuesAndExtrapolations> currentMetricsByBroker) {

        ConcurrencyAdjustingRecommendation concurrencyAdjustingRecommendation = new ConcurrencyAdjustingRecommendation();
        // Stores the brokers whose metrics value are above the threshold. This is only for logging purpose.
        Map<String, StringBuilder> overLimitDetailsByMetricName = new HashMap<>();
        for (String metricName : CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.keySet()) {
            overLimitDetailsByMetricName.put(metricName, new StringBuilder());
        }

        int numBrokersAboveMetricLimit = 0;

        // Iterate through brokers and adjust concurrency based on the current broker metric
        for (Map.Entry<BrokerEntity, ValuesAndExtrapolations> entry : currentMetricsByBroker.entrySet()) {
            BrokerEntity broker = entry.getKey();
            ValuesAndExtrapolations current = entry.getValue();

            boolean withinAdjusterLimit = withinConcurrencyAdjusterLimit(broker.brokerId(), current, overLimitDetailsByMetricName);
            if (withinAdjusterLimit) {
                concurrencyAdjustingRecommendation.recommendConcurrencyIncrease(broker.brokerId());
            } else {
                concurrencyAdjustingRecommendation.recommendConcurrencyDecrease(broker.brokerId());
                numBrokersAboveMetricLimit += 1;
            }
        }

        for (Map.Entry<String, StringBuilder> entry : overLimitDetailsByMetricName.entrySet()) {
            StringBuilder brokersWithValues = entry.getValue();
            if (!brokersWithValues.isEmpty()) {
                LOG.info("{} was over the acceptable limit for brokers with values: {}.", entry.getKey(), brokersWithValues);
            }
        }

        if (numBrokersAboveMetricLimit < minNumBrokersViolateMetricLimitToDecreaseClusterConcurrency) {
            concurrencyAdjustingRecommendation.recommendIncreaseClusterConcurrency();
        } else {
            concurrencyAdjustingRecommendation.recommendDecreaseClusterConcurrency();
        }

        return concurrencyAdjustingRecommendation;
    }

    /**
     * Populates the given sets for partitions that are (1) UnderMinISR without any ({@code withOfflineReplicas=false}) or with at least one
     * ({@code withOfflineReplicas=true}) offline replicas, (2) AtMinISR without any ({@code withOfflineReplicas=false}) or with at least
     * one ({@code withOfflineReplicas=true}) offline replicas and (3) OneAboveMinISR without any ({@code withOfflineReplicas=false}) or with at least
     * one ({@code withOfflineReplicas=true}) offline replicas using the topics from the given Kafka cluster and
     * {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} from the given {@code minIsrWithTimeByTopic}.
     * <p>
     * If the minISR value for a topic in the given Kafka cluster is missing from the given {@code minIsrWithTimeByTopic}, this function skips
     * populating minIsr state for partitions of that topic.
     *
     * @param cluster                              Kafka cluster.
     * @param minIsrWithTimeByTopic                Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
     * @param underMinIsrWithoutOfflineReplicas    UnderMinISR without offline replicas.
     * @param atMinIsrWithoutOfflineReplicas       AtMinISR partitions without offline replicas.
     * @param oneAboveMinIsrWithoutOfflineReplicas oneAboveMinISR partitions without offline replicas.
     * @param withOfflineReplicas                  {@code true} to retrieve (At/Under/OneAbove)MinISR partitions each containing at least an offline replica,
     *                                             {@code false} to retrieve (At/Under/OneAbove)MinISR partitions without any offline replicas.
     */
    public static void populateMinIsrState(Cluster cluster,
                                           Map<String, MinIsrWithTime> minIsrWithTimeByTopic,
                                           Set<PartitionInfo> underMinIsrWithoutOfflineReplicas,
                                           Set<PartitionInfo> atMinIsrWithoutOfflineReplicas,
                                           Set<PartitionInfo> oneAboveMinIsrWithoutOfflineReplicas,
                                           boolean withOfflineReplicas) {
        for (String topic : cluster.topics()) {
            MinIsrWithTime minIsrWithTime = minIsrWithTimeByTopic.get(topic);
            if (minIsrWithTime == null) {
                continue;
            }

            int minISR = minIsrWithTime.minISR();
            for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
                boolean hasOfflineReplica = partitionInfo.offlineReplicas().length != 0;
                if (hasOfflineReplica != withOfflineReplicas) {
                    continue;
                }

                int numInSyncReplicas = partitionInfo.inSyncReplicas().length;
                if (numInSyncReplicas < minISR && underMinIsrWithoutOfflineReplicas != null) {
                    underMinIsrWithoutOfflineReplicas.add(partitionInfo);
                } else if (numInSyncReplicas == minISR && atMinIsrWithoutOfflineReplicas != null) {
                    atMinIsrWithoutOfflineReplicas.add(partitionInfo);
                } else if (numInSyncReplicas == minISR + 1 && oneAboveMinIsrWithoutOfflineReplicas != null) {
                    oneAboveMinIsrWithoutOfflineReplicas.add(partitionInfo);
                }
            }
        }
    }

    /**
     * Populates the given sets for partitions that are (1) UnderMinISR without any ({@code withOfflineReplicas=false}) or with at least one
     * ({@code withOfflineReplicas=true}) offline replicas and (2) AtMinISR without any ({@code withOfflineReplicas=false}) or with at least
     * one ({@code withOfflineReplicas=true}) offline replicas using the topics from the given Kafka cluster and
     * {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} from the given {@code minIsrWithTimeByTopic}.
     * <p>
     * If the minISR value for a topic in the given Kafka cluster is missing from the given {@code minIsrWithTimeByTopic}, this function skips
     * populating minIsr state for partitions of that topic.
     *
     * @param cluster                           Kafka cluster.
     * @param minIsrWithTimeByTopic             Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
     * @param underMinIsrWithoutOfflineReplicas UnderMinISR without offline replicas.
     * @param atMinIsrWithoutOfflineReplicas    AtMinISR partitions without offline replicas.
     * @param withOfflineReplicas               {@code true} to retrieve (At/Under)MinISR partitions each containing at least an offline replica,
     *                                          {@code false} to retrieve (At/Under)MinISR partitions without any offline replicas.
     */
    public static void populateMinIsrState(Cluster cluster,
                                           Map<String, MinIsrWithTime> minIsrWithTimeByTopic,
                                           Set<PartitionInfo> underMinIsrWithoutOfflineReplicas,
                                           Set<PartitionInfo> atMinIsrWithoutOfflineReplicas,
                                           boolean withOfflineReplicas) {
        populateMinIsrState(cluster, minIsrWithTimeByTopic,
                underMinIsrWithoutOfflineReplicas, atMinIsrWithoutOfflineReplicas, null, withOfflineReplicas);
    }

    /**
     * Retrieve the set of {@link TopicPartition partitions} that are currently being reassigned.
     *
     * @param adminClient The adminClient to ask for ongoing partition reassignments.
     * @return The set of {@link TopicPartition partitions} that are being reassigned.
     */
    public static Set<TopicPartition> partitionsBeingReassigned(AdminClient adminClient)
            throws InterruptedException, ExecutionException, TimeoutException {
        return ongoingPartitionReassignments(adminClient).keySet();
    }

    /**
     * Retrieve the map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
     * <p>
     * If the response times out, this method retries up to {@link #listPartitionReassignmentsMaxAttempts} times.
     * The maximum time to wait for the admin client response is computed as:
     * {@link #listPartitionReassignmentsTimeoutMs} * ({@link #DEFAULT_RETRY_BACKOFF_BASE} ^ {@code attempt}).
     *
     * @param adminClient The adminClient to ask for ongoing partition reassignments.
     * @return The map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
     */
    public static Map<TopicPartition, PartitionReassignment> ongoingPartitionReassignments(AdminClient adminClient)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<TopicPartition, PartitionReassignment> partitionReassignments = null;
        int attempts = 0;
        long timeoutMs = listPartitionReassignmentsTimeoutMs;
        do {
            ListPartitionReassignmentsResult responseResult = adminClient.listPartitionReassignments();
            try {
                // A successful response is expected to be non-null.
                partitionReassignments = responseResult.reassignments().get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.info("Failed to list partition reassignments in {}ms (attempt={}). Consider increasing the value of {} config.",
                        timeoutMs, attempts + 1, ExecutorConfig.LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG);
                if (++attempts == listPartitionReassignmentsMaxAttempts) {
                    throw e;
                }
                timeoutMs *= DEFAULT_RETRY_BACKOFF_BASE;
            }
        } while (partitionReassignments == null);

        return partitionReassignments;
    }

    /**
     * Package private for unit tests.
     *
     * @return A value to indicate a cancelled reassignment for any partition.
     */
    static Optional<NewPartitionReassignment> cancelReassignmentValue() {
        return java.util.Optional.empty();
    }

    private static Optional<NewPartitionReassignment> reassignmentValue(List<Integer> targetReplicas) {
        return Optional.of(new NewPartitionReassignment(targetReplicas));
    }

    /**
     * Submits the given preferred leader election tasks for execution using the given admin client.
     *
     * @param adminClient The adminClient to submit new preferred leader election tasks.
     * @param tasks       Preferred leader election tasks to execute.
     * @return The {@link ElectLeadersResult result} of preferred leader election request -- cannot be {@code null}.
     */
    public static ElectLeadersResult submitPreferredLeaderElection(AdminClient adminClient, List<ExecutionTask> tasks) {
        if (validateNotNull(tasks, "Tasks to execute cannot be null.").isEmpty()) {
            throw new IllegalArgumentException("Tasks to execute cannot be empty.");
        }
        Set<TopicPartition> partitions = new HashSet<>();
        for (ExecutionTask task : tasks) {
            switch (task.state()) {
                case ABORTING:
                case ABORTED:
                case COMPLETED:
                case DEAD:
                    // No action needed. Contrary to replica reassignments, there is no need to cancel a leadership movement as attempts to elect
                    // the preferred leader, which is down will fail with an Errors#PREFERRED_LEADER_NOT_AVAILABLE exception.
                    LOG.debug("No further action needed for task {}.", task);
                    break;
                case IN_PROGRESS:
                    // No need to check whether the topic exists or being deleted as the server response will indicate those.
                    partitions.add(task.proposal().topicPartition());
                    LOG.debug("Task {} will be executed.", task);
                    break;
                default:
                    throw new IllegalStateException(String.format("Unrecognized task state %s.", task.state()));
            }
        }

        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("All tasks submitted for leader election are already completed.");
        }
        return adminClient.electLeaders(ElectionType.PREFERRED, partitions);
    }

    /**
     * Submits the given inter-broker replica reassignment tasks for execution using the given admin client.
     *
     * @param adminClient The adminClient to submit new inter-broker replica reassignments.
     * @param tasks       Inter-broker replica reassignment tasks to execute.
     * @return The {@link AlterPartitionReassignmentsResult result} of reassignment request -- cannot be {@code null}.
     */
    public static AlterPartitionReassignmentsResult submitReplicaReassignmentTasks(AdminClient adminClient, List<ExecutionTask> tasks) {
        if (validateNotNull(tasks, "Tasks to execute cannot be null.").isEmpty()) {
            throw new IllegalArgumentException("Tasks to execute cannot be empty.");
        }

        // Update the ongoing replica reassignments in case the task status has changed.
        Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments = new HashMap<>();
        for (ExecutionTask task : tasks) {
            TopicPartition tp = task.proposal().topicPartition();
            List<Integer> newReplicas = new ArrayList<>(task.proposal().newReplicas().size());
            for (ReplicaPlacementInfo replicaPlacementInfo : task.proposal().newReplicas()) {
                newReplicas.add(replicaPlacementInfo.brokerId());
            }
            switch (task.state()) {
                case ABORTING:
                case ABORTED:
                case DEAD:
                    // A task in one of these states should not have a corresponding partition movement -- cancel it.
                    newReassignments.put(tp, cancelReassignmentValue());
                    LOG.debug("The ongoing reassignment will be cancelled for task {}.", task);
                    break;
                case COMPLETED:
                    // No action needed.
                    LOG.debug("Task {} has already been completed.", task);
                    break;
                case IN_PROGRESS:
                    // No need to check whether the topic exists or being deleted as the server response will indicate those.
                    // Likewise, no need to check if there is already an ongoing execution for the partition, because if one
                    // exists, it will be modified to execute the desired task.
                    newReassignments.put(tp, reassignmentValue(newReplicas));
                    LOG.debug("Task {} will be executed.", task);
                    break;
                default:
                    throw new IllegalStateException(String.format("Unrecognized task state %s.", task.state()));
            }
        }

        if (newReassignments.isEmpty()) {
            throw new IllegalArgumentException("All tasks submitted for replica reassignment are already completed.");
        }
        return adminClient.alterPartitionReassignments(newReassignments);
    }

    /**
     * Stops the inter-broker replica reassignments using the given admin client.
     *
     * @param adminClient The adminClient to stop the inter-broker replica reassignments.
     * @return The {@link AlterPartitionReassignmentsResult result} of stop reassignment request, {@code null} if there isn't any reassignments.
     */
    public static AlterPartitionReassignmentsResult maybeStopPartitionReassignment(AdminClient adminClient) {
        Set<TopicPartition> partitionsBeingReassigned;
        try {
            partitionsBeingReassigned = partitionsBeingReassigned(adminClient);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            // This may indicate transient (e.g. network) issues.
            throw new IllegalStateException("Cannot stop partition reassignments due to failure to retrieve whether the Kafka cluster has "
                    + "an already ongoing partition reassignment.", e);
        }
        if (partitionsBeingReassigned.isEmpty()) {
            return null;
        }

        // Cancel all the ongoing replica reassignments.
        Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments = new HashMap<>();
        for (TopicPartition tp : partitionsBeingReassigned) {
            newReassignments.put(tp, cancelReassignmentValue());
        }
        return adminClient.alterPartitionReassignments(newReassignments);
    }

    /**
     * Find the tasks that need to be re-executed from all candidate inter-broker replica movement tasks.
     *
     * @param partitionsInMovement      The partitions that are being moved in kafka cluster
     * @param candidateTasksToReexecute The candidate inter-broker replica movement tasks to re-execute.
     * @return the list of inter-broker replica movement tasks to re-execute
     */
    public static List<ExecutionTask> getInterBrokerReplicaTasksToReexecute(Set<TopicPartition> partitionsInMovement,
                                                                            Collection<ExecutionTask> candidateTasksToReexecute) {
        List<ExecutionTask> tasksToReexecute = new ArrayList<>();

        for (ExecutionTask executionTask : candidateTasksToReexecute) {
            TopicPartition tp = executionTask.proposal().topicPartition();
            if (!partitionsInMovement.contains(tp)) {
                tasksToReexecute.add(executionTask);
            }
        }

        if (!tasksToReexecute.isEmpty()) {
            LOG.info("Found tasks to re-execute: {} while detected in-movement partitions: {}.", tasksToReexecute, partitionsInMovement);
        }

        return tasksToReexecute;
    }

    /**
     * Process the given {@link AlterPartitionReassignmentsResult result} of alterPartitionReassignments request to:
     * <ul>
     *   <li>ensure that the corresponding request has been accepted,</li>
     *   <li>identify the set of partitions that were deleted upon submission of the corresponding inter-broker replica
     *   reassignment tasks and populate the given set</li>
     *   <li>identify the set of partitions that were dead upon submission of the corresponding inter-broker replica
     *   reassignment tasks and populate the given set, and</li>
     *   <li>identify the set of partitions that were not in progress upon submission of the corresponding cancellation/
     *   rollback for the inter-broker replica reassignment tasks and populate the given set</li>
     * </ul>
     *
     * @param result                                the result of a request to alter partition reassignments, or {@code null} if no new reassignment submitted.
     * @param deletedTopicPartitions                a set to populate with partitions that were deleted upon submission of the corresponding
     *                                              inter-broker replica reassignment tasks.
     * @param deadTopicPartitions                   a set to populate with partitions that were dead upon submission of the corresponding
     *                                              inter-broker replica reassignment tasks.
     * @param noReassignmentToCancelTopicPartitions a set to populate with partitions that were not in progress upon
     *                                              submission of the corresponding cancellation/rollback for the
     *                                              inter-broker replica reassignment tasks.
     */
    public static void processAlterPartitionReassignmentsResult(AlterPartitionReassignmentsResult result,
                                                                Set<TopicPartition> deletedTopicPartitions,
                                                                Set<TopicPartition> deadTopicPartitions,
                                                                Set<TopicPartition> noReassignmentToCancelTopicPartitions) {
        if (result == null) {
            return;
        }
        for (Map.Entry<TopicPartition, KafkaFuture<Void>> entry : result.values().entrySet()) {
            TopicPartition tp = entry.getKey();
            try {
                entry.getValue().get();
                LOG.debug("Replica reassignment for {} has been accepted.", tp);
            } catch (ExecutionException ee) {
                if (Errors.INVALID_REPLICA_ASSIGNMENT.exception().getClass() == ee.getCause().getClass()) {
                    deadTopicPartitions.add(tp);
                    LOG.debug("Replica reassignment failed for {} due to dead destination broker(s).", tp);
                } else if (Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getClass() == ee.getCause().getClass()) {
                    deletedTopicPartitions.add(tp);
                    LOG.debug("Replica reassignment failed for {} due to its unknown topic or partition.", tp);
                } else if (Errors.NO_REASSIGNMENT_IN_PROGRESS.exception().getClass() == ee.getCause().getClass()) {
                    // Attempt to cancel/rollback a reassignment that does not exist.
                    noReassignmentToCancelTopicPartitions.add(tp);
                    LOG.debug("Rollback failed for {} due to lack of corresponding ongoing replica reassignment.", tp);
                } else if (Errors.REQUEST_TIMED_OUT.exception().getClass() == ee.getCause().getClass()) {
                    throw new IllegalStateException(String.format("alterPartitionReassignments request timed out for partitions: %s. Check for Kafka "
                                    + "broker- or controller-side issues and consider increasing the configured timeout "
                                    + "(see %s).",
                            result.values().keySet(),
                            ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG), ee.getCause());
                } else {
                    // Not expected to happen.
                    throw new IllegalStateException(String.format("%s encountered an unknown execution exception.", tp), ee.getCause());
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted during the process of AlterPartitionReassignmentsResult on {}.", result.values().keySet(), e);
            }
        }
    }

    /**
     * Process the given {@link ElectLeadersResult result} of electLeaders request to:
     * <ul>
     *   <li>ensure that the corresponding request has been accepted,</li>
     *   <li>identify the set of partitions that were deleted upon submission of the corresponding leadership tasks and populate the given set</li>
     * </ul>
     *
     * @param result                 the result of a request to elect leaders, or {@code null} if no new leader election is submitted.
     * @param deletedTopicPartitions a set to populate with partitions that were deleted upon submission of the corresponding leadership tasks.
     */
    public static void processElectLeadersResult(ElectLeadersResult result, Set<TopicPartition> deletedTopicPartitions) {
        if (result == null) {
            return;
        }
        try {
            Map<TopicPartition, Optional<Throwable>> partitions = result.partitions().get();
            Set<TopicPartition> noElectionNeeded = new HashSet<>();
            Set<TopicPartition> preferredLeaderUnavailable = new HashSet<>();

            for (Map.Entry<TopicPartition, Optional<Throwable>> entry : partitions.entrySet()) {
                TopicPartition tp = entry.getKey();
                if (entry.getValue().isEmpty()) {
                    LOG.debug("Leader election for {} has succeeded.", tp);
                } else {
                    if (Errors.ELECTION_NOT_NEEDED.exception().getClass() == entry.getValue().get().getClass()) {
                        // The leader is already the preferred leader.
                        noElectionNeeded.add(tp);
                    } else if (Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getClass() == entry.getValue().get().getClass()
                            || Errors.INVALID_TOPIC_EXCEPTION.exception().getClass() == entry.getValue().get().getClass()) {
                        // Topic (1) has been deleted -- i.e. since partition does not exist, it is assumed to be deleted or (2) is being deleted.
                        deletedTopicPartitions.add(tp);
                    } else if (Errors.PREFERRED_LEADER_NOT_AVAILABLE.exception().getClass() == entry.getValue().get().getClass()) {
                        // We tried to elect the preferred leader but it is offline.
                        preferredLeaderUnavailable.add(tp);
                    } else {
                        // Based on the KafkaAdminClient code, looks like there is no handling / retry in response to a Errors.NOT_CONTROLLER -- e.g. if
                        // the controller changes during a request, the leader election will be dropped with an error response. In such cases, a
                        // followup execution would be needed (i.e. see Executor#maybeReexecuteLeadershipTasks(Set<TopicPartition>)).
                        LOG.warn("Failed to elect preferred leader for {}.", tp, entry.getValue().get());
                    }
                }
            }

            // Log relevant information for debugging purposes.
            if (!noElectionNeeded.isEmpty()) {
                LOG.debug("Leader election not needed for {}.", noElectionNeeded);
            }
            if (!preferredLeaderUnavailable.isEmpty()) {
                LOG.debug("The preferred leader was not available for {}.", preferredLeaderUnavailable);
            }
            if (!deletedTopicPartitions.isEmpty()) {
                LOG.debug("Corresponding topics have been deleted before leader election {}.", deletedTopicPartitions);
            }
        } catch (ExecutionException ee) {
            if (Errors.REQUEST_TIMED_OUT.exception().getClass() == ee.getCause().getClass()) {
                throw new IllegalStateException(String.format("electLeaders request timed out. Check for Kafka broker- or controller-side issues and"
                                + " consider increasing the configured timeout (see %s).",
                        ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG), ee.getCause());
            } else if (Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass() == ee.getCause().getClass()) {
                throw new IllegalStateException("Cruise Control is not authorized to trigger leader election", ee.getCause());
            } else {
                // Not expected to happen.
                throw new IllegalStateException("An unknown execution exception is encountered in response to electLeaders.", ee.getCause());
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted during the process of ElectLeadersResult.", e);
        }
    }

    /**
     * For an inter-broker replica movement action, the completion depends on the task state:
     * <ul>
     *   <li>{@link ExecutionTaskState#IN_PROGRESS}: when the current replica list is the same as the new replica list
     *   and all replicas are in-sync.</li>
     *   <li>{@link ExecutionTaskState#ABORTING}: done when the current replica list is the same as the old replica list.
     *   Due to race condition, we also consider it done if the current replica list is the same as the new replica list
     *   and all replicas are in-sync.</li>
     *   <li>{@link ExecutionTaskState#DEAD}: always considered as done because we neither move forward or rollback.</li>
     * </ul>
     * <p>
     * There should be no other task state seen here.
     *
     * @param cluster Kafka cluster.
     * @param task    Task to check for being done.
     * @return {@code true} if task is done, {@code false} otherwise.
     */
    static boolean isInterBrokerReplicaActionDone(Cluster cluster, ExecutionTask task) {
        PartitionInfo partitionInfo = cluster.partition(task.proposal().topicPartition());
        return switch (task.state()) {
            case IN_PROGRESS -> task.proposal().isInterBrokerMovementCompleted(partitionInfo);
            case ABORTING -> task.proposal().isInterBrokerMovementAborted(partitionInfo);
            case DEAD -> true;
            default -> throw new IllegalStateException("Should never be here. State " + task.state());
        };
    }

    /**
     * Check whether intra-broker replica movement is done by comparing replica's current logdir with the logdir proposed
     * by task's proposal.
     *
     * @param logdirInfoByTask Disk information by task.
     * @param task             Task to check for being done.
     * @return {@code true} if task is done, {@code false} otherwise.
     */
    static boolean isIntraBrokerReplicaActionDone(Map<ExecutionTask, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> logdirInfoByTask,
                                                  ExecutionTask task) {
        if (logdirInfoByTask.containsKey(task)) {
            return logdirInfoByTask.get(task).getCurrentReplicaLogDir()
                    .equals(task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId()).logdir());
        }
        return false;
    }

    /**
     * The completeness of leadership movement depends on the task state:
     * <ul>
     *   <li>{@link ExecutionTaskState#IN_PROGRESS}: Done when either (1) the proposed leader becomes the leader, (2) the
     *   partition has no leader in the cluster (e.g. deleted or offline), or (3) the partition has another leader and the
     *   proposed leader is out of ISR.</li>
     *   <li>{@link ExecutionTaskState#ABORTING} or {@link ExecutionTaskState#DEAD}: Always considered as done. The
     *   destination cannot become leader anymore.</li>
     * </ul>
     * <p>
     * There should be no other task state seen here.
     *
     * @param cluster Kafka cluster.
     * @param task    Task to check for being done.
     * @return {@code true} if task is done, {@code false} otherwise.
     */
    static boolean isLeadershipMovementDone(Cluster cluster, ExecutionTask task) {
        switch (task.state()) {
            case IN_PROGRESS:
                TopicPartition tp = task.proposal().topicPartition();
                Node leader = cluster.leaderFor(tp);
                return (leader != null && leader.id() == task.proposal().newLeader().brokerId())
                        || leader == null
                        || !isInIsr(task.proposal().newLeader().brokerId(), cluster, tp);
            case ABORTING:
            case DEAD:
                return true;
            default:
                throw new IllegalStateException("Should never be here.");
        }
    }

    private static boolean isInIsr(Integer leader, Cluster cluster, TopicPartition tp) {
        return Arrays.stream(cluster.partition(tp).inSyncReplicas()).anyMatch(node -> node.id() == leader);
    }
}
