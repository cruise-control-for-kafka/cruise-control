/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ConcurrencyAdjustingRecommendation;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.common.utils.Utils.*;
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
  // The metrics over their concurrency adjuster limit that count for replication throttle adjustment only when they rise -- see
  // withinReplicationThrottleAdjusterLimit.
  static final Set<String> RISE_TRIGGERED_LIMIT_METRIC_NAMES = Set.of(BROKER_LOG_FLUSH_TIME_MS_999TH.name());
  // Cluster concurrency would be decreased if the number of brokers with metrics above the limits defined in
  // CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME is greater than this value.
  private static int minNumBrokersViolateMetricLimitToDecreaseClusterConcurrency;
  private static long listPartitionReassignmentsTimeoutMs;
  private static int listPartitionReassignmentsMaxAttempts;
  static final double BYTES_IN_KB = 1024.0;
  // The concurrency adjuster ignores a replication throttle change smaller than this fraction of the current throttle: the network
  // headroom fluctuates with the traffic, and each change costs a config update and verification of the broker.
  static final double REPLICATION_THROTTLE_CHANGE_TOLERANCE = 0.05;
  // The additive increase of the replication throttle of a broker is at least this fraction of its network capacity in that
  // direction, so brokers with larger network capacity converge to their network headroom in a similar time.
  static final double REPLICATION_THROTTLE_MIN_INCREASE_FRACTION_OF_CAPACITY = 0.05;
  // The progress of an out-of-sync replica is measured over its lag at this many consecutive concurrency adjuster runs -- i.e. over one
  // fewer intervals: long enough that retention, which deletes whole segments at slightly different times on the leader and on the
  // replica, does not hide the progress of a replica that catches up slowly.
  static final int REPLICATION_THROTTLE_PROGRESS_WINDOW_RUNS = 6;
  // The minimum decrease (bytes) of the lag of an out-of-sync replica over the progress window to count as progress.
  static final long REPLICATION_THROTTLE_MIN_PROGRESS_BYTES = 1024L * 1024L;
  // A throttle is considered at the needed throttle if it is at most this factor of it.
  static final double REPLICATION_THROTTLE_AT_NEED_FACTOR = 1.05;
  // A throttle is considered full if the out-of-sync replicas fetching through it copy at least this fraction of it: Kafka's quota
  // enforcement and fetch sizes keep a throttle-bound movement well below its throttle (40-75% measured at 1 MB/sec).
  static final double REPLICATION_THROTTLE_FULL_USE_FRACTION = 0.5;
  // A throttle is considered far above its use -- mostly idle -- if the out-of-sync replicas fetching through it copy less than this
  // fraction of it: what holds them back is not the throttle (e.g. 3% of it on a production cluster with a disk-bound movement).
  static final double REPLICATION_THROTTLE_LOW_USE_FRACTION = 0.25;
  // A throttle far above its use at this many consecutive metric-based checks is lowered to its use.
  static final int REPLICATION_THROTTLE_LOW_USE_CHECKS_TO_LOWER = 2;
  // Bounds and adjustment parameters of the replication throttle (bytes/sec) used by the concurrency adjuster.
  private static long minReplicationThrottle;
  private static long maxReplicationThrottle;
  private static long additiveIncreaseReplicationThrottle;
  private static int multiplicativeDecreaseReplicationThrottle;
  private static double replicationThrottleTargetNetworkUtilization;
  private static double replicationThrottleWriteRateMargin;
  private static double initialReplicationThrottleHeadroomFraction;

  private ExecutionUtils() { }

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
    minReplicationThrottle = config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_REPLICATION_THROTTLE_CONFIG);
    maxReplicationThrottle = config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_REPLICATION_THROTTLE_CONFIG);
    additiveIncreaseReplicationThrottle = config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_REPLICATION_THROTTLE_CONFIG);
    multiplicativeDecreaseReplicationThrottle =
        config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_REPLICATION_THROTTLE_CONFIG);
    replicationThrottleTargetNetworkUtilization =
        config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_TARGET_NETWORK_UTILIZATION_CONFIG);
    replicationThrottleWriteRateMargin = config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_WRITE_RATE_MARGIN_CONFIG);
    initialReplicationThrottleHeadroomFraction =
        config.getDouble(ExecutorConfig.CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_INITIAL_HEADROOM_FRACTION_CONFIG);
  }

  /**
   * Get the latest value of the given broker metric.
   *
   * @param currentMetrics Current metrics of a broker.
   * @param metricDef The broker metric to retrieve.
   * @return The latest value of the given broker metric, or {@code null} if the metric is not available.
   */
  @Nullable
  static Double latestBrokerMetricValue(ValuesAndExtrapolations currentMetrics, KafkaMetricDef metricDef) {
    MetricValues metricValues = currentMetrics.metricValues().valuesFor(KafkaMetricDef.brokerMetricDef().metricInfo(metricDef.name()).id());
    return metricValues == null ? null : (double) metricValues.latest();
  }

  /**
   * Check whether the given current metrics of an entity (i.e. metrics of the current metric window) have valid values. They do not if
   * the entity has no metric sample in the current window -- e.g. it missed the sampling round that started the window (a new
   * window starts with a sample) -- in which case all values are {@code 0}.
   *
   * @param currentMetrics Current metrics of an entity, or {@code null} if unavailable.
   * @return {@code true} if the given current metrics have valid values, {@code false} otherwise.
   */
  static boolean hasValidCurrentWindowValues(@Nullable ValuesAndExtrapolations currentMetrics) {
    return currentMetrics != null && currentMetrics.extrapolations().get(0) != Extrapolation.NO_VALID_EXTRAPOLATION;
  }

  /**
   * Get the write rate of partitions, i.e. the latest value of their {@link KafkaMetricDef#LEADER_BYTES_IN} metric. If the current
   * metrics of a partition have no valid values (see {@link #hasValidCurrentWindowValues}), its last known write rate is used.
   *
   * @param currentMetricsByPartition Current metrics of partitions.
   * @param lastKnownWriteRateKBpsByPartition The last known write rate (KB/sec) by partition, which is updated with the valid write
   *                                          rates of the given current metrics.
   * @return The write rate (KB/sec) by partition, for the partitions whose write rate is valid or known.
   */
  static Map<TopicPartition, Double> partitionWriteRatesKBps(Map<PartitionEntity, ValuesAndExtrapolations> currentMetricsByPartition,
                                                             Map<TopicPartition, Double> lastKnownWriteRateKBpsByPartition) {
    short leaderBytesInId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN);
    for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : currentMetricsByPartition.entrySet()) {
      MetricValues metricValues = entry.getValue().metricValues().valuesFor(leaderBytesInId);
      if (metricValues != null && hasValidCurrentWindowValues(entry.getValue())) {
        lastKnownWriteRateKBpsByPartition.put(entry.getKey().tp(), (double) metricValues.latest());
      }
    }
    return new HashMap<>(lastKnownWriteRateKBpsByPartition);
  }

  /**
   * Populate the replication throttle (bytes/sec) that the given in-progress inter-broker replica movements need on each
   * broker, i.e. (1 + {@link ExecutorConfig#CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_WRITE_RATE_MARGIN_CONFIG}) times the total
   * write rate of the partitions being moved. A throttle below the write rate stalls the movements, because the new replicas
   * cannot catch up with the leader; a new replica of a partition at its retention limit needs more than twice the write rate,
   * because retention truncates a replica that falls behind the log start of the leader. For a partition with write rate
   * {@code w}:
   * <ul>
   *   <li>Leader throttle: its current leader (or its old leader, if the current leader is unknown), which the out-of-sync replicas
   *   fetch from, needs {@code w} for each out-of-sync replica -- see {@link #outOfSyncReplicas(ExecutionProposal, Cluster)} -- plus
   *   the write rate for each in-sync follower -- see {@link #inSyncFollowers(ExecutionProposal, Cluster)}.</li>
   *   <li>Follower throttle: each broker with an out-of-sync replica needs {@code w}, until the replica is in sync, and each broker
   *   with an in-sync follower needs the write rate -- if its replica is a follower throttled replica (see
   *   {@link #followerThrottled(ExecutionProposal, int)}).</li>
   * </ul>
   * Kafka counts the replication traffic of every replica of a throttled partition against the throttle, in sync or not, and only
   * spares the in-sync ones from being blocked once the throttle is exceeded. So the traffic of the in-sync followers takes its share
   * of the throttle first: a throttle at the need of the out-of-sync replicas alone would leave them only what the in-sync followers do
   * not use -- e.g. nothing for a new replica, if the throttle is at the traffic of a kept replica. The old replicas of the movement are
   * throttled too, so once one falls out of the ISR (e.g. under the load of the movements), it needs {@code w} like a new replica: a
   * replica to keep to catch up -- without it, the concurrency adjuster could decrease the throttle of its broker to the minimum
   * throttle and keep it out of the ISR for good -- and a replica to remove because it keeps fetching under that throttle, taking its
   * share from the others there.
   * Each broker with in-progress movements in a direction has an entry in the corresponding map, even if the write rate of the
   * partitions being moved is unknown (counted as {@code 0}). Tasks other than in-progress inter-broker replica movements are
   * ignored. The throttle needed by a movement that made no progress at its needed throttle is escalated -- see
   * {@link MovementProgress}.
   *
   * @param tasks Execution tasks.
   * @param writeRateKBpsByPartition The write rate (KB/sec) by partition.
   * @param cluster Kafka cluster, to get the current leader of partitions.
   * @param escalatedNeedByPartition The escalated throttle (bytes/sec) needed for each new replica of a partition being moved, if
   *                                 its movement has been escalated.
   * @param neededLeaderThrottleByBroker A map to populate with the leader throttle (bytes/sec) needed by each broker.
   * @param neededFollowerThrottleByBroker A map to populate with the follower throttle (bytes/sec) needed by each broker.
   */
  static void neededReplicationThrottles(Collection<ExecutionTask> tasks,
                                         Map<TopicPartition, Double> writeRateKBpsByPartition,
                                         Cluster cluster,
                                         Map<TopicPartition, Long> escalatedNeedByPartition,
                                         Map<Integer, Long> neededLeaderThrottleByBroker,
                                         Map<Integer, Long> neededFollowerThrottleByBroker) {
    for (ExecutionTask task : tasks) {
      if (task.type() != ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION || task.state() != ExecutionTaskState.IN_PROGRESS) {
        continue;
      }
      ExecutionProposal proposal = task.proposal();
      List<Integer> outOfSyncReplicas = outOfSyncReplicas(proposal, cluster);
      if (outOfSyncReplicas.isEmpty()) {
        continue;
      }
      TopicPartition tp = proposal.topicPartition();
      double writeRateKBps = writeRateKBpsByPartition.getOrDefault(tp, 0.0);
      long neededPerReplica = Math.max(neededReplicationThrottle(writeRateKBps), escalatedNeedByPartition.getOrDefault(tp, 0L));
      long inSyncPerReplica = (long) (writeRateKBps * BYTES_IN_KB);
      List<Integer> inSyncFollowers = inSyncFollowers(proposal, cluster);
      int leader = movementLeader(proposal, cluster);
      neededLeaderThrottleByBroker.merge(leader, neededPerReplica * outOfSyncReplicas.size() + inSyncPerReplica * inSyncFollowers.size(),
                                         Long::sum);
      for (int brokerId : outOfSyncReplicas) {
        if (followerThrottled(proposal, brokerId)) {
          neededFollowerThrottleByBroker.merge(brokerId, neededPerReplica, Long::sum);
        }
      }
      for (int brokerId : inSyncFollowers) {
        if (followerThrottled(proposal, brokerId)) {
          neededFollowerThrottleByBroker.merge(brokerId, inSyncPerReplica, Long::sum);
        }
      }
    }
  }

  /**
   * The replicas of a movement that fetch from its leader under the replication throttle and are blocked once it is exceeded: Kafka
   * blocks every replica of a throttled partition that is not in sync (the throttled replicas of a movement are its old replicas and
   * its replicas to add), i.e.
   * <ul>
   *   <li>the replicas to add that are not in the ISR yet -- all of them if the partition is unknown,</li>
   *   <li>the replicas to keep that are out of the ISR -- e.g. a follower that fell behind under the load of the movements, which
   *   must catch up for the partition to be fully in sync again, and (with ZooKeeper) for its reassignment to complete, and</li>
   *   <li>the replicas to remove that are out of the ISR but still assigned: they need not catch up, but keep fetching under the
   *   throttle of their broker -- sharing it with the other replicas there -- until the reassignment completes.</li>
   * </ul>
   *
   * @param proposal The proposal of an inter-broker replica movement.
   * @param cluster Kafka cluster.
   * @return Ids of the brokers whose replica of the movement is out of sync, replicas to add first.
   */
  static List<Integer> outOfSyncReplicas(ExecutionProposal proposal, Cluster cluster) {
    PartitionInfo partitionInfo = cluster.partition(proposal.topicPartition());
    Set<Integer> inSync = new HashSet<>();
    if (partitionInfo != null && partitionInfo.inSyncReplicas() != null) {
      Arrays.stream(partitionInfo.inSyncReplicas()).forEach(node -> inSync.add(node.id()));
    }
    List<Integer> outOfSyncReplicas = new ArrayList<>();
    proposal.replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId).filter(id -> !inSync.contains(id))
            .forEach(outOfSyncReplicas::add);
    if (!inSync.isEmpty()) {
      // A partition without in-sync replicas (e.g. offline) has nothing for its replicas to catch up with, and a replica that is no
      // longer assigned (e.g. removed by a reassignment that completed) does not fetch.
      Set<Integer> assigned = new HashSet<>();
      Arrays.stream(partitionInfo.replicas()).forEach(node -> assigned.add(node.id()));
      proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).filter(id -> assigned.contains(id) && !inSync.contains(id))
              .forEach(outOfSyncReplicas::add);
    }
    return outOfSyncReplicas;
  }

  /**
   * The in-sync followers of a movement: the assigned replicas in the ISR other than the leader. Kafka never blocks them, but counts
   * what they fetch -- the write rate of the partition -- against the replication throttle of the leader, and against their own
   * follower throttle if their replica is a follower throttled replica (see {@link #followerThrottled(ExecutionProposal, int)}).
   * E.g. a replica to keep, a replica to remove until the movement completes, or a replica to add that caught up.
   *
   * @param proposal The proposal of an inter-broker replica movement.
   * @param cluster Kafka cluster.
   * @return Ids of the brokers whose replica of the movement is an in-sync follower, empty if the partition or its leader is unknown.
   */
  static List<Integer> inSyncFollowers(ExecutionProposal proposal, Cluster cluster) {
    PartitionInfo partitionInfo = cluster.partition(proposal.topicPartition());
    if (partitionInfo == null || partitionInfo.leader() == null || partitionInfo.inSyncReplicas() == null) {
      return Collections.emptyList();
    }
    Set<Integer> assigned = new HashSet<>();
    Arrays.stream(partitionInfo.replicas()).forEach(node -> assigned.add(node.id()));
    List<Integer> inSyncFollowers = new ArrayList<>();
    for (Node node : partitionInfo.inSyncReplicas()) {
      if (node.id() != partitionInfo.leader().id() && assigned.contains(node.id())) {
        inSyncFollowers.add(node.id());
      }
    }
    return inSyncFollowers;
  }

  /**
   * @param proposal The proposal of an inter-broker replica movement.
   * @param brokerId Id of a broker with a replica of the movement.
   * @return {@code true} if the follower throttle of the broker applies to its replica of the movement, i.e. if the replica is a
   * follower throttled replica: {@link ReplicationThrottleHelper} lists the old replicas and the replicas to add of a movement.
   */
  static boolean followerThrottled(ExecutionProposal proposal, int brokerId) {
    return proposal.oldReplicas().stream().anyMatch(r -> r.brokerId() == brokerId)
           || proposal.replicasToAdd().stream().anyMatch(r -> r.brokerId() == brokerId);
  }

  /**
   * @param writeRateKBps The write rate (KB/sec) of a partition.
   * @return The throttle (bytes/sec) needed for a new replica of the partition to catch up: (1 +
   * {@link ExecutorConfig#CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_WRITE_RATE_MARGIN_CONFIG}) times its write rate.
   */
  static long neededReplicationThrottle(double writeRateKBps) {
    return (long) (writeRateKBps * (1 + replicationThrottleWriteRateMargin) * BYTES_IN_KB);
  }

  /**
   * @param proposal The proposal of an inter-broker replica movement.
   * @param cluster Kafka cluster.
   * @return The broker that the new replicas of the movement fetch from: the current leader of the partition, or its old leader if
   * the current leader is unknown.
   */
  static int movementLeader(ExecutionProposal proposal, Cluster cluster) {
    Node currentLeader = cluster.leaderFor(proposal.topicPartition());
    return currentLeader != null ? currentLeader.id() : proposal.oldLeader().brokerId();
  }

  /**
   * The progress of the out-of-sync replicas of an in-progress inter-broker replica movement (see
   * {@link #outOfSyncReplicas(ExecutionProposal, Cluster)}), from their lag behind the leader at the last
   * {@link #REPLICATION_THROTTLE_PROGRESS_WINDOW_RUNS} concurrency adjuster runs. It gives
   * <ul>
   *   <li>the rate at which each replica copies the partition (see {@link #copyRate(int, double)}), i.e. its share of the replication
   *   throttles it fetches through -- which tells whether a throttle is full, and</li>
   *   <li>whether a replica makes no progress (see {@link #stalled(int)}), in which case the needed throttle of the movement is
   *   escalated if a throttle it fetches through is at its need and in use -- i.e. if more throttle is what it lacks. A replica of a
   *   partition at its retention limit is truncated whenever retention deletes past it, so it may need several times the write rate,
   *   depending on the segment size, the retention and the throttle actually achieved.</li>
   * </ul>
   */
  static final class MovementProgress {
    // The samples {time (ms), lag (bytes), size (bytes)} of each out-of-sync replica by broker, oldest first.
    private final Map<Integer, ArrayDeque<long[]>> _lagSamplesByBroker = new HashMap<>();
    private long _escalatedNeed = 0L;
    private boolean _stallReported = false;

    /**
     * Forget the replicas that are no longer out of sync -- e.g. a kept replica that is back in the ISR.
     *
     * @param brokerIds Ids of the brokers whose replica is still out of sync.
     */
    void retainReplicas(Collection<Integer> brokerIds) {
      _lagSamplesByBroker.keySet().retainAll(brokerIds);
    }

    /**
     * Record the lag of an out-of-sync replica, i.e. the size of the leader replica minus the size of that replica, and its size.
     *
     * @param brokerId Id of the broker of the replica.
     * @param timeMs The time of the observation.
     * @param lagBytes The lag (bytes).
     * @param sizeBytes The size (bytes) of the replica.
     */
    void recordLag(int brokerId, long timeMs, long lagBytes, long sizeBytes) {
      ArrayDeque<long[]> samples = _lagSamplesByBroker.computeIfAbsent(brokerId, b -> new ArrayDeque<>());
      samples.addLast(new long[]{timeMs, lagBytes, sizeBytes});
      while (samples.size() > REPLICATION_THROTTLE_PROGRESS_WINDOW_RUNS) {
        samples.removeFirst();
      }
    }

    /**
     * The rate at which a replica copies the partition, over its recorded samples: the larger of
     * <ul>
     *   <li>the write rate plus the rate at which its lag shrinks -- right also at the retention limit, where retention deletes the
     *   same old segments from the leader and from the replica, and</li>
     *   <li>the sum of its size increases over the samples -- right while a replica that falls behind the log start of the leader is
     *   truncated over and over (a partition at its retention limit copied at about its write rate): each truncation makes its lag
     *   jump, but not what it copied between truncations.</li>
     * </ul>
     *
     * @param brokerId Id of the broker of the replica.
     * @param writeRateBytesPerSec The write rate (bytes/sec) of the partition, {@code 0} if unknown.
     * @return The copy rate (bytes/sec) of the replica, or {@code -1} if fewer than two samples are recorded.
     */
    double copyRate(int brokerId, double writeRateBytesPerSec) {
      ArrayDeque<long[]> samples = _lagSamplesByBroker.get(brokerId);
      if (samples == null || samples.size() < 2 || samples.getLast()[0] <= samples.getFirst()[0]) {
        return -1.0;
      }
      double seconds = (samples.getLast()[0] - samples.getFirst()[0]) / 1000.0;
      double fromLag = writeRateBytesPerSec + (samples.getFirst()[1] - samples.getLast()[1]) / seconds;
      long growth = 0L;
      long[] previous = null;
      for (long[] sample : samples) {
        if (previous != null) {
          growth += Math.max(0L, sample[2] - previous[2]);
        }
        previous = sample;
      }
      return Math.max(0.0, Math.max(fromLag, growth / seconds));
    }

    /**
     * @param brokerId Id of the broker of the replica.
     * @return {@code true} if the lag of the replica was recorded at the last {@link #REPLICATION_THROTTLE_PROGRESS_WINDOW_RUNS} runs,
     * and shrank by less than {@link #REPLICATION_THROTTLE_MIN_PROGRESS_BYTES} over them.
     */
    boolean stalled(int brokerId) {
      ArrayDeque<long[]> samples = _lagSamplesByBroker.get(brokerId);
      return samples != null && samples.size() == REPLICATION_THROTTLE_PROGRESS_WINDOW_RUNS
             && samples.getFirst()[1] - samples.getLast()[1] < REPLICATION_THROTTLE_MIN_PROGRESS_BYTES;
    }

    /**
     * @param brokerId Id of the broker of the replica.
     * @return The last recorded lag (bytes) of the replica, or {@code -1} if none.
     */
    long lag(int brokerId) {
      ArrayDeque<long[]> samples = _lagSamplesByBroker.get(brokerId);
      return samples == null || samples.isEmpty() ? -1L : samples.getLast()[1];
    }

    /**
     * Double the needed throttle of the movement (at least the minimum throttle, at most the maximum throttle), and measure the
     * progress of its replicas afresh, at the escalated throttle.
     *
     * @param currentNeed The current throttle (bytes/sec) needed for each out-of-sync replica of the movement.
     * @return The escalated need (bytes/sec).
     */
    long escalate(long currentNeed) {
      _escalatedNeed = Math.min(maxReplicationThrottle, 2 * Math.max(minReplicationThrottle, Math.max(currentNeed, _escalatedNeed)));
      _lagSamplesByBroker.clear();
      _stallReported = false;
      return _escalatedNeed;
    }

    /**
     * @return {@code true} the first time it is called since the movement was tracked or escalated: a stall that more throttle
     * would not fix is reported once.
     */
    boolean reportStall() {
      boolean firstReport = !_stallReported;
      _stallReported = true;
      return firstReport;
    }

    /**
     * @return The escalated throttle (bytes/sec) needed for each out-of-sync replica of the movement, or {@code 0} if not escalated.
     */
    long escalatedNeed() {
      return _escalatedNeed;
    }
  }

  /**
   * The replication traffic (bytes/sec) of the in-progress movements on each broker side: of their out-of-sync replicas, from their
   * copy rates -- see {@link MovementProgress#copyRate(int, double)} -- and of their in-sync followers, from the write rate (see
   * {@link #inSyncFollowers(ExecutionProposal, Cluster)}), which Kafka counts against the throttle too. A side with a replica whose
   * copy rate is unknown is incomplete: its traffic is a lower bound.
   */
  static final class ReplicationThrottleUse {
    private final Map<Integer, Double> _leaderUseByBroker = new HashMap<>();
    private final Map<Integer, Double> _followerUseByBroker = new HashMap<>();
    private final Set<Integer> _incompleteLeaderSides = new HashSet<>();
    private final Set<Integer> _incompleteFollowerSides = new HashSet<>();

    /**
     * @param leader Id of the broker that the replica fetches from.
     * @param follower Id of the broker of the replica.
     * @param copyRate The copy rate (bytes/sec) of the replica, negative if unknown.
     */
    void add(int leader, int follower, double copyRate) {
      add(leader, follower, true, copyRate);
    }

    /**
     * @param leader Id of the broker that the replica fetches from.
     * @param follower Id of the broker of the replica.
     * @param followerThrottled {@code true} if the follower throttle of its broker applies to the replica (see
     *                          {@link #followerThrottled(ExecutionProposal, int)}), {@code false} to count it on the leader side only.
     * @param copyRate The copy rate (bytes/sec) of the replica, negative if unknown.
     */
    void add(int leader, int follower, boolean followerThrottled, double copyRate) {
      if (copyRate < 0) {
        _incompleteLeaderSides.add(leader);
        _leaderUseByBroker.putIfAbsent(leader, 0.0);
        if (followerThrottled) {
          _incompleteFollowerSides.add(follower);
          _followerUseByBroker.putIfAbsent(follower, 0.0);
        }
      } else {
        _leaderUseByBroker.merge(leader, copyRate, Double::sum);
        if (followerThrottled) {
          _followerUseByBroker.merge(follower, copyRate, Double::sum);
        }
      }
    }

    /**
     * @param brokerId Broker id.
     * @param leader {@code true} for the leader side, {@code false} for the follower side.
     * @return The measured replication traffic (bytes/sec) on the given side of the broker, {@code 0} if none.
     */
    double use(int brokerId, boolean leader) {
      return (leader ? _leaderUseByBroker : _followerUseByBroker).getOrDefault(brokerId, 0.0);
    }

    /**
     * @param brokerId Broker id.
     * @param leader {@code true} for the leader side, {@code false} for the follower side.
     * @return {@code true} if the copy rate of every replica catching up through the given side of the broker is known.
     */
    boolean complete(int brokerId, boolean leader) {
      return !(leader ? _incompleteLeaderSides : _incompleteFollowerSides).contains(brokerId);
    }
  }

  /**
   * @param use The replication traffic (bytes/sec) through a throttle.
   * @param throttle The throttle (bytes/sec).
   * @return {@code true} if the traffic is at least {@link #REPLICATION_THROTTLE_FULL_USE_FRACTION} of the throttle.
   */
  static boolean throttleFull(double use, long throttle) {
    return use >= REPLICATION_THROTTLE_FULL_USE_FRACTION * throttle;
  }

  /**
   * @param use The replication traffic (bytes/sec) through a throttle.
   * @param throttle The throttle (bytes/sec).
   * @param neededThrottle The throttle (bytes/sec) needed by the in-progress movements through it.
   * @return {@code true} if the throttle is at its need (see {@link #atNeededThrottle(long, long)}) and in use, i.e. not far above its
   * use (see {@link #throttleFarAboveUse(double, long)}): a replica that makes no progress through it lacks throttle, which only an
   * escalation of its need gives it. Kafka's quota enforcement keeps a throttle-bound movement well below a small throttle, so "full"
   * is not required; a throttle the movements leave mostly idle is not what holds them back.
   */
  static boolean throttleLimitsAtNeed(double use, long throttle, long neededThrottle) {
    return !throttleFarAboveUse(use, throttle) && atNeededThrottle(throttle, neededThrottle);
  }

  /**
   * @param throttle The throttle (bytes/sec) of a broker in one direction.
   * @param neededThrottle The throttle (bytes/sec) needed by the in-progress movements of the broker in that direction.
   * @return {@code true} if the throttle is at most {@link #REPLICATION_THROTTLE_AT_NEED_FACTOR} times the needed throttle (at least
   * the minimum throttle) -- i.e. the throttle may be what the movements lack.
   */
  static boolean atNeededThrottle(long throttle, long neededThrottle) {
    return throttle <= REPLICATION_THROTTLE_AT_NEED_FACTOR * Math.max(minReplicationThrottle, neededThrottle);
  }

  /**
   * Populate the replication traffic (KB/sec) of in-sync followers on each broker, using the write rate of partitions: an in-sync
   * follower of a partition with write rate {@code w} fetches {@code w} from the leader. Kafka never blocks in-sync followers, so
   * this is replication traffic that the throttle does not limit -- unlike the traffic of out-of-sync replicas, such as the new
   * replicas of the throttled replica movements -- although Kafka counts it against the throttle for the partitions being moved
   * (see {@link #neededReplicationThrottles}).
   *
   * <p>The broker-level replication metrics cannot be used instead: Kafka reports {@code ReplicationBytesInPerSec} and
   * {@code ReplicationBytesOutPerSec} for all topics only, and the broker metric sample replaces them with the sum of the
   * corresponding topic-level metrics -- i.e. {@code 0}.</p>
   *
   * @param cluster Kafka cluster, to get the leader and the in-sync replicas of partitions.
   * @param writeRateKBpsByPartition The write rate (KB/sec) by partition. Partitions whose write rate is unknown are ignored.
   * @param followerInKBpsByBroker A map to populate with the inbound replication traffic (KB/sec) of in-sync followers on each broker.
   * @param leaderOutKBpsByBroker A map to populate with the outbound replication traffic (KB/sec) to in-sync followers of each broker.
   */
  static void inSyncReplicationTrafficKBps(Cluster cluster,
                                           Map<TopicPartition, Double> writeRateKBpsByPartition,
                                           Map<Integer, Double> followerInKBpsByBroker,
                                           Map<Integer, Double> leaderOutKBpsByBroker) {
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        Double writeRateKBps = writeRateKBpsByPartition.get(new TopicPartition(topic, partitionInfo.partition()));
        Node leader = partitionInfo.leader();
        if (writeRateKBps == null || leader == null) {
          continue;
        }
        for (Node inSyncReplica : partitionInfo.inSyncReplicas()) {
          if (inSyncReplica.id() != leader.id()) {
            followerInKBpsByBroker.merge(inSyncReplica.id(), writeRateKBps, Double::sum);
            leaderOutKBpsByBroker.merge(leader.id(), writeRateKBps, Double::sum);
          }
        }
      }
    }
  }

  /**
   * Get the brokers that are behind on an (At/Under)MinISR partition without offline replicas: the brokers whose replica of such a
   * partition is out of sync, except the replicas being added by the given in-progress inter-broker replica movements, which are out
   * of sync until they catch up. If {@link ExecutorConfig#CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_MIN_ISR_CHECK_ENABLED_CONFIG} is
   * enabled, the concurrency adjuster decreases their replication throttles, and the executor starts no new inter-broker replica
   * movement to or from them, until they are back in sync. Partitions with offline replicas are excluded, since moving their replicas
   * is often the very purpose of the execution. A partition that is (At/Under)MinISR by its configuration (e.g. a replication factor
   * of 1, or equal to its minISR) has no broker behind while its replicas are in sync.
   *
   * <p>Only the brokers that are behind are slowed down, not the other brokers hosting the partition: a broker whose replicas fall
   * behind is the one that struggles (e.g. its disks are saturated by the movements and its clients), and slowing down the healthy
   * brokers would only slow the execution down. A replica that is behind catches up through the leader throttle of its leader, which
   * keeps what it needs (see {@link #neededReplicationThrottles}), and through the follower throttle of its own broker if it is a
   * follower throttled replica (see {@link #followerThrottled(ExecutionProposal, int)}), which keeps what it needs too.</p>
   *
   * @param cluster Kafka cluster.
   * @param minIsrWithTimeByTopic Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
   * @param tasks Execution tasks, to exclude the replicas being added by in-progress inter-broker replica movements.
   * @param brokersBehind A set to populate with the brokers whose replica of an (At/Under)MinISR partition without offline replicas
   *                      is out of sync.
   */
  static void brokersBehindOnMinIsrPartitions(Cluster cluster,
                                              Map<String, MinIsrWithTime> minIsrWithTimeByTopic,
                                              Collection<ExecutionTask> tasks,
                                              Set<Integer> brokersBehind) {
    Map<TopicPartition, Set<Integer>> replicasToAddByPartition = new HashMap<>();
    for (ExecutionTask task : tasks) {
      if (task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION && task.state() == ExecutionTaskState.IN_PROGRESS) {
        Set<Integer> replicasToAdd = replicasToAddByPartition.computeIfAbsent(task.proposal().topicPartition(), tp -> new HashSet<>());
        task.proposal().replicasToAdd().forEach(r -> replicasToAdd.add(r.brokerId()));
      }
    }
    Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    Set<PartitionInfo> underMinIsr = new TreeSet<>(comparator);
    Set<PartitionInfo> atMinIsr = new TreeSet<>(comparator);
    populateMinIsrState(cluster, minIsrWithTimeByTopic, underMinIsr, atMinIsr, false);
    for (Set<PartitionInfo> partitions : Arrays.asList(underMinIsr, atMinIsr)) {
      for (PartitionInfo partitionInfo : partitions) {
        Set<Integer> replicasToAdd = replicasToAddByPartition.getOrDefault(
            new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), Collections.emptySet());
        Set<Integer> inSync = new HashSet<>();
        Arrays.stream(partitionInfo.inSyncReplicas()).forEach(node -> inSync.add(node.id()));
        for (Node replica : partitionInfo.replicas()) {
          if (!inSync.contains(replica.id()) && !replicasToAdd.contains(replica.id())) {
            brokersBehind.add(replica.id());
          }
        }
      }
    }
  }

  /**
   * Recommend the replication throttle of a broker in one traffic direction (i.e. inbound for the follower throttle and outbound
   * for the leader throttle) based on the network headroom of the broker in that direction.
   *
   * <p>The target throttle is the throttle that would bring the network utilization of the broker to the target utilization
   * ({@link ExecutorConfig#CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_TARGET_NETWORK_UTILIZATION_CONFIG}), i.e. the target network
   * utilization minus the traffic that the throttle does not limit: the leader (i.e. client) traffic and the replication traffic
   * of in-sync followers (see {@link #inSyncReplicationTrafficKBps}).</p>
   *
   * <ul>
   *   <li>If the target is below the current throttle, the throttle is decreased to the target at once (bounded by the minimum
   *   throttle).</li>
   *   <li>Otherwise, if {@code canIncrease} is {@code true} and the broker has in-progress throttled replica movements in this
   *   direction, the throttle follows the replication traffic measured through it (see {@link ReplicationThrottleUse}):
   *   <ul>
   *     <li>If the traffic fills it (see {@link #throttleFull(double, long)}) -- i.e. only if the throttle is what limits the
   *     movements -- it is raised, never above the target or the maximum throttle: doubled during slow start (from the start of the
   *     execution until the first decrease or concurrency adjuster limit violation of the broker), by the additive increase (see
   *     {@link #additiveIncreaseStep}) after that.</li>
   *     <li>If it is far above the traffic at consecutive checks ({@code lowerToUse}, see {@link #throttleFarAboveUse(double, long)}),
   *     it is lowered to the traffic plus the additive increase: a throttle far above what the movements use protects nothing, as
   *     the movements burst up to it as soon as whatever holds them back clears.</li>
   *     <li>Otherwise, or if the traffic is unknown, it is left unchanged.</li>
   *   </ul></li>
   *   <li>In all cases, the throttle is at least the throttle needed by the in-progress replica movements (see
   *   {@link #neededReplicationThrottles}), even above the target: a throttle below it would stall them -- see
   *   {@link #withNeededThrottleFloor}. Only the maximum throttle bounds it.</li>
   * </ul>
   * The concurrency adjuster checks for a decrease at each run, and allows an increase only at metric-based checks. A change smaller
   * than {@link #REPLICATION_THROTTLE_CHANGE_TOLERANCE} of the current throttle is ignored.
   *
   * @param currentThrottle The current throttle (bytes/sec) of the broker in the relevant direction.
   * @param neededThrottle The throttle (bytes/sec) needed by the in-progress replica movements of the broker in the relevant
   *                       direction, or {@code 0} if unknown.
   * @param hasInProgressMovements {@code true} if the broker has in-progress throttled replica movements in the relevant direction,
   *                               {@code false} otherwise.
   * @param networkCapacityKBps The network capacity (KB/sec) of the broker in the relevant direction.
   * @param nonThrottledTrafficKBps The traffic (KB/sec) of the broker in the relevant direction that the throttle does not limit,
   *                                i.e. the leader traffic and the replication traffic of in-sync followers.
   * @param canIncrease {@code true} if the throttle may be changed by the replication traffic through it, {@code false} if it may
   *                    only be decreased to the target.
   * @param useBytesPerSec The replication traffic (bytes/sec) measured through the throttle, or a negative value if unknown.
   * @param slowStart {@code true} to double the throttle when it is full, {@code false} to raise it by the additive increase.
   * @param lowerToUse {@code true} if the throttle has been far above the replication traffic through it at consecutive checks.
   * @return The recommended throttle (bytes/sec) of the broker in the relevant direction.
   */
  static long recommendedReplicationThrottle(long currentThrottle,
                                             long neededThrottle,
                                             boolean hasInProgressMovements,
                                             double networkCapacityKBps,
                                             double nonThrottledTrafficKBps,
                                             boolean canIncrease,
                                             double useBytesPerSec,
                                             boolean slowStart,
                                             boolean lowerToUse) {
    long targetThrottle = targetReplicationThrottle(networkCapacityKBps, nonThrottledTrafficKBps);
    long recommendedThrottle = currentThrottle;
    if (targetThrottle < currentThrottle) {
      // Decrease at once (MIN: minReplicationThrottle, and the needed throttle below).
      recommendedThrottle = Math.min(currentThrottle, Math.max(minReplicationThrottle, targetThrottle));
    } else if (canIncrease && hasInProgressMovements && useBytesPerSec >= 0) {
      long step = additiveIncreaseStep(networkCapacityKBps);
      if (throttleFull(useBytesPerSec, currentThrottle)) {
        // Increase (MAX: target throttle and maxReplicationThrottle): double during slow start, additive increase after it.
        long maxThrottle = Math.min(targetThrottle, maxReplicationThrottle);
        long increase = slowStart ? Math.max(currentThrottle, step) : step;
        recommendedThrottle = Math.max(currentThrottle, Math.min(maxThrottle, currentThrottle + increase));
      } else if (lowerToUse) {
        // Lower to the traffic plus a step (MIN: minReplicationThrottle, and the needed throttle below).
        recommendedThrottle = Math.min(currentThrottle, Math.max(minReplicationThrottle, (long) useBytesPerSec + step));
      }
    }
    return withNeededThrottleFloor(currentThrottle, recommendedThrottle, neededThrottle);
  }

  /**
   * @param use The replication traffic (bytes/sec) through a throttle.
   * @param throttle The throttle (bytes/sec).
   * @return {@code true} if the traffic is below {@link #REPLICATION_THROTTLE_LOW_USE_FRACTION} of the throttle.
   */
  static boolean throttleFarAboveUse(double use, long throttle) {
    return use < REPLICATION_THROTTLE_LOW_USE_FRACTION * throttle;
  }

  /**
   * @param networkCapacityKBps The network capacity (KB/sec) of a broker in one traffic direction.
   * @return The additive increase (bytes/sec) of the replication throttle of the broker in that direction: the larger of
   * {@link ExecutorConfig#CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_REPLICATION_THROTTLE_CONFIG} and
   * {@link #REPLICATION_THROTTLE_MIN_INCREASE_FRACTION_OF_CAPACITY} of the capacity.
   */
  static long additiveIncreaseStep(double networkCapacityKBps) {
    return Math.max(additiveIncreaseReplicationThrottle,
                    (long) (REPLICATION_THROTTLE_MIN_INCREASE_FRACTION_OF_CAPACITY * networkCapacityKBps * BYTES_IN_KB));
  }

  /**
   * Apply the floor of the replication throttle of a broker in one direction to a recommended throttle: the throttle needed by the
   * in-progress movements in that direction, bounded only by the maximum throttle. A throttle below it would stall the movements --
   * a stalled movement keeps using its throttle without making progress -- so no decrease goes below it, even below the network
   * headroom, and a throttle below it is raised to it. Then ignore a change smaller than {@link #REPLICATION_THROTTLE_CHANGE_TOLERANCE}
   * of the current throttle.
   *
   * @param currentThrottle The current throttle (bytes/sec).
   * @param recommendedThrottle The recommended throttle (bytes/sec) before the floor.
   * @param neededThrottle The throttle (bytes/sec) needed by the in-progress movements, or {@code 0} if none or unknown.
   * @return The recommended throttle (bytes/sec).
   */
  static long withNeededThrottleFloor(long currentThrottle, long recommendedThrottle, long neededThrottle) {
    long throttle = Math.max(recommendedThrottle, Math.min(maxReplicationThrottle, neededThrottle));
    return Math.abs(throttle - currentThrottle) < REPLICATION_THROTTLE_CHANGE_TOLERANCE * currentThrottle ? currentThrottle : throttle;
  }

  /**
   * @param networkCapacityKBps The network capacity (KB/sec) of a broker in one traffic direction.
   * @param nonThrottledTrafficKBps The traffic (KB/sec) of the broker in that direction that the throttle does not limit.
   * @return The throttle (bytes/sec) that would bring the network utilization of the broker in that direction to the target network
   * utilization, or {@code 0} if the broker is already above it.
   */
  static long targetReplicationThrottle(double networkCapacityKBps, double nonThrottledTrafficKBps) {
    double targetThrottleKBps = replicationThrottleTargetNetworkUtilization * networkCapacityKBps - nonThrottledTrafficKBps;
    return (long) Math.max(0.0, targetThrottleKBps * BYTES_IN_KB);
  }

  /**
   * Get a fraction of the network headroom of a broker in one traffic direction (see {@link #targetReplicationThrottle}), bounded by
   * the minimum and maximum throttles of the concurrency adjuster -- e.g. the replication throttle to start a broker at, for an
   * execution that was started without a replication throttle while replication throttle adjustment is enabled (see
   * {@link #initialReplicationThrottleHeadroomFraction()}).
   *
   * @param networkCapacityKBps The network capacity (KB/sec) of the broker in that direction.
   * @param nonThrottledTrafficKBps The traffic (KB/sec) of the broker in that direction that the throttle does not limit.
   * @param headroomFraction The fraction (in (0, 1]) of the network headroom.
   * @return The replication throttle (bytes/sec) of the broker in that direction.
   */
  static long initialReplicationThrottle(double networkCapacityKBps, double nonThrottledTrafficKBps, double headroomFraction) {
    long headroom = targetReplicationThrottle(networkCapacityKBps, nonThrottledTrafficKBps);
    return Math.min(maxReplicationThrottle, Math.max(minReplicationThrottle, (long) (headroomFraction * headroom)));
  }

  /**
   * @return The fraction of its network headroom that each broker starts at, in an execution started without a replication throttle
   * while replication throttle adjustment is enabled -- see
   * {@link ExecutorConfig#CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_INITIAL_HEADROOM_FRACTION_CONFIG}.
   */
  static double initialReplicationThrottleHeadroomFraction() {
    return initialReplicationThrottleHeadroomFraction;
  }

  /**
   * @return The maximum replication throttle (bytes/sec) that the concurrency adjuster can set on a broker.
   */
  static long maxReplicationThrottle() {
    return maxReplicationThrottle;
  }

  /**
   * Get the metrics of brokers to adjust the replication throttle with: the metrics of the current metric window if they have
   * valid values, or otherwise the last valid metrics of the broker if they were seen within the given maximum age -- e.g. if the
   * broker missed the sampling round that started the current window. Brokers with neither are omitted.
   *
   * @param currentMetricsByBrokerId Current metrics by broker id.
   * @param lastValidMetricsByBrokerId The last valid metrics by broker id, with the time they were seen; updated with the valid
   *                                   current metrics.
   * @param nowMs The current time (ms).
   * @param maxAgeMs The maximum age (ms) of the last valid metrics to fall back to.
   * @return The metrics to use by broker id.
   */
  static Map<Integer, ValuesAndExtrapolations> brokerMetricsWithLastKnownFallback(
      Map<Integer, ValuesAndExtrapolations> currentMetricsByBrokerId,
      Map<Integer, TimedMetrics> lastValidMetricsByBrokerId,
      long nowMs,
      long maxAgeMs) {
    for (Map.Entry<Integer, ValuesAndExtrapolations> entry : currentMetricsByBrokerId.entrySet()) {
      if (hasValidCurrentWindowValues(entry.getValue())) {
        lastValidMetricsByBrokerId.put(entry.getKey(), new TimedMetrics(entry.getValue(), nowMs));
      }
    }
    Map<Integer, ValuesAndExtrapolations> metricsByBrokerId = new HashMap<>();
    for (Map.Entry<Integer, TimedMetrics> entry : lastValidMetricsByBrokerId.entrySet()) {
      if (nowMs - entry.getValue().timeMs() <= maxAgeMs) {
        metricsByBrokerId.put(entry.getKey(), entry.getValue().metrics());
      }
    }
    return metricsByBrokerId;
  }

  /**
   * Metrics of an entity with the time they were seen.
   */
  static final class TimedMetrics {
    private final ValuesAndExtrapolations _metrics;
    private final long _timeMs;

    TimedMetrics(ValuesAndExtrapolations metrics, long timeMs) {
      _metrics = metrics;
      _timeMs = timeMs;
    }

    ValuesAndExtrapolations metrics() {
      return _metrics;
    }

    long timeMs() {
      return _timeMs;
    }
  }

  /**
   * Multiplicative-decrease the given replication throttle (MIN: minimum replication throttle of concurrency adjuster, and the
   * throttle needed by the in-progress movements -- see {@link #withNeededThrottleFloor}).
   *
   * @param currentThrottle The current throttle (bytes/sec).
   * @param neededThrottle The throttle (bytes/sec) needed by the in-progress movements, or {@code 0} if none or unknown.
   * @return The decreased throttle (bytes/sec).
   */
  static long decreasedReplicationThrottle(long currentThrottle, long neededThrottle) {
    long decreased = Math.min(currentThrottle, Math.max(minReplicationThrottle, currentThrottle / multiplicativeDecreaseReplicationThrottle));
    return withNeededThrottleFloor(currentThrottle, decreased, neededThrottle);
  }

  public static String toMetricName(Short metricId) {
    return KafkaMetricDef.brokerMetricDef().metricInfo(metricId).name();
  }

  /**
   * Check whether the current metrics are within the limit specified by {@link #CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME}.
   * Package private for unit tests.
   *
   * @param brokerId the broker id to check if its metrics is in limit value
   * @param currentMetrics Current metrics of the broker.
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
   * Check whether the current metrics of a broker are within the concurrency adjuster limits for replication throttle adjustment:
   * like {@link #withinConcurrencyAdjusterLimit(int, ValuesAndExtrapolations, Map)}, except that a metric in
   * {@link #RISE_TRIGGERED_LIMIT_METRIC_NAMES} counts only if it is over its limit and rose since the previous metric-based check
   * of the broker -- not at the first check, and not while it stays at the same value.
   *
   * <p>Kafka reports the log flush time from a histogram of about a thousand recent samples, and logs are flushed rarely (about once
   * per segment roll), so its 99.9th percentile is in effect the slowest of the recent flushes: a single slow flush keeps it over its
   * limit for hours, at a value that does not change. A rise means a new, slower flush since the previous check.</p>
   *
   * @param brokerId Broker id.
   * @param currentMetrics Current metrics of the broker.
   * @param lastRiseTriggeredValueByMetricName The values of the metrics in {@link #RISE_TRIGGERED_LIMIT_METRIC_NAMES} at the
   *                                           previous metric-based check of the broker; updated with the current values.
   * @param overLimitDetailsByMetricName The string builder to build the over-limit report.
   * @return {@code true} if the broker is within the limits, {@code false} otherwise.
   */
  static boolean withinReplicationThrottleAdjusterLimit(int brokerId,
                                                        ValuesAndExtrapolations currentMetrics,
                                                        Map<String, Double> lastRiseTriggeredValueByMetricName,
                                                        Map<String, StringBuilder> overLimitDetailsByMetricName) {
    boolean withinAdjusterLimit = true;
    for (Short metricId : currentMetrics.metricValues().metricIds()) {
      String metricName = toMetricName(metricId);
      Double limit = CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.get(metricName);
      if (limit == null) {
        continue;
      }
      double metricValue = currentMetrics.metricValues().valuesFor(metricId).latest();
      boolean overLimit = metricValue > limit;
      if (RISE_TRIGGERED_LIMIT_METRIC_NAMES.contains(metricName)) {
        Double previousValue = lastRiseTriggeredValueByMetricName.put(metricName, metricValue);
        overLimit = overLimit && previousValue != null && metricValue > previousValue;
      }
      if (overLimit) {
        overLimitDetailsByMetricName.get(metricName).append(String.format("%d(%.2f) ", brokerId, metricValue));
        withinAdjusterLimit = false;
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
   * @param cluster Kafka cluster.
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
      for (PartitionInfo partitionInfo: atMinIsr) {
        for (Node replica: partitionInfo.replicas()) {
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
   *
   * If the minISR value for a topic in the given Kafka cluster is missing from the given {@code minIsrWithTimeByTopic}, this function skips
   * populating minIsr state for partitions of that topic.
   *
   * @param cluster Kafka cluster.
   * @param minIsrWithTimeByTopic Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
   * @param underMinIsrWithoutOfflineReplicas UnderMinISR without offline replicas.
   * @param atMinIsrWithoutOfflineReplicas AtMinISR partitions without offline replicas.
   * @param oneAboveMinIsrWithoutOfflineReplicas oneAboveMinISR partitions without offline replicas.
   * @param withOfflineReplicas {@code true} to retrieve (At/Under/OneAbove)MinISR partitions each containing at least an offline replica,
   * {@code false} to retrieve (At/Under/OneAbove)MinISR partitions without any offline replicas.
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
   *
   * If the minISR value for a topic in the given Kafka cluster is missing from the given {@code minIsrWithTimeByTopic}, this function skips
   * populating minIsr state for partitions of that topic.
   *
   * @param cluster Kafka cluster.
   * @param minIsrWithTimeByTopic Value and capture time of {@link org.apache.kafka.common.config.TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} by topic.
   * @param underMinIsrWithoutOfflineReplicas UnderMinISR without offline replicas.
   * @param atMinIsrWithoutOfflineReplicas AtMinISR partitions without offline replicas.
   * @param withOfflineReplicas {@code true} to retrieve (At/Under)MinISR partitions each containing at least an offline replica,
   * {@code false} to retrieve (At/Under)MinISR partitions without any offline replicas.
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
  public static Set<TopicPartition> partitionsBeingReassigned(Admin adminClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    return ongoingPartitionReassignments(adminClient).keySet();
  }

  /**
   * Retrieve the map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   *
   * If the response times out, this method retries up to {@link #listPartitionReassignmentsMaxAttempts} times.
   * The maximum time to wait for the admin client response is computed as:
   * {@link #listPartitionReassignmentsTimeoutMs} * ({@link #DEFAULT_RETRY_BACKOFF_BASE} ^ {@code attempt}).
   *
   * @param adminClient The adminClient to ask for ongoing partition reassignments.
   * @return The map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   */
  public static Map<TopicPartition, PartitionReassignment> ongoingPartitionReassignments(Admin adminClient)
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
   * @param tasks Preferred leader election tasks to execute.
   * @return The {@link ElectLeadersResult result} of preferred leader election request -- cannot be {@code null}.
   */
  public static ElectLeadersResult submitPreferredLeaderElection(Admin adminClient, List<ExecutionTask> tasks) {
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
   * @param tasks Inter-broker replica reassignment tasks to execute.
   * @return The {@link AlterPartitionReassignmentsResult result} of reassignment request -- cannot be {@code null}.
   */
  public static AlterPartitionReassignmentsResult submitReplicaReassignmentTasks(Admin adminClient, List<ExecutionTask> tasks) {
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
  public static AlterPartitionReassignmentsResult maybeStopPartitionReassignment(Admin adminClient) {
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
   * @param partitionsInMovement The partitions that are being moved in kafka cluster
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
   * @param result the result of a request to alter partition reassignments, or {@code null} if no new reassignment submitted.
   * @param deletedTopicPartitions a set to populate with partitions that were deleted upon submission of the corresponding
   *                               inter-broker replica reassignment tasks.
   * @param deadTopicPartitions a set to populate with partitions that were dead upon submission of the corresponding
   *                            inter-broker replica reassignment tasks.
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
    for (Map.Entry<TopicPartition, KafkaFuture<Void>> entry: result.values().entrySet()) {
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
   * @param result the result of a request to elect leaders, or {@code null} if no new leader election is submitted.
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
   *
   * There should be no other task state seen here.
   *
   * @param cluster Kafka cluster.
   * @param task Task to check for being done.
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
   * @param task Task to check for being done.
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
   *
   * There should be no other task state seen here.
   *
   * @param cluster Kafka cluster.
   * @param task Task to check for being done.
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
