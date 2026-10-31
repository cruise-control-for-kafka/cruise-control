/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ConcurrencyAdjustingRecommendation;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.nimbusds.jose.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ConcurrencyAdjusterTest {
  private static final double MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT = 100.0;
  private static final int NUM_BROKERS = 4;
  private static final Random RANDOM = new Random(0xDEADBEEF);
  private static final int MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA = 2;
  private static final int MOCK_ADDITIVE_INCREASE_LEADERSHIP = 50;
  private static final int MOCK_MD_INTER_BROKER_REPLICA = 2;
  private static final int MOCK_MD_LEADERSHIP = 3;
  private static final int MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER = 12;
  private static final int MOCK_MAX_LEADERSHIP_MOVEMENTS = 1000;
  private static final int MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER = 1;
  private static final int MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG = 50;
  private static final long MOCK_TIME_MS = 100L;
  private static final long MB_IN_BYTES = 1024L * 1024L;
  private static final double MB_IN_KB = 1024.0;
  private static final long MOCK_MIN_REPLICATION_THROTTLE = MB_IN_BYTES;
  private static final long MOCK_MAX_REPLICATION_THROTTLE = 100 * MB_IN_BYTES;
  private static final long MOCK_ADDITIVE_INCREASE_REPLICATION_THROTTLE = 10 * MB_IN_BYTES;
  private static final int MOCK_MD_REPLICATION_THROTTLE = 2;
  private static final double MOCK_REPLICATION_THROTTLE_TARGET_NETWORK_UTILIZATION = 0.8;
  private static final double MOCK_REPLICATION_THROTTLE_WRITE_RATE_MARGIN = 0.2;
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final TopicPartition TP1 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC2, 0);

  /**
   * Setup the test.
   */
  @BeforeClass
  public static void setup() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getExecutorProperties());
    ExecutionUtils.init(config);
  }

  private static Map<Short, Double> populateMetricValues(int numOverLimitMetrics) {
    Map<Short, Double> metricValueById = new HashMap<>();
    int remainingOverLimitMetrics = numOverLimitMetrics;
    for (String name : ExecutionUtils.CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.keySet()) {
      // Generate a number in [1.0, MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT + 1.0]
      double diff = 1.0 + (MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT * RANDOM.nextDouble());
      if (--remainingOverLimitMetrics < 0) {
        diff *= -1.0;
      }
      metricValueById.put(KafkaMetricDef.brokerMetricDef().metricInfo(name).id(), MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT + diff);
    }

    return metricValueById;
  }

  private static Map<String, StringBuilder> initOverLimitDetailsByMetricNameMap() {
    Map<String, StringBuilder> overLimitDetailsByMetricNameMap = new HashMap<>();
    for (String name : ExecutionUtils.CONCURRENCY_ADJUSTER_LIMIT_BY_METRIC_NAME.keySet()) {
      overLimitDetailsByMetricNameMap.put(name, new StringBuilder());
    }
    return overLimitDetailsByMetricNameMap;
  }

  /**
   * Create current load snapshot for brokers.
   * @param metricValueByIdPerBroker A set of metrics with corresponding load value per broker.
   * @return The load for the brokers.
   */
  private static Map<BrokerEntity, ValuesAndExtrapolations> createCurrentMetrics(List<Map<Short, Double>> metricValueByIdPerBroker) {
    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = new HashMap<>();
    for (int i = 0; i < metricValueByIdPerBroker.size(); i++) {
      ValuesAndExtrapolations currentValuesAndExtrapolations = buildValuesAndExtrapolations(metricValueByIdPerBroker.get(i));
      currentMetrics.put(new BrokerEntity(ExecutorTest.class.getSimpleName(), i), currentValuesAndExtrapolations);
    }

    return currentMetrics;
  }

  private static ValuesAndExtrapolations buildValuesAndExtrapolations(Map<Short, Double> metricValueById) {
    return buildValuesAndExtrapolations(metricValueById, null);
  }

  /**
   * Build the values and extrapolations of the current metric window with the given metric values.
   *
   * @param metricValueById Metric value by metric id.
   * @param extrapolation The extrapolation of the current metric window, or {@code null} for no extrapolation.
   * @return The values and extrapolations of the current metric window.
   */
  private static ValuesAndExtrapolations buildValuesAndExtrapolations(Map<Short, Double> metricValueById, Extrapolation extrapolation) {
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    for (Map.Entry<Short, Double> entry : metricValueById.entrySet()) {
      MetricValues currentMetricValues = new MetricValues(1);
      double[] values = new double[] {entry.getValue()};
      currentMetricValues.add(values);
      valuesByMetricId.put(entry.getKey(), currentMetricValues);
    }
    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    Map<Integer, Extrapolation> extrapolations = extrapolation == null ? Collections.emptyMap() : Map.of(0, extrapolation);
    ValuesAndExtrapolations currentValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, extrapolations);
    List<Long> windows = new ArrayList<>(1);
    windows.add(1L);
    currentValuesAndExtrapolations.setWindows(windows);
    return currentValuesAndExtrapolations;
  }

  /**
   * A cluster that contains partitions with two replicas on Nodes 0 and 1.
   * Staring from node-0, given number of replicas are in-sync.
   * Out of sync replicas are offline if isOutOfSyncOffline is {@code true}, they are online otherwise.
   *
   * @param topicNumInSyncs topicPartition: Number of in-sync replicas.
   * @param isOutOfSyncOffline {@code true} if out of sync replicas are offline, {@code false} otherwise.
   * @return A cluster that contains partitions with two replicas, containing given number of in-sync replicas.
   */
  private static Cluster getClusterWithOutOfSyncPartition(List<Pair<TopicPartition, Integer>> topicNumInSyncs, boolean isOutOfSyncOffline) {
    Node node0 = new Node(0, "host0", 100);
    Node node1 = new Node(1, "host1", 100);
    Set<PartitionInfo> partitionInfos = new HashSet<>();
    for (Pair<TopicPartition, Integer> topicNumInSync : topicNumInSyncs) {
      int numInSync = topicNumInSync.getRight();
      if (numInSync > 2 || numInSync < 0) {
        throw new IllegalArgumentException(String.format("numInSync must be in [0,2] (Given: %d).", numInSync));
      }

      Node[] replicas = new Node[2];
      replicas[0] = node0;
      replicas[1] = node1;
      Node[] inSyncReplicas = new Node[numInSync];
      if (numInSync > 0) {
        inSyncReplicas[0] = node0;
        if (numInSync > 1) {
          inSyncReplicas[1] = node1;
        }
      }
      PartitionInfo partitionInfo;
      TopicPartition tp = topicNumInSync.getLeft();
      if (!isOutOfSyncOffline) {
        partitionInfo = new PartitionInfo(tp.topic(), tp.partition(), node0, replicas, inSyncReplicas);
      } else {
        Node[] offlineReplicas = new Node[2 - numInSync];
        if (numInSync == 1) {
          offlineReplicas[0] = node1;
        } else if (numInSync == 0) {
          offlineReplicas[0] = node0;
          offlineReplicas[1] = node1;
        }
        partitionInfo = new PartitionInfo(tp.topic(), tp.partition(), node0, replicas, inSyncReplicas, offlineReplicas);
      }
      partitionInfos.add(partitionInfo);
    }
    return new Cluster("id", Arrays.asList(node0, node1), partitionInfos, Collections.emptySet(), Collections.emptySet());

  }

  @Test
  public void testRecommendedMinIsrBasedConcurrency() {
    // Cluster with an online out of sync partition.
    Cluster cluster = getClusterWithOutOfSyncPartition(Arrays.asList(Pair.of(TP1, 2), Pair.of(TP2, 1)), false);

    // 1. Verify a recommended decrease in concurrency for different concurrency types due to AtMinISR partitions without offline replicas.
    // Cache with a single entry that makes the TP2 in cluster AtMinISR. Since there are 2 replicas of the minISR partition, both brokers
    // should decrease the concurrency.
    Map<String, MinIsrWithTime> minIsrWithTimeByTopic = Map.of(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS),
        TOPIC2, new MinIsrWithTime((short) 1, MOCK_TIME_MS));

    ConcurrencyAdjustingRecommendation concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(cluster,
                                                                                                                  minIsrWithTimeByTopic);
    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertFalse(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertEquals(2, concurrencyAdjustingRecommendation.getBrokersToDecreaseConcurrency().size());
    assertTrue(concurrencyAdjustingRecommendation.shouldDecreaseClusterConcurrency());

    // 2. Verify a recommended cancellation of the execution (i.e. concurrency types is irrelevant) due to UnderMinISR partitions without
    // offline replicas.
    // Cache with a single entry that makes the TP2 in cluster UnderMinISR.
    minIsrWithTimeByTopic = Map.of(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS),
        TOPIC2, new MinIsrWithTime((short) 2, MOCK_TIME_MS));
    concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(cluster, minIsrWithTimeByTopic);
    assertTrue(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldDecreaseClusterConcurrency());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldIncreaseClusterConcurrency());

    // 3. Verify that if the minISR value for topics containing (At/Under)MinISR partitions in the given Kafka cluster is missing from the
    // given cache, then no change in concurrency is recommended.
    concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(cluster, Collections.emptyMap());
    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertTrue(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldDecreaseClusterConcurrency());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldIncreaseClusterConcurrency());

    // 4. Verify no change in concurrency due to lack of (At/Under)MinISR partitions (i.e. concurrency types is irrelevant)
    // Cluster with an all in-sync partition.
    cluster = getClusterWithOutOfSyncPartition(Arrays.asList(Pair.of(TP1, 2), Pair.of(TP2, 2)), false);
    // Cache with a single entry that makes the TP1 in cluster not (At/Under)MinISR.
    minIsrWithTimeByTopic = Map.of(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS),
        TOPIC2, new MinIsrWithTime((short) 1, MOCK_TIME_MS));

    concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(cluster, minIsrWithTimeByTopic);
    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertTrue(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldDecreaseClusterConcurrency());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldIncreaseClusterConcurrency());

    // 5. Verify no change in concurrency due to (At/Under)MinISR partitions with an offline replica (i.e. concurrency types is irrelevant)
    // Cluster with an offline out of sync partition.
    cluster = getClusterWithOutOfSyncPartition(Arrays.asList(Pair.of(TP1, 2), Pair.of(TP2, 1)), true);
    concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(cluster,
                                                                   minIsrWithTimeByTopic);
    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertTrue(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldDecreaseClusterConcurrency());
    assertFalse(ConcurrencyAdjustingRecommendation.STOP_EXECUTION.shouldIncreaseClusterConcurrency());
  }

  @Test
  public void testRecommendedConcurrency() {
    List<Map<Short, Double>> metricValueByIdPerBroker = new ArrayList<>(NUM_BROKERS);
    // Add 1 broker that is within the concurrency limit
    metricValueByIdPerBroker.add(populateMetricValues(0));

    // Add 2 brokers that exceeds the concurrency limit
    metricValueByIdPerBroker.add(populateMetricValues(1));
    metricValueByIdPerBroker.add(populateMetricValues(5));

    Map<BrokerEntity, ValuesAndExtrapolations> currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    // Add 1 broker that doesn't have metric value, expecting it is treated as exceeds the concurrency limit
    currentMetrics.put(new BrokerEntity("", -1), null);

    ConcurrencyAdjustingRecommendation concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(currentMetrics);

    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertFalse(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertEquals(1, concurrencyAdjustingRecommendation.getBrokersToIncreaseConcurrency().size());
    assertEquals(3, concurrencyAdjustingRecommendation.getBrokersToDecreaseConcurrency().size());
    assertTrue(concurrencyAdjustingRecommendation.shouldDecreaseClusterConcurrency());
    assertFalse(concurrencyAdjustingRecommendation.shouldIncreaseClusterConcurrency());

    // Test on all brokers having metrics within limit
    metricValueByIdPerBroker.clear();
    metricValueByIdPerBroker.add(populateMetricValues(0));
    metricValueByIdPerBroker.add(populateMetricValues(0));
    metricValueByIdPerBroker.add(populateMetricValues(0));
    currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(currentMetrics);

    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertFalse(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertEquals(3, concurrencyAdjustingRecommendation.getBrokersToIncreaseConcurrency().size());
    assertEquals(0, concurrencyAdjustingRecommendation.getBrokersToDecreaseConcurrency().size());
    assertFalse(concurrencyAdjustingRecommendation.shouldDecreaseClusterConcurrency());
    assertTrue(concurrencyAdjustingRecommendation.shouldIncreaseClusterConcurrency());

    // Test one broker having metrics above limit
    metricValueByIdPerBroker.clear();
    metricValueByIdPerBroker.add(populateMetricValues(1));
    metricValueByIdPerBroker.add(populateMetricValues(0));
    metricValueByIdPerBroker.add(populateMetricValues(0));
    currentMetrics = createCurrentMetrics(metricValueByIdPerBroker);
    concurrencyAdjustingRecommendation = ExecutionUtils.recommendedConcurrency(currentMetrics);

    assertFalse(concurrencyAdjustingRecommendation.shouldStopExecution());
    assertFalse(concurrencyAdjustingRecommendation.noChangeRecommended());
    assertEquals(2, concurrencyAdjustingRecommendation.getBrokersToIncreaseConcurrency().size());
    assertEquals(1, concurrencyAdjustingRecommendation.getBrokersToDecreaseConcurrency().size());
    assertFalse(concurrencyAdjustingRecommendation.shouldDecreaseClusterConcurrency());
    assertTrue(concurrencyAdjustingRecommendation.shouldIncreaseClusterConcurrency());
  }

  @Test
  public void testWithinConcurrencyAdjusterLimit() {
    Map<String, StringBuilder> overLimitDetailsByMetricNameMap = initOverLimitDetailsByMetricNameMap();

    // Test within concurrency adjuster limit
    ValuesAndExtrapolations valuesAndExtrapolations0 = buildValuesAndExtrapolations(populateMetricValues(0));
    assertTrue(ExecutionUtils.withinConcurrencyAdjusterLimit(0, valuesAndExtrapolations0, overLimitDetailsByMetricNameMap));

    // Test above concurrency adjuster limit
    ValuesAndExtrapolations valuesAndExtrapolations1 = buildValuesAndExtrapolations(populateMetricValues(1));
    assertFalse(ExecutionUtils.withinConcurrencyAdjusterLimit(0, valuesAndExtrapolations1, overLimitDetailsByMetricNameMap));

    // Test null metrics, expecting to return NOT within concurrency adjuster limit
    assertFalse(ExecutionUtils.withinConcurrencyAdjusterLimit(0, null, overLimitDetailsByMetricNameMap));
  }

  /**
   * Get the recommended replication throttle for the given inputs, all in MB/sec.
   *
   * @param currentThrottleMB Current throttle (MB/sec).
   * @param capacityMB Network capacity (MB/sec).
   * @param nonThrottledMB Traffic that the throttle does not limit, i.e. leader and in-sync replication traffic (MB/sec).
   * @param hasInProgressMovements {@code true} if the broker has in-progress throttled movements, {@code false} otherwise.
   * @return The recommended replication throttle (MB/sec).
   */
  private static double recommendedReplicationThrottleInMB(double currentThrottleMB, double capacityMB, double nonThrottledMB,
                                                           boolean hasInProgressMovements) {
    long recommended = ExecutionUtils.recommendedReplicationThrottle((long) (currentThrottleMB * MB_IN_BYTES), 0L, hasInProgressMovements,
                                                                     capacityMB * MB_IN_KB, nonThrottledMB * MB_IN_KB, true);
    return (double) recommended / MB_IN_BYTES;
  }

  /**
   * Simulate the replication throttle adjustments of a broker in one traffic direction, whose throttled replica movements are
   * stalled: the new replicas cannot catch up, so the movements stay in progress. Each round is a metric-based check of the
   * concurrency adjuster (i.e. once per {@link ExecutorConfig#CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG}).
   *
   * @param initialThrottleMB Initial throttle (MB/sec).
   * @param neededMB Throttle needed by the in-progress movements (MB/sec), or {@code 0} if unknown.
   * @param capacityMB Network capacity (MB/sec).
   * @param inSyncReplicationMB Replication traffic of in-sync followers (MB/sec), which the throttle does not limit.
   * @param leaderMBByRound Leader (i.e. client) traffic (MB/sec) in each round.
   * @return The throttle (MB/sec) after each round.
   */
  private static List<Double> simulateStalledMovementThrottleInMB(double initialThrottleMB, double neededMB, double capacityMB,
                                                                  double inSyncReplicationMB, double... leaderMBByRound) {
    List<Double> throttleMBByRound = new ArrayList<>(leaderMBByRound.length);
    long throttle = (long) (initialThrottleMB * MB_IN_BYTES);
    for (double leaderMB : leaderMBByRound) {
      throttle = ExecutionUtils.recommendedReplicationThrottle(throttle, (long) (neededMB * MB_IN_BYTES), true, capacityMB * MB_IN_KB,
                                                               (leaderMB + inSyncReplicationMB) * MB_IN_KB, true);
      throttleMBByRound.add((double) throttle / MB_IN_BYTES);
    }
    return throttleMBByRound;
  }

  @Test
  public void testRecommendedReplicationThrottle() {
    double delta = 1.0 / MB_IN_BYTES;
    // Headroom: 0.8 * 100 - 15 = 65 MB/sec > 20 MB/sec. Increase is bounded by the additive increase.
    assertEquals(30.0, recommendedReplicationThrottleInMB(20.0, 100.0, 15.0, true), delta);
    // Headroom: 0.8 * 100 - 15 = 65 MB/sec > 60 MB/sec. Increase is bounded by the headroom.
    assertEquals(65.0, recommendedReplicationThrottleInMB(60.0, 100.0, 15.0, true), delta);
    // Headroom: 0.8 * 1000 - 0 = 800 MB/sec > 95 MB/sec. Increase is bounded by the maximum throttle.
    assertEquals(100.0, recommendedReplicationThrottleInMB(95.0, 1000.0, 0.0, true), delta);
    // Headroom: 0.8 * 100 - 75 = 5 MB/sec < 30 MB/sec. Decrease to the headroom at once.
    assertEquals(5.0, recommendedReplicationThrottleInMB(30.0, 100.0, 75.0, true), delta);
    // Headroom: 0.8 * 100 - 90 < 0 MB/sec. Decrease is bounded by the minimum throttle.
    assertEquals(1.0, recommendedReplicationThrottleInMB(30.0, 100.0, 90.0, true), delta);
    // Headroom: 0.8 * 100 - 10 = 70 MB/sec > 20 MB/sec, but the broker has no in-progress throttled movements in this direction --
    // i.e. the throttle is not limiting any movement. No increase.
    assertEquals(20.0, recommendedReplicationThrottleInMB(20.0, 100.0, 10.0, false), delta);
    // ... but a decrease to the headroom still applies.
    assertEquals(5.0, recommendedReplicationThrottleInMB(30.0, 100.0, 75.0, false), delta);
    // Headroom: 0.8 * 100 - 100 < 0 MB/sec, but the current throttle (0.5 MB/sec) is already below the minimum throttle. No change.
    assertEquals(0.5, recommendedReplicationThrottleInMB(0.5, 100.0, 100.0, true), delta);
    // Headroom: 0.8 * 1000 - 0 = 800 MB/sec, but the current throttle (200 MB/sec) is already above the maximum throttle. No change.
    assertEquals(200.0, recommendedReplicationThrottleInMB(200.0, 1000.0, 0.0, true), delta);
    // Headroom: 0.8 * 100 - 10 = 70 MB/sec, which is equal to the current throttle. No change.
    assertEquals(70.0, recommendedReplicationThrottleInMB(70.0, 100.0, 10.0, true), delta);

    // Between metric-based checks, only decreases apply: a throttle above the headroom is decreased at once, ...
    long mb = MB_IN_BYTES;
    assertEquals(5 * mb, ExecutionUtils.recommendedReplicationThrottle(30 * mb, 0L, true, 100 * MB_IN_KB, 75 * MB_IN_KB, false));
    // ... and the additive increase does not apply, but the needed throttle does: it is a floor, enforced at each run.
    assertEquals(20 * mb, ExecutionUtils.recommendedReplicationThrottle(20 * mb, 0L, true, 100 * MB_IN_KB, 10 * MB_IN_KB, false));
    assertEquals(40 * mb, ExecutionUtils.recommendedReplicationThrottle(20 * mb, 40 * mb, true, 100 * MB_IN_KB, 10 * MB_IN_KB, false));

    // A change smaller than 5% of the current throttle is ignored: the headroom fluctuates with the traffic.
    // Headroom: 0.8 * 100 - 12 = 68 MB/sec, 2.9% below 70 MB/sec. No change.
    assertEquals(70.0, recommendedReplicationThrottleInMB(70.0, 100.0, 12.0, true), delta);
    // Headroom: 0.8 * 100 - 20 = 60 MB/sec, 14% below 70 MB/sec. Decrease.
    assertEquals(60.0, recommendedReplicationThrottleInMB(70.0, 100.0, 20.0, true), delta);
    // Headroom: 0.8 * 100 - 8 = 72 MB/sec, 2.9% above 70 MB/sec. No change.
    assertEquals(70.0, recommendedReplicationThrottleInMB(70.0, 100.0, 8.0, true), delta);
  }

  @Test
  public void testInitialReplicationThrottle() {
    long mb = MB_IN_BYTES;
    // The headroom: 0.8 * 100 - 30 = 50 MB/sec.
    assertEquals(50 * mb, ExecutionUtils.initialReplicationThrottle(100 * MB_IN_KB, 30 * MB_IN_KB));
    // Bounded by the minimum throttle when there is no headroom ...
    assertEquals(MOCK_MIN_REPLICATION_THROTTLE, ExecutionUtils.initialReplicationThrottle(100 * MB_IN_KB, 90 * MB_IN_KB));
    // ... and by the maximum throttle.
    assertEquals(MOCK_MAX_REPLICATION_THROTTLE, ExecutionUtils.initialReplicationThrottle(1000 * MB_IN_KB, 0.0));
    assertEquals(MOCK_MAX_REPLICATION_THROTTLE, ExecutionUtils.maxReplicationThrottle());
  }

  @Test
  public void testBrokerMetricsWithLastKnownFallback() {
    short leaderBytesInId = KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id();
    ValuesAndExtrapolations valid0 = buildValuesAndExtrapolations(Map.of(leaderBytesInId, 10.0), Extrapolation.AVG_AVAILABLE);
    ValuesAndExtrapolations valid1 = buildValuesAndExtrapolations(Map.of(leaderBytesInId, 20.0), Extrapolation.AVG_AVAILABLE);
    ValuesAndExtrapolations empty = buildValuesAndExtrapolations(Map.of(leaderBytesInId, 0.0), Extrapolation.NO_VALID_EXTRAPOLATION);
    Map<Integer, ExecutionUtils.TimedMetrics> lastValid = new HashMap<>();
    long windowMs = 300_000L;

    // Brokers 0 and 1 have valid current metrics, broker 2 has none and no history: it is omitted.
    assertEquals(Map.of(0, valid0, 1, valid1),
                 ExecutionUtils.brokerMetricsWithLastKnownFallback(Map.of(0, valid0, 1, valid1, 2, empty), lastValid, 0L, windowMs));
    // The window rolls: broker 0 has no sample yet and falls back to its last valid metrics, broker 1 has valid metrics again.
    ValuesAndExtrapolations valid1b = buildValuesAndExtrapolations(Map.of(leaderBytesInId, 30.0), Extrapolation.AVG_AVAILABLE);
    assertEquals(Map.of(0, valid0, 1, valid1b),
                 ExecutionUtils.brokerMetricsWithLastKnownFallback(Map.of(0, empty, 1, valid1b), lastValid, 60_000L, windowMs));
    // The last valid metrics of broker 0 are older than one window by now: it is omitted.
    assertEquals(Map.of(1, valid1b),
                 ExecutionUtils.brokerMetricsWithLastKnownFallback(Map.of(0, empty, 1, valid1b), lastValid, 300_001L, windowMs));
  }

  @Test
  public void testReplicationThrottleAdjustmentForStalledMovement() {
    // A broker with 100 MB/sec network capacity, 20 MB/sec client traffic and 10 MB/sec other replication traffic is the destination
    // of movements whose partitions are written at 35 MB/sec, but its follower throttle is 5 MB/sec: the movements are stalled.
    // Headroom: 0.8 * 100 - (20 + 10) = 50 MB/sec.
    double writeRateMB = 35.0;
    // The movements need 35 * (1 + 0.2) = 42 MB/sec.
    double neededMB = writeRateMB * (1 + MOCK_REPLICATION_THROTTLE_WRITE_RATE_MARGIN);

    // Without the needed throttle, the throttle is increased by the additive increase per round while the movements are in progress,
    // so it exceeds the write rate only after 4 rounds (i.e. 24 minutes with the default concurrency adjuster interval of 6
    // minutes), and stops at the headroom.
    List<Double> throttleMBByRound = simulateStalledMovementThrottleInMB(5.0, 0.0, 100.0, 10.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0);
    assertThrottlesInMB(List.of(15.0, 25.0, 35.0, 45.0, 50.0, 50.0), throttleMBByRound);
    assertTrue(throttleMBByRound.get(2) <= writeRateMB && throttleMBByRound.get(3) > writeRateMB);

    // With the needed throttle, the throttle is increased to the needed throttle at once, then by the additive increase up to the
    // headroom, as long as the movements are in progress.
    throttleMBByRound = simulateStalledMovementThrottleInMB(5.0, neededMB, 100.0, 10.0, 20.0, 20.0, 20.0);
    assertEquals(neededMB, throttleMBByRound.get(0), 1e-3);
    assertTrue(throttleMBByRound.get(0) > writeRateMB);
    assertThrottlesInMB(List.of(50.0, 50.0), throttleMBByRound.subList(1, 3));

    // If the client traffic rises to 60 MB/sec, the throttle is decreased at once towards the new headroom -- i.e.
    // 0.8 * 100 - (60 + 10) = 10 MB/sec -- but not below the needed throttle (42 MB/sec): below it, the movements would stall and keep
    // using their throttle without making progress.
    throttleMBByRound = simulateStalledMovementThrottleInMB(5.0, neededMB, 100.0, 10.0, 20.0, 20.0, 60.0, 60.0);
    assertThrottlesInMB(List.of(50.0, neededMB, neededMB), throttleMBByRound.subList(1, 4));

    // Without headroom (0.8 * 100 - (70 + 10) = 0 MB/sec), the throttle is still the needed throttle.
    throttleMBByRound = simulateStalledMovementThrottleInMB(5.0, neededMB, 100.0, 10.0, 70.0, 70.0, 70.0);
    assertThrottlesInMB(List.of(neededMB, neededMB, neededMB), throttleMBByRound);
    // Without in-progress needs, it is decreased to the minimum throttle.
    throttleMBByRound = simulateStalledMovementThrottleInMB(5.0, 0.0, 100.0, 10.0, 70.0, 70.0, 70.0);
    assertThrottlesInMB(List.of(1.0, 1.0, 1.0), throttleMBByRound);

    // The needed throttle never exceeds the maximum throttle: movements written at 150 MB/sec need 180 MB/sec, but the throttle stops
    // at 100 MB/sec despite 800 MB/sec headroom.
    throttleMBByRound = simulateStalledMovementThrottleInMB(50.0, 150.0 * (1 + MOCK_REPLICATION_THROTTLE_WRITE_RATE_MARGIN), 1000.0, 0.0,
                                                            0.0, 0.0, 0.0);
    assertThrottlesInMB(List.of(100.0, 100.0, 100.0), throttleMBByRound);

    // A throttle below the minimum throttle is raised like any other: to the higher of the needed throttle and the additive increase.
    throttleMBByRound = simulateStalledMovementThrottleInMB(0.5, 6.0, 100.0, 0.0, 10.0);
    assertThrottlesInMB(List.of(10.5), throttleMBByRound);
    throttleMBByRound = simulateStalledMovementThrottleInMB(0.5, 20.0, 100.0, 0.0, 10.0);
    assertThrottlesInMB(List.of(20.0), throttleMBByRound);

    // A needed throttle just above the current throttle never raises it by less than the additive increase: 5 -> 15 MB/sec, not
    // 5 -> 6 MB/sec, so a movement needing more than its write rate (e.g. a partition at its retention limit) is not held back.
    throttleMBByRound = simulateStalledMovementThrottleInMB(5.0, 6.0, 100.0, 10.0, 20.0, 20.0);
    assertThrottlesInMB(List.of(15.0, 25.0), throttleMBByRound);
  }

  @Test
  public void testNeededReplicationThrottles() {
    TopicPartition tp0 = new TopicPartition(TOPIC1, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC1, 1);
    TopicPartition tp2 = new TopicPartition(TOPIC1, 2);
    TopicPartition tp3 = new TopicPartition(TOPIC1, 3);
    TopicPartition tp4 = new TopicPartition(TOPIC1, 4);
    ReplicaPlacementInfo[] r = new ReplicaPlacementInfo[7];
    Node[] nodes = new Node[7];
    for (int brokerId = 0; brokerId < r.length; brokerId++) {
      r[brokerId] = new ReplicaPlacementInfo(brokerId);
      nodes[brokerId] = new Node(brokerId, "host" + brokerId, 100);
    }
    // tp0 (10 MB/sec): broker 0 (leader) -> one new replica on broker 2.
    ExecutionTask task0 = inProgressTask(0L, new ExecutionProposal(tp0, 0, r[0], List.of(r[0], r[1]), List.of(r[0], r[2])),
                                         ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
    // tp1 (5 MB/sec): broker 0 (old leader) -> three new replicas on brokers 3, 4 and 5. Its leadership has moved to broker 1.
    ExecutionTask task1 = inProgressTask(1L, new ExecutionProposal(tp1, 0, r[0], List.of(r[0], r[1], r[2]), List.of(r[3], r[4], r[5])),
                                         ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
    // tp2 (40 MB/sec): pending inter-broker replica movement, and in-progress leadership movement. Both are ignored.
    ExecutionProposal tp2Proposal = new ExecutionProposal(tp2, 0, r[1], List.of(r[1], r[2]), List.of(r[2], r[3]));
    ExecutionTask task2 = new ExecutionTask(2L, tp2Proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, MOCK_TIME_MS);
    ExecutionTask task3 = inProgressTask(3L, tp2Proposal, ExecutionTask.TaskType.LEADER_ACTION);
    // tp3: broker 1 (old leader) -> one new replica on broker 6, write rate unknown. Counted as 0, so that brokers 1 and 6 still have
    // in-progress movements.
    ExecutionTask task4 = inProgressTask(4L, new ExecutionProposal(tp3, 0, r[1], List.of(r[1]), List.of(r[6])),
                                         ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
    // tp4 (2 MB/sec): broker 2 (old leader) -> one new replica on broker 5. Its current leader is unknown.
    ExecutionTask task5 = inProgressTask(5L, new ExecutionProposal(tp4, 0, r[2], List.of(r[2]), List.of(r[2], r[5])),
                                         ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
    Cluster cluster = new Cluster("id", Arrays.asList(nodes),
                                  Set.of(new PartitionInfo(TOPIC1, 0, nodes[0], new Node[]{nodes[0], nodes[1]}, new Node[]{nodes[0], nodes[1]}),
                                         new PartitionInfo(TOPIC1, 1, nodes[1], new Node[]{nodes[0], nodes[1], nodes[2]},
                                                           new Node[]{nodes[0], nodes[1], nodes[2]})),
                                  Collections.emptySet(), Collections.emptySet());

    short leaderBytesInId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN);
    short replicationBytesInId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.REPLICATION_BYTES_IN_RATE);
    Map<PartitionEntity, ValuesAndExtrapolations> currentMetricsByPartition = Map.of(
        new PartitionEntity(tp0), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 10 * MB_IN_KB)),
        new PartitionEntity(tp1), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 5 * MB_IN_KB)),
        new PartitionEntity(tp2), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 40 * MB_IN_KB)),
        new PartitionEntity(tp3), buildValuesAndExtrapolations(Map.of(replicationBytesInId, 1.0)),
        new PartitionEntity(tp4), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 2 * MB_IN_KB)));
    Map<TopicPartition, Double> writeRateKBpsByPartition = ExecutionUtils.partitionWriteRatesKBps(currentMetricsByPartition, new HashMap<>());
    assertEquals(Map.of(tp0, 10 * MB_IN_KB, tp1, 5 * MB_IN_KB, tp2, 40 * MB_IN_KB, tp4, 2 * MB_IN_KB), writeRateKBpsByPartition);

    Map<Integer, Long> neededLeaderThrottleByBroker = new HashMap<>();
    Map<Integer, Long> neededFollowerThrottleByBroker = new HashMap<>();
    ExecutionUtils.neededReplicationThrottles(List.of(task0, task1, task2, task3, task4, task5), writeRateKBpsByPartition, cluster,
                                              Collections.emptyMap(), neededLeaderThrottleByBroker, neededFollowerThrottleByBroker);
    double margin = 1 + MOCK_REPLICATION_THROTTLE_WRITE_RATE_MARGIN;
    // Leader throttle: broker 0 (current leader of tp0) serves one new replica of tp0, broker 1 (current leader of tp1) serves three
    // new replicas of tp1, and broker 2 (old leader of tp4, whose current leader is unknown) serves one new replica of tp4.
    assertEquals(Set.of(0, 1, 2), neededLeaderThrottleByBroker.keySet());
    assertEquals(10 * margin * MB_IN_BYTES, neededLeaderThrottleByBroker.get(0), 1.0);
    assertEquals(3 * 5 * margin * MB_IN_BYTES, neededLeaderThrottleByBroker.get(1), 1.0);
    assertEquals(2 * margin * MB_IN_BYTES, neededLeaderThrottleByBroker.get(2), 1.0);
    // Follower throttle: brokers 2 (tp0), 3, 4 (tp1), 5 (tp1 and tp4) and 6 (tp3, unknown write rate) fetch new replicas.
    assertEquals(Set.of(2, 3, 4, 5, 6), neededFollowerThrottleByBroker.keySet());
    assertEquals(0L, neededFollowerThrottleByBroker.get(6).longValue());
    assertEquals(10 * margin * MB_IN_BYTES, neededFollowerThrottleByBroker.get(2), 1.0);
    assertEquals(5 * margin * MB_IN_BYTES, neededFollowerThrottleByBroker.get(3), 1.0);
    assertEquals(5 * margin * MB_IN_BYTES, neededFollowerThrottleByBroker.get(4), 1.0);
    assertEquals((5 + 2) * margin * MB_IN_BYTES, neededFollowerThrottleByBroker.get(5), 1.0);

    // tp1's movement is escalated to 50 MB/sec per new replica, and tp3's (unknown write rate) to 3 MB/sec.
    neededLeaderThrottleByBroker.clear();
    neededFollowerThrottleByBroker.clear();
    ExecutionUtils.neededReplicationThrottles(List.of(task0, task1, task2, task3, task4, task5), writeRateKBpsByPartition, cluster,
                                              Map.of(tp1, 50 * MB_IN_BYTES, tp3, 3 * MB_IN_BYTES), neededLeaderThrottleByBroker,
                                              neededFollowerThrottleByBroker);
    assertEquals(3 * 50 * MB_IN_BYTES + 3 * MB_IN_BYTES, neededLeaderThrottleByBroker.get(1), 1.0);
    assertEquals(50 * MB_IN_BYTES, neededFollowerThrottleByBroker.get(3), 1.0);
    assertEquals(50 * MB_IN_BYTES + 2 * margin * MB_IN_BYTES, neededFollowerThrottleByBroker.get(5), 1.0);
    assertEquals(3 * MB_IN_BYTES, neededFollowerThrottleByBroker.get(6), 1.0);
    // An escalated need below the need derived from the write rate has no effect.
    neededLeaderThrottleByBroker.clear();
    neededFollowerThrottleByBroker.clear();
    ExecutionUtils.neededReplicationThrottles(List.of(task0), writeRateKBpsByPartition, cluster, Map.of(tp0, MB_IN_BYTES),
                                              neededLeaderThrottleByBroker, neededFollowerThrottleByBroker);
    assertEquals(10 * margin * MB_IN_BYTES, neededFollowerThrottleByBroker.get(2), 1.0);

    // New replicas in the ISR are not throttled, so they need nothing: tp1's new replica on broker 3 is in sync, and tp0's new replica
    // on broker 2 caught up and became the leader before the movement was noticed to be completed.
    Cluster caughtUpCluster = new Cluster("id", Arrays.asList(nodes),
                                          Set.of(new PartitionInfo(TOPIC1, 0, nodes[2], new Node[]{nodes[0], nodes[2]},
                                                                   new Node[]{nodes[0], nodes[2]}),
                                                 new PartitionInfo(TOPIC1, 1, nodes[1], new Node[]{nodes[0], nodes[1], nodes[2], nodes[3]},
                                                                   new Node[]{nodes[0], nodes[1], nodes[2], nodes[3]})),
                                          Collections.emptySet(), Collections.emptySet());
    neededLeaderThrottleByBroker.clear();
    neededFollowerThrottleByBroker.clear();
    ExecutionUtils.neededReplicationThrottles(List.of(task0, task1), writeRateKBpsByPartition, caughtUpCluster, Collections.emptyMap(),
                                              neededLeaderThrottleByBroker, neededFollowerThrottleByBroker);
    assertEquals(Set.of(1), neededLeaderThrottleByBroker.keySet());
    assertEquals(2 * 5 * margin * MB_IN_BYTES, neededLeaderThrottleByBroker.get(1), 1.0);
    assertEquals(Set.of(4, 5), neededFollowerThrottleByBroker.keySet());
  }

  @Test
  public void testNeededThrottleFloorAndCapacityScaledIncrease() {
    long mb = MB_IN_BYTES;
    // The needed throttle beats the network headroom: 0.8 * 100 - 75 = 5 MB/sec, but the movements need 30 MB/sec.
    assertEquals(30 * mb, ExecutionUtils.recommendedReplicationThrottle(50 * mb, 30 * mb, true, 100 * MB_IN_KB, 75 * MB_IN_KB, true));
    // ... and is reached at once from below, even at a run that only allows decreases.
    assertEquals(30 * mb, ExecutionUtils.recommendedReplicationThrottle(2 * mb, 30 * mb, true, 100 * MB_IN_KB, 75 * MB_IN_KB, false));
    // Only the maximum throttle (100 MB/sec) bounds it.
    assertEquals(MOCK_MAX_REPLICATION_THROTTLE,
                 ExecutionUtils.recommendedReplicationThrottle(50 * mb, 150 * mb, true, 100 * MB_IN_KB, 75 * MB_IN_KB, true));

    // The additive increase is at least 5% of the broker's network capacity: 10 MB/sec (configured) on a 125 MB/sec (1 Gbps)
    // broker, 62.5 MB/sec on a 1250 MB/sec (10 Gbps) broker.
    assertEquals(10 * mb, ExecutionUtils.additiveIncreaseStep(125 * MB_IN_KB));
    assertEquals((long) (62.5 * mb), ExecutionUtils.additiveIncreaseStep(1250 * MB_IN_KB));
    // On the 10 Gbps broker, a throttle of 20 MB/sec rises to 82.5 MB/sec in one metric-based check (MAX: 100 MB/sec).
    assertEquals((long) (82.5 * mb),
                 ExecutionUtils.recommendedReplicationThrottle(20 * mb, 0L, true, 1250 * MB_IN_KB, 100 * MB_IN_KB, true));
  }

  @Test
  public void testMovementProgressEscalation() {
    long mb = MB_IN_BYTES;
    // A movement making progress is never escalated.
    ExecutionUtils.MovementProgress progress = new ExecutionUtils.MovementProgress();
    for (long lag = 60; lag > 0; lag -= 10) {
      assertFalse(progress.recordLag(lag * mb, true, mb));
    }
    assertEquals(0L, progress.escalatedNeed());

    // A movement at its needed throttle whose lag does not shrink (a new replica truncated by retention over and over) is escalated
    // after two runs without progress: its needed throttle doubles.
    progress = new ExecutionUtils.MovementProgress();
    assertFalse(progress.recordLag(40 * mb, true, mb));
    assertFalse(progress.recordLag(65 * mb, true, mb));
    assertTrue(progress.recordLag(41 * mb, true, mb));
    assertEquals(2 * mb, progress.escalatedNeed());
    // It needs progress at the escalated throttle: two more runs without it double it again.
    assertFalse(progress.recordLag(50 * mb, true, 2 * mb));
    assertTrue(progress.recordLag(60 * mb, true, 2 * mb));
    assertEquals(4 * mb, progress.escalatedNeed());
    // Progress resets the count.
    assertFalse(progress.recordLag(30 * mb, true, 4 * mb));
    assertFalse(progress.recordLag(35 * mb, true, 4 * mb));
    assertFalse(progress.recordLag(20 * mb, true, 4 * mb));
    assertEquals(4 * mb, progress.escalatedNeed());

    // A movement above its needed throttle is not throttle-bound, so it is never escalated, and only consecutive runs at the needed
    // throttle count.
    progress = new ExecutionUtils.MovementProgress();
    assertFalse(progress.recordLag(40 * mb, false, mb));
    assertFalse(progress.recordLag(40 * mb, false, mb));
    assertFalse(progress.recordLag(40 * mb, true, mb));
    progress.notAtNeededThrottle();
    assertFalse(progress.recordLag(40 * mb, true, mb));
    assertTrue(progress.recordLag(40 * mb, true, mb));

    // An escalation with an unknown write rate starts from the minimum throttle, and never exceeds the maximum throttle.
    progress = new ExecutionUtils.MovementProgress();
    progress.recordLag(10 * mb, true, 0L);
    progress.recordLag(10 * mb, true, 0L);
    assertTrue(progress.recordLag(10 * mb, true, 0L));
    assertEquals(2 * MOCK_MIN_REPLICATION_THROTTLE, progress.escalatedNeed());
    progress = new ExecutionUtils.MovementProgress();
    progress.recordLag(10 * mb, true, 80 * mb);
    progress.recordLag(10 * mb, true, 80 * mb);
    assertTrue(progress.recordLag(10 * mb, true, 80 * mb));
    assertEquals(MOCK_MAX_REPLICATION_THROTTLE, progress.escalatedNeed());
  }

  @Test
  public void testAtNeededThrottle() {
    long mb = MB_IN_BYTES;
    assertTrue(ExecutionUtils.atNeededThrottle(10 * mb, 10 * mb));
    assertTrue(ExecutionUtils.atNeededThrottle(10 * mb + mb / 4, 10 * mb));
    assertFalse(ExecutionUtils.atNeededThrottle(20 * mb, 10 * mb));
    // A movement with an unknown write rate (no need) is at its needed throttle at the minimum throttle.
    assertTrue(ExecutionUtils.atNeededThrottle(MOCK_MIN_REPLICATION_THROTTLE, 0L));
    assertFalse(ExecutionUtils.atNeededThrottle(5 * mb, 0L));
  }

  @Test
  public void testPartitionWriteRatesWithLastKnownFallback() {
    TopicPartition tp0 = new TopicPartition(TOPIC1, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC1, 1);
    TopicPartition tp2 = new TopicPartition(TOPIC1, 2);
    TopicPartition tp3 = new TopicPartition(TOPIC1, 3);
    short leaderBytesInId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN);
    Map<TopicPartition, Double> lastKnownWriteRateKBpsByPartition = new HashMap<>(Map.of(tp1, 5.0, tp3, 7.0));

    // tp0 has a valid write rate, which is recorded as its last known write rate. tp1 and tp2 have no sample in the current metric
    // window yet (e.g. their brokers missed the latest sampling round): tp1 falls back to its last known write rate, tp2 has none. tp3 has no
    // current metrics, so its last known write rate is kept.
    Map<PartitionEntity, ValuesAndExtrapolations> currentMetricsByPartition = Map.of(
        new PartitionEntity(tp0), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 10.0), Extrapolation.AVG_AVAILABLE),
        new PartitionEntity(tp1), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 0.0), Extrapolation.NO_VALID_EXTRAPOLATION),
        new PartitionEntity(tp2), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 0.0), Extrapolation.NO_VALID_EXTRAPOLATION));
    assertEquals(Map.of(tp0, 10.0, tp1, 5.0, tp3, 7.0),
                 ExecutionUtils.partitionWriteRatesKBps(currentMetricsByPartition, lastKnownWriteRateKBpsByPartition));
    assertEquals(Map.of(tp0, 10.0, tp1, 5.0, tp3, 7.0), lastKnownWriteRateKBpsByPartition);

    // tp0 has no sample in the next metric window yet, so it falls back to its last known write rate. tp1 has a valid write rate again.
    currentMetricsByPartition = Map.of(
        new PartitionEntity(tp0), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 0.0), Extrapolation.NO_VALID_EXTRAPOLATION),
        new PartitionEntity(tp1), buildValuesAndExtrapolations(Map.of(leaderBytesInId, 6.0)));
    assertEquals(Map.of(tp0, 10.0, tp1, 6.0, tp3, 7.0),
                 ExecutionUtils.partitionWriteRatesKBps(currentMetricsByPartition, lastKnownWriteRateKBpsByPartition));
  }

  @Test
  public void testHasValidCurrentWindowValues() {
    short leaderBytesInId = KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id();
    assertFalse(ExecutionUtils.hasValidCurrentWindowValues(null));
    assertFalse(ExecutionUtils.hasValidCurrentWindowValues(buildValuesAndExtrapolations(Map.of(leaderBytesInId, 0.0),
                                                                                        Extrapolation.NO_VALID_EXTRAPOLATION)));
    assertTrue(ExecutionUtils.hasValidCurrentWindowValues(buildValuesAndExtrapolations(Map.of(leaderBytesInId, 1.0),
                                                                                       Extrapolation.AVG_AVAILABLE)));
    assertTrue(ExecutionUtils.hasValidCurrentWindowValues(buildValuesAndExtrapolations(Map.of(leaderBytesInId, 1.0))));
  }

  @Test
  public void testBrokersHostingMinIsrPartitions() {
    Node[] nodes = new Node[5];
    for (int brokerId = 0; brokerId < nodes.length; brokerId++) {
      nodes[brokerId] = new Node(brokerId, "host" + brokerId, 100);
    }
    // TP1 on brokers 0 and 1 has one in-sync replica, TP2 on brokers 2 and 3 has two in-sync replicas, and TP3 on brokers 3 and 4 has
    // one in-sync replica and an offline replica.
    TopicPartition tp3 = new TopicPartition(TOPIC2, 1);
    Cluster cluster = new Cluster("id", Arrays.asList(nodes),
                                  Set.of(new PartitionInfo(TOPIC1, TP1.partition(), nodes[0], new Node[]{nodes[0], nodes[1]},
                                                           new Node[]{nodes[0]}),
                                         new PartitionInfo(TOPIC2, TP2.partition(), nodes[2], new Node[]{nodes[2], nodes[3]},
                                                           new Node[]{nodes[2], nodes[3]}),
                                         new PartitionInfo(TOPIC2, tp3.partition(), nodes[3], new Node[]{nodes[3], nodes[4]},
                                                           new Node[]{nodes[3]}, new Node[]{nodes[4]})),
                                  Collections.emptySet(), Collections.emptySet());

    // TP1 is AtMinISR with an out-of-sync replica: only its brokers are affected.
    assertEquals(Set.of(0, 1), ExecutionUtils.brokersHostingMinIsrPartitions(
        cluster, Map.of(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS), TOPIC2, new MinIsrWithTime((short) 1, MOCK_TIME_MS)),
        Collections.emptyList()));
    // TP1 is UnderMinISR with an out-of-sync replica: its brokers are affected. TP2 is AtMinISR by its configuration (replication
    // factor = minISR, all replicas in sync), and TP3 has an offline replica: both are excluded.
    assertEquals(Set.of(0, 1), ExecutionUtils.brokersHostingMinIsrPartitions(
        cluster, Map.of(TOPIC1, new MinIsrWithTime((short) 2, MOCK_TIME_MS), TOPIC2, new MinIsrWithTime((short) 2, MOCK_TIME_MS)),
        Collections.emptyList()));
    // Partitions of topics with unknown minISR are skipped.
    assertTrue(ExecutionUtils.brokersHostingMinIsrPartitions(cluster, Map.of(TOPIC2, new MinIsrWithTime((short) 1, MOCK_TIME_MS)),
                                                             Collections.emptyList()).isEmpty());

    // A partition with a single replica (TP1) is AtMinISR by its configuration: excluded. TP2 on brokers 0, 1 and 2 is AtMinISR
    // (minISR 2), and its replica on broker 2 is out of sync.
    cluster = new Cluster("id", Arrays.asList(nodes),
                          Set.of(new PartitionInfo(TOPIC1, TP1.partition(), nodes[4], new Node[]{nodes[4]}, new Node[]{nodes[4]}),
                                 new PartitionInfo(TOPIC2, TP2.partition(), nodes[0], new Node[]{nodes[0], nodes[1], nodes[2]},
                                                   new Node[]{nodes[0], nodes[1]})),
                          Collections.emptySet(), Collections.emptySet());
    Map<String, MinIsrWithTime> minIsrByTopic = Map.of(TOPIC1, new MinIsrWithTime((short) 1, MOCK_TIME_MS),
                                                       TOPIC2, new MinIsrWithTime((short) 2, MOCK_TIME_MS));
    assertEquals(Set.of(0, 1, 2), ExecutionUtils.brokersHostingMinIsrPartitions(cluster, minIsrByTopic, Collections.emptyList()));
    // The out-of-sync replica of TP2 is the new replica of an in-progress movement: excluded.
    ReplicaPlacementInfo[] r = new ReplicaPlacementInfo[]{new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1),
                                                          new ReplicaPlacementInfo(2)};
    ExecutionProposal proposal = new ExecutionProposal(TP2, 0, r[0], List.of(r[0], r[1]), List.of(r[0], r[1], r[2]));
    assertTrue(ExecutionUtils.brokersHostingMinIsrPartitions(
        cluster, minIsrByTopic, List.of(inProgressTask(0L, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION))).isEmpty());
    // ... but not if its movement is not in progress yet.
    assertEquals(Set.of(0, 1, 2), ExecutionUtils.brokersHostingMinIsrPartitions(
        cluster, minIsrByTopic, List.of(new ExecutionTask(0L, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, MOCK_TIME_MS))));
  }

  @Test
  public void testInSyncReplicationTrafficKBps() {
    Node[] nodes = new Node[4];
    for (int brokerId = 0; brokerId < nodes.length; brokerId++) {
      nodes[brokerId] = new Node(brokerId, "host" + brokerId, 100);
    }
    TopicPartition tp0 = new TopicPartition(TOPIC1, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC1, 1);
    TopicPartition tp2 = new TopicPartition(TOPIC1, 2);
    // tp0 (10 KB/sec): leader 0, followers 1 and 2 in sync. tp1 (5 KB/sec): leader 1, follower 2 in sync and follower 3 out of sync
    // (e.g. a new replica of a throttled movement). tp2 (7 KB/sec) has no leader, and the write rate of tp3 is unknown: both ignored.
    Cluster cluster = new Cluster("id", Arrays.asList(nodes),
                                  Set.of(new PartitionInfo(TOPIC1, 0, nodes[0], new Node[]{nodes[0], nodes[1], nodes[2]},
                                                           new Node[]{nodes[0], nodes[1], nodes[2]}),
                                         new PartitionInfo(TOPIC1, 1, nodes[1], new Node[]{nodes[1], nodes[2], nodes[3]},
                                                           new Node[]{nodes[1], nodes[2]}),
                                         new PartitionInfo(TOPIC1, 2, null, new Node[]{nodes[2], nodes[3]}, new Node[]{nodes[2], nodes[3]}),
                                         new PartitionInfo(TOPIC1, 3, nodes[3], new Node[]{nodes[3], nodes[0]}, new Node[]{nodes[3], nodes[0]})),
                                  Collections.emptySet(), Collections.emptySet());
    Map<Integer, Double> followerInKBpsByBroker = new HashMap<>();
    Map<Integer, Double> leaderOutKBpsByBroker = new HashMap<>();
    ExecutionUtils.inSyncReplicationTrafficKBps(cluster, Map.of(tp0, 10.0, tp1, 5.0, tp2, 7.0), followerInKBpsByBroker,
                                                leaderOutKBpsByBroker);
    assertEquals(Map.of(1, 10.0, 2, 15.0), followerInKBpsByBroker);
    assertEquals(Map.of(0, 20.0, 1, 5.0), leaderOutKBpsByBroker);
  }

  private static void assertThrottlesInMB(List<Double> expectedThrottleMBs, List<Double> actualThrottleMBs) {
    assertEquals(expectedThrottleMBs.size(), actualThrottleMBs.size());
    for (int i = 0; i < expectedThrottleMBs.size(); i++) {
      assertEquals("Throttle after round " + (i + 1), expectedThrottleMBs.get(i), actualThrottleMBs.get(i), 1e-3);
    }
  }

  private static ExecutionTask inProgressTask(long executionId, ExecutionProposal proposal, ExecutionTask.TaskType type) {
    ExecutionTask task = new ExecutionTask(executionId, proposal, type, MOCK_TIME_MS);
    task.inProgress(MOCK_TIME_MS);
    return task;
  }

  @Test
  public void testDecreasedReplicationThrottle() {
    // Multiplicative decrease.
    assertEquals(15 * MB_IN_BYTES, ExecutionUtils.decreasedReplicationThrottle(30 * MB_IN_BYTES, 0L));
    // Multiplicative decrease is bounded by the minimum throttle.
    assertEquals(MOCK_MIN_REPLICATION_THROTTLE, ExecutionUtils.decreasedReplicationThrottle(3 * MB_IN_BYTES / 2, 0L));
    // Current throttle is already below the minimum throttle. No change.
    assertEquals(MB_IN_BYTES / 2, ExecutionUtils.decreasedReplicationThrottle(MB_IN_BYTES / 2, 0L));
    // Multiplicative decrease is bounded by the needed throttle of the in-progress movements ...
    assertEquals(20 * MB_IN_BYTES, ExecutionUtils.decreasedReplicationThrottle(30 * MB_IN_BYTES, 20 * MB_IN_BYTES));
    // ... and a throttle below it is raised to it.
    assertEquals(20 * MB_IN_BYTES, ExecutionUtils.decreasedReplicationThrottle(5 * MB_IN_BYTES, 20 * MB_IN_BYTES));
  }

  @Test
  public void testLatestBrokerMetricValue() {
    short leaderBytesInId = KafkaMetricDef.brokerMetricDef().metricInfo(KafkaMetricDef.LEADER_BYTES_IN.name()).id();
    ValuesAndExtrapolations currentMetrics = buildValuesAndExtrapolations(Collections.singletonMap(leaderBytesInId, 123.0));
    assertEquals(123.0, ExecutionUtils.latestBrokerMetricValue(currentMetrics, KafkaMetricDef.LEADER_BYTES_IN), 0.0);
    assertNull(ExecutionUtils.latestBrokerMetricValue(currentMetrics, KafkaMetricDef.REPLICATION_BYTES_IN_RATE));
  }

  private static Properties getExecutorProperties() {
    Properties props = new Properties();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG, "10");
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "400");
    props.setProperty(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "200");
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_LOG_FLUSH_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_FOLLOWER_FETCH_LOCAL_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_PRODUCE_LOCAL_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_CONSUMER_FETCH_LOCAL_TIME_MS_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_LIMIT_REQUEST_QUEUE_SIZE_CONFIG,
                      Double.toString(MOCK_COMMON_CONCURRENCY_ADJUSTER_LIMIT));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_INTER_BROKER_REPLICA_CONFIG,
                      Integer.toString(MOCK_ADDITIVE_INCREASE_INTER_BROKER_REPLICA));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_LEADERSHIP_CONFIG,
                      Integer.toString(MOCK_ADDITIVE_INCREASE_LEADERSHIP));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_INTER_BROKER_REPLICA_CONFIG,
                      Integer.toString(MOCK_MD_INTER_BROKER_REPLICA));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_LEADERSHIP_CONFIG,
                      Integer.toString(MOCK_MD_LEADERSHIP));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                      Integer.toString(MOCK_MAX_PARTITION_MOVEMENTS_PER_BROKER));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG,
                      Integer.toString(MOCK_MAX_LEADERSHIP_MOVEMENTS));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                      Integer.toString(MOCK_MIN_PARTITION_MOVEMENTS_PER_BROKER));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG,
                      Integer.toString(MOCK_MIN_LEADERSHIP_MOVEMENTS_CONFIG));
    props.setProperty(ExecutorConfig.MIN_NUM_BROKERS_VIOLATE_METRIC_LIMIT_TO_DECREASE_CLUSTER_CONCURRENCY_CONFIG, "2");
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_REPLICATION_THROTTLE_CONFIG, Long.toString(MOCK_MIN_REPLICATION_THROTTLE));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_REPLICATION_THROTTLE_CONFIG, Long.toString(MOCK_MAX_REPLICATION_THROTTLE));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_ADDITIVE_INCREASE_REPLICATION_THROTTLE_CONFIG,
                      Long.toString(MOCK_ADDITIVE_INCREASE_REPLICATION_THROTTLE));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_MULTIPLICATIVE_DECREASE_REPLICATION_THROTTLE_CONFIG,
                      Integer.toString(MOCK_MD_REPLICATION_THROTTLE));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_TARGET_NETWORK_UTILIZATION_CONFIG,
                      Double.toString(MOCK_REPLICATION_THROTTLE_TARGET_NETWORK_UTILIZATION));
    props.setProperty(ExecutorConfig.CONCURRENCY_ADJUSTER_REPLICATION_THROTTLE_WRITE_RATE_MARGIN_CONFIG,
                      Double.toString(MOCK_REPLICATION_THROTTLE_WRITE_RATE_MARGIN));

    return props;
  }
}
