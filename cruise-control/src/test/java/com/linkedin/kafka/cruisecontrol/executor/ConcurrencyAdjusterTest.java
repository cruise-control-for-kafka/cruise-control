/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ConcurrencyAdjustingRecommendation;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
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
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    for (Map.Entry<Short, Double> entry : metricValueById.entrySet()) {
      MetricValues currentMetricValues = new MetricValues(1);
      double[] values = new double[] {entry.getValue()};
      currentMetricValues.add(values);
      valuesByMetricId.put(entry.getKey(), currentMetricValues);
    }
    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    ValuesAndExtrapolations currentValuesAndExtrapolations = new ValuesAndExtrapolations(aggregatedMetricValues, null);
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

    return props;
  }
}
