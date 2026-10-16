/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.EMPTY_BROKER_CAPACITY;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.*;
import static com.linkedin.kafka.cruisecontrol.model.ModelUtils.estimateLeaderCpuUtilPerCore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


/**
 * Unit test for CruiseControlMetricsProcessor
 */
public class CruiseControlMetricsProcessorTest {
  private static final double MOCK_NUM_CPU_CORES = 32;
  private static final int BYTES_IN_KB = 1024;
  private static final int BYTES_IN_MB = 1024 * 1024;
  private static final int P0 = 0;
  private static final int P1 = 1;
  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, P0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, P1);
  private static final TopicPartition T2P0 = new TopicPartition(TOPIC2, P0);
  private static final TopicPartition T2P1 = new TopicPartition(TOPIC2, P1);
  private static final int BROKER_ID_0 = 0;
  private static final int BROKER_ID_1 = 1;
  private static final double DELTA = 0.001;
  private static final double B0_CPU = 50.0;
  private static final double B1_CPU = 30.0;
  private static final double B0_ALL_TOPIC_BYTES_IN = 820.0;
  private static final double B1_ALL_TOPIC_BYTES_IN = 500.0;
  private static final double B0_ALL_TOPIC_BYTES_OUT = 1380.0;
  private static final double B1_ALL_TOPIC_BYTES_OUT = 500.0;
  private static final double B0_TOPIC1_BYTES_IN = 20.0;
  private static final double B1_TOPIC1_BYTES_IN = 500.0;
  private static final double B0_TOPIC2_BYTES_IN = 800.0;
  private static final double B0_TOPIC1_BYTES_OUT = 80.0;
  private static final double B1_TOPIC1_BYTES_OUT = 500.0;
  private static final double B0_TOPIC2_BYTES_OUT = 1300.0;
  private static final double B1_TOPIC1_REPLICATION_BYTES_IN = 20.0;
  private static final double B0_TOPIC1_REPLICATION_BYTES_IN = 500.0;
  private static final double B1_TOPIC2_REPLICATION_BYTES_IN = 800.0;
  private static final double B0_TOPIC1_REPLICATION_BYTES_OUT = 20.0;
  private static final double B1_TOPIC1_REPLICATION_BYTES_OUT = 500.0;
  private static final double B0_TOPIC2_REPLICATION_BYTES_OUT = 800.0;
  private static final double T1P0_BYTES_SIZE = 100.0;
  private static final double T1P1_BYTES_SIZE = 300.0;
  private static final double T2P0_BYTES_SIZE = 200.0;
  private static final double T2P1_BYTES_SIZE = 500.0;
  private static final Set<TopicPartition> TEST_PARTITIONS = Set.of(T1P0, T1P1, T2P0, T2P1);
  private static final Map<TopicPartition, Double> CPU_UTIL =
      Map.of(T1P0, MOCK_NUM_CPU_CORES
                   * estimateLeaderCpuUtilPerCore(B0_CPU,
                                                  B0_ALL_TOPIC_BYTES_IN,
                                                  B0_ALL_TOPIC_BYTES_OUT + B0_TOPIC1_REPLICATION_BYTES_OUT + B0_TOPIC2_REPLICATION_BYTES_OUT,
                                                  B0_TOPIC1_REPLICATION_BYTES_IN,
                                                  B0_TOPIC1_BYTES_IN,
                                                  B0_TOPIC1_BYTES_OUT + B0_TOPIC1_REPLICATION_BYTES_OUT),
             T1P1, MOCK_NUM_CPU_CORES
                   * estimateLeaderCpuUtilPerCore(B1_CPU,
                                                  B1_ALL_TOPIC_BYTES_IN,
                                                  B1_ALL_TOPIC_BYTES_OUT + B1_TOPIC1_REPLICATION_BYTES_OUT,
                                                  B1_TOPIC1_REPLICATION_BYTES_IN + B1_TOPIC2_REPLICATION_BYTES_IN,
                                                  B1_TOPIC1_BYTES_IN,
                                                  B1_TOPIC1_BYTES_OUT + B1_TOPIC1_REPLICATION_BYTES_OUT),
             T2P0, MOCK_NUM_CPU_CORES
                   * estimateLeaderCpuUtilPerCore(B0_CPU,
                                                  B0_ALL_TOPIC_BYTES_IN,
                                                  B0_ALL_TOPIC_BYTES_OUT + B0_TOPIC1_REPLICATION_BYTES_OUT + B0_TOPIC2_REPLICATION_BYTES_OUT,
                                                  B0_TOPIC1_REPLICATION_BYTES_IN,
                                                  B0_TOPIC2_BYTES_IN / 2,
                                                  (B0_TOPIC2_BYTES_OUT + B0_TOPIC2_REPLICATION_BYTES_OUT) / 2),
             T2P1, MOCK_NUM_CPU_CORES
                   * estimateLeaderCpuUtilPerCore(B0_CPU,
                                                  B0_ALL_TOPIC_BYTES_IN,
                                                  B0_ALL_TOPIC_BYTES_OUT + B0_TOPIC1_REPLICATION_BYTES_OUT + B0_TOPIC2_REPLICATION_BYTES_OUT,
                                                  B0_TOPIC1_REPLICATION_BYTES_IN,
                                                  B0_TOPIC2_BYTES_IN / 2,
                                                  (B0_TOPIC2_BYTES_OUT + B0_TOPIC2_REPLICATION_BYTES_OUT) / 2));
  private final Time _time = new MockTime(0, 100L, TimeUnit.NANOSECONDS.convert(100L, TimeUnit.MILLISECONDS));

  private static BrokerCapacityConfigResolver mockBrokerCapacityConfigResolver() throws TimeoutException, BrokerCapacityResolutionException {
    BrokerCapacityConfigResolver brokerCapacityConfigResolver = EasyMock.mock(BrokerCapacityConfigResolver.class);
    EasyMock.expect(brokerCapacityConfigResolver.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyInt(),
                                                                   EasyMock.anyLong(), EasyMock.anyBoolean()))
            .andReturn(new BrokerCapacityInfo(EMPTY_BROKER_CAPACITY, Collections.emptyMap(), MOCK_NUM_CPU_CORES)).anyTimes();
    EasyMock.replay(brokerCapacityConfigResolver);
    return brokerCapacityConfigResolver;
  }

  @Test
  public void testWithCpuCapacityEstimation() throws TimeoutException, BrokerCapacityResolutionException {
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    // All estimated.
    BrokerCapacityConfigResolver brokerCapacityConfigResolverAllEstimated = EasyMock.mock(BrokerCapacityConfigResolver.class);
    EasyMock.expect(brokerCapacityConfigResolverAllEstimated.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(),
                                                                               EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.eq(false)))
            .andThrow(new BrokerCapacityResolutionException("Unable to resolve capacity.")).anyTimes();
    EasyMock.replay(brokerCapacityConfigResolverAllEstimated);

    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(brokerCapacityConfigResolverAllEstimated, false);
    for (CruiseControlMetric cruiseControlMetric : metrics) {
      processor.addMetric(cruiseControlMetric);
    }

    Cluster cluster = getCluster();
    processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    for (Node node : cluster.nodes()) {
      assertNull(processor.cachedNumCoresByBroker().get(node.id()));
    }

    // Capacity resolver unable to retrieve broker capacity.
    BrokerCapacityConfigResolver brokerCapacityConfigResolverTimeout = EasyMock.mock(BrokerCapacityConfigResolver.class);
    EasyMock.expect(brokerCapacityConfigResolverTimeout.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(),
                                                                          EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyBoolean()))
            .andThrow(new TimeoutException("Unable to resolve capacity.")).anyTimes();
    EasyMock.replay(brokerCapacityConfigResolverTimeout);

    processor = new CruiseControlMetricsProcessor(brokerCapacityConfigResolverTimeout, false);
    for (CruiseControlMetric cruiseControlMetric : metrics) {
      processor.addMetric(cruiseControlMetric);
    }

    cluster = getCluster();
    processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    for (Node node : cluster.nodes()) {
      assertNull(processor.cachedNumCoresByBroker().get(node.id()));
    }

    // Some estimated.
    BrokerCapacityConfigResolver brokerCapacityConfigResolverSomeEstimated = EasyMock.mock(BrokerCapacityConfigResolver.class);
    EasyMock.expect(brokerCapacityConfigResolverSomeEstimated.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(),
                                                                                EasyMock.eq(BROKER_ID_1), EasyMock.anyLong(), EasyMock.anyBoolean()))
            .andThrow(new TimeoutException("Unable to resolve capacity.")).anyTimes();
    EasyMock.expect(brokerCapacityConfigResolverSomeEstimated.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(),
                                                                                EasyMock.eq(BROKER_ID_0), EasyMock.anyLong(), EasyMock.anyBoolean()))
            .andReturn(new BrokerCapacityInfo(EMPTY_BROKER_CAPACITY, Collections.emptyMap(), MOCK_NUM_CPU_CORES)).anyTimes();
    EasyMock.replay(brokerCapacityConfigResolverSomeEstimated);

    processor = new CruiseControlMetricsProcessor(brokerCapacityConfigResolverSomeEstimated, false);
    for (CruiseControlMetric metric : metrics) {
      processor.addMetric(metric);
    }
    processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    assertEquals(MOCK_NUM_CPU_CORES, processor.cachedNumCoresByBroker().get(BROKER_ID_0), DELTA);
    assertNull(processor.cachedNumCoresByBroker().get(BROKER_ID_1));
    EasyMock.verify(brokerCapacityConfigResolverTimeout, brokerCapacityConfigResolverSomeEstimated, brokerCapacityConfigResolverAllEstimated);
  }

  @Test
  public void testBasic() throws TimeoutException, BrokerCapacityResolutionException {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(mockBrokerCapacityConfigResolver(), false);
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    Cluster cluster = getCluster();
    metrics.forEach(processor::addMetric);

    MetricSampler.Samples samples = processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    for (Node node : cluster.nodes()) {
      assertEquals(MOCK_NUM_CPU_CORES, processor.cachedNumCoresByBroker().get(node.id()), DELTA);
    }

    assertEquals(4, samples.partitionMetricSamples().size());
    assertEquals(2, samples.brokerMetricSamples().size());

    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      if (sample.entity().tp().equals(T1P0)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, CPU_UTIL.get(T1P0),
                                      B0_TOPIC1_BYTES_IN, B0_TOPIC1_BYTES_OUT, T1P0_BYTES_SIZE);
      } else if (sample.entity().tp().equals(T1P1)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, CPU_UTIL.get(T1P1),
                                      B1_TOPIC1_BYTES_IN, B1_TOPIC1_BYTES_OUT, T1P1_BYTES_SIZE);
      } else if (sample.entity().tp().equals(T2P0)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, CPU_UTIL.get(T2P0),
                                      B0_TOPIC2_BYTES_IN / 2, B0_TOPIC2_BYTES_OUT / 2, T2P0_BYTES_SIZE);
      } else if (sample.entity().tp().equals(T2P1)) {
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, CPU_UTIL.get(T2P1),
                                      B0_TOPIC2_BYTES_IN / 2, B0_TOPIC2_BYTES_OUT / 2, T2P1_BYTES_SIZE);
      } else {
        fail("Should never have partition " + sample.entity().tp());
      }
    }

    for (BrokerMetricSample sample : samples.brokerMetricSamples()) {
      if (sample.metricValue(CPU_USAGE) == B0_CPU) {
        assertEquals(B0_TOPIC1_REPLICATION_BYTES_IN, sample.metricValue(REPLICATION_BYTES_IN_RATE), DELTA);
      } else if (sample.metricValue(CPU_USAGE) == B1_CPU) {
        assertEquals(B1_TOPIC1_REPLICATION_BYTES_IN + B1_TOPIC2_REPLICATION_BYTES_IN,
                     sample.metricValue(REPLICATION_BYTES_IN_RATE), DELTA);
      } else {
        fail("Should never have broker cpu util " + sample.metricValue(CPU_USAGE));
      }
    }

    assertFalse(samples.partitionMetricSamples().isEmpty());
  }

  @Test
  public void testMissingBrokerCpuUtilization() throws TimeoutException, BrokerCapacityResolutionException {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(mockBrokerCapacityConfigResolver(), false);
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    for (CruiseControlMetric metric : metrics) {
      if (metric.rawMetricType() == RawMetricType.BROKER_CPU_UTIL && metric.brokerId() == BROKER_ID_0) {
        // Do nothing and skip the metric.
      } else {
        processor.addMetric(metric);
      }
    }
    Cluster cluster = getCluster();
    MetricSampler.Samples samples = processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    assertEquals("Should have ignored partitions on broker 0", 1, samples.partitionMetricSamples().size());
    assertEquals("Should have ignored broker 0", 1, samples.brokerMetricSamples().size());
  }

  @Test
  public void testMissingOtherBrokerMetrics() throws TimeoutException, BrokerCapacityResolutionException {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(mockBrokerCapacityConfigResolver(), false);
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    Cluster cluster = getCluster();
    for (CruiseControlMetric metric : metrics) {
      if (metric.rawMetricType() == RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX && metric.brokerId() == BROKER_ID_0) {
        // Do nothing and skip the metric.
      } else {
        processor.addMetric(metric);
      }
    }
    MetricSampler.Samples samples = processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    assertEquals("Should have all 4 partition metrics.", 4, samples.partitionMetricSamples().size());
    assertEquals("Should have ignored broker 0", 1, samples.brokerMetricSamples().size());
  }

  @Test
  public void testMissingPartitionSizeMetric() throws TimeoutException, BrokerCapacityResolutionException {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(mockBrokerCapacityConfigResolver(), false);
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    for (CruiseControlMetric metric : metrics) {
      boolean shouldAdd = true;
      if (metric.rawMetricType() == RawMetricType.PARTITION_SIZE) {
        PartitionMetric pm = (PartitionMetric) metric;
        if (pm.topic().equals(TOPIC1) && pm.partition() == P0) {
          shouldAdd = false;
        }
      }
      if (shouldAdd) {
        processor.addMetric(metric);
      }
    }
    Cluster cluster = getCluster();
    MetricSampler.Samples samples = processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    assertEquals("Should have ignored partition " + T1P0, 3, samples.partitionMetricSamples().size());
    assertEquals("Should have reported both brokers", 2, samples.brokerMetricSamples().size());
  }

  @Test
  public void testMissingTopicBytesInMetric() throws TimeoutException, BrokerCapacityResolutionException {
    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(mockBrokerCapacityConfigResolver(), false);
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    Set<RawMetricType> metricTypeToExclude = new HashSet<>(Arrays.asList(TOPIC_BYTES_IN,
                                                                         TOPIC_BYTES_OUT,
                                                                         TOPIC_REPLICATION_BYTES_IN,
                                                                         TOPIC_REPLICATION_BYTES_OUT));
    for (CruiseControlMetric metric : metrics) {
      if (metricTypeToExclude.contains(metric.rawMetricType())) {
        TopicMetric tm = (TopicMetric) metric;
        if (tm.brokerId() == BROKER_ID_0 && tm.topic().equals(TOPIC1)) {
          continue;
        }
      }
      processor.addMetric(metric);
    }

    Cluster cluster = getCluster();
    MetricSampler.Samples samples = processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
    assertEquals(4, samples.partitionMetricSamples().size());
    assertEquals(2, samples.brokerMetricSamples().size());

    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      if (sample.entity().tp().equals(T1P0)) {
        // T1P0 should not have any IO or CPU usage.
        validatePartitionMetricSample(sample, _time.milliseconds() + 2, 0.0, 0.0, 0.0, T1P0_BYTES_SIZE);
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingBrokerCapacity() throws TimeoutException, BrokerCapacityResolutionException {
    Set<CruiseControlMetric> metrics = getCruiseControlMetrics();
    // All estimated.
    BrokerCapacityConfigResolver brokerCapacityConfigResolver = EasyMock.mock(BrokerCapacityConfigResolver.class);
    EasyMock.expect(brokerCapacityConfigResolver.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyInt(),
                                                                   EasyMock.anyLong(), EasyMock.anyBoolean()))
            .andReturn(new BrokerCapacityInfo(Collections.emptyMap(), Collections.emptyMap(), MOCK_NUM_CPU_CORES)).anyTimes();
    EasyMock.replay(brokerCapacityConfigResolver);

    CruiseControlMetricsProcessor processor = new CruiseControlMetricsProcessor(brokerCapacityConfigResolver, false);
    for (CruiseControlMetric cruiseControlMetric : metrics) {
      processor.addMetric(cruiseControlMetric);
    }
    EasyMock.verify(brokerCapacityConfigResolver);
    Cluster cluster = getCluster();
    processor.process(cluster, TEST_PARTITIONS, MetricSampler.SamplingMode.ALL);
  }

  @Test
  public void testEstimatedCPUUtilProduceOnly() {
    // In a produce only case there will not be any client bytes out since there is no consumer running.
    // Also, ReplicationBytes at Topic level are not present yet.
    double allTopicBytesIn = 1000.0;
    // No consumer running
    double allTopicBytesOut = 0.0;
    double partitionBytesIn = 1000.0;
    // No consumer running
    double partitionBytesOut = 0.0;
    // Replication bytes metrics unavailable at topic-partition level
    double partitionReplicationBytesOut = 0.0;
    double allTopicReplicationBytesIn = 2000.0;
    double allTopicReplicationBytesOut = 2000.0;
    double estimatedCpuUtilPerCore = estimateLeaderCpuUtilPerCore(B0_CPU,
                                                                  allTopicBytesIn,
                                                                  allTopicBytesOut,
                                                                  allTopicReplicationBytesIn,
                                                                  partitionBytesIn,
                                                                  partitionBytesOut + partitionReplicationBytesOut);
    assertEquals(35.0, estimatedCpuUtilPerCore, 0.01);
  }

  /**
   * <ul>
   * <li>T1P0(B0): NW_IN = {@link #B0_TOPIC1_BYTES_IN} KB, NW_OUT = {@link #B0_TOPIC1_BYTES_OUT} KB,
   * size = {@link #T1P0_BYTES_SIZE} MB</li>
   * <li>T1P1(B1): NW_IN = {@link #B1_TOPIC1_BYTES_IN} KB, NW_OUT = {@link #B1_TOPIC1_BYTES_OUT} KB,
   * size = {@link #T1P1_BYTES_SIZE} MB</li>
   * <li>T2P0(B0): NW_IN = est. {@link #B0_TOPIC2_BYTES_IN}/2 KB, NW_OUT = est. {@link #B0_TOPIC2_BYTES_OUT}/2 KB,
   * size = {@link #T2P0_BYTES_SIZE} MB</li>
   * <li>T2P1(B0): NW_IN = est. {@link #B0_TOPIC2_BYTES_IN}/2 KB, NW_OUT = est. {@link #B0_TOPIC2_BYTES_OUT}/2 KB,
   * size = {@link #T2P1_BYTES_SIZE} MB</li>
   * <li>B0: CPU = {@link #B0_CPU}%</li>
   * <li>B1: CPU = {@link #B1_CPU}%</li>
   * </ul>
   * @return Cruise Control metrics.
   */
  private Set<CruiseControlMetric> getCruiseControlMetrics() {
    Set<CruiseControlMetric> metrics = new HashSet<>();

    int i = 0;
    for (RawMetricType rawMetricType : RawMetricType.brokerMetricTypesDiffForVersion(BrokerMetricSample.MIN_SUPPORTED_VERSION)) {
      switch (rawMetricType) {
        case ALL_TOPIC_BYTES_IN:
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, _time.milliseconds(), BROKER_ID_0,
                                       B0_ALL_TOPIC_BYTES_IN * BYTES_IN_KB));
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, _time.milliseconds(), BROKER_ID_1,
                                       B1_ALL_TOPIC_BYTES_IN * BYTES_IN_KB));
          break;
        case ALL_TOPIC_BYTES_OUT:
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_0,
                                       B0_ALL_TOPIC_BYTES_OUT * BYTES_IN_KB));
          metrics.add(new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_1,
                                       B1_ALL_TOPIC_BYTES_OUT * BYTES_IN_KB));
          break;
        case BROKER_CPU_UTIL:
          metrics.add(new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, _time.milliseconds(), BROKER_ID_0, B0_CPU));
          metrics.add(new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, _time.milliseconds(), BROKER_ID_1, B1_CPU));
          break;
        default:
          metrics.add(new BrokerMetric(rawMetricType, _time.milliseconds(), BROKER_ID_0, i++ * BYTES_IN_MB));
          metrics.add(new BrokerMetric(rawMetricType, _time.milliseconds(), BROKER_ID_1, i++ * BYTES_IN_MB));
          break;
      }
    }

    for (RawMetricType rawMetricType : RawMetricType.topicMetricTypes()) {
      switch (rawMetricType) {
        case TOPIC_BYTES_IN:
          metrics.add(new TopicMetric(TOPIC_BYTES_IN, _time.milliseconds() + 1, BROKER_ID_0, TOPIC1,
                                      B0_TOPIC1_BYTES_IN * BYTES_IN_KB));
          metrics.add(new TopicMetric(TOPIC_BYTES_IN, _time.milliseconds() + 2, BROKER_ID_1, TOPIC1,
                                      B1_TOPIC1_BYTES_IN * BYTES_IN_KB));
          metrics.add(new TopicMetric(TOPIC_BYTES_IN, _time.milliseconds(), BROKER_ID_0, TOPIC2,
                                      B0_TOPIC2_BYTES_IN * BYTES_IN_KB));
          break;
        case TOPIC_BYTES_OUT:
          metrics.add(new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC1,
                                      B0_TOPIC1_BYTES_OUT * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_1, TOPIC1,
                                      B1_TOPIC1_BYTES_OUT * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC2,
                                      B0_TOPIC2_BYTES_OUT * BYTES_IN_KB));
          break;
        case TOPIC_REPLICATION_BYTES_IN:
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, _time.milliseconds(), BROKER_ID_1, TOPIC1,
                                      B1_TOPIC1_REPLICATION_BYTES_IN * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, _time.milliseconds(), BROKER_ID_0, TOPIC1,
                                      B0_TOPIC1_REPLICATION_BYTES_IN * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_IN, _time.milliseconds(), BROKER_ID_1, TOPIC2,
                                      B1_TOPIC2_REPLICATION_BYTES_IN * BYTES_IN_KB));
          break;
        case TOPIC_REPLICATION_BYTES_OUT:
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC1,
                                      B0_TOPIC1_REPLICATION_BYTES_OUT * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, _time.milliseconds(), BROKER_ID_1, TOPIC1,
                                      B1_TOPIC1_REPLICATION_BYTES_OUT * BYTES_IN_KB));
          metrics.add(new TopicMetric(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, _time.milliseconds(), BROKER_ID_0, TOPIC2,
                                      B0_TOPIC2_REPLICATION_BYTES_OUT * BYTES_IN_KB));
          break;
        default:
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(), BROKER_ID_0, TOPIC1, i * BYTES_IN_MB));
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(), BROKER_ID_1, TOPIC1, i * BYTES_IN_MB));
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(), BROKER_ID_0, TOPIC2, i * BYTES_IN_MB));
          metrics.add(new TopicMetric(rawMetricType, _time.milliseconds(), BROKER_ID_1, TOPIC2, i * BYTES_IN_MB));
          break;
      }
    }

    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC1, P0,
                                    T1P0_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC1, P1,
                                    T1P1_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC2, P0,
                                    T2P0_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_0, TOPIC2, P1,
                                    T2P1_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC1, P0,
                                    T1P0_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC1, P1,
                                    T1P1_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC2, P0,
                                    T2P0_BYTES_SIZE * BYTES_IN_MB));
    metrics.add(new PartitionMetric(RawMetricType.PARTITION_SIZE, _time.milliseconds(), BROKER_ID_1, TOPIC2, P1,
                                    T2P1_BYTES_SIZE * BYTES_IN_MB));
    return metrics;
  }

  private void validatePartitionMetricSample(PartitionMetricSample sample, long time, double cpu, double bytesIn, double bytesOut,
                                             double disk) {
    assertEquals(time, sample.sampleTime());
    assertEquals(cpu, sample.metricValue(KafkaMetricDef.commonMetricDefId(CPU_USAGE)), DELTA);
    assertEquals(bytesIn, sample.metricValue(KafkaMetricDef.commonMetricDefId(LEADER_BYTES_IN)), DELTA);
    assertEquals(bytesOut, sample.metricValue(KafkaMetricDef.commonMetricDefId(LEADER_BYTES_OUT)), DELTA);
    assertEquals(disk, sample.metricValue(KafkaMetricDef.commonMetricDefId(DISK_USAGE)), DELTA);
  }

  private Cluster getCluster() {
    Node node0 = new Node(BROKER_ID_0, "localhost", 100, "rack0");
    Node node1 = new Node(BROKER_ID_1, "localhost", 100, "rack1");
    Node[] nodes = {node0, node1};
    Set<Node> allNodes = new HashSet<>();
    allNodes.add(node0);
    allNodes.add(node1);
    Set<PartitionInfo> parts = new HashSet<>();
    parts.add(new PartitionInfo(TOPIC1, P0, node0, nodes, nodes));
    parts.add(new PartitionInfo(TOPIC1, P1, node1, nodes, nodes));
    parts.add(new PartitionInfo(TOPIC2, P0, node0, nodes, nodes));
    parts.add(new PartitionInfo(TOPIC2, P1, node0, nodes, nodes));
    return new Cluster("testCluster", allNodes, parts, Collections.emptySet(), Collections.emptySet());
  }
}
