/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.Test;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.model.LinearRegressionModelParameters.ModelCoefficient.LEADER_BYTES_OUT;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.getCluster;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.getMetadata;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.NODE_0;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.nodeIds;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUnitTestUtils.NO_PARTITION_LEADER_EPOCH;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.DISK_USAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.LEADER_BYTES_IN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link KafkaPartitionMetricSampleAggregator}.
 */
public class KafkaPartitionMetricSampleAggregatorTest {
  private static final int PARTITION = 0;
  private static final int NUM_WINDOWS = 20;
  private static final long WINDOW_MS = TimeUnit.SECONDS.toMillis(1);
  private static final int MIN_SAMPLES_PER_WINDOW = 4;
  private static final TopicPartition TP = new TopicPartition(TOPIC0, PARTITION);
  private static final PartitionEntity PE = new PartitionEntity(TP);

  @Test
  public void testAggregate() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator);

    MetricSampleAggregationResult<String, PartitionEntity> result =
        metricSampleAggregator.aggregate(metadata.fetch(), Long.MAX_VALUE, new OperationProgress());
    Map<PartitionEntity, ValuesAndExtrapolations> valuesAndExtrapolations = result.valuesAndExtrapolations();

    assertEquals("The windows should only have one partition", 1, valuesAndExtrapolations.size());
    ValuesAndExtrapolations partitionValuesAndExtrapolations = valuesAndExtrapolations.get(PE);
    assertNotNull(partitionValuesAndExtrapolations);
    assertEquals(NUM_WINDOWS, partitionValuesAndExtrapolations.metricValues().length());
    for (int i = 0; i < NUM_WINDOWS; i++) {
      assertEquals((NUM_WINDOWS - i) * WINDOW_MS, result.valuesAndExtrapolations().get(PE).window(i));
      for (Resource resource : Resource.cachedValues()) {
        Collection<Short> metricIds = KafkaMetricDef.resourceToMetricIds(resource);
        double expectedValue = (resource == Resource.DISK ? (NUM_WINDOWS - 1 - i) * 10 + MIN_SAMPLES_PER_WINDOW - 1
                                                          : (NUM_WINDOWS - 1 - i) * 10 + (MIN_SAMPLES_PER_WINDOW - 1) / 2.0)
                               / (resource == Resource.CPU ? UNIT_INTERVAL_TO_PERCENTAGE : 1.0) * metricIds.size();
        assertEquals("The utilization for " + resource + " should be " + expectedValue,
                     expectedValue, partitionValuesAndExtrapolations.metricValues().valuesForGroup(resource.name(),
                                                                                                   KafkaMetricDef.commonMetricDef(),
                                                                                                   true).get(i), 0.01);
      }
    }

    // Verify the metric completeness checker state
    MetadataClient.ClusterAndGeneration clusterAndGeneration =
        new MetadataClient.ClusterAndGeneration(metadata.fetch(), 1);
    assertEquals(NUM_WINDOWS, metricSampleAggregator.validWindows(clusterAndGeneration.cluster(), 1.0).size());
    Map<Long, Float> monitoredPercentages = metricSampleAggregator.validPartitionRatioByWindows(clusterAndGeneration.cluster());
    for (double percentage : monitoredPercentages.values()) {
      assertEquals(1.0, percentage, 0.0);
    }
    assertEquals(NUM_WINDOWS, metricSampleAggregator.availableWindows().size());
  }

  @Test
  public void testAggregateWithUpdatedCluster() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator);

    TopicPartition tp1 = new TopicPartition(TOPIC0 + "1", 0);
    Cluster cluster = getCluster(Arrays.asList(TP, tp1));

    List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>(2);
    topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE,
                                                         TOPIC0,
                                                         false,
                                                         Collections.singletonList(new MetadataResponse.PartitionMetadata(
                                                             Errors.NONE, TP, Optional.of(NODE_0.id()),
                                                             Optional.of(NO_PARTITION_LEADER_EPOCH),
                                                             nodeIds(), nodeIds(),
                                                             Collections.emptyList()))));
    topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE,
                                                         TOPIC0 + "1",
                                                         false,
                                                         Collections.singletonList(new MetadataResponse.PartitionMetadata(
                                                             Errors.NONE, tp1, Optional.of(NODE_0.id()),
                                                             Optional.of(NO_PARTITION_LEADER_EPOCH),
                                                             nodeIds(), nodeIds(),
                                                             Collections.emptyList()))));

    MetadataResponse metadataResponse = KafkaCruiseControlUtils.prepareMetadataResponse(cluster.nodes(),
                                                                                        cluster.clusterResource().clusterId(),
                                                                                        MetadataResponse.NO_CONTROLLER_ID,
                                                                                        topicMetadata);
    metadata.update(KafkaCruiseControlUtils.REQUEST_VERSION_UPDATE, metadataResponse, false, 1);

    Map<PartitionEntity, ValuesAndExtrapolations> aggregateResult =
        metricSampleAggregator.aggregate(cluster, Long.MAX_VALUE, new OperationProgress()).valuesAndExtrapolations();
    // Partition "topic-0" should be valid in all NUM_WINDOW windows and Partition "topic1-0" should not since
    // there is no sample for it.
    assertEquals(1, aggregateResult.size());
    assertEquals(NUM_WINDOWS, aggregateResult.get(PE).windows().size());

    ModelCompletenessRequirements requirements =
        new ModelCompletenessRequirements(1, 0.0, true);
    MetricSampleAggregationResult<String, PartitionEntity> result =
        metricSampleAggregator.aggregate(cluster, -1, Long.MAX_VALUE, requirements, new OperationProgress());
    aggregateResult = result.valuesAndExtrapolations();
    assertNotNull("tp1 should be included because includeAllTopics is set to true",
                  aggregateResult.get(new PartitionEntity(tp1)));
    Map<Integer, Extrapolation> extrapolations = aggregateResult.get(new PartitionEntity(tp1)).extrapolations();
    assertEquals(NUM_WINDOWS, extrapolations.size());

    for (int i = 0; i < NUM_WINDOWS; i++) {
      assertEquals(Extrapolation.NO_VALID_EXTRAPOLATION, extrapolations.get(i));
    }
  }

  @Test
  public void testAggregateWithPartitionExtrapolations() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    TopicPartition tp1 = new TopicPartition(TOPIC0, 1);
    Cluster cluster = getCluster(Arrays.asList(TP, tp1));
    PartitionEntity pe1 = new PartitionEntity(tp1);

    List<MetadataResponse.PartitionMetadata> partitionMetadata =
        Collections.singletonList(new MetadataResponse.PartitionMetadata(Errors.NONE, tp1, Optional.of(NODE_0.id()),
                                                                         Optional.of(NO_PARTITION_LEADER_EPOCH),
                                                                         nodeIds(), nodeIds(), Collections.emptyList()));
    List<MetadataResponse.TopicMetadata> topicMetadata = Collections.singletonList(
        new MetadataResponse.TopicMetadata(Errors.NONE, TOPIC0, false, partitionMetadata));

    MetadataResponse metadataResponse = KafkaCruiseControlUtils.prepareMetadataResponse(cluster.nodes(),
                                                                                        cluster.clusterResource().clusterId(),
                                                                                        MetadataResponse.NO_CONTROLLER_ID,
                                                                                        topicMetadata);
    metadata.update(KafkaCruiseControlUtils.REQUEST_VERSION_UPDATE, metadataResponse, false, 1);
    populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator);
    //Populate partition 1 but leave 1 hole at NUM_WINDOWS'th window.
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_WINDOWS - 2, MIN_SAMPLES_PER_WINDOW,
                                                        metricSampleAggregator,
                                                        pe1,
                                                        0, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_WINDOW,
                                                        metricSampleAggregator,
                                                        pe1,
                                                        NUM_WINDOWS - 1, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
    MetricSampleAggregationResult<String, PartitionEntity> result =
        metricSampleAggregator.aggregate(cluster, Long.MAX_VALUE, new OperationProgress());
    assertEquals(2, result.valuesAndExtrapolations().size());
    assertTrue(result.valuesAndExtrapolations().get(PE).extrapolations().isEmpty());
    assertEquals(1, result.valuesAndExtrapolations().get(pe1).extrapolations().size());
    assertTrue(result.valuesAndExtrapolations().get(pe1).extrapolations().containsKey(1));
    assertEquals((NUM_WINDOWS - 1) * WINDOW_MS, result.valuesAndExtrapolations().get(pe1).window(1));
    assertEquals(Extrapolation.AVG_ADJACENT, result.valuesAndExtrapolations().get(pe1).extrapolations().get(1));
  }

  @Test
  public void testFallbackToAvgAvailable() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    // Only give two sample to the aggregator.
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_WINDOWS - 1, MIN_SAMPLES_PER_WINDOW,
                                                        metricSampleAggregator, PE, 2, WINDOW_MS, KafkaMetricDef.commonMetricDef());
    MetricSampleAggregationResult<String, PartitionEntity> result =
        metricSampleAggregator.aggregate(metadata.fetch(), NUM_WINDOWS * WINDOW_MS, new OperationProgress());
    // Partition "topic-0" is expected to be a valid partition in result with valid sample values for window [3, NUM_WINDOWS].
    assertEquals(NUM_WINDOWS - 2, result.valuesAndExtrapolations().get(PE).windows().size());

    populateSampleAggregator(2, MIN_SAMPLES_PER_WINDOW - 2, metricSampleAggregator);

    result = metricSampleAggregator.aggregate(metadata.fetch(), NUM_WINDOWS * WINDOW_MS, new OperationProgress());
    int numWindows = result.valuesAndExtrapolations().get(PE).metricValues().length();
    assertEquals(NUM_WINDOWS, numWindows);
    int numExtrapolations = 0;
    for (Map.Entry<Integer, Extrapolation> entry : result.valuesAndExtrapolations().get(PE).extrapolations().entrySet()) {
      assertEquals(Extrapolation.AVG_AVAILABLE, entry.getValue());
      numExtrapolations++;
    }
    assertEquals(2, numExtrapolations);
  }

  @Test
  public void testFallbackToAvgAdjacent() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    TopicPartition anotherTopicPartition = new TopicPartition("AnotherTopic", 1);
    PartitionEntity anotherPartitionEntity = new PartitionEntity(anotherTopicPartition);
    Metadata metadata = getMetadata(Arrays.asList(TP, anotherTopicPartition));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    // Only give one sample to the aggregator for previous period.
    populateSampleAggregator(NUM_WINDOWS, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator);
    // Create let (NUM_WINDOWS + 1) have enough samples.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator,
                                                        PE, NUM_WINDOWS, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
    // Let a window exist but not containing samples for partition 0
    CruiseControlUnitTestUtils.populateSampleAggregator(1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator,
                                                        anotherPartitionEntity, NUM_WINDOWS + 1, WINDOW_MS, KafkaMetricDef
                                                            .commonMetricDef());
    // Let the rest of the window has enough samples.
    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_WINDOW,
                                                        metricSampleAggregator, PE,
                                                        NUM_WINDOWS + 2, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());

    MetricSampleAggregationResult<String, PartitionEntity> result =
        metricSampleAggregator.aggregate(metadata.fetch(), NUM_WINDOWS * WINDOW_MS * 2, new OperationProgress());
    int numWindows = result.valuesAndExtrapolations().get(PE).metricValues().length();
    assertEquals(NUM_WINDOWS, numWindows);
    int numExtrapolations = 0;
    for (Map.Entry<Integer, Extrapolation> entry : result.valuesAndExtrapolations().get(PE).extrapolations().entrySet()) {
      assertEquals(Extrapolation.AVG_ADJACENT, entry.getValue());
      numExtrapolations++;
    }
    assertEquals(1, numExtrapolations);
  }

  @Test
  public void testTooManyFlaws() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    // Only give two samples to the aggregator.
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_WINDOWS - 2, MIN_SAMPLES_PER_WINDOW,
                                                        metricSampleAggregator, PE, 3, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());

    MetricSampleAggregationResult<String, PartitionEntity> result =
        metricSampleAggregator.aggregate(metadata.fetch(), NUM_WINDOWS * WINDOW_MS, new OperationProgress());
    // Partition "topic-0" is expected to be a valid partition in result, with valid sample values collected for window [1, NUM_WINDOW - 3].
    assertEquals(NUM_WINDOWS - 3, result.valuesAndExtrapolations().get(PE).windows().size());
  }

  @Test
  public void testNotEnoughWindows() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator);

    try {
      // Only 4 windows have smaller timestamp than the timestamp we passed in.
      ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(NUM_WINDOWS, 0.0, false);
      metricSampleAggregator.aggregate(metadata.fetch(),
                                       -1L,
                                       (NUM_WINDOWS - 1) * WINDOW_MS - 1,
                                       requirements,
                                       new OperationProgress());
      fail("Should throw NotEnoughValidWindowsException");
    } catch (NotEnoughValidWindowsException nse) {
      // let it go
    }
  }

  @Test
  public void testExcludeInvalidMetricSample() throws NotEnoughValidWindowsException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(Collections.singleton(TP));
    KafkaPartitionMetricSampleAggregator
        metricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();

    populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, metricSampleAggregator);
    // Set the leader to be node 1, which is different from the leader in the metadata.
    PartitionMetricSample sampleWithDifferentLeader = new PartitionMetricSample(1, TP);
    sampleWithDifferentLeader.record(metricDef.metricInfo(DISK_USAGE.name()), 10000);
    sampleWithDifferentLeader.record(metricDef.metricInfo(CPU_USAGE.name()), 10000);
    sampleWithDifferentLeader.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), 10000);
    sampleWithDifferentLeader.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), 10000);
    sampleWithDifferentLeader.close(0);

    // Only populate the CPU metric
    PartitionMetricSample incompletePartitionMetricSample = new PartitionMetricSample(0, TP);
    incompletePartitionMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), 10000);
    incompletePartitionMetricSample.close(0);

    metricSampleAggregator.addSample(sampleWithDifferentLeader);
    metricSampleAggregator.addSample(incompletePartitionMetricSample);

    // Check the window value and make sure the metric samples above are excluded.
    Map<PartitionEntity, ValuesAndExtrapolations> valuesAndExtrapolations =
        metricSampleAggregator.aggregate(metadata.fetch(), NUM_WINDOWS * WINDOW_MS, new OperationProgress())
                              .valuesAndExtrapolations();
    ValuesAndExtrapolations partitionValuesAndExtrapolations = valuesAndExtrapolations.get(PE);
    for (Resource resource : Resource.cachedValues()) {
      Collection<Short> metricIds = KafkaMetricDef.resourceToMetricIds(resource);
      double expectedValue = (resource == Resource.DISK ? MIN_SAMPLES_PER_WINDOW - 1 : (MIN_SAMPLES_PER_WINDOW - 1) / 2.0)
                             / (resource == Resource.CPU ? UNIT_INTERVAL_TO_PERCENTAGE : 1.0) * metricIds.size();
      assertEquals("The utilization for " + resource + " should be " + expectedValue,
                   expectedValue, partitionValuesAndExtrapolations.metricValues().valuesForGroup(resource.name(),
                                                                                                 KafkaMetricDef.commonMetricDef(),
                                                                                                 true).get(NUM_WINDOWS - 1), 0.01);
    }
  }

  @Test
  public void testValidWindows() {
    TestContext ctx = setupScenario1();
    KafkaPartitionMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    SortedSet<Long> validWindows = aggregator.validWindows(clusterAndGeneration.cluster(), 1.0);
    assertEquals(NUM_WINDOWS, validWindows.size());
    assertValidWindows(validWindows, NUM_WINDOWS, Collections.emptySet());
  }

  @Test
  public void testValidWindowsWithInvalidPartitions() {
    TestContext ctx = setupScenario2();
    KafkaPartitionMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);

    SortedSet<Long> validWindows = aggregator.validWindows(clusterAndGeneration.cluster(), 1.0);
    assertEquals("Should have three invalid windows.", NUM_WINDOWS - 3, validWindows.size());
    assertValidWindows(validWindows, NUM_WINDOWS - 1, Arrays.asList(6, 7));
    // reduced monitored percentage should include every window.
    assertEquals(NUM_WINDOWS, aggregator.validWindows(clusterAndGeneration.cluster(), 0.5).size());
  }

  @Test
  public void testValidWindowWithDifferentInvalidPartitions() {
    TestContext ctx = setupScenario3();
    KafkaPartitionMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    SortedSet<Long> validWindows = aggregator.validWindows(clusterAndGeneration.cluster(), 0.75);
    assertEquals("Should have two invalid windows.", NUM_WINDOWS - 2, validWindows.size());
    assertValidWindows(validWindows, NUM_WINDOWS, Arrays.asList(6, 7));
  }

  @Test
  public void testValidWindowsWithTooManyExtrapolations() {
    TestContext ctx = setupScenario4();
    KafkaPartitionMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);

    SortedSet<Long> validWindows = aggregator.validWindows(clusterAndGeneration.cluster(), 0.75);
    assertEquals("Should have two invalid windows.", NUM_WINDOWS - 2, validWindows.size());
    assertValidWindows(validWindows, NUM_WINDOWS, Arrays.asList(6, 7));
  }

  @Test
  public void testMonitoredPercentage() {
    TestContext ctx = setupScenario1();
    KafkaPartitionMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals(1.0, aggregator.monitoredPercentage(clusterAndGeneration.cluster()), 0.01);

    ctx = setupScenario2();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals(0.75, aggregator.monitoredPercentage(clusterAndGeneration.cluster()), 0.01);

    ctx = setupScenario3();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals((double) 4 / 6, aggregator.monitoredPercentage(clusterAndGeneration.cluster()), 0.01);

    ctx = setupScenario4();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    assertEquals((double) 4 / 6, aggregator.monitoredPercentage(clusterAndGeneration.cluster()), 0.01);
  }

  @Test
  public void testMonitoredPercentagesByWindows() {
    TestContext ctx = setupScenario1();
    KafkaPartitionMetricSampleAggregator aggregator = ctx.aggregator();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = ctx.clusterAndGeneration(0);

    Map<Long, Float> percentages = aggregator.validPartitionRatioByWindows(clusterAndGeneration.cluster());
    assertEquals(NUM_WINDOWS, percentages.size());
    for (Map.Entry<Long, Float> entry : percentages.entrySet()) {
      assertEquals(1.0, entry.getValue(), 0.01);
    }

    ctx = setupScenario2();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    percentages = aggregator.validPartitionRatioByWindows(clusterAndGeneration.cluster());
    assertEquals(NUM_WINDOWS, percentages.size());
    for (Map.Entry<Long, Float> entry : percentages.entrySet()) {
      long window = entry.getKey();
      if (window == 6000 || window == 7000 || window == 20000) {
        assertEquals(0.5, entry.getValue(), 0.01);
      } else {
        assertEquals(1.0, entry.getValue(), 0.01);
      }
    }

    ctx = setupScenario3();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    percentages = aggregator.validPartitionRatioByWindows(clusterAndGeneration.cluster());
    assertEquals(NUM_WINDOWS, percentages.size());
    for (Map.Entry<Long, Float> entry : percentages.entrySet()) {
      long window = entry.getKey();
      if (window == 6000 || window == 7000 || window == 18000 || window == 19000) {
        assertEquals((double) 4 / 6, entry.getValue(), 0.01);
      } else {
        assertEquals(1.0, entry.getValue(), 0.01);
      }
    }

    ctx = setupScenario4();
    aggregator = ctx.aggregator();
    clusterAndGeneration = ctx.clusterAndGeneration(0);
    percentages = aggregator.validPartitionRatioByWindows(clusterAndGeneration.cluster());
    assertEquals(NUM_WINDOWS, percentages.size());
    for (Map.Entry<Long, Float> entry : percentages.entrySet()) {
      long window = entry.getKey();
      if (window == 6000 || window == 7000) {
        assertEquals((double) 2 / 6, entry.getValue(), 0.01);
      } else {
        assertEquals((double) 4 / 6, entry.getValue(), 0.01);
      }
    }
  }

  /**
   * Two topics with 2 partitions each. No data missing.
   * @return Setup scenario #1
   */
  private TestContext setupScenario1() {
    TopicPartition t0p1 = new TopicPartition(TOPIC0, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaPartitionMetricSampleAggregator aggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : allPartitions) {
      populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, aggregator, tp);
    }
    return new TestContext(metadata, aggregator);
  }

  /**
   * Two topics with 2 partitions each.
   * T1P1 misses window 6000 (index=5), 7000 (index=6) and 20000 (index=19)
   * Other partitions has full data.
   * @return Setup scenario #2
   */
  private TestContext setupScenario2() {
    TopicPartition t0p1 = new TopicPartition(TOPIC0, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaPartitionMetricSampleAggregator aggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : Arrays.asList(TP, t0p1, t1p0)) {
      populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, aggregator, tp);
    }
    // Let t1p1 miss two consecutive windows and the most recent window.
    populateSampleAggregator(5, MIN_SAMPLES_PER_WINDOW, aggregator, t1p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_WINDOWS - 8, MIN_SAMPLES_PER_WINDOW,
                                                        aggregator,
                                                        new PartitionEntity(t1p1),
                                                        7, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
    return new TestContext(metadata, aggregator);
  }

  /**
   * Three topics with 2 partitions each.
   * T0P1 missing window 18000 (index=17), 19000 (index=18)
   * T1P1 missing window 6000 (index=5), 7000 (index=6)
   * Other partitions have all data.
   * @return Setup scenario #3
   */
  private TestContext setupScenario3() {
    TopicPartition t0p1 = new TopicPartition(TOPIC0, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    TopicPartition t2p0 = new TopicPartition("TOPIC2", 0);
    TopicPartition t2p1 = new TopicPartition("TOPIC2", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1, t2p0, t2p1);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    Metadata metadata = getMetadata(allPartitions);
    KafkaPartitionMetricSampleAggregator aggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : Arrays.asList(TP, t1p0, t2p0, t2p1)) {
      populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, aggregator, tp);
    }
    // Let t0p1 miss the second and the third latest window.
    populateSampleAggregator(NUM_WINDOWS - 3, MIN_SAMPLES_PER_WINDOW, aggregator, t0p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, MIN_SAMPLES_PER_WINDOW, aggregator,
                                                        new PartitionEntity(t0p1),
                                                        NUM_WINDOWS - 1, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
    // let t1p1 miss another earlier window
    populateSampleAggregator(5, MIN_SAMPLES_PER_WINDOW, aggregator, t1p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_WINDOWS - 6, MIN_SAMPLES_PER_WINDOW,
                                                        aggregator, new PartitionEntity(t1p1),
                                                        7, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
    return new TestContext(metadata, aggregator);
  }

  /**
   * 3 Topics with 2 partitions each.
   * T0P1 has all the windows with AVG_AVAILABLE as extrapolations.
   * T1P1 misses window 6000 (index=5), 7000 (index=6)
   * All other partitions have full data.
   * @return Setup scenario #4
   */
  private TestContext setupScenario4() {
    TopicPartition t0p1 = new TopicPartition(TOPIC0, 1);
    TopicPartition t1p0 = new TopicPartition("TOPIC1", 0);
    TopicPartition t1p1 = new TopicPartition("TOPIC1", 1);
    TopicPartition t2p0 = new TopicPartition("TOPIC2", 0);
    TopicPartition t2p1 = new TopicPartition("TOPIC2", 1);
    List<TopicPartition> allPartitions = Arrays.asList(TP, t0p1, t1p0, t1p1, t2p0, t2p1);
    Properties props = getLoadMonitorProperties();
    props.setProperty(MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG, "0");
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    Metadata metadata = getMetadata(allPartitions);
    KafkaPartitionMetricSampleAggregator aggregator = new KafkaPartitionMetricSampleAggregator(config, metadata);

    for (TopicPartition tp : Arrays.asList(TP, t1p0, t2p0, t2p1)) {
      populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW, aggregator, tp);
    }
    // Let t0p1 have too many extrapolations.
    populateSampleAggregator(NUM_WINDOWS + 1, MIN_SAMPLES_PER_WINDOW - 1, aggregator, t0p1);
    // let t1p1 miss another earlier window
    populateSampleAggregator(5, MIN_SAMPLES_PER_WINDOW, aggregator, t1p1);
    CruiseControlUnitTestUtils.populateSampleAggregator(NUM_WINDOWS - 6, MIN_SAMPLES_PER_WINDOW,
                                                        aggregator, new PartitionEntity(t1p1), 7, WINDOW_MS, KafkaMetricDef
                                                            .commonMetricDef());
    return new TestContext(metadata, aggregator);
  }

  private void assertValidWindows(SortedSet<Long> actualValidWindows, int firstValidWindowIndex, Collection<Integer> invalidWindowIndices) {
    int windowIndex = firstValidWindowIndex;
    for (long window : actualValidWindows) {
      while (invalidWindowIndices.contains(windowIndex)) {
        windowIndex--;
      }
      assertEquals(windowIndex * WINDOW_MS, window);
      windowIndex--;
    }
  }

  private MetadataClient.ClusterAndGeneration clusterAndGeneration(Cluster cluster) {
    return new MetadataClient.ClusterAndGeneration(cluster, 0);
  }

  private void populateSampleAggregator(int numWindows,
                                        int numSamplesPerWindow,
                                        KafkaPartitionMetricSampleAggregator metricSampleAggregator) {
    populateSampleAggregator(numWindows, numSamplesPerWindow, metricSampleAggregator, TP);
  }

  private void populateSampleAggregator(int numWindows,
                                        int numSamplesPerWindow,
                                        KafkaPartitionMetricSampleAggregator metricSampleAggregator,
                                        TopicPartition tp) {
    CruiseControlUnitTestUtils.populateSampleAggregator(numWindows, numSamplesPerWindow, metricSampleAggregator,
                                                        new PartitionEntity(tp), 0, WINDOW_MS,
                                                        KafkaMetricDef.commonMetricDef());
  }

  private Properties getLoadMonitorProperties() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(PARTITION_METRICS_WINDOW_MS_CONFIG, Long.toString(WINDOW_MS));
    props.setProperty(NUM_PARTITION_METRICS_WINDOWS_CONFIG, Integer.toString(NUM_WINDOWS));
    props.setProperty(MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
                      Integer.toString(MIN_SAMPLES_PER_WINDOW));
    return props;
  }

  private static class TestContext {
    private final Metadata _metadata;
    private final KafkaPartitionMetricSampleAggregator _aggregator;

    TestContext(Metadata metadata, KafkaPartitionMetricSampleAggregator aggregator) {
      _metadata = metadata;
      _aggregator = aggregator;
    }

    private MetadataClient.ClusterAndGeneration clusterAndGeneration(int generation) {
      return new MetadataClient.ClusterAndGeneration(_metadata.fetch(), generation);
    }

    private KafkaPartitionMetricSampleAggregator aggregator() {
      return _aggregator;
    }
  }
}
