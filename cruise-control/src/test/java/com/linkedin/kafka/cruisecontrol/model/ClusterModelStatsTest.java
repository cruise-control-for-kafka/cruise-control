/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ClusterModelStatsTest {
  private static final double DELTA = 1e-6;
  // Each disk has half of the broker disk capacity.
  private static final double DISK_CAPACITY = TestConstants.LARGE_BROKER_CAPACITY / 2;
  private static final BalancingConstraint BALANCING_CONSTRAINT =
      new BalancingConstraint(new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

  /**
   * Two brokers with two disks each, with the utilization of the disks:
   * <ul>
   *   <li>Broker 0: 85% on {@link TestConstants#LOGDIR0} and 20% on {@link TestConstants#LOGDIR1} -- i.e. both disks are
   *   unbalanced, and the former is above the disk capacity threshold (80%).</li>
   *   <li>Broker 1: 50% on each disk.</li>
   * </ul>
   *
   * @return The cluster model.
   */
  private static ClusterModel clusterModel() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo capacity = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY);
    for (int brokerId = 0; brokerId < 2; brokerId++) {
      clusterModel.createRack(Integer.toString(brokerId));
      clusterModel.createBroker(Integer.toString(brokerId), Integer.toString(brokerId), brokerId, capacity, true);
    }
    createReplica(clusterModel, 0, 0, LOGDIR0, 0.85);
    createReplica(clusterModel, 1, 0, LOGDIR1, 0.2);
    createReplica(clusterModel, 2, 1, LOGDIR0, 0.5);
    createReplica(clusterModel, 3, 1, LOGDIR1, 0.5);
    return clusterModel;
  }

  private static void createReplica(ClusterModel clusterModel, int partition, int brokerId, String logdir, double diskUtilization) {
    TopicPartition tp = new TopicPartition("topic", partition);
    String rackId = Integer.toString(brokerId);
    clusterModel.createReplica(rackId, brokerId, tp, 0, true, false, logdir, false);
    clusterModel.setReplicaLoad(rackId, brokerId, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, diskUtilization * DISK_CAPACITY),
                                Collections.singletonList(1L));
  }

  @Test
  public void testDiskBalanceStats() {
    ClusterModelStats stats = clusterModel().getClusterStats(BALANCING_CONSTRAINT);
    DiskBalanceStats diskBalanceStats = stats.diskBalanceStats();
    DiskBalanceStats.DiskUsageStats usage = diskBalanceStats.usage();

    assertEquals(4, diskBalanceStats.numAliveDisks());
    assertEquals(2, usage.numUnbalancedDisks());
    assertEquals(1, usage.numDisksAboveCapacityLimit());
    assertEquals((85.0 + 20.0 + 50.0 + 50.0) / 4, usage.avg().utilizationPct(), DELTA);
    assertDiskUtilization(usage.max(), 85.0, 0, LOGDIR0);
    assertDiskUtilization(usage.min(), 20.0, 0, LOGDIR1);
    // The disks of broker 0 are 32.5pp away from their broker average (52.5%), and the disks of broker 1 are at it.
    assertEquals(Math.sqrt(2 * Math.pow(32.5, 2) / 4), usage.stDev().utilizationPct(), DELTA);
    assertEquals(stats.diskUtilizationStandardDeviation() * 100, usage.stDev().utilizationPct(), DELTA);
    assertDiskUtilization(usage.maxSpreadWithinBroker(), 65.0, 0, null);

    String statsString = stats.toString();
    assertTrue(statsString, statsString.contains("AVG:{cpu"));
    assertTrue(statsString, statsString.contains(String.format(
        "%nDISKS:{alive: 4}%n"
        + "DISK USAGE:{unbalanced: 2 aboveCapacityLimit: 1 maxSpreadWithinBroker: 65.00pp (broker 0)}%n"
        + "DISK USAGE AVG:{utilization:  51.25%%}%n"
        + "DISK USAGE MAX:{utilization:  85.00%% (broker 0 %s)}%n"
        + "DISK USAGE MIN:{utilization:  20.00%% (broker 0 %s)}%n"
        + "DISK USAGE STD:{utilization:  22.98pp}", LOGDIR0, LOGDIR1)));

    Map<String, Object> json = stats.getJsonStructure();
    assertTrue(json.containsKey("metadata"));
    assertTrue(json.containsKey("statistics"));
    assertEquals(diskBalanceStats.getJsonStructure(), json.get("diskStatistics"));
    assertEquals(4, diskBalanceStats.getJsonStructure().get("numAliveDisks"));
    Map<String, Object> usageJson = cast(diskBalanceStats.getJsonStructure().get("usage"));
    Map<String, Object> max = cast(usageJson.get("MAX"));
    assertEquals(Set.of("utilizationPct", "broker", "logdir"), max.keySet());
    assertEquals(85.0, (Double) max.get("utilizationPct"), DELTA);
    assertEquals(0, max.get("broker"));
    assertEquals(LOGDIR0, max.get("logdir"));
    Map<String, Object> maxSpreadWithinBroker = cast(usageJson.get("maxSpreadWithinBroker"));
    assertEquals(Set.of("utilizationPct", "broker"), maxSpreadWithinBroker.keySet());
    assertEquals(65.0, (Double) maxSpreadWithinBroker.get("utilizationPct"), DELTA);
    assertEquals(Set.of("utilizationPct"), ((Map<?, ?>) usageJson.get("AVG")).keySet());
    assertEquals(Set.of("utilizationPct"), ((Map<?, ?>) usageJson.get("STD")).keySet());
    assertEquals(2, usageJson.get("numUnbalancedDisks"));
    assertEquals(1, usageJson.get("numDisksAboveCapacityLimit"));
  }

  @Test
  public void testUnbalancedDisksUseTheBalanceBandOfTheGoal() {
    // With disk.balance.threshold=1.10, IntraBrokerDiskUsageDistributionGoal keeps a disk within 9% of its broker utilization
    // (0.9 margin): disks at 54.75% and 45.25% of a broker at 50% are unbalanced for the goal, though within 10%.
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    clusterModel.createRack("0");
    clusterModel.createBroker("0", "0", 0, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY), true);
    createReplica(clusterModel, 0, 0, LOGDIR0, 0.5475);
    createReplica(clusterModel, 1, 0, LOGDIR1, 0.4525);
    ClusterModelStats stats = clusterModel.getClusterStats(BALANCING_CONSTRAINT);

    assertEquals(2, stats.diskBalanceStats().usage().numUnbalancedDisks());
    // The count that the stats comparator of the goal uses is unchanged.
    assertEquals(0, stats.numUnbalancedDisks());
  }

  @Test
  public void testDiskBalanceStatsLeaveOutDisksMarkedForRemoval() {
    ClusterModel clusterModel = clusterModel();
    clusterModel.markDiskForRemoval(1, LOGDIR1);
    DiskBalanceStats diskBalanceStats = clusterModel.getClusterStats(BALANCING_CONSTRAINT).diskBalanceStats();
    DiskBalanceStats.DiskUsageStats usage = diskBalanceStats.usage();

    // The disk marked for removal has no capacity, hence no utilization percentage: it is neither counted as unbalanced nor
    // above the capacity limit, and broker 1 has a single disk left to compute the spread for.
    assertEquals(3, diskBalanceStats.numAliveDisks());
    assertEquals(2, usage.numUnbalancedDisks());
    assertEquals(1, usage.numDisksAboveCapacityLimit());
    assertEquals((85.0 + 20.0 + 50.0) / 3, usage.avg().utilizationPct(), DELTA);
    assertDiskUtilization(usage.max(), 85.0, 0, LOGDIR0);
    assertDiskUtilization(usage.min(), 20.0, 0, LOGDIR1);
    assertDiskUtilization(usage.maxSpreadWithinBroker(), 65.0, 0, null);
    assertEquals(Math.sqrt(2 * Math.pow(32.5, 2) / 3), usage.stDev().utilizationPct(), DELTA);
  }

  @Test
  public void testNoDiskBalanceStatsWithoutDiskInfo() {
    ClusterModelStats stats = DeterministicCluster.unbalanced().getClusterStats(BALANCING_CONSTRAINT);

    assertNull(stats.diskBalanceStats());
    assertFalse(stats.toString().contains("DISKS:{"));
    assertFalse(stats.toString().contains("DISK USAGE"));
    assertFalse(stats.getJsonStructure().containsKey("diskStatistics"));
  }

  private static void assertDiskUtilization(DiskBalanceStats.DiskUtilizationValue value, double utilizationPct, Integer brokerId,
                                            String logdir) {
    assertEquals(utilizationPct, value.utilizationPct(), DELTA);
    assertEquals(brokerId, value.brokerId());
    assertEquals(logdir, value.logdir());
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object object) {
    return (T) object;
  }
}
