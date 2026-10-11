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

    assertEquals(4, diskBalanceStats.numAliveDisks());
    assertEquals(2, diskBalanceStats.numUnbalancedDisks());
    assertEquals(stats.numUnbalancedDisks(), diskBalanceStats.numUnbalancedDisks());
    assertEquals(1, diskBalanceStats.numDisksAboveCapacityLimit());
    assertEquals(85.0, diskBalanceStats.maxDiskUtilizationPct(), DELTA);
    assertEquals(0, diskBalanceStats.maxDiskUtilizationBrokerId());
    assertEquals(LOGDIR0, diskBalanceStats.maxDiskUtilizationLogdir());
    assertEquals(65.0, diskBalanceStats.maxDiskUtilizationSpreadPct(), DELTA);
    assertEquals(0, diskBalanceStats.maxDiskUtilizationSpreadBrokerId().intValue());
    // The disks of broker 0 are 32.5pp away from their broker average (52.5%), and the disks of broker 1 are at it.
    assertEquals(Math.sqrt(2 * Math.pow(32.5, 2) / 4), diskBalanceStats.diskUtilizationStDevPct(), DELTA);
    assertEquals(stats.diskUtilizationStandardDeviation() * 100, diskBalanceStats.diskUtilizationStDevPct(), DELTA);

    String statsString = stats.toString();
    assertTrue(statsString, statsString.contains("AVG:{"));
    assertTrue(statsString, statsString.contains(String.format("%nDISKS:{alive: 4 unbalanced: 2 aboveCapacityLimit: 1 "
                                                               + "maxUtilization: 85.00%% (broker 0 %s) maxSpreadWithinBroker: "
                                                               + "65.00pp (broker 0) utilizationStDevFromBrokerAvg: 22.98pp}", LOGDIR0)));

    Map<String, Object> json = stats.getJsonStructure();
    assertTrue(json.containsKey("metadata"));
    assertTrue(json.containsKey("statistics"));
    assertEquals(diskBalanceStats.getJsonStructure(), json.get("diskStatistics"));
    assertEquals(85.0, (Double) diskBalanceStats.getJsonStructure().get("maxDiskUtilizationPct"), DELTA);
    assertEquals(LOGDIR0, diskBalanceStats.getJsonStructure().get("maxDiskUtilizationLogdir"));
  }

  @Test
  public void testDiskBalanceStatsLeaveOutDisksMarkedForRemoval() {
    ClusterModel clusterModel = clusterModel();
    clusterModel.markDiskForRemoval(1, LOGDIR1);
    DiskBalanceStats diskBalanceStats = clusterModel.getClusterStats(BALANCING_CONSTRAINT).diskBalanceStats();

    // The disk marked for removal has no capacity, hence no utilization percentage: it is neither counted as unbalanced nor
    // above the capacity limit, and broker 1 has a single disk left to compute the spread for.
    assertEquals(3, diskBalanceStats.numAliveDisks());
    assertEquals(2, diskBalanceStats.numUnbalancedDisks());
    assertEquals(1, diskBalanceStats.numDisksAboveCapacityLimit());
    assertEquals(85.0, diskBalanceStats.maxDiskUtilizationPct(), DELTA);
    assertEquals(65.0, diskBalanceStats.maxDiskUtilizationSpreadPct(), DELTA);
    assertEquals(0, diskBalanceStats.maxDiskUtilizationSpreadBrokerId().intValue());
    assertEquals(Math.sqrt(2 * Math.pow(32.5, 2) / 3), diskBalanceStats.diskUtilizationStDevPct(), DELTA);
  }

  @Test
  public void testNoDiskBalanceStatsWithoutDiskInfo() {
    ClusterModelStats stats = DeterministicCluster.unbalanced().getClusterStats(BALANCING_CONSTRAINT);

    assertNull(stats.diskBalanceStats());
    assertFalse(stats.toString().contains("DISKS"));
    assertFalse(stats.getJsonStructure().containsKey("diskStatistics"));
  }
}
