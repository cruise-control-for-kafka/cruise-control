/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.*;


public class BrokerStatsTest {
  private static final double DELTA = 1e-6;
  private static final String DEAD_LOGDIR = "/mnt/dead";
  private static final String TOPIC = "topic";
  private static final List<String> NW_RATES = List.of("LeaderNwInRate", "FollowerNwInRate", "NwOutRate", "PnwOutRate");

  @Test
  public void testBrokerStatsCoreNumPrinting() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    BrokerStats brokerStats = new BrokerStats(kafkaCruiseControlConfig);

    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    clusterModel.createRack("0");

    // Test broker with int cores (numCpuCores does not have fractional digits)
    BrokerCapacityInfo brokerCapacityInfoWithIntCores = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, 32.0);
    Broker intCoreBroker = clusterModel.createBroker("0", "0", 0, brokerCapacityInfoWithIntCores, false);
    brokerStats.addSingleBrokerStats(intCoreBroker, 0.0, true);
    assertTrue(brokerStats.toString().contains(",                 32,"));

    // Test broker with double cores (numCpuCores has fractional digits)
    BrokerCapacityInfo brokerCapacityInfoWithFractionalCores = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, 32.25111);
    Broker fractionalCoreBroker = clusterModel.createBroker("0", "0", 0, brokerCapacityInfoWithFractionalCores, false);
    brokerStats.addSingleBrokerStats(fractionalCoreBroker, 0.0, true);
    assertTrue(brokerStats.toString().contains(",              32.25,"));
  }

  /**
   * Three brokers with two disks each, and four partitions with two replicas each:
   * <ul>
   *   <li>p0: leader on broker 0 / {@link TestConstants#LOGDIR0}, follower on broker 1 / {@link TestConstants#LOGDIR0}</li>
   *   <li>p1: leader on broker 1 / {@link TestConstants#LOGDIR1}, follower on broker 0 / {@link TestConstants#LOGDIR1}</li>
   *   <li>p2: leader on broker 0 / {@link TestConstants#LOGDIR1}, follower on broker 1 / {@link TestConstants#LOGDIR0}</li>
   *   <li>p3: leader on broker 1 / {@link TestConstants#LOGDIR1}, offline follower on the dead disk of broker 2</li>
   * </ul>
   * The NW_IN rate of partition i is {@code 10 * (i + 1)}, and the NW_OUT rate of its leader is {@code 100 * (i + 1)}.
   *
   * @return The cluster model.
   */
  private static ClusterModel jbodClusterModel() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0), 1.0);
    BrokerCapacityInfo capacity = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY);
    for (int brokerId = 0; brokerId < 3; brokerId++) {
      clusterModel.createRack(Integer.toString(brokerId));
      clusterModel.createBroker(Integer.toString(brokerId), Integer.toString(brokerId), brokerId, capacity, true);
    }
    createReplica(clusterModel, 0, 0, LOGDIR0, true, false);
    createReplica(clusterModel, 0, 1, LOGDIR0, false, false);
    createReplica(clusterModel, 1, 1, LOGDIR1, true, false);
    createReplica(clusterModel, 1, 0, LOGDIR1, false, false);
    createReplica(clusterModel, 2, 0, LOGDIR1, true, false);
    createReplica(clusterModel, 2, 1, LOGDIR0, false, false);
    createReplica(clusterModel, 3, 1, LOGDIR1, true, false);
    createReplica(clusterModel, 3, 2, DEAD_LOGDIR, false, true);
    return clusterModel;
  }

  private static void createReplica(ClusterModel clusterModel, int partition, int brokerId, String logdir, boolean isLeader,
                                    boolean isOffline) {
    TopicPartition tp = new TopicPartition(TOPIC, partition);
    String rackId = Integer.toString(brokerId);
    clusterModel.createReplica(rackId, brokerId, tp, isLeader ? 0 : 1, isLeader, isOffline, logdir, false);
    clusterModel.setReplicaLoad(rackId, brokerId, tp,
                                getAggregatedMetricValues(1.0, 10.0 * (partition + 1), isLeader ? 100.0 * (partition + 1) : 0.0, 1000.0),
                                Collections.singletonList(1L));
  }

  @Test
  public void testDiskNetworkRatesAddUpToBrokerRates() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    BrokerStats brokerStats = jbodClusterModel().brokerStats(config);
    Map<String, Object> json = new Gson().fromJson(new Gson().toJson(brokerStats.getJsonStructure()),
                                                   new TypeToken<Map<String, Object>>() { }.getType());
    List<Map<String, Object>> brokers = cast(json.get("brokers"));
    assertEquals(3, brokers.size());
    for (Map<String, Object> broker : brokers) {
      Map<String, Map<String, Object>> diskStates = cast(broker.get("DiskState"));
      if (((Number) broker.get("Broker")).intValue() == 2) {
        // The rates of a dead disk are reported as DEAD, as is its usage.
        for (String rate : NW_RATES) {
          assertEquals("DEAD", diskStates.get(DEAD_LOGDIR).get(rate));
        }
        continue;
      }
      for (String rate : NW_RATES) {
        double sumOfDisks = diskStates.values().stream().mapToDouble(diskState -> (Double) diskState.get(rate)).sum();
        assertEquals("Broker " + broker.get("Broker") + " " + rate, (Double) broker.get(rate), sumOfDisks, DELTA);
      }
    }

    // Broker 0 / LOGDIR1 has the leader of p2 and a follower of p1: its potential NW_OUT also includes the NW_OUT of p1's leader.
    Map<String, Map<String, Object>> broker0Disks = cast(brokers.stream()
                                                                .filter(b -> ((Number) b.get("Broker")).intValue() == 0)
                                                                .findFirst().orElseThrow().get("DiskState"));
    Map<String, Object> disk = broker0Disks.get(LOGDIR1);
    assertEquals(30.0, (Double) disk.get("LeaderNwInRate"), DELTA);
    assertEquals(20.0, (Double) disk.get("FollowerNwInRate"), DELTA);
    assertEquals(300.0, (Double) disk.get("NwOutRate"), DELTA);
    assertEquals(500.0, (Double) disk.get("PnwOutRate"), DELTA);
  }

  @Test
  public void testDiskNetworkRatesInPlainTextResponse() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    List<String> lines = List.of(jbodClusterModel().brokerStats(config).toString().split(System.lineSeparator()));
    // Rows of broker 0: the broker row, then the row of LOGDIR1 and LOGDIR0.
    int brokerRowIndex = 0;
    while (!lines.get(brokerRowIndex).trim().startsWith("0,")) {
      brokerRowIndex++;
    }
    String brokerRow = lines.get(brokerRowIndex);
    String diskRow = lines.get(brokerRowIndex + 1);
    assertTrue(diskRow, diskRow.contains(LOGDIR1));
    // LEADER_NW_IN, FOLLOWER_NW_IN, NW_OUT and PNW_OUT of the logdir are aligned with those of the broker.
    assertEquals(columnEnd(brokerRow, "40.000"), columnEnd(diskRow, "30.000"));
    assertEquals(columnEnd(brokerRow, "20.000"), columnEnd(diskRow, "20.000"));
    assertEquals(columnEnd(brokerRow, "400.000"), columnEnd(diskRow, "300.000"));
    assertEquals(columnEnd(brokerRow, "600.000"), columnEnd(diskRow, "500.000"));
    // Disk capacity, core number, CPU and network capacities are per broker, hence not reported for logdirs.
    assertFalse(diskRow, diskRow.contains("300000.000"));
    assertFalse(diskRow, diskRow.contains("200000.000"));

    String deadDiskRow = lines.stream().filter(line -> line.contains(DEAD_LOGDIR)).findFirst().orElseThrow();
    assertEquals(deadDiskRow, 6, deadDiskRow.split("DEAD", -1).length - 1);
  }

  private static int columnEnd(String row, String value) {
    int index = row.indexOf(value + ",");
    assertTrue(value + " not found in " + row, index >= 0);
    return index + value.length();
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object o) {
    return (T) o;
  }
}
