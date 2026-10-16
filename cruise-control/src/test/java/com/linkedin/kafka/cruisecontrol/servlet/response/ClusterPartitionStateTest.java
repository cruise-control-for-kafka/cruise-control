/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterPartitionStateTest {
  private static final String TOPIC = "sample-topic";
  private static final int PARTITION = 0;
  private static final boolean VERBOSE = true;
  private static final Pattern TOPIC_PATTERN = null;
  private static final Map<String, Properties> ALL_TOPIC_CONFIGS = new HashMap<>();
  private static final Properties CLUSTER_CONFIGS = new Properties();
  private static final Node[] OFFLINE_REPLICAS = { };

  @Test
  public void testPopulateKafkaPartitionStateWithNoOfflinePartitions() {
    Node leader = new Node(0, "localhost", 9092);
    Node[] replicas = new Node[] { leader, new Node(1, "localhost", 9092)};
    Node[] inSyncReplicas = { replicas[0], replicas[1] };

    PartitionInfo partitionInfo = new PartitionInfo(TOPIC, PARTITION, leader, replicas, inSyncReplicas, OFFLINE_REPLICAS);

    Cluster kafkaCluster = new Cluster("clusterId", List.of(replicas), List.of(partitionInfo), Set.of(), Set.of());

    ClusterPartitionState clusterPartitionState = new ClusterPartitionState(VERBOSE, TOPIC_PATTERN, kafkaCluster, ALL_TOPIC_CONFIGS, CLUSTER_CONFIGS);

    assertTrue(clusterPartitionState._offlinePartitions.isEmpty());
  }

  @Test
  public void testPopulateKafkaPartitionStateWithOfflinePartitions() {
    Node leader = null;
    Node[] replicas = { };
    Node[] inSyncReplicas = { };

    PartitionInfo partitionInfo = new PartitionInfo(TOPIC, PARTITION, leader, replicas, inSyncReplicas, OFFLINE_REPLICAS);

    Cluster kafkaCluster = new Cluster("clusterId", List.of(replicas), List.of(partitionInfo), Set.of(), Set.of());

    Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
    Set<PartitionInfo> offlinePartitions = new TreeSet<>(comparator);
    offlinePartitions.add(partitionInfo);

    ClusterPartitionState clusterPartitionState = new ClusterPartitionState(VERBOSE, TOPIC_PATTERN, kafkaCluster, ALL_TOPIC_CONFIGS, CLUSTER_CONFIGS);
    assertEquals(offlinePartitions, clusterPartitionState._offlinePartitions);
  }
}
