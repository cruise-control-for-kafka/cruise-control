/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class ReplicaPlacementFingerprintTest {
  private static final List<Node> NODES = Arrays.asList(new Node(0, "h0", 9092), new Node(1, "h1", 9092), new Node(2, "h2", 9092));
  private static final TopicPartition TP0 = new TopicPartition("topic", 0);
  private static final TopicPartition TP1 = new TopicPartition("topic", 1);
  private static final TopicPartition TP2 = new TopicPartition("other", 0);

  private static PartitionInfo partition(TopicPartition tp, Integer... replicas) {
    Node[] replicaNodes = Arrays.stream(replicas).map(NODES::get).toArray(Node[]::new);
    return new PartitionInfo(tp.topic(), tp.partition(), replicaNodes[0], replicaNodes, replicaNodes);
  }

  private static Cluster cluster(PartitionInfo... partitions) {
    List<PartitionInfo> partitionInfos = new ArrayList<>(Arrays.asList(partitions));
    return new Cluster("cluster", NODES, partitionInfos, Collections.emptySet(), Collections.emptySet());
  }

  @Test
  public void testFingerprintIgnoresOrderAndExcludedPartitions() {
    ReplicaPlacementFingerprint fingerprint = ReplicaPlacementFingerprint.of(
        cluster(partition(TP0, 0, 1), partition(TP1, 1, 2), partition(TP2, 2, 0)), Set.of(TP0));
    // The order of the replicas (i.e. leadership preference) and the excluded partitions do not matter.
    assertEquals(fingerprint, ReplicaPlacementFingerprint.of(cluster(partition(TP2, 0, 2), partition(TP1, 2, 1), partition(TP0, 2, 1)),
                                                             Set.of(TP0)));
    assertEquals(2, fingerprint.numPartitions());
    assertEquals(Integer.valueOf(2), fingerprint.numReplicasByBroker().get(2));
    assertTrue(fingerprint.differencesTo(fingerprint).isEmpty());
  }

  @Test
  public void testDifferences() {
    ReplicaPlacementFingerprint before = ReplicaPlacementFingerprint.of(cluster(partition(TP1, 1, 2), partition(TP2, 2, 0)),
                                                                        Collections.emptySet());
    // A replica moved from broker 2 to broker 0.
    ReplicaPlacementFingerprint moved = ReplicaPlacementFingerprint.of(cluster(partition(TP1, 1, 0), partition(TP2, 2, 0)),
                                                                       Collections.emptySet());
    assertNotEquals(before, moved);
    assertEquals(List.of("broker 0: 1 -> 2 replicas outside the execution", "broker 2: 2 -> 1 replicas outside the execution"),
                 before.differencesTo(moved));
    // Replicas swapped between partitions -- the number of replicas per broker does not change.
    ReplicaPlacementFingerprint swapped = ReplicaPlacementFingerprint.of(cluster(partition(TP1, 2, 0), partition(TP2, 1, 2)),
                                                                         Collections.emptySet());
    assertEquals(List.of("replicas outside the execution were moved between brokers"), before.differencesTo(swapped));
    // A partition was created.
    ReplicaPlacementFingerprint created = ReplicaPlacementFingerprint.of(cluster(partition(TP0, 0), partition(TP1, 1, 2),
                                                                                 partition(TP2, 2, 0)), Collections.emptySet());
    assertEquals("partitions outside the execution: 2 -> 3", before.differencesTo(created).get(0));
  }
}
