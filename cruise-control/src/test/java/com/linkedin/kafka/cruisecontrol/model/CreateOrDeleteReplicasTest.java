/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class CreateOrDeleteReplicasTest {
  private static final TopicPartition T1_P0 = new TopicPartition(DeterministicCluster.T1, 0);
  private static final Node[] NODES = {new Node(0, "host0", 100), new Node(1, "host1", 100), new Node(2, "host2", 100)};
  private static final Node[] NO_NODES = new Node[0];

  @Test
  public void testCreateOrDeleteReplicasSkippedOnModelInconsistency() {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Cluster clusterFromClusterModel = DeterministicCluster.generateClusterFromClusterModel(clusterModel);

    Map<Short, Set<String>> topicsByRF = new HashMap<>();
    topicsByRF.put((short) 1, Set.of(DeterministicCluster.T1));
    clusterModel.createOrDeleteReplicas(topicsByRF, Collections.emptyMap(), Collections.emptyMap(), clusterFromClusterModel);

    // Verify the delete replica works when the cluster is consistent with cluster model
    Assert.assertEquals(1, clusterModel.partition(new TopicPartition(DeterministicCluster.T1, 0)).replicas().size());

    Cluster updatedCluster = DeterministicCluster.generateClusterFromClusterModel(clusterModel);
    clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    clusterModel.createOrDeleteReplicas(topicsByRF, Collections.emptyMap(), Collections.emptyMap(),
                                        updatedCluster);
    // Verify the delete replica is skipped when the cluster is inconsistent with cluster model
    Assert.assertEquals(2, clusterModel.partition(new TopicPartition(DeterministicCluster.T1, 0)).replicas().size());
  }

  @Test
  public void testDecreaseReplicationFactorRemovesReplicasFromTheEndOfReplicaList() {
    ClusterModel clusterModel = smallClusterModelWithThreeReplicasForT1P0();
    // All replicas are online and in-sync.
    Cluster cluster = clusterForT1(new Node[]{NODES[0], NODES[2], NODES[1]}, NO_NODES);

    clusterModel.createOrDeleteReplicas(Collections.singletonMap((short) 2, Set.of(DeterministicCluster.T1)),
                                        Collections.emptyMap(), Collections.emptyMap(), cluster);

    Assert.assertEquals(Arrays.asList(0, 2), replicaBrokerIds(clusterModel, T1_P0));
  }

  @Test
  public void testDecreaseReplicationFactorRemovesOutOfSyncReplicasFirst() {
    ClusterModel clusterModel = smallClusterModelWithThreeReplicasForT1P0();
    // The replica on broker 2 is out-of-sync.
    Cluster cluster = clusterForT1(new Node[]{NODES[0], NODES[1]}, NO_NODES);

    clusterModel.createOrDeleteReplicas(Collections.singletonMap((short) 2, Set.of(DeterministicCluster.T1)),
                                        Collections.emptyMap(), Collections.emptyMap(), cluster);

    Assert.assertEquals(Arrays.asList(0, 1), replicaBrokerIds(clusterModel, T1_P0));
  }

  @Test
  public void testDecreaseReplicationFactorRemovesOfflineReplicasFirst() {
    ClusterModel clusterModel = smallClusterModelWithThreeReplicasForT1P0();
    // Broker 2 is dead in the cluster model -- even though the metadata is not aware of it yet.
    clusterModel.setBrokerState(2, Broker.State.DEAD);
    Assert.assertTrue(clusterModel.selfHealingEligibleReplicas().stream().anyMatch(r -> r.topicPartition().equals(T1_P0)));
    Cluster cluster = clusterForT1(new Node[]{NODES[0], NODES[2], NODES[1]}, NO_NODES);

    clusterModel.createOrDeleteReplicas(Collections.singletonMap((short) 2, Set.of(DeterministicCluster.T1)),
                                        Collections.emptyMap(), Collections.emptyMap(), cluster);

    Assert.assertEquals(Arrays.asList(0, 1), replicaBrokerIds(clusterModel, T1_P0));
    // The deleted offline replica no longer needs to be moved away from the dead broker.
    Assert.assertTrue(clusterModel.selfHealingEligibleReplicas().stream().noneMatch(r -> r.topicPartition().equals(T1_P0)));
  }

  @Test
  public void testDecreaseReplicationFactorRemovesReplicasReportedOfflineInMetadataFirst() {
    ClusterModel clusterModel = smallClusterModelWithThreeReplicasForT1P0();
    // The replica on broker 2 is reported as offline (e.g. on a broken disk) in the metadata -- ISR is kept intact to verify that
    // offline replicas are removed regardless of the ISR.
    Cluster cluster = clusterForT1(new Node[]{NODES[0], NODES[2], NODES[1]}, new Node[]{NODES[2]});

    clusterModel.createOrDeleteReplicas(Collections.singletonMap((short) 2, Set.of(DeterministicCluster.T1)),
                                        Collections.emptyMap(), Collections.emptyMap(), cluster);

    Assert.assertEquals(Arrays.asList(0, 1), replicaBrokerIds(clusterModel, T1_P0));
  }

  @Test
  public void testReplicasToRetainOnReplicationFactorDecrease() {
    // Replicas: [2, 0, 1, 3], leader: 0, in-sync: [0, 3], offline: [2].
    Node node3 = new Node(3, "host3", 100);
    Node[] replicas = {NODES[2], NODES[0], NODES[1], node3};
    PartitionInfo partitionInfo = new PartitionInfo(DeterministicCluster.T1, 0, NODES[0], replicas, new Node[]{NODES[0], node3},
                                                    new Node[]{NODES[2]});
    Set<Integer> offline = Set.of(2);

    // Leader, then in-sync, then out-of-sync, then offline replicas.
    Assert.assertEquals(Arrays.asList(0, 3, 1, 2), ModelUtils.replicasToRetainOnReplicationFactorDecrease(partitionInfo, offline, 4));
    Assert.assertEquals(Arrays.asList(0, 3, 1), ModelUtils.replicasToRetainOnReplicationFactorDecrease(partitionInfo, offline, 3));
    Assert.assertEquals(Arrays.asList(0, 3), ModelUtils.replicasToRetainOnReplicationFactorDecrease(partitionInfo, offline, 2));
    Assert.assertEquals(Collections.singletonList(0), ModelUtils.replicasToRetainOnReplicationFactorDecrease(partitionInfo, offline, 1));

    // No leader (e.g. no online replica is in-sync) -- online replicas are still preferred over offline ones.
    PartitionInfo leaderlessPartitionInfo = new PartitionInfo(DeterministicCluster.T1, 0, null, replicas, NO_NODES, new Node[]{NODES[2]});
    Assert.assertEquals(Arrays.asList(0, 1), ModelUtils.replicasToRetainOnReplicationFactorDecrease(leaderlessPartitionInfo, offline, 2));
  }

  /**
   * @return {@link DeterministicCluster#smallClusterModel(Map)}, where T1-0 has replicas on brokers [0 (leader), 2, 1].
   */
  private static ClusterModel smallClusterModelWithThreeReplicasForT1P0() {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    String rack = clusterModel.broker(1).rack().id();
    Load load = clusterModel.partition(T1_P0).leader().getFollowerLoadFromLeader();
    clusterModel.createReplica(rack, 1, T1_P0, 2, false);
    clusterModel.setReplicaLoad(rack, 1, T1_P0, load.loadByWindows(), load.windows());
    return clusterModel;
  }

  /**
   * @param inSyncReplicas In-sync replicas of T1-0.
   * @param offlineReplicas Offline replicas of T1-0.
   * @return A cluster containing T1-0 with replicas on brokers [0 (leader), 2, 1] and T1-1 with replicas on brokers [1 (leader), 0].
   */
  private static Cluster clusterForT1(Node[] inSyncReplicas, Node[] offlineReplicas) {
    Node[] t1p1Replicas = {NODES[1], NODES[0]};
    List<PartitionInfo> partitions = Arrays.asList(
        new PartitionInfo(DeterministicCluster.T1, 0, NODES[0], new Node[]{NODES[0], NODES[2], NODES[1]}, inSyncReplicas, offlineReplicas),
        new PartitionInfo(DeterministicCluster.T1, 1, NODES[1], t1p1Replicas, t1p1Replicas));
    return new Cluster("cluster_id", Arrays.asList(NODES), partitions, Collections.emptySet(), Collections.emptySet());
  }

  private static List<Integer> replicaBrokerIds(ClusterModel clusterModel, TopicPartition tp) {
    return clusterModel.partition(tp).replicas().stream().map(r -> r.broker().id()).collect(Collectors.toList());
  }
}
