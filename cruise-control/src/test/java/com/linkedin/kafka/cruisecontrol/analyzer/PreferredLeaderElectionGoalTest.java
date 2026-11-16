/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC3;


public class PreferredLeaderElectionGoalTest {
  private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, 0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, 1);
  private static final TopicPartition T0P2 = new TopicPartition(TOPIC0, 2);

  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, 1);
  private static final TopicPartition T1P2 = new TopicPartition(TOPIC1, 2);

  private static final TopicPartition T2P0 = new TopicPartition(TOPIC2, 0);
  private static final TopicPartition T2P1 = new TopicPartition(TOPIC2, 1);
  private static final TopicPartition T2P2 = new TopicPartition(TOPIC2, 2);

  private static final TopicPartition T3P0 = new TopicPartition(TOPIC3, 0);
  private static final TopicPartition T3P1 = new TopicPartition(TOPIC3, 1);
  private static final TopicPartition T3P2 = new TopicPartition(TOPIC3, 2);

  private static final int NUM_RACKS = 4;

  @Test
  public void testOptimizeWithoutDemotedBrokers() {
    ClusterModel clusterModel = createClusterModel(true, false).clusterModel();

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        List<Replica> replicas = clusterModel.partition(new TopicPartition(t, p)).replicas();
        for (int i = 0; i < 3; i++) {
          // only the first replica should be leader.
          assertEquals(i == 0, replicas.get(i).isLeader());
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokers() {
    ClusterModel clusterModel = createClusterModel(true, false).clusterModel();
    clusterModel.setBrokerState(0, Broker.State.DEMOTED);

    Set<TopicPartition> leaderPartitionsOnDemotedBroker = new HashSet<>();
    clusterModel.broker(0).leaderReplicas().forEach(r -> leaderPartitionsOnDemotedBroker.add(r.topicPartition()));
    Map<TopicPartition, Integer> leaderDistributionBeforeBrokerDemotion = new HashMap<>();
    clusterModel.brokers().forEach(b -> b.leaderReplicas().forEach(r -> leaderDistributionBeforeBrokerDemotion.put(r.topicPartition(), b.id())));

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (!leaderPartitionsOnDemotedBroker.contains(tp)) {
          int oldLeaderBroker = leaderDistributionBeforeBrokerDemotion.get(tp);
          assertEquals("Tp " + tp, oldLeaderBroker, clusterModel.partition(tp).leader().broker().id());
        } else {
          List<Replica> replicas = clusterModel.partition(tp).replicas();
          for (int i = 0; i < 3; i++) {
            Replica replica = replicas.get(i);
            // only the first replica should be leader.
            assertEquals(i == 0, replica.isLeader());
            if (clusterModel.broker(0).replicas().contains(replica)) {
              // The demoted replica should be in the last position.
              assertEquals(replicas.size() - 1, i);
            }
          }
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedDisks() {
    ClusterModel clusterModel = createClusterModel(true, true).clusterModel();
    clusterModel.broker(0).disk(LOGDIR0).setState(Disk.State.DEMOTED);
    clusterModel.broker(1).disk(LOGDIR1).setState(Disk.State.DEMOTED);

    Set<TopicPartition> leaderPartitionsOnDemotedDisk = new HashSet<>();
    clusterModel.broker(0).disk(LOGDIR0).leaderReplicas()
                .forEach(r -> leaderPartitionsOnDemotedDisk.add(r.topicPartition()));
    clusterModel.broker(1).disk(LOGDIR1).leaderReplicas()
                .forEach(r -> leaderPartitionsOnDemotedDisk.add(r.topicPartition()));

    Map<TopicPartition, Integer> leaderDistributionBeforeBrokerDemotion = new HashMap<>();
    clusterModel.brokers().forEach(b -> b.leaderReplicas().forEach(r -> leaderDistributionBeforeBrokerDemotion.put(r.topicPartition(), b.id())));

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (!leaderPartitionsOnDemotedDisk.contains(tp)) {
          int oldLeaderBroker = leaderDistributionBeforeBrokerDemotion.get(tp);
          assertEquals("Tp " + tp, oldLeaderBroker, clusterModel.partition(tp).leader().broker().id());
        } else {
          List<Replica> replicas = clusterModel.partition(tp).replicas();
          for (int i = 0; i < 3; i++) {
            Replica replica = replicas.get(i);
            // only the first replica should be leader.
            assertEquals(i == 0, replica.isLeader());
            if (clusterModel.broker(0).disk(LOGDIR0).replicas().contains(replica)
                || clusterModel.broker(1).disk(LOGDIR1).replicas().contains(replica)) {
              // The demoted replica should be in the last position.
              assertEquals(replica.topicPartition() + " broker " + replica.broker().id(), replicas.size() - 1, i);
            }
          }
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndDisks() {
    ClusterModel clusterModel = createClusterModel(true, true).clusterModel();
    clusterModel.setBrokerState(0, Broker.State.DEMOTED);
    clusterModel.broker(1).disk(LOGDIR0).setState(Disk.State.DEMOTED);

    Set<TopicPartition> leaderPartitionsToBeDemoted = new HashSet<>();
    clusterModel.broker(0).leaderReplicas().forEach(r -> leaderPartitionsToBeDemoted.add(r.topicPartition()));
    clusterModel.broker(1).disk(LOGDIR0).leaderReplicas()
                .forEach(r -> leaderPartitionsToBeDemoted.add(r.topicPartition()));

    Map<TopicPartition, Integer> leaderDistributionBeforeBrokerDemotion = new HashMap<>();
    clusterModel.brokers().forEach(b -> b.leaderReplicas().forEach(r -> leaderDistributionBeforeBrokerDemotion.put(r.topicPartition(), b.id())));

    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (!leaderPartitionsToBeDemoted.contains(tp)) {
          int oldLeaderBroker = leaderDistributionBeforeBrokerDemotion.get(tp);
          assertEquals("Tp " + tp, oldLeaderBroker, clusterModel.partition(tp).leader().broker().id());
        } else {
          List<Replica> replicas = clusterModel.partition(tp).replicas();
          for (int i = 0; i < 3; i++) {
            Replica replica = replicas.get(i);
            // only the first replica should be leader.
            assertEquals(i == 0, replica.isLeader());
            if (clusterModel.broker(0).replicas().contains(replica)
                || clusterModel.broker(1).disk(LOGDIR0).replicas().contains(replica)) {
              // The demoted replica should be in the last position.
              assertEquals(replicas.size() - 1, i);
            }
          }
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndSkipUrpDemotion() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModel(false, false);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    Cluster cluster = clusterModelAndInfo.clusterInfo();
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    Map<TopicPartition, List<ReplicaPlacementInfo>> originalReplicaDistribution = clusterModel.getReplicaDistribution();
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(true, false, cluster);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

    // Operation on under replicated partitions should be skipped.
    for (String t : Arrays.asList(TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        assertEquals("Tp " + tp, originalReplicaDistribution.get(tp), clusterModel.getReplicaDistribution().get(tp));
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndExcludeFollowerDemotion() {
    ClusterModel clusterModel = createClusterModel(true, false).clusterModel();
    clusterModel.setBrokerState(2, Broker.State.DEMOTED);

    Map<TopicPartition, ReplicaPlacementInfo> originalLeaderDistribution = clusterModel.getLeaderDistribution();
    Map<TopicPartition, List<ReplicaPlacementInfo>> originalReplicaDistribution = clusterModel.getReplicaDistribution();
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, true, null);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    Map<TopicPartition, List<ReplicaPlacementInfo>> optimizedReplicaDistribution = clusterModel.getReplicaDistribution();

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (originalReplicaDistribution.get(tp).contains(2)) {
          if (originalLeaderDistribution.get(tp).brokerId() == 2) {
            List<Integer> replicas = optimizedReplicaDistribution.get(tp).stream().mapToInt(ReplicaPlacementInfo::brokerId)
                                                                 .boxed().toList();
            assertEquals("Tp " + tp, 2, replicas.get(replicas.size() - 1).intValue());
          } else {
            assertEquals("Tp " + tp, originalReplicaDistribution.get(tp), optimizedReplicaDistribution.get(tp));
          }
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndSkipUrpDemotionAndExcludeFollowerDemotion() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModel(false, false);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    Cluster cluster = clusterModelAndInfo.clusterInfo();
    clusterModel.setBrokerState(0, Broker.State.DEMOTED);

    Map<TopicPartition, ReplicaPlacementInfo> originalLeaderDistribution = clusterModel.getLeaderDistribution();
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(true, true, cluster);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    Map<TopicPartition, ReplicaPlacementInfo> optimizedLeaderDistribution = clusterModel.getLeaderDistribution();
    Map<TopicPartition, List<ReplicaPlacementInfo>> optimizedReplicaDistribution = clusterModel.getReplicaDistribution();

    for (String t : Arrays.asList(TOPIC0, TOPIC1, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        TopicPartition tp = new TopicPartition(t, p);
        if (originalLeaderDistribution.get(tp).brokerId() == 0 && t.equals(TOPIC0)) {
          List<Integer> replicas = optimizedReplicaDistribution.get(tp).stream().mapToInt(ReplicaPlacementInfo::brokerId)
                                                               .boxed().toList();
          assertEquals("Tp " + tp, 0, replicas.get(replicas.size() - 1).intValue());
        } else {
          assertEquals("Tp " + tp, originalLeaderDistribution.get(tp), optimizedLeaderDistribution.get(tp));
        }
      }
    }
  }

  @Test
  public void testOptimizeWithDemotedBrokersAndExcludedTopics() {
    ClusterModel clusterModel = createClusterModel(true, false).clusterModel();
    // Broker 1 is the leader of T0P1 and T1P0.
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    Map<TopicPartition, ReplicaPlacementInfo> originalLeaderDistribution = clusterModel.getLeaderDistribution();
    Map<TopicPartition, List<ReplicaPlacementInfo>> originalReplicaDistribution = clusterModel.getReplicaDistribution();
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null, true);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.singleton(TOPIC0),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));
    // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
    assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

    // The replica order and leadership of the partitions of the excluded topic are expected to remain unchanged.
    assertPartitionsUnchanged(clusterModel, TOPIC0, originalReplicaDistribution, originalLeaderDistribution);
    assertEquals(1, clusterModel.partition(T0P1).leader().broker().id());
    // The leadership of the partitions of the other topics is expected to be moved away from the demoted broker.
    assertDemoted(clusterModel, T1P0, 1);
  }

  @Test
  public void testOptimizeWithDemotedBrokersIgnoresExcludedTopicsByDefault() {
    ClusterModel clusterModel = createClusterModel(true, false).clusterModel();
    // Broker 1 is the leader of T0P1 and T1P0.
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    // Unless the goal is asked to honor the excluded topics, it ignores them.
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null);
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.singleton(TOPIC0),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));

    assertDemoted(clusterModel, T0P1, 1);
    assertDemoted(clusterModel, T1P0, 1);
  }

  @Test
  public void testOptimizeWithDemotedDisksAndExcludedTopics() {
    ClusterModel clusterModel = createClusterModel(true, true).clusterModel();
    // LOGDIR0 of broker 4 hosts the leaders of T2P0, T2P2, and T3P2.
    clusterModel.broker(4).disk(LOGDIR0).setState(Disk.State.DEMOTED);

    Map<TopicPartition, ReplicaPlacementInfo> originalLeaderDistribution = clusterModel.getLeaderDistribution();
    Map<TopicPartition, List<ReplicaPlacementInfo>> originalReplicaDistribution = clusterModel.getReplicaDistribution();
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null, true);
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.singleton(TOPIC2),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));

    // The replica order and leadership of the partitions of the excluded topic are expected to remain unchanged.
    assertPartitionsUnchanged(clusterModel, TOPIC2, originalReplicaDistribution, originalLeaderDistribution);
    assertEquals(4, clusterModel.partition(T2P0).leader().broker().id());
    assertEquals(4, clusterModel.partition(T2P2).leader().broker().id());
    // The leadership of the partitions of the other topics is expected to be moved away from the demoted disk.
    assertDemoted(clusterModel, T3P2, 4);
  }

  @Test
  public void testOptimizeWithoutDemotedBrokersAndWithExcludedTopics() {
    ClusterModel clusterModel = createClusterModel(true, false).clusterModel();

    Map<TopicPartition, ReplicaPlacementInfo> originalLeaderDistribution = clusterModel.getLeaderDistribution();
    Map<TopicPartition, List<ReplicaPlacementInfo>> originalReplicaDistribution = clusterModel.getReplicaDistribution();
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(false, false, null, true);
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.singleton(TOPIC1),
                                                                                Collections.emptySet(),
                                                                                Collections.emptySet()));

    // The leaders of the partitions of the excluded topic (i.e. not the first replicas) are expected to remain unchanged.
    assertPartitionsUnchanged(clusterModel, TOPIC1, originalReplicaDistribution, originalLeaderDistribution);
    for (String t : Arrays.asList(TOPIC0, TOPIC2)) {
      for (int p = 0; p < 3; p++) {
        List<Replica> replicas = clusterModel.partition(new TopicPartition(t, p)).replicas();
        for (int i = 0; i < 3; i++) {
          // only the first replica should be leader.
          assertEquals(i == 0, replicas.get(i).isLeader());
        }
      }
    }
  }

  @Test
  public void testDemotedBrokerLeadershipMovesOnlyToInSyncReplicas() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModelWithIsr(false);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    optimizeWithIsr(clusterModel, clusterModelAndInfo.clusterInfo(), false, Collections.emptySet());
    assertInSyncDemotion(clusterModel);
  }

  @Test
  public void testDemotedDiskLeadershipMovesOnlyToInSyncReplicas() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModelWithIsr(true);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    clusterModel.broker(1).disk(LOGDIR0).setState(Disk.State.DEMOTED);

    optimizeWithIsr(clusterModel, clusterModelAndInfo.clusterInfo(), false, Collections.emptySet());
    assertInSyncDemotion(clusterModel);
  }

  @Test
  public void testDemotionWithSkipUrpDemotionAndIsr() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModelWithIsr(false);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    optimizeWithIsr(clusterModel, clusterModelAndInfo.clusterInfo(), true, Collections.emptySet());
    // Only the partition with all replicas in sync is demoted.
    assertReplicaOrder(clusterModel, T0P0, 2, 3, 1);
    assertReplicaOrder(clusterModel, T0P1, 1, 2, 3);
    assertReplicaOrder(clusterModel, T0P2, 1, 3, 2);
    assertReplicaOrder(clusterModel, T1P0, 1, 2, 3);
    assertReplicaOrder(clusterModel, T1P1, 2, 1, 3);
  }

  @Test
  public void testDemotionSkipsPartitionsWhoseInSyncReplicasAreExcludedForLeadership() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModelWithIsr(false);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);

    optimizeWithIsr(clusterModel, clusterModelAndInfo.clusterInfo(), false, Collections.singleton(3));
    assertReplicaOrder(clusterModel, T0P0, 2, 3, 1);
    // The only other in-sync replica is on broker 3, which is excluded for leadership.
    assertReplicaOrder(clusterModel, T0P1, 1, 2, 3);
    assertReplicaOrder(clusterModel, T0P2, 2, 3, 1);
    assertReplicaOrder(clusterModel, T1P0, 1, 2, 3);
  }

  @Test
  public void testDemotionOfTwoBrokersMovesLeadershipOffBoth() {
    ClusterModelAndInfo clusterModelAndInfo = createClusterModelWithIsr(false);
    ClusterModel clusterModel = clusterModelAndInfo.clusterModel();
    clusterModel.setBrokerState(1, Broker.State.DEMOTED);
    clusterModel.setBrokerState(2, Broker.State.DEMOTED);

    optimizeWithIsr(clusterModel, clusterModelAndInfo.clusterInfo(), false, Collections.emptySet());
    // The followers on the demoted brokers stay in place (excludeFollowerDemotion), but do not take over the leadership.
    assertReplicaOrder(clusterModel, T0P0, 3, 2, 1);
    assertReplicaOrder(clusterModel, T0P1, 3, 2, 1);
    // Broker 2 is in sync but demoted, and broker 3 is not in sync.
    assertReplicaOrder(clusterModel, T0P2, 1, 3, 2);
    assertReplicaOrder(clusterModel, T1P0, 1, 2, 3);
    assertReplicaOrder(clusterModel, T1P1, 3, 1, 2);
  }

  // The expected result of demoting broker 1, or its disk, in the cluster of createClusterModelWithIsr.
  private static void assertInSyncDemotion(ClusterModel clusterModel) {
    // All replicas are in sync: the next replica takes over the leadership.
    assertReplicaOrder(clusterModel, T0P0, 2, 3, 1);
    // Only broker 3 is in sync: it takes over the leadership, although broker 2 is next in the replica list.
    assertReplicaOrder(clusterModel, T0P1, 3, 2, 1);
    // Only broker 2 is in sync: it takes over the leadership, although broker 3 is next in the replica list.
    assertReplicaOrder(clusterModel, T0P2, 2, 3, 1);
    // No other replica is in sync: the leadership and the replica order stay unchanged.
    assertReplicaOrder(clusterModel, T1P0, 1, 2, 3);
    // Followers are not demoted (excludeFollowerDemotion).
    assertReplicaOrder(clusterModel, T1P1, 2, 1, 3);
  }

  private static void optimizeWithIsr(ClusterModel clusterModel,
                                      Cluster cluster,
                                      boolean skipUrpDemotion,
                                      Set<Integer> excludedBrokersForLeadership) {
    PreferredLeaderElectionGoal goal = new PreferredLeaderElectionGoal(skipUrpDemotion, true, cluster, false);
    goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                excludedBrokersForLeadership,
                                                                                Collections.emptySet()));
  }

  // Assert that the replicas of the given partition are on the given brokers in the given order, and only the first one leads.
  private static void assertReplicaOrder(ClusterModel clusterModel, TopicPartition tp, Integer... brokerIds) {
    List<Replica> replicas = clusterModel.partition(tp).replicas();
    assertEquals("Tp " + tp, Arrays.asList(brokerIds), replicas.stream().map(r -> r.broker().id()).toList());
    for (int i = 0; i < replicas.size(); i++) {
      assertEquals("Tp " + tp, i == 0, replicas.get(i).isLeader());
    }
  }

  // Assert that the given partition is led by its first replica, and its replica on the given demoted broker is the last replica.
  private static void assertDemoted(ClusterModel clusterModel, TopicPartition tp, int demotedBrokerId) {
    List<Replica> replicas = clusterModel.partition(tp).replicas();
    for (int i = 0; i < replicas.size(); i++) {
      assertEquals("Tp " + tp, i == 0, replicas.get(i).isLeader());
    }
    assertEquals("Tp " + tp, demotedBrokerId, replicas.get(replicas.size() - 1).broker().id());
  }

  // Assert that the replica order and leadership of the partitions of the given topic are unchanged.
  private static void assertPartitionsUnchanged(ClusterModel clusterModel,
                                                String topic,
                                                Map<TopicPartition, List<ReplicaPlacementInfo>> originalReplicaDistribution,
                                                Map<TopicPartition, ReplicaPlacementInfo> originalLeaderDistribution) {
    Map<TopicPartition, List<ReplicaPlacementInfo>> replicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> leaderDistribution = clusterModel.getLeaderDistribution();
    for (int p = 0; p < 3; p++) {
      TopicPartition tp = new TopicPartition(topic, p);
      assertEquals("Tp " + tp, originalReplicaDistribution.get(tp), replicaDistribution.get(tp));
      assertEquals("Tp " + tp, originalLeaderDistribution.get(tp), leaderDistribution.get(tp));
    }
  }

  private static class ClusterModelAndInfo {
    private final ClusterModel _clusterModel;
    private final Cluster _clusterInfo;

    ClusterModelAndInfo(ClusterModel clusterModel, Cluster clusterInfo) {
      _clusterInfo = clusterInfo;
      _clusterModel = clusterModel;
    }

    public ClusterModel clusterModel() {
      return _clusterModel;
    }

    public Cluster clusterInfo() {
      return _clusterInfo;
    }
  }

  private ClusterModelAndInfo createClusterModel(boolean skipClusterInfoGeneration, boolean populateDiskInfo) {

    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                                                 1.0);
    for (int i = 0; i < NUM_RACKS; i++) {
      clusterModel.createRack("r" + i);
    }
    BrokerCapacityInfo commonBrokerCapacityInfo = populateDiskInfo ? new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY,
                                                                                            null,
                                                                                            TestConstants.DISK_CAPACITY)
                                                                   : new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    int i = 0;
    for (; i < 2; i++) {
      clusterModel.createBroker("r0", "h" + i, i, commonBrokerCapacityInfo, populateDiskInfo);
    }
    for (int j = 1; j < NUM_RACKS; j++, i++) {
      clusterModel.createBroker("r" + j, "h" + i, i, commonBrokerCapacityInfo, populateDiskInfo);
    }

    createReplicaAndSetLoad(clusterModel, "r0", 0, logdir(populateDiskInfo, 0, 0), T0P0, 0, true);
    createReplicaAndSetLoad(clusterModel, "r0", 1, logdir(populateDiskInfo, 0, 1), T0P1, 0, true);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 0, 2), T0P2, 0, true);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 0, 3), T1P0, 0, false);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 0, 4), T1P1, 0, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, logdir(populateDiskInfo, 0, 0), T1P2, 0, false);
    createReplicaAndSetLoad(clusterModel, "r0", 1, logdir(populateDiskInfo, 0, 1), T2P0, 0, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 0, 2), T2P1, 0, false);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 0, 3), T2P2, 0, false);

    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 1, 4), T0P0, 1, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 1, 2), T0P1, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, logdir(populateDiskInfo, 1, 0), T0P2, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 1, logdir(populateDiskInfo, 1, 1), T1P0, 1, true);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 1, 3), T1P1, 1, true);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 1, 4), T1P2, 1, true);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 1, 2), T2P0, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, logdir(populateDiskInfo, 1, 0), T2P1, 1, false);
    createReplicaAndSetLoad(clusterModel, "r0", 1, logdir(populateDiskInfo, 1, 1), T2P2, 1, false);

    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 2, 3), T0P0, 2, false);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 2, 4), T0P1, 2, false);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 2, 3), T0P2, 2, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 2, 2), T1P0, 2, false);
    createReplicaAndSetLoad(clusterModel, "r0", 0, logdir(populateDiskInfo, 2, 0), T1P1, 2, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 2, 2), T1P2, 2, false);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 2, 4), T2P0, 2, true);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 2, 3), T2P1, 2, true);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 2, 4), T2P2, 2, true);

    // The following topic exists in cluster model and not in the cluster
    // This is to simulate a case where a topic is deleted from the cluster during an ongoing PLE
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 0, 4), T3P0, 0, true);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 0, 3), T3P1, 0, true);
    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 0, 4), T3P2, 0, true);

    createReplicaAndSetLoad(clusterModel, "r0", 0, logdir(populateDiskInfo, 1, 4), T3P0, 1, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 1, 3), T3P1, 1, false);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 1, 4), T3P2, 1, false);

    createReplicaAndSetLoad(clusterModel, "r3", 4, logdir(populateDiskInfo, 2, 4), T3P0, 2, false);
    createReplicaAndSetLoad(clusterModel, "r2", 3, logdir(populateDiskInfo, 2, 3), T3P1, 2, false);
    createReplicaAndSetLoad(clusterModel, "r1", 2, logdir(populateDiskInfo, 2, 4), T3P2, 2, false);

    Cluster cluster = null;
    if (!skipClusterInfoGeneration) {
      Node [] nodes = new Node [NUM_RACKS + 1];
      for (i = 0; i < NUM_RACKS + 1; i++) {
        nodes[i] = new Node(i, "h" + i, 100);
      }
      List<PartitionInfo> partitions = new ArrayList<>(9);
      // Make topic1 and topic2's partitions under replicated.
      partitions.add(new PartitionInfo(T0P0.topic(), T0P0.partition(), nodes[0], new Node[]{nodes[0], nodes[4], nodes[3]},
                                       new Node[]{nodes[0], nodes[4], nodes[3]}));

      partitions.add(new PartitionInfo(T0P1.topic(), T0P1.partition(), nodes[1], new Node[]{nodes[1], nodes[2], nodes[4]},
                                       new Node[]{nodes[1], nodes[2], nodes[4]}));

      partitions.add(new PartitionInfo(T0P2.topic(), T0P2.partition(), nodes[2], new Node[]{nodes[2], nodes[0], nodes[3]},
                                       new Node[]{nodes[2], nodes[0], nodes[3]}));

      partitions.add(new PartitionInfo(T1P0.topic(), T1P0.partition(), nodes[1], new Node[]{nodes[1], nodes[3]},
                                       new Node[]{nodes[1], nodes[3], nodes[2]}));

      partitions.add(new PartitionInfo(T1P1.topic(), T1P1.partition(), nodes[3], new Node[]{nodes[3], nodes[4]},
                                       new Node[]{nodes[3], nodes[4], nodes[0]}));

      partitions.add(new PartitionInfo(T1P2.topic(), T1P2.partition(), nodes[4], new Node[]{nodes[4], nodes[2]},
                                       new Node[]{nodes[4], nodes[2], nodes[0]}));

      partitions.add(new PartitionInfo(T2P0.topic(), T2P0.partition(), nodes[4], new Node[]{nodes[4], nodes[2]},
                                       new Node[]{nodes[4], nodes[2], nodes[1]}));

      partitions.add(new PartitionInfo(T2P1.topic(), T2P1.partition(), nodes[3], new Node[]{nodes[3], nodes[0]},
                                       new Node[]{nodes[3], nodes[0], nodes[2]}));

      partitions.add(new PartitionInfo(T2P2.topic(), T2P2.partition(), nodes[4], new Node[]{nodes[4], nodes[1]},
                                       new Node[]{nodes[4], nodes[1], nodes[3]}));

      cluster = new Cluster("id", Arrays.asList(nodes), partitions, Collections.emptySet(), Collections.emptySet());
    }
    return new ClusterModelAndInfo(clusterModel, cluster);
  }

  /**
   * Brokers 1, 2 and 3, one per rack, with all replicas on {@code LOGDIR0} if disk info is populated. Broker 1 leads four
   * partitions with different in-sync replicas, and follows a fifth one:
   * <ul>
   *   <li>T0P0 replicas [1, 2, 3], ISR [1, 2, 3]</li>
   *   <li>T0P1 replicas [1, 2, 3], ISR [1, 3]</li>
   *   <li>T0P2 replicas [1, 3, 2], ISR [1, 2]</li>
   *   <li>T1P0 replicas [1, 2, 3], ISR [1]</li>
   *   <li>T1P1 replicas [2, 1, 3], ISR [1, 2, 3]</li>
   * </ul>
   *
   * @param populateDiskInfo {@code true} to create the brokers with disks, {@code false} otherwise.
   * @return The cluster model, and the Kafka cluster with the in-sync replicas above.
   */
  private ClusterModelAndInfo createClusterModelWithIsr(boolean populateDiskInfo) {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0), 1.0);
    BrokerCapacityInfo capacity = populateDiskInfo ? new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, null, TestConstants.DISK_CAPACITY)
                                                   : new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY);
    Node[] nodes = new Node[4];
    for (int b = 1; b <= 3; b++) {
      clusterModel.createRack("r" + b);
      clusterModel.createBroker("r" + b, "h" + b, b, capacity, populateDiskInfo);
      nodes[b] = new Node(b, "h" + b, 100);
    }
    Map<TopicPartition, int[][]> replicasAndIsr = new LinkedHashMap<>();
    replicasAndIsr.put(T0P0, new int[][]{{1, 2, 3}, {1, 2, 3}});
    replicasAndIsr.put(T0P1, new int[][]{{1, 2, 3}, {1, 3}});
    replicasAndIsr.put(T0P2, new int[][]{{1, 3, 2}, {1, 2}});
    replicasAndIsr.put(T1P0, new int[][]{{1, 2, 3}, {1}});
    replicasAndIsr.put(T1P1, new int[][]{{2, 1, 3}, {1, 2, 3}});
    List<PartitionInfo> partitions = new ArrayList<>();
    for (Map.Entry<TopicPartition, int[][]> entry : replicasAndIsr.entrySet()) {
      TopicPartition tp = entry.getKey();
      int[] replicas = entry.getValue()[0];
      for (int i = 0; i < replicas.length; i++) {
        createReplicaAndSetLoad(clusterModel, "r" + replicas[i], replicas[i], populateDiskInfo ? LOGDIR0 : null, tp, i, i == 0);
      }
      partitions.add(new PartitionInfo(tp.topic(), tp.partition(), nodes[replicas[0]],
                                       Arrays.stream(replicas).mapToObj(b -> nodes[b]).toArray(Node[]::new),
                                       Arrays.stream(entry.getValue()[1]).mapToObj(b -> nodes[b]).toArray(Node[]::new)));
    }
    Cluster cluster = new Cluster("id", Arrays.asList(nodes[1], nodes[2], nodes[3]), partitions, Collections.emptySet(),
                                  Collections.emptySet());
    return new ClusterModelAndInfo(clusterModel, cluster);
  }

  private String logdir(boolean populateDiskInfo, int index, int brokerId) {
    return !populateDiskInfo ? null : (index + brokerId) % 2 == 0 ? LOGDIR0 : LOGDIR1;
  }

  private void createReplicaAndSetLoad(ClusterModel clusterModel,
                                       String rack,
                                       int brokerId,
                                       String logdir,
                                       TopicPartition tp,
                                       int index,
                                       boolean isLeader) {
    clusterModel.createReplica(rack, brokerId, tp, index, isLeader, false, logdir, false);
    MetricValues metricValues = new MetricValues(1);
    Map<Short, MetricValues> metricValuesByResource = new HashMap<>();
    Resource.cachedValues().forEach(r -> {
      for (short id : KafkaMetricDef.resourceToMetricIds(r)) {
        metricValuesByResource.put(id, metricValues);
      }
    });
    clusterModel.setReplicaLoad(rack, brokerId, tp, new AggregatedMetricValues(metricValuesByResource),
                                Collections.singletonList(1L));
  }
}
