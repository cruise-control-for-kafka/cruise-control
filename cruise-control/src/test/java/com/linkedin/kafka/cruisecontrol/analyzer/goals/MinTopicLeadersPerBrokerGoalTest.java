/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils.goal;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for what {@link MinTopicLeadersPerBrokerGoal} holds rather than enforces: topics that the optimization cannot
 * move, and brokers that it may not move replicas to. Neither fails the optimization or makes the goal violated.
 * In {@link DeterministicCluster#smallClusterModel(Map)}, the leaders of {@link DeterministicCluster#T2} are on brokers 0, 0 and 1,
 * so broker 2 is below a minimum of one leader per broker; the leaders of {@link DeterministicCluster#T1} are on brokers 0 and 1.
 */
public class MinTopicLeadersPerBrokerGoalTest {
  private static final TopicPartition T1_P0 = new TopicPartition(T1, 0);
  private static final TopicPartition T2_P0 = new TopicPartition(T2, 0);
  private static final TopicPartition T2_P1 = new TopicPartition(T2, 1);

  @Test
  public void testExcludedTopicIsHeldNotFailed() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    Goal goal = minTopicLeadersGoal(T2);

    // T2 is excluded: no failure, no movement, and the goal is not violated, although broker 2 is still below the minimum.
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), options(Set.of(T2), false)));
    assertEquals(0, clusterModel.broker(2).numLeadersFor(T2));
    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    assertTrue(proposals.toString(), proposals.isEmpty());

    // A held topic is not made worse: broker 1 holds the minimum of one leader of T2, broker 0 holds two.
    assertEquals(REPLICA_REJECT, goal.actionAcceptance(new BalancingAction(T2_P0, 1, 2, LEADERSHIP_MOVEMENT), clusterModel));
    assertEquals(ACCEPT, goal.actionAcceptance(new BalancingAction(T2_P1, 0, 2, LEADERSHIP_MOVEMENT), clusterModel));
  }

  @Test
  public void testExcludedTopicWithTooFewLeadersIsHeld() throws Exception {
    // T1 has two leaders for three brokers, so a minimum of one leader per broker cannot be enforced. Excluded, it is held.
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Goal goal = minTopicLeadersGoal(T1);
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), options(Set.of(T1), false)));

    // Not excluded, it is enforced, and the optimization fails as before.
    ClusterModel otherClusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    assertThrows(OptimizationFailureException.class,
                 () -> minTopicLeadersGoal(T1).optimize(otherClusterModel, Collections.emptySet(), options(Set.of(), false)));
  }

  @Test
  public void testTopicWithoutImmigrantsIsHeldWhenOnlyImmigrantsMove() throws Exception {
    // A request that may move only new replicas, e.g. a replication factor change of T1: T1 has an immigrant, T2 has none.
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    clusterModel.relocateReplica(T1_P0, 2, 1);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();

    // T2 cannot be fixed with T1's new replica, so it is held instead of failing the request.
    assertTrue(minTopicLeadersGoal(T2).optimize(clusterModel, Collections.emptySet(), options(Set.of(), true)));
    assertTrue(AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel).isEmpty());

    // A request that may move every replica still enforces T2: broker 2 gets a leader of T2.
    ClusterModel otherClusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    assertTrue(minTopicLeadersGoal(T2).optimize(otherClusterModel, Collections.emptySet(), options(Set.of(), false)));
    assertEquals(1, otherClusterModel.broker(2).numLeadersFor(T2));
  }

  @Test
  public void testBrokerOutsideDestinationsGetsLeadershipMovements() throws Exception {
    // Broker 2 is not a destination, but it holds a follower of T2-1, whose leader is on broker 0 (two leaders of T2): promoting it
    // gives broker 2 its leader, without moving any replica.
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    assertTrue(minTopicLeadersGoal(T2).optimize(clusterModel, Collections.emptySet(), options(Set.of(), false, Set.of(0))));
    assertEquals(1, clusterModel.broker(2).numLeadersFor(T2));
    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    assertEquals(proposals.toString(), 1, proposals.size());
    ExecutionProposal proposal = proposals.iterator().next();
    assertEquals(T2_P1, proposal.topicPartition());
    assertEquals(Set.copyOf(proposal.oldReplicas()), Set.copyOf(proposal.newReplicas()));
  }

  @Test
  public void testBrokerOutsideDestinationsIsHeldNotFailed() throws Exception {
    // Move T2-1's follower from broker 2 to broker 1: broker 2 keeps only a follower of T2-0, whose leader (broker 1) is at the
    // minimum, so only a replica move can give broker 2 a leader of T2.
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    clusterModel.relocateReplica(T2_P1, 2, 1);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();

    // Broker 2 is not a destination: no failure, no movement, the goal is not violated, and broker 2 stays below the minimum.
    assertTrue(minTopicLeadersGoal(T2).optimize(clusterModel, Collections.emptySet(), options(Set.of(), false, Set.of(1))));
    assertEquals(0, clusterModel.broker(2).numLeadersFor(T2));
    assertTrue(AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, clusterModel).isEmpty());

    // Broker 2 is a destination: it gets a leader replica of T2 from broker 0.
    ClusterModel otherClusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    otherClusterModel.relocateReplica(T2_P1, 2, 1);
    assertTrue(minTopicLeadersGoal(T2).optimize(otherClusterModel, Collections.emptySet(), options(Set.of(), false, Set.of(2))));
    assertEquals(1, otherClusterModel.broker(2).numLeadersFor(T2));
  }

  private static Goal minTopicLeadersGoal(String topicPattern) throws Exception {
    Properties configOverrides = new Properties();
    configOverrides.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, topicPattern);
    configOverrides.setProperty(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG, "1");
    return goal(MinTopicLeadersPerBrokerGoal.class, configOverrides);
  }

  private static OptimizationOptions options(Set<String> excludedTopics, boolean onlyMoveImmigrantReplicas) {
    return options(excludedTopics, onlyMoveImmigrantReplicas, Collections.emptySet());
  }

  private static OptimizationOptions options(Set<String> excludedTopics, boolean onlyMoveImmigrantReplicas,
                                             Set<Integer> requestedDestinationBrokerIds) {
    return new OptimizationOptions(excludedTopics, Collections.emptySet(), Collections.emptySet(), false, requestedDestinationBrokerIds,
                                   onlyMoveImmigrantReplicas);
  }
}
