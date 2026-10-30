/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicPatternBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.TOPIC_COLD;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.TOPIC_HOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link BrokerSetAwareGoal} with {@link TopicPatternBrokerSetMappingPolicy}, using
 * {@link DeterministicCluster#brokerSetTopicPatternSatisfiable()} and testBrokerSetsWithTopicPatterns.json: topic "hot" is pinned
 * to broker set "fast" (brokers 1, 2, 3), broker 0 is unlisted, and topic "cold" is unconstrained.
 */
public class BrokerSetAwareGoalTest {
  private static final Set<Integer> FAST_BROKERS = Set.of(1, 2, 3);
  private static final OptimizationOptions NO_OPTIONS =
      new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

  /**
   * Pinned replicas move into their broker set; unconstrained replicas are not moved and do not fail the final check.
   */
  @Test
  public void testPinnedTopicMovesIntoBrokerSet() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    Map<TopicPartition, List<ReplicaPlacementInfo>> coldDistribution = distribution(clusterModel, TOPIC_COLD);

    assertTrue(optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint())));

    assertTrue(FAST_BROKERS.containsAll(brokerIds(clusterModel, TOPIC_HOT)));
    assertEquals(coldDistribution, distribution(clusterModel, TOPIC_COLD));
  }

  /**
   * Goals optimized after BrokerSetAwareGoal cannot move pinned replicas out of their broker set, by a move or by a swap.
   */
  @Test
  public void testLaterGoalsCannotMovePinnedReplicasOutOfBrokerSet() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    BalancingConstraint constraint = constraint();

    optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint), new ReplicaDistributionGoal(constraint),
             new DiskUsageDistributionGoal(constraint), new TopicReplicaDistributionGoal(constraint));

    assertTrue(FAST_BROKERS.containsAll(brokerIds(clusterModel, TOPIC_HOT)));
  }

  /**
   * Pinned replicas cannot move or be swapped out of their broker set, unconstrained replicas can move anywhere, and leadership
   * moves are always accepted.
   */
  @Test
  public void testActionAcceptance() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    BrokerSetAwareGoal goal = new BrokerSetAwareGoal(constraint());
    optimize(clusterModel, NO_OPTIONS, goal);

    // hot_P0 stays on broker 1, cold_P0 and cold_P1 stay on brokers 0 and 2.
    TopicPartition hotP0 = new TopicPartition(TOPIC_HOT, 0);
    TopicPartition coldP0 = new TopicPartition(TOPIC_COLD, 0);
    TopicPartition coldP1 = new TopicPartition(TOPIC_COLD, 1);
    assertEquals(BROKER_REJECT,
                 goal.actionAcceptance(new BalancingAction(hotP0, 1, 0, ActionType.INTER_BROKER_REPLICA_MOVEMENT), clusterModel));
    assertEquals(ACCEPT, goal.actionAcceptance(new BalancingAction(hotP0, 1, 3, ActionType.INTER_BROKER_REPLICA_MOVEMENT), clusterModel));
    assertEquals(ACCEPT, goal.actionAcceptance(new BalancingAction(coldP0, 0, 3, ActionType.INTER_BROKER_REPLICA_MOVEMENT), clusterModel));
    assertEquals(ACCEPT, goal.actionAcceptance(new BalancingAction(coldP1, 2, 0, ActionType.INTER_BROKER_REPLICA_MOVEMENT), clusterModel));
    // A swap is rejected on its pinned side only.
    assertEquals(BROKER_REJECT,
                 goal.actionAcceptance(new BalancingAction(hotP0, 1, 0, ActionType.INTER_BROKER_REPLICA_SWAP, coldP0), clusterModel));
    assertEquals(REPLICA_REJECT,
                 goal.actionAcceptance(new BalancingAction(coldP0, 0, 1, ActionType.INTER_BROKER_REPLICA_SWAP, hotP0), clusterModel));
    assertEquals(ACCEPT,
                 goal.actionAcceptance(new BalancingAction(hotP0, 1, 2, ActionType.INTER_BROKER_REPLICA_SWAP, coldP1), clusterModel));
    // Leadership moves are always accepted.
    assertEquals(ACCEPT, goal.actionAcceptance(new BalancingAction(hotP0, 1, 0, ActionType.LEADERSHIP_MOVEMENT), clusterModel));
  }

  /**
   * Self-healing a dead unlisted broker moves its unconstrained replicas to any alive broker and its pinned replicas into their set.
   */
  @Test
  public void testDeadUnlistedBroker() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    clusterModel.setBrokerState(0, Broker.State.DEAD);

    assertTrue(optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint())));

    assertTrue(clusterModel.broker(0).replicas().isEmpty());
    assertTrue(FAST_BROKERS.containsAll(brokerIds(clusterModel, TOPIC_HOT)));
  }

  /**
   * Self-healing a dead pinned broker moves its pinned replicas within their broker set.
   */
  @Test
  public void testDeadPinnedBroker() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    clusterModel.setBrokerState(1, Broker.State.DEAD);

    assertTrue(optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint())));

    assertTrue(clusterModel.broker(1).replicas().isEmpty());
    assertTrue(Set.of(2, 3).containsAll(brokerIds(clusterModel, TOPIC_HOT)));
  }

  /**
   * Offline replicas on a bad disk of an alive broker are moved: the pinned one within its broker set (even though its broker is
   * in that set), the unconstrained one to any alive broker.
   */
  @Test
  public void testOfflineReplicasOnBadDisk() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternWithBadDisk();

    assertTrue(optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint())));

    for (Replica replica : replicas(clusterModel, TOPIC_HOT, TOPIC_COLD)) {
      assertFalse("Replica is still offline: " + replica, replica.isCurrentOffline());
    }
    assertTrue(FAST_BROKERS.containsAll(brokerIds(clusterModel, TOPIC_HOT)));
    assertFalse(partitionBrokerIds(clusterModel, new TopicPartition(TOPIC_HOT, 2)).contains(1));
    assertFalse(partitionBrokerIds(clusterModel, new TopicPartition(TOPIC_COLD, 2)).contains(1));
  }

  /**
   * A pinned topic in excluded_topics is neither moved nor checked.
   */
  @Test
  public void testExcludedPinnedTopic() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    Map<TopicPartition, List<ReplicaPlacementInfo>> hotDistribution = distribution(clusterModel, TOPIC_HOT);
    OptimizationOptions options = new OptimizationOptions(Set.of(TOPIC_HOT), Collections.emptySet(), Collections.emptySet());

    assertTrue(optimize(clusterModel, options, new BrokerSetAwareGoal(constraint())));

    assertEquals(hotDistribution, distribution(clusterModel, TOPIC_HOT));
  }

  /**
   * A pinned topic that must have a minimum number of leaders per broker is neither moved nor checked.
   */
  @Test
  public void testPinnedTopicWithMinLeadersPerBroker() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    Map<TopicPartition, List<ReplicaPlacementInfo>> hotDistribution = distribution(clusterModel, TOPIC_HOT);
    Properties props = new Properties();
    props.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, TOPIC_HOT);

    assertTrue(optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint(props))));

    assertEquals(hotDistribution, distribution(clusterModel, TOPIC_HOT));
  }

  /**
   * The optimization fails with the existing error if the pinned broker set has too few alive brokers for the replication factor.
   */
  @Test
  public void testPinnedBrokerSetTooSmall() {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    clusterModel.setBrokerState(2, Broker.State.DEAD);
    clusterModel.setBrokerState(3, Broker.State.DEAD);

    OptimizationFailureException e = assertThrows(OptimizationFailureException.class,
                                                  () -> optimize(clusterModel, NO_OPTIONS, new BrokerSetAwareGoal(constraint())));
    assertMessageContains(e, "Cannot move replica", "on brokerSet fast");
  }

  /**
   * The optimization fails with the existing error if all brokers of the pinned broker set are excluded for replica moves.
   */
  @Test
  public void testPinnedBrokerSetExcludedForReplicaMove() {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    OptimizationOptions options = new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), FAST_BROKERS);

    OptimizationFailureException e = assertThrows(OptimizationFailureException.class,
                                                  () -> optimize(clusterModel, options, new BrokerSetAwareGoal(constraint())));
    assertMessageContains(e, "Cannot move replica", "on brokerSet fast", "excluded from replica moves");
  }

  /**
   * The optimization fails with the destination mitigation if destination_broker_ids holds no broker of the pinned broker set.
   */
  @Test
  public void testDestinationBrokerIdsWithoutPinnedBrokerSet() {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    OptimizationOptions options =
        new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), false, Set.of(0));

    OptimizationFailureException e = assertThrows(OptimizationFailureException.class,
                                                  () -> optimize(clusterModel, options, new BrokerSetAwareGoal(constraint())));
    assertMessageContains(e, "Cannot move replica", "on brokerSet fast", "Relax the constraint on destination brokers");
  }

  /**
   * An immigrants-only run moves the pinned immigrant replica into its broker set, and does not fail on the pinned replicas that
   * were already outside the set and that it was not allowed to move.
   */
  @Test
  public void testOnlyMoveImmigrantReplicas() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    TopicPartition hotP3 = new TopicPartition(TOPIC_HOT, 3);
    clusterModel.relocateReplica(hotP3, 3, 0);
    OptimizationOptions options =
        new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), false, Collections.emptySet(), true);

    assertTrue(optimize(clusterModel, options, new BrokerSetAwareGoal(constraint())));

    assertTrue(FAST_BROKERS.containsAll(partitionBrokerIds(clusterModel, hotP3)));
    assertTrue(partitionBrokerIds(clusterModel, new TopicPartition(TOPIC_HOT, 0)).contains(0));
    assertTrue(partitionBrokerIds(clusterModel, new TopicPartition(TOPIC_HOT, 1)).contains(0));
  }

  private static BalancingConstraint constraint() {
    return constraint(new Properties());
  }

  private static BalancingConstraint constraint(Properties overrides) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG, TopicPatternBrokerSetMappingPolicy.class.getName());
    props.setProperty(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG,
                      Objects.requireNonNull(BrokerSetAwareGoalTest.class.getClassLoader()
                                                                         .getResource("testBrokerSetsWithTopicPatterns.json")).getFile());
    overrides.stringPropertyNames().forEach(k -> props.setProperty(k, overrides.getProperty(k)));
    return new BalancingConstraint(new KafkaCruiseControlConfig(props));
  }

  /**
   * Optimize the given goals in order, each with the previously optimized goals, as the goal optimizer does.
   *
   * @param clusterModel The state of the cluster.
   * @param options Options to take into account during optimization.
   * @param goals Goals to optimize, by priority.
   * @return {@code true} if all goals succeeded, {@code false} otherwise.
   */
  private static boolean optimize(ClusterModel clusterModel, OptimizationOptions options, Goal... goals)
      throws KafkaCruiseControlException {
    Set<Goal> optimizedGoals = new HashSet<>();
    boolean succeeded = true;
    for (Goal goal : goals) {
      succeeded &= goal.optimize(clusterModel, optimizedGoals, options);
      optimizedGoals.add(goal);
    }
    return succeeded;
  }

  private static void assertMessageContains(Exception e, String... parts) {
    for (String part : parts) {
      assertTrue(String.format("Message '%s' does not contain '%s'", e.getMessage(), part), e.getMessage().contains(part));
    }
  }

  private static Set<Replica> replicas(ClusterModel clusterModel, String... topics) {
    Set<Replica> replicas = new HashSet<>();
    for (String topic : topics) {
      clusterModel.getPartitionsByTopic().get(topic).forEach(partition -> replicas.addAll(partition.replicas()));
    }
    return replicas;
  }

  private static Set<Integer> brokerIds(ClusterModel clusterModel, String topic) {
    return replicas(clusterModel, topic).stream().map(replica -> replica.broker().id()).collect(Collectors.toSet());
  }

  private static Set<Integer> partitionBrokerIds(ClusterModel clusterModel, TopicPartition tp) {
    return clusterModel.partition(tp).partitionBrokers().stream().map(Broker::id).collect(Collectors.toSet());
  }

  private static Map<TopicPartition, List<ReplicaPlacementInfo>> distribution(ClusterModel clusterModel, String topic) {
    return clusterModel.getReplicaDistribution().entrySet().stream()
                       .filter(entry -> entry.getKey().topic().equals(topic))
                       .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
