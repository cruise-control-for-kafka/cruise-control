/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTER_BROKER_REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * HARD GOAL: Generate leadership movement and leader replica movement proposals to ensure that each alive broker that
 * is not excluded for replica leadership moves has at least the minimum number (specified by
 * {@link AnalyzerConfig#MIN_TOPIC_LEADERS_PER_BROKER_CONFIG}) of leader replica of each topic in a configured set of topics
 * (specified by {@link AnalyzerConfig#TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG}).
 *
 * <p>
 * Topics that the optimization cannot move are held rather than enforced: topics excluded from partition movement, and,
 * when only immigrant replicas may move (e.g. a replication factor change), topics without immigrant replicas. A held topic
 * does not fail the optimization, and this goal does not move its replicas, but it still rejects any action that would take a
 * leader of the topic away from a broker at or below the minimum.
 * </p>
 *
 * <p>
 * Likewise, a broker that the optimization may not move replicas to (it is not among the requested destination brokers) is
 * brought to the minimum only by leadership movements. If that is not enough, the broker is held at its current number of leaders.
 * </p>
 *
 * <p>
 * Neither fails the optimization nor marks this goal as violated: the shortfalls this goal cannot reach are only logged, the
 * same way {@link RackAwareGoal} does not check excluded topics.
 * </p>
 */
public class MinTopicLeadersPerBrokerGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(MinTopicLeadersPerBrokerGoal.class);
  private static final int MAX_TOPICS_TO_LOG = 20;
  private final String _replicaSortName = replicaSortName(this, true, false);
  // Map holding the number of min leaders per broker for each topic
  // When {@link AnalyzerConfig#MIN_TOPIC_LEADERS_PER_BROKER_CONFIG} is set to 0 the number
  // of leaders are computed dynamically as no-of-topic-leaders / no-of-brokers-available.
  // When set to a positive value, this is used instead
  private Map<String, Integer> _mustHaveTopicMinLeadersPerBroker;
  // Topics with a minimum, whose minimum is enforced in this optimization.
  private Set<String> _topicsToEnforce;
  // Topics with a minimum, which this optimization cannot move: not enforced, but never made worse.
  private Set<String> _topicsToHold;

  public MinTopicLeadersPerBrokerGoal() {

  }

  /**
   * Package private for unit test.
   */
  MinTopicLeadersPerBrokerGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): A configured set of topics that should have at least a minimum number
   * of leader replicas on each alive broker that is not excluded for replica leadership moves
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    if (!actionAffectsRelevantTopics(action)) {
      return ACCEPT;
    }
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
      case INTER_BROKER_REPLICA_MOVEMENT:
        Replica replicaToBeRemoved = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        return doesLeaderRemoveViolateOptimizedGoal(replicaToBeRemoved) ? REPLICA_REJECT : ACCEPT;
      case INTER_BROKER_REPLICA_SWAP:
        Replica srcReplicaToSwap = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
        Replica dstReplicaToSwap = clusterModel.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition());
        return acceptReplicaSwap(srcReplicaToSwap, dstReplicaToSwap);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private ActionAcceptance acceptReplicaSwap(Replica srcReplicaToSwap, Replica dstReplicaToSwap) {
    if (!srcReplicaToSwap.isLeader() && !dstReplicaToSwap.isLeader()) {
      // Swapping two follower replicas is always accepted
      return ACCEPT;
    }
    String srcTopicName = srcReplicaToSwap.topicPartition().topic();
    String dstTopicName = dstReplicaToSwap.topicPartition().topic();
    if (srcReplicaToSwap.isLeader() && dstReplicaToSwap.isLeader() && Objects.equals(srcTopicName, dstTopicName)) {
      // Swapping two leader replicas of the same topic is always accepted
      return ACCEPT;
    }
    if (doesLeaderRemoveViolateOptimizedGoal(srcReplicaToSwap) || doesLeaderRemoveViolateOptimizedGoal(dstReplicaToSwap)) {
      return REPLICA_REJECT;
    }
    return ACCEPT;
  }

  /**
   * Check whether this goal is violated after removing the given leader replica. It is violated if there is not enough
   * leader replicas on that broker for that topic
   *
   * @param replicaToBeRemoved replica to be removed
   * @return {@code true} if the given replica move would violate the optimized goal (i.e. the move is not acceptable),
   * {@code false} otherwise.
   */
  private boolean doesLeaderRemoveViolateOptimizedGoal(Replica replicaToBeRemoved) {
    if (!replicaToBeRemoved.isLeader()) {
      // Moving a follower replica does not violate/affect this goal
      return false;
    }
    final String topic = replicaToBeRemoved.topicPartition().topic();
    if (!_mustHaveTopicMinLeadersPerBroker.containsKey(topic)) {
      // Moving a replica from a non-applicable topic does not violate/affect this goal
      return false;
    }
    int topicLeaderCountOnSourceBroker = replicaToBeRemoved.broker().numLeadersFor(topic);
    return topicLeaderCountOnSourceBroker <= minTopicLeadersPerBroker(topic);
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Sanity Check: The total number of leader replicas of certain topics is sufficient so that these leader replicas
   * can be moved to satisfy this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<String> mustHaveTopicLeadersPerBroker = Collections.unmodifiableSet(
        Utils.getTopicNamesMatchedWithPattern(_balancingConstraint.topicsWithMinLeadersPerBrokerPattern(), clusterModel::topics));
    // populate min leaders per broker for each topic
    _mustHaveTopicMinLeadersPerBroker = new HashMap<>();
    _topicsToEnforce = Collections.emptySet();
    _topicsToHold = Collections.emptySet();
    if (mustHaveTopicLeadersPerBroker.isEmpty()) {
      return;
    }
    Map<String, Integer> numLeadersByTopicNames = clusterModel.numLeadersPerTopic(mustHaveTopicLeadersPerBroker);
    Set<Broker> eligibleBrokersForLeadership = eligibleBrokersForLeadership(clusterModel, optimizationOptions);

    for (String topicName : mustHaveTopicLeadersPerBroker) {
      int topicNumLeaders = numLeadersByTopicNames.get(topicName);
      _mustHaveTopicMinLeadersPerBroker.put(topicName,
              _balancingConstraint.minTopicLeadersPerBroker() == 0
                      ? eligibleBrokersForLeadership.isEmpty() ? 0 : topicNumLeaders / eligibleBrokersForLeadership.size()
                      : _balancingConstraint.minTopicLeadersPerBroker());
    }
    _topicsToHold = Collections.unmodifiableSet(topicsToHold(mustHaveTopicLeadersPerBroker, clusterModel, optimizationOptions));
    Set<String> topicsToEnforce = new HashSet<>(mustHaveTopicLeadersPerBroker);
    topicsToEnforce.removeAll(_topicsToHold);
    _topicsToEnforce = Collections.unmodifiableSet(topicsToEnforce);
    if (!_topicsToHold.isEmpty()) {
      LOG.info("[{}] Holding {} topic(s) that this optimization cannot move: their minimum leaders per broker is not enforced, but"
               + " not made worse either.", name(), _topicsToHold.size());
      LOG.debug("[{}] Held topics: {}.", name(), _topicsToHold);
    }
    // Sanity checks
    Map<String, Integer> numLeadersByTopicToEnforce = new HashMap<>(numLeadersByTopicNames);
    numLeadersByTopicToEnforce.keySet().retainAll(_topicsToEnforce);
    validateEnoughLeaderToDistribute(numLeadersByTopicToEnforce, eligibleBrokersForLeadership);
    validateBrokersAllowedReplicaMoveExist(clusterModel, optimizationOptions);
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(), onlyMoveImmigrantReplicas)
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnIncludedTopics(_topicsToEnforce))
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(), !onlyMoveImmigrantReplicas)
                              .trackSortedReplicasFor(_replicaSortName, clusterModel);
  }

  private int minTopicLeadersPerBroker(String topic) {
    return _mustHaveTopicMinLeadersPerBroker.get(topic);
  }

  /**
   * Get the topics with a minimum that this optimization cannot move. These are held: their minimum is not enforced, but an
   * action that would take a leader of such a topic away from a broker at or below the minimum is still rejected.
   * <ul>
   *   <li>Topics excluded from partition movement (by the request or by
   *   {@link AnalyzerConfig#TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG}).</li>
   *   <li>If only immigrant replicas can be moved, topics without an immigrant replica: e.g. topics whose replication factor a
   *   topic configuration request does not change.</li>
   * </ul>
   *
   * @param mustHaveTopics Topics that must have a minimum number of leaders per broker.
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   * @return Topics to hold.
   */
  private static Set<String> topicsToHold(Set<String> mustHaveTopics, ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Set<String> topicsToHold = new TreeSet<>();
    for (String topic : mustHaveTopics) {
      if (optimizationOptions.excludedTopics().contains(topic)) {
        topicsToHold.add(topic);
      }
    }
    if (optimizationOptions.onlyMoveImmigrantReplicas()) {
      Set<String> topicsWithImmigrants = new HashSet<>();
      clusterModel.brokers().forEach(broker -> broker.immigrantReplicas().forEach(r -> topicsWithImmigrants.add(r.topicPartition().topic())));
      for (String topic : mustHaveTopics) {
        if (!topicsWithImmigrants.contains(topic)) {
          topicsToHold.add(topic);
        }
      }
    }
    return topicsToHold;
  }

  private void validateEnoughLeaderToDistribute(Map<String, Integer> numLeadersByTopicNames, Set<Broker> eligibleBrokersForLeadership)
      throws OptimizationFailureException {
    for (Map.Entry<String, Integer> numLeadersPerTopic : numLeadersByTopicNames.entrySet()) {
      int totalMinimumLeaderCount = eligibleBrokersForLeadership.size() * minTopicLeadersPerBroker(numLeadersPerTopic.getKey());
      if (numLeadersPerTopic.getValue() < totalMinimumLeaderCount) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numPartitions(totalMinimumLeaderCount).topicPattern(Pattern.compile(numLeadersPerTopic.getKey())).build();
        throw new OptimizationFailureException(
            String.format("[%s] Cannot distribute %d leaders over %d broker(s) with minimum required per broker leader count %d for topic %s.",
                          name(), numLeadersPerTopic.getValue(), eligibleBrokersForLeadership.size(),
                          minTopicLeadersPerBroker(numLeadersPerTopic.getKey()), numLeadersPerTopic.getKey()), recommendation);
      }
    }
  }

  private void validateBrokersAllowedReplicaMoveExist(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<Integer> brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
    }
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return {@code true} if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Replica replicaToBeMoved = sourceBroker.replica(action.topicPartition());

    if (replicaToBeMoved.broker().replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }
    // Moving leader replica from more abundant broker is considered as self-satisfied
    String topicName = replicaToBeMoved.topicPartition().topic();
    return sourceBroker.numLeadersFor(topicName) > minTopicLeadersPerBroker(topicName);
  }

  private boolean actionAffectsRelevantTopics(BalancingAction action) {
    if (_mustHaveTopicMinLeadersPerBroker.containsKey(action.topic())) {
      return true;
    }
    return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP
            && _mustHaveTopicMinLeadersPerBroker.containsKey(action.destinationTopic());
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   *  @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    ensureBrokersAllHaveEnoughLeaderOfTopics(clusterModel, optimizationOptions);
    logMinimumsOutOfReach(clusterModel, optimizationOptions);
    finish();
  }

  /**
   * Log the brokers that stay below the minimum of a topic because this optimization cannot bring them to it: a held topic on an
   * eligible broker, or any topic on an eligible broker that may not receive replicas. This neither fails the optimization nor
   * marks this goal as violated.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  private void logMinimumsOutOfReach(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Map<String, SortedSet<Integer>> brokersBelowMinimumByTopic = new TreeMap<>();
    for (String topic : _mustHaveTopicMinLeadersPerBroker.keySet()) {
      boolean isHeld = _topicsToHold.contains(topic);
      for (Broker broker : clusterModel.aliveBrokers()) {
        if (isEligibleToHaveLeaders(broker, optimizationOptions)
            && (isHeld || !canReceiveReplicas(broker, optimizationOptions))
            && broker.numLeadersFor(topic) < minTopicLeadersPerBroker(topic)) {
          brokersBelowMinimumByTopic.computeIfAbsent(topic, t -> new TreeSet<>()).add(broker.id());
        }
      }
    }
    if (!brokersBelowMinimumByTopic.isEmpty()) {
      LOG.info("[{}] {} topic(s) stay below their minimum leaders per broker, because this optimization cannot move them or cannot"
               + " move replicas to these brokers (not a violation of this goal). Brokers below the minimum by topic{}: {}.", name(),
               brokersBelowMinimumByTopic.size(),
               brokersBelowMinimumByTopic.size() > MAX_TOPICS_TO_LOG ? String.format(" (first %d)", MAX_TOPICS_TO_LOG) : "",
               brokersBelowMinimumByTopic.entrySet().stream().limit(MAX_TOPICS_TO_LOG).map(Object::toString)
                                         .collect(Collectors.joining(", ")));
    }
  }

  private void ensureBrokersAllHaveEnoughLeaderOfTopics(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (_topicsToEnforce.isEmpty()) {
      // Early termination to avoid some unnecessary computation
      return;
    }
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!isEligibleToHaveLeaders(broker, optimizationOptions) || !canReceiveReplicas(broker, optimizationOptions)) {
        // A broker that may not receive replicas is held, not enforced (see logMinimumsOutOfReach).
        continue;
      }
      for (String mustHaveLeaderPerBrokerTopicName : _topicsToEnforce) {
        int leaderCount = broker.numLeadersFor(mustHaveLeaderPerBrokerTopicName);
        if (leaderCount < minTopicLeadersPerBroker(mustHaveLeaderPerBrokerTopicName)) {
          throw new OptimizationFailureException(String.format("[%s] Broker %d has insufficient per-broker leaders for topic %s (required: %d "
                                                               + "current: %d).", name(), broker.id(), mustHaveLeaderPerBrokerTopicName,
                                                               minTopicLeadersPerBroker(mustHaveLeaderPerBrokerTopicName), leaderCount));
        }
      }
    }
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    moveAwayOfflineReplicas(broker, clusterModel, optimizedGoals, optimizationOptions);
    if (_topicsToEnforce.isEmpty()) {
      // Early termination to avoid some unnecessary computation
      return;
    }
    if (!(broker.isAlive() && isEligibleToHaveLeaders(broker, optimizationOptions))) {
      return;
    }
    for (String topicMustHaveLeaderPerBroker : _topicsToEnforce) {
      maybeMoveLeaderOfTopicToBroker(topicMustHaveLeaderPerBroker, broker, clusterModel, optimizedGoals, optimizationOptions);
    }
  }

  private void maybeMoveLeaderOfTopicToBroker(String topicMustHaveLeaderPerBroker,
                                              Broker broker,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    int topicLeaderCountOnReceiverBroker = broker.numLeadersFor(topicMustHaveLeaderPerBroker);
    if (topicLeaderCountOnReceiverBroker >= minTopicLeadersPerBroker(topicMustHaveLeaderPerBroker)) {
      // This broker has enough leader replica(s) for the given topic
      return;
    }
    // Try to elect follower replica(s) of the interested topic on this broker to be leader
    List<Replica> followerReplicas = broker.trackedSortedReplicas(_replicaSortName)
                                           .sortedReplicas(false).stream().filter(
            replica -> !replica.isLeader()
                       && replica.topicPartition().topic().equals(topicMustHaveLeaderPerBroker)).toList();

    for (Replica followerReplica : followerReplicas) {
      Replica leader = clusterModel.partition(followerReplica.topicPartition()).leader();
      if (leader.broker().numLeadersFor(topicMustHaveLeaderPerBroker) > minTopicLeadersPerBroker(topicMustHaveLeaderPerBroker)) {
        if (maybeApplyBalancingAction(clusterModel, leader, Collections.singleton(broker),
                                      LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions) != null) {
          topicLeaderCountOnReceiverBroker++;
          if (topicLeaderCountOnReceiverBroker >= minTopicLeadersPerBroker(topicMustHaveLeaderPerBroker)) {
            // This broker satisfies this goal for the given topic
            return;
          }
        }
      }
    }

    if (!canReceiveReplicas(broker, optimizationOptions)) {
      // This broker may not receive replicas, so it is held with the leaders it has (see logMinimumsOutOfReach).
      return;
    }

    // Try to move leader replica(s) of the interested topic from other brokers to this broker
    PriorityQueue<Broker> brokersWithExcessiveLeaderToMove =
        getBrokersWithExcessiveLeaderToMove(topicMustHaveLeaderPerBroker, clusterModel, broker);

    while (!brokersWithExcessiveLeaderToMove.isEmpty()) {
      Broker brokerWithExcessiveLeaderToMove = brokersWithExcessiveLeaderToMove.poll();
      List<Replica> leadersOfTopic = brokerWithExcessiveLeaderToMove.trackedSortedReplicas(_replicaSortName)
                                                                    .sortedReplicas(false).stream()
                                                                    .filter(replica ->
                                                                                replica.isLeader()
                                                                                && replica.topicPartition()
                                                                                          .topic()
                                                                                          .equals(topicMustHaveLeaderPerBroker))
                                                                    .toList();
      boolean leaderMoved = false;
      int topicLeaderCountOnGiverBroker = leadersOfTopic.size();
      for (Replica leaderOfTopic : leadersOfTopic) {
        Broker destinationBroker = maybeApplyBalancingAction(clusterModel, leaderOfTopic, Collections.singleton(broker),
                                                             INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
        if (destinationBroker != null) {
          leaderMoved = true;
          // Successfully move one leader replica
          break;
        }
      }
      if (leaderMoved) {
        topicLeaderCountOnReceiverBroker++;
        if (topicLeaderCountOnReceiverBroker >= minTopicLeadersPerBroker(topicMustHaveLeaderPerBroker)) {
          // This broker satisfies this goal for the given topic
          return;
        }
        topicLeaderCountOnGiverBroker--;
        if (topicLeaderCountOnGiverBroker > minTopicLeadersPerBroker(topicMustHaveLeaderPerBroker)) {
          // Still have excessive topic leader to give
          brokersWithExcessiveLeaderToMove.add(brokerWithExcessiveLeaderToMove);
        }
      }
    }
    throw new OptimizationFailureException(String.format("[%s] Cannot make broker %d have at least %d leaders from topic %s.",
                                                         name(), broker.id(),
                                                         minTopicLeadersPerBroker(topicMustHaveLeaderPerBroker), topicMustHaveLeaderPerBroker));
  }

  /**
   * This method creates a priority queue which has the broker with the most number of leader replicas of a given topic at the top of queue
   * @param topicName name of the given topic
   * @param clusterModel cluster model
   * @param originalBroker original broker which needs to be excluded from the priority queue
   * @return a priority queue which has the broker with the most number of leader replicas of a given topic at the top of queue
   */
  private PriorityQueue<Broker> getBrokersWithExcessiveLeaderToMove(String topicName, ClusterModel clusterModel, Broker originalBroker) {
    PriorityQueue<Broker> brokersWithExcessiveLeaderToMove = new PriorityQueue<>((broker1, broker2) -> {
      int broker1LeaderCount = broker1.numLeadersFor(topicName);
      int broker2LeaderCount = broker2.numLeadersFor(topicName);
      int leaderCountCompareResult = Integer.compare(broker2LeaderCount, broker1LeaderCount);
      return leaderCountCompareResult == 0 ? Integer.compare(broker1.id(), broker2.id()) : leaderCountCompareResult;
    });
    clusterModel.aliveBrokers()
                .stream()
                .filter(broker -> broker.numLeadersFor(topicName) > minTopicLeadersPerBroker(topicName))
                .forEach(brokersWithExcessiveLeaderToMove::add);
    return brokersWithExcessiveLeaderToMove;
  }

  private static Set<Broker> eligibleBrokersForLeadership(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    return clusterModel.aliveBrokers()
                       .stream()
                       .filter(broker -> isEligibleToHaveLeaders(broker, optimizationOptions))
                       .collect(Collectors.toSet());
  }

  private static boolean isEligibleToHaveLeaders(Broker broker, OptimizationOptions optimizationOptions) {
    return !optimizationOptions.excludedBrokersForLeadership().contains(broker.id())
           && !optimizationOptions.excludedBrokersForReplicaMove().contains(broker.id());
  }

  /**
   * A broker that is eligible to have leaders but is not among the requested destination brokers (if any) may receive leadership,
   * but no replica.
   *
   * @param broker Broker to check.
   * @param optimizationOptions Options to take into account during optimization.
   * @return {@code true} if the optimization may move replicas to the given broker, {@code false} otherwise.
   */
  private static boolean canReceiveReplicas(Broker broker, OptimizationOptions optimizationOptions) {
    Set<Integer> requestedDestinationBrokerIds = optimizationOptions.requestedDestinationBrokerIds();
    return requestedDestinationBrokerIds.isEmpty() || requestedDestinationBrokerIds.contains(broker.id());
  }

  private void moveAwayOfflineReplicas(Broker srcBroker,
                                       ClusterModel clusterModel,
                                       Set<Goal> optimizedGoals,
                                       OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    if (srcBroker.currentOfflineReplicas().isEmpty()) {
      return;
    }
    SortedSet<Broker> eligibleBrokersToMoveOfflineReplicasTo = new TreeSet<>(
        Comparator.comparingInt((Broker broker) -> broker.replicas().size()).thenComparingInt(Broker::id));
    eligibleBrokersToMoveOfflineReplicasTo.addAll(clusterModel.aliveBrokers());
    Set<Replica> offlineReplicas = new HashSet<>(srcBroker.currentOfflineReplicas());
    for (Replica offlineReplica : offlineReplicas) {
      if (maybeApplyBalancingAction(clusterModel, offlineReplica, eligibleBrokersToMoveOfflineReplicasTo,
                                    INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build();
        throw new OptimizationFailureException(String.format("[%s] Cannot remove %s from %s broker %d (has %d replicas).", name(),
                                                             offlineReplica, srcBroker.state(), srcBroker.id(), srcBroker.replicas().size()),
                                               recommendation);
      }
    }
  }
}
