/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.REMOVE;


/**
 * Soft goal to balance collocations of leader replicas of the same topic over alive brokers not excluded for replica moves.
 * <ul>
 * <li>Under: (the average number of topic leader replicas per broker) * (1 + topic leader replica count balance percentage)</li>
 * <li>Above: (the average number of topic leader replicas per broker) * Math.max(0, 1 - topic leader replica count balance percentage)</li>
 * </ul>
 *
 * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG
 * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG
 * @see #balancePercentageWithMargin(OptimizationOptions)
 *
 */
public class TopicLeaderReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(TopicLeaderReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the replica balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;

  private final Map<String, Set<Integer>> _brokerIdsAboveBalanceUpperLimitByTopic;
  private final Map<String, Set<Integer>> _brokerIdsUnderBalanceLowerLimitByTopic;
  // Must contain only the topics to be rebalanced.
  private final Map<String, Double> _avgTopicLeaderReplicasOnAliveBroker;
  // Must contain all topics to ensure that the lower priority goals work w/o an NPE.
  private final Map<String, Integer> _balanceUpperLimitByTopic;
  private final Map<String, Integer> _balanceLowerLimitByTopic;
  // This is used to identify brokers not excluded for replica moves.
  private Set<Integer> _brokersAllowedReplicaMove;

  /**
   * A soft goal to balance collocations of replicas of the same topic.
   */
  public TopicLeaderReplicaDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimitByTopic = new HashMap<>();
    _brokerIdsUnderBalanceLowerLimitByTopic = new HashMap<>();
    _avgTopicLeaderReplicasOnAliveBroker = new HashMap<>();
    _balanceUpperLimitByTopic = new HashMap<>();
    _balanceLowerLimitByTopic = new HashMap<>();
  }

  public TopicLeaderReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be {@link BalancingConstraint#topicLeaderReplicaBalancePercentage()}, we use
   * ({@link BalancingConstraint#topicLeaderReplicaBalancePercentage()}-1)*{@link #BALANCE_MARGIN} instead.
   *
   * @param optimizationOptions Options to adjust balance percentage with margin in case goal optimization is triggered
   * by goal violation detector.
   * @return The rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(OptimizationOptions optimizationOptions) {
    double balancePercentage = _balancingConstraint.topicLeaderReplicaBalancePercentage();
    if (optimizationOptions.isTriggeredByGoalViolation()) {
      balancePercentage *= _balancingConstraint.goalViolationDistributionThresholdMultiplier();
    }
    return (balancePercentage - 1) * BALANCE_MARGIN;
  }

  /**
   * Ensure that the given balance limit falls into min/max limits determined by min/max gaps for topic replica balance.
   * If the computed balance limit is out of these gap-based limits, use the relevant max/min gap-based balance limit.
   *
   * @param computedLimit Computed balance upper or lower limit
   * @param average Average topic replicas on broker.
   * @param isLowerLimit is determining lower limit.
   *
   * @return A balance limit that falls into [minGap, maxGap] for topic replica balance.
   * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_DOC
   * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_DOC
   */
  private int gapBasedBalanceLimit(int computedLimit, double average, boolean isLowerLimit) {
    int minLimit;
    int maxLimit;
    if (isLowerLimit) {
      maxLimit = Math.max(0, (int) (Math.floor(average) - _balancingConstraint.topicLeaderReplicaBalanceMinGap()));
      minLimit = Math.max(0, (int) (Math.floor(average) - _balancingConstraint.topicLeaderReplicaBalanceMaxGap()));
    } else {
      minLimit = (int) (Math.ceil(average) + _balancingConstraint.topicLeaderReplicaBalanceMinGap());
      maxLimit = (int) (Math.ceil(average) + _balancingConstraint.topicLeaderReplicaBalanceMaxGap());
    }
    return Math.max(minLimit, Math.min(computedLimit, maxLimit));
  }

  /**
   * @param topic Topic for which the upper limit is requested.
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The topic replica balance upper threshold in number of topic replicas.
   */
  private int balanceUpperLimit(String topic, OptimizationOptions optimizationOptions) {
    int computedUpperLimit = (int) Math.ceil(_avgTopicLeaderReplicasOnAliveBroker.get(topic)
                                             * (1 + balancePercentageWithMargin(optimizationOptions)));
    return gapBasedBalanceLimit(computedUpperLimit, _avgTopicLeaderReplicasOnAliveBroker.get(topic), false);
  }

  /**
   * @param topic Topic for which the lower limit is requested.
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The replica balance lower threshold in number of topic replicas.
   */
  private int balanceLowerLimit(String topic, OptimizationOptions optimizationOptions) {
    int computedLowerLimit = (int) Math.floor(_avgTopicLeaderReplicasOnAliveBroker.get(topic)
                                              * Math.max(0, (1 - balancePercentageWithMargin(optimizationOptions))));
    return gapBasedBalanceLimit(computedLowerLimit, _avgTopicLeaderReplicasOnAliveBroker.get(topic), true);
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of topic replicas at
   * (1) the source broker does not go under the allowed limit -- unless the source broker is excluded for replica moves.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();
    Replica sourceReplica = sourceBroker.replica(action.topicPartition());

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        String destinationTopic = action.destinationTopic();
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        if (sourceTopic.equals(destinationTopic) && sourceReplica.isLeader() && destinationReplica.isLeader()) {
          return ACCEPT;
        }
        if (!sourceReplica.isLeader() && !destinationReplica.isLeader()) {
          return ACCEPT;
        }
        if (sourceReplica.isLeader() && !destinationReplica.isLeader()) {
          return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker) ? ACCEPT : REPLICA_REJECT;
        }
        if (!sourceReplica.isLeader() && destinationReplica.isLeader()) {
          return isLeadershipGoalSatisfiable(destinationTopic, destinationBroker, sourceBroker) ? ACCEPT : REPLICA_REJECT;
        }
        // Both replicas are leaders but for different topics
        return (isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker)
                && isLeadershipGoalSatisfiable(destinationTopic, destinationBroker, sourceBroker)) ? ACCEPT : REPLICA_REJECT;
      case LEADERSHIP_MOVEMENT:
        return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker) ? ACCEPT : REPLICA_REJECT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        if (!sourceReplica.isLeader()) {
          return ACCEPT;
        }
        return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isLeadershipGoalSatisfiable(String sourceLeaderTopic, Broker sourceBroker, Broker destinationBroker) {
    return isTopicLeaderCountUnderBalanceUpperLimitAfterChange(sourceLeaderTopic, destinationBroker, ADD)
            && (isExcludedForReplicaMove(sourceBroker)
            || isTopicLeaderCountAboveBalanceLowerLimitAfterChange(sourceLeaderTopic, sourceBroker, REMOVE));
  }

  private boolean isTopicLeaderCountUnderBalanceUpperLimitAfterChange(String topic,
                                                                      Broker broker,
                                                                      ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicLeaders = broker.numLeadersFor(topic);
    int brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicLeaders + 1 <= brokerBalanceUpperLimit : numTopicLeaders - 1 <= brokerBalanceUpperLimit;
  }

  private boolean isTopicLeaderCountAboveBalanceLowerLimitAfterChange(String topic,
                                                                      Broker broker,
                                                                      ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicLeaders = broker.numLeadersFor(topic);
    int brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicLeaders + 1 >= brokerBalanceLowerLimit : numTopicLeaders - 1 >= brokerBalanceLowerLimit;
  }

  /**
   * Check whether the given broker is excluded for replica moves.
   * Such a broker cannot receive replicas, but can give them away.
   *
   * @param broker Broker to check for exclusion from replica moves.
   * @return {@code true} if the given broker is excluded for replica moves, {@code false} otherwise.
   */
  private boolean isExcludedForReplicaMove(Broker broker) {
    return !_brokersAllowedReplicaMove.contains(broker.id());
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public String name() {
    return TopicLeaderReplicaDistributionGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  /**
   * Initiates this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<String> topicsToRebalance = GoalUtils.topicsToRebalance(clusterModel, excludedTopics);
    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    _brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (_brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
    }
    // Initialize the average replicas on an alive broker.
    final Map<String, Integer> numTopicLeadersMap = clusterModel.numLeadersPerTopic(clusterModel.topics());
    for (String topic : clusterModel.topics()) {
      int numTopicLeaders = numTopicLeadersMap.get(topic);
      _avgTopicLeaderReplicasOnAliveBroker.put(topic, (numTopicLeaders / (double) _brokersAllowedReplicaMove.size()));
      _balanceUpperLimitByTopic.put(topic, balanceUpperLimit(topic, optimizationOptions));
      _balanceLowerLimitByTopic.put(topic, balanceLowerLimit(topic, optimizationOptions));
      // Retain only the topics to rebalance in _avgTopicReplicasOnAliveBroker
      if (!topicsToRebalance.contains(topic)) {
        _avgTopicLeaderReplicasOnAliveBroker.remove(topic);
      }
    }
    // Filter out replicas to be considered for replica movement.
    for (Broker broker : clusterModel.brokers()) {
      new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                       optimizationOptions.onlyMoveImmigrantReplicas())
                                .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrantOrOfflineReplicas(),
                                                       !clusterModel.selfHealingEligibleReplicas().isEmpty() && broker.isAlive())
                                .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                                .addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                                .trackSortedReplicasFor(replicaSortName(this, false, true), broker);
    }

    _fixOfflineReplicasOnly = false;
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model. Assumed to be
   * of type {@link ActionType#INTER_BROKER_REPLICA_MOVEMENT}.
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly && sourceBroker.replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }

    //Check that destination and source would not become unbalanced.
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker);
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (!_brokerIdsAboveBalanceUpperLimitByTopic.isEmpty()) {
      _brokerIdsAboveBalanceUpperLimitByTopic.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimitByTopic.isEmpty()) {
      _brokerIdsUnderBalanceLowerLimitByTopic.clear();
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.info("Ignoring topic replica balance limit to move replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  private static boolean skipBrokerRebalance(Broker broker,
                                             ClusterModel clusterModel,
                                             Collection<Replica> leaderReplicas,
                                             boolean requireLessReplicas,
                                             boolean requireMoreReplicas,
                                             boolean hasOfflineTopicReplicas,
                                             boolean moveImmigrantReplicaOnly) {
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Broker {} is already within the limit for replicas {}.", broker, leaderReplicas);
      return true;
    } else if (!clusterModel.newBrokers().isEmpty() && !broker.isNew() && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Cluster has new brokers and this broker {} is not new, but does not require less load "
                + "for replicas {}. Hence, it does not have any offline replicas.", broker, leaderReplicas);
      return true;
    }
    boolean hasImmigrantTopicReplicas = leaderReplicas.stream()
            .anyMatch(replica -> replica.isLeader() && broker.immigrantReplicas().contains(replica));
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && requireLessReplicas
               && !hasOfflineTopicReplicas && !hasImmigrantTopicReplicas) {
      LOG.trace("Skip rebalance: Cluster is in self-healing mode and the broker {} requires less load, but none of its "
                + "current offline or immigrant replicas are from the topic being balanced {}.", broker, leaderReplicas);
      return true;
    } else if (moveImmigrantReplicaOnly && requireLessReplicas && !hasImmigrantTopicReplicas) {
      LOG.trace("Skip rebalance: Only immigrant replicas can be moved, but none of broker {}'s "
                + "current immigrant replicas are from the topic being balanced {}.", broker, leaderReplicas);
      return true;
    }

    return false;
  }

  private boolean isTopicExcludedFromRebalance(String topic) {
    return _avgTopicLeaderReplicasOnAliveBroker.get(topic) == null;
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimitByTopic,
              _balanceUpperLimitByTopic);

    for (String topic : broker.topics()) {
      if (isTopicExcludedFromRebalance(topic)) {
        continue;
      }

      Collection<Replica> leaderReplicas = leadersOfTopicInBroker(broker, topic);
      long numTopicLeaders = leaderReplicas.size();
      int numOfflineTopicLeaders = GoalUtils.retainCurrentOfflineBrokerReplicas(broker, leaderReplicas).size();
      boolean isExcludedForReplicaMove = isExcludedForReplicaMove(broker);

      boolean requireLessLeaders = numOfflineTopicLeaders > 0 || numTopicLeaders > _balanceUpperLimitByTopic.get(topic)
                                    || isExcludedForReplicaMove;
      boolean requireMoreLeaders = !isExcludedForReplicaMove && broker.isAlive()
                                    && numTopicLeaders - numOfflineTopicLeaders < _balanceLowerLimitByTopic.get(topic);

      if (skipBrokerRebalance(broker, clusterModel, leaderReplicas, requireLessLeaders, requireMoreLeaders, numOfflineTopicLeaders > 0,
                              optimizationOptions.onlyMoveImmigrantReplicas())) {
        continue;
      }

      // Update broker ids over the balance limit for logging purposes.
      if (requireLessLeaders && rebalanceByMovingLeadersOut(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        _brokerIdsAboveBalanceUpperLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently decrease leaders of topic {} in broker {} with replica movements. Replicas: {}.",
                  topic, broker.id(), broker.numLeadersFor(topic));
      }
      if (requireMoreLeaders && rebalanceByMovingLeadersIn(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        _brokerIdsUnderBalanceLowerLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently increase leaders of topic {} in broker {} with replica movements. Replicas: {}.",
                  topic, broker.id(), broker.numLeadersFor(topic));
      }
      if (!_brokerIdsAboveBalanceUpperLimitByTopic.getOrDefault(topic, Collections.emptySet()).contains(broker.id())
          && !_brokerIdsUnderBalanceLowerLimitByTopic.getOrDefault(topic, Collections.emptySet()).contains(broker.id())) {
        LOG.debug("Successfully balanced leaders of topic {} in broker {} by moving replicas. Replicas: {}",
                  topic, broker.id(), broker.numLeadersFor(topic));
      }
    }
  }

  private Set<Replica> leadersOfTopicInBroker(Broker broker, String topic) {
    return broker.replicasOfTopicInBroker(topic).stream().filter(Replica::isLeader).collect(Collectors.toSet());
  }

  private SortedSet<Replica> replicasToMoveOut(Broker broker, String topic) {
    SortedSet<Replica> replicasToMoveOut = new TreeSet<>(broker.replicaComparator());
    replicasToMoveOut.addAll(leadersOfTopicInBroker(broker, topic));
    replicasToMoveOut.retainAll(broker.trackedSortedReplicas(replicaSortName(this, false, true)).sortedReplicas(false));
    return replicasToMoveOut;
  }

  private boolean rebalanceByMovingLeadersOut(Broker broker,
                                              String topic,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              OptimizationOptions optimizationOptions) {
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingInt((Broker b) -> b.numLeadersFor(topic))
                .thenComparingInt(b -> b.leaderReplicas().size())
                .thenComparingInt(Broker::id));

    candidateBrokers.addAll(_fixOfflineReplicasOnly ? clusterModel.aliveBrokers() : clusterModel
        .aliveBrokers()
        .stream()
        .filter(b -> b.numLeadersFor(topic) < _balanceUpperLimitByTopic.get(topic))
        .collect(Collectors.toSet()));

    Collection<Replica> leadersOfTopicInBroker = leadersOfTopicInBroker(broker, topic);
    int numLeadersOfTopicInBroker = leadersOfTopicInBroker.size();
    int numOfflineTopicReplicas = GoalUtils.retainCurrentOfflineBrokerReplicas(broker, leadersOfTopicInBroker).size();
    // If the source broker is excluded for replica move, set its upper limit to 0.
    int balanceUpperLimitForSourceBroker = isExcludedForReplicaMove(broker) ? 0 : _balanceUpperLimitByTopic.get(topic);

    boolean wasUnableToMoveOfflineReplica = false;
    for (Replica replica : replicasToMoveOut(broker, topic)) {
      if (wasUnableToMoveOfflineReplica && !replica.isCurrentOffline() && numLeadersOfTopicInBroker <= balanceUpperLimitForSourceBroker) {
        // Was unable to move offline replicas from the broker, and remaining replica count is under the balance limit.
        return false;
      }

      boolean wasOffline = replica.isCurrentOffline();
      Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
              optimizedGoals, optimizationOptions);
      if (b == null) {
        b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, ActionType.LEADERSHIP_MOVEMENT,
                optimizedGoals, optimizationOptions);
      }
      // Only check if we successfully moved something.
      if (b != null) {
        if (wasOffline) {
          numOfflineTopicReplicas--;
        }
        if (--numLeadersOfTopicInBroker <= (numOfflineTopicReplicas == 0 ? balanceUpperLimitForSourceBroker : 0)) {
          return false;
        }

        // Remove and reinsert the broker so the order is correct.
        // Because a TreeSet is used here, and lookups are by comparator first, I'm seeing failed deletes
        final int brokerId = b.id();
        boolean isRemoved = candidateBrokers.removeIf(cb -> cb.id() == brokerId);
        LOG.info("result of removing broker {} from candidateBrokers = {}", b.id(), isRemoved);
        if (b.numLeadersFor(topic) < _balanceUpperLimitByTopic.get(topic) || _fixOfflineReplicasOnly) {
          candidateBrokers.add(b);
        }
      } else if (wasOffline) {
        wasUnableToMoveOfflineReplica = true;
      }
    }
    // All the topic replicas has been moved away from the broker.
    return !leadersOfTopicInBroker(broker, topic).isEmpty();
  }

  private boolean rebalanceByMovingLeadersIn(Broker aliveDestBroker,
                                             String topic,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             OptimizationOptions optimizationOptions) {
    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      // Brokers are sorted by (1) current offline topic leader count then
      // (2) all topic leaders count then (3) all leaders count then (4) broker id.

      // B2 Info
      Collection<Replica> leadersOfTopicInB2 = leadersOfTopicInBroker(b2, topic);
      int numLeadersOfTopicInB2 = leadersOfTopicInB2.size();
      int numOfflineTopicReplicasInB2 = GoalUtils.retainCurrentOfflineBrokerReplicas(b2, leadersOfTopicInB2).size();
      // B1 Info
      Collection<Replica> leadersOfTopicInB1 = leadersOfTopicInBroker(b1, topic);
      int numLeadersOfTopicInB1 = leadersOfTopicInB1.size();
      int numOfflineTopicReplicasInB1 = GoalUtils.retainCurrentOfflineBrokerReplicas(b1, leadersOfTopicInB1).size();

      int resultByOfflineLeaders = Integer.compare(numOfflineTopicReplicasInB2, numOfflineTopicReplicasInB1);
      if (resultByOfflineLeaders == 0) {
        int resultByTopicLeaders = Integer.compare(numLeadersOfTopicInB2, numLeadersOfTopicInB1);
        if (resultByTopicLeaders == 0) {
          int resultByAllLeaders = Integer.compare(b2.leaderReplicas().size(), b1.leaderReplicas().size());
          return resultByAllLeaders == 0 ? Integer.compare(b2.id(), b1.id()) : resultByAllLeaders;
        } else {
          return resultByTopicLeaders;
        }
      }
      return resultByOfflineLeaders;
    });

    // Source broker can be dead, alive, or may have bad disks.
    if (_fixOfflineReplicasOnly) {
      clusterModel.brokers().stream().filter(sourceBroker -> sourceBroker.id() != aliveDestBroker.id())
                  .forEach(eligibleBrokers::add);
    } else {
      for (Broker sourceBroker : clusterModel.brokers()) {
        if (sourceBroker.numLeadersFor(topic) > _balanceLowerLimitByTopic.get(topic)
            || !sourceBroker.currentOfflineReplicas().isEmpty() || isExcludedForReplicaMove(sourceBroker)) {
          eligibleBrokers.add(sourceBroker);
        }
      }
    }

    Collection<Replica> leadersOfTopicInBroker = leadersOfTopicInBroker(aliveDestBroker, topic);
    int numLeadersOfTopicInBroker = leadersOfTopicInBroker.size();

    Set<Broker> candidateBrokers = Collections.singleton(aliveDestBroker);

    // Stop when no topic replicas can be moved in anymore.
    while (!eligibleBrokers.isEmpty()) {
      Broker sourceBroker = eligibleBrokers.poll();
      SortedSet<Replica> replicasToMove = replicasToMoveOut(sourceBroker, topic);
      int numOfflineTopicReplicas = GoalUtils.retainCurrentOfflineBrokerReplicas(sourceBroker, replicasToMove).size();

      for (Replica replica : replicasToMove) {
        boolean wasOffline = replica.isCurrentOffline();
        ActionType action = (aliveDestBroker.replica(replica.topicPartition()) == null)
                ? ActionType.INTER_BROKER_REPLICA_MOVEMENT : ActionType.LEADERSHIP_MOVEMENT;
        Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, action,
                  optimizedGoals, optimizationOptions);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (wasOffline) {
            numOfflineTopicReplicas--;
          }
          if (++numLeadersOfTopicInBroker >= _balanceLowerLimitByTopic.get(topic)) {
            // Note that the broker passed to this method is always alive; hence, there is no need to check if it is dead.
            return false;
          }
          // If the source broker has no offline replicas and a lower number of topic replicas than the next broker in
          // the eligible broker in the queue, we reenqueue the source broker and switch to the next broker.
          if (!eligibleBrokers.isEmpty() && numOfflineTopicReplicas == 0
              && sourceBroker.numLeadersFor(topic) < eligibleBrokers.peek().numLeadersFor(topic)) {
            eligibleBrokers.add(sourceBroker);
            break;
          }
        }
      }
    }
    return true;
  }
}
