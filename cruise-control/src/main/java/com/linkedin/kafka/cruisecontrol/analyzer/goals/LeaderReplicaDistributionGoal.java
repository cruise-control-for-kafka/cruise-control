/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.remainingTimeMs;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.*;

/**
 * SOFT GOAL: Generate leadership movement and leader replica movement proposals to ensure that the number of leader replicas
 * on each broker is
 * <ul>
 * <li>Under: (the average number of leader replicas per broker) * (1 + leader replica count balance percentage)</li>
 * <li>Above: (the average number of leader replicas per broker) * Math.max(0, 1 - leader replica count balance percentage)</li>
 * </ul>
 */
public class LeaderReplicaDistributionGoal extends ReplicaDistributionAbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderReplicaDistributionGoal.class);

  /**
   * Constructor for Leader Replica Distribution Goal.
   */
  public LeaderReplicaDistributionGoal() {

  }

  /**
   * Package private for unit test.
   * @param balancingConstraint Balancing constraint.
   */
  public LeaderReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  @Override
  int numInterestedReplicas(ClusterModel clusterModel) {
    return clusterModel.numLeaderReplicas();
  }

  /**
   * The rebalance threshold for this goal is set by
   * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG}
   */
  @Override
  double balancePercentage() {
    return _balancingConstraint.leaderReplicaBalancePercentage();
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of leader replicas at
   * (1) the broker which gives away leader replica does not go under the allowed limit -- unless it is excluded for replica moves.
   * (2) the broker which receives leader replica does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Replica sourceReplica = sourceBroker.replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        if (sourceReplica.isLeader() && !destinationReplica.isLeader()) {
          return isLeaderMovementSatisfiable(sourceBroker, destinationBroker);
        } else if (!sourceReplica.isLeader() && destinationReplica.isLeader()) {
          return isLeaderMovementSatisfiable(destinationBroker, sourceBroker);
        }
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        if (sourceReplica.isLeader()) {
          return isLeaderMovementSatisfiable(sourceBroker, destinationBroker);
        }
        return ACCEPT;
      case LEADERSHIP_MOVEMENT:
        return isLeaderMovementSatisfiable(sourceBroker, destinationBroker);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private ActionAcceptance isLeaderMovementSatisfiable(Broker sourceBroker, Broker destinationBroker) {
    return (isReplicaCountUnderBalanceUpperLimitAfterChange(destinationBroker, destinationBroker.leaderReplicas().size(), ADD)
           && (isExcludedForReplicaMove(sourceBroker)
               || isReplicaCountAboveBalanceLowerLimitAfterChange(sourceBroker, sourceBroker.leaderReplicas().size(), REMOVE)))
           ? ACCEPT : REPLICA_REJECT;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new LeaderReplicaDistributionGoalStatsComparator();
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
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimit, _balanceUpperLimit);
    int numLeaderReplicas = broker.leaderReplicas().size();
    boolean isExcludedForReplicaMove = isExcludedForReplicaMove(broker);
    boolean requireLessLeaderReplicas = broker.isAlive() && numLeaderReplicas > (isExcludedForReplicaMove ? 0 : _balanceUpperLimit);
    boolean requireMoreLeaderReplicas = !isExcludedForReplicaMove && broker.isAlive() && numLeaderReplicas < _balanceLowerLimit;
    boolean requireLessReplicas = _fixOfflineReplicasOnly && !broker.currentOfflineReplicas().isEmpty();
    // Update broker ids over the balance limit for logging purposes.
    if (((requireLessLeaderReplicas
        && rebalanceByMovingLeadershipOut(broker, clusterModel, optimizedGoals, optimizationOptions))
        || requireLessReplicas)
        && rebalanceByMovingReplicasOut(broker, clusterModel, optimizedGoals, optimizationOptions)) {
      if (!requireLessReplicas) {
        _brokerIdsAboveBalanceUpperLimit.add(broker.id());
        LOG.debug("Failed to sufficiently decrease leader replica count in broker {}. Leader replicas: {}.",
                  broker.id(), broker.leaderReplicas().size());
      }
    } else if (requireMoreLeaderReplicas
               && rebalanceByMovingLeadershipIn(broker, clusterModel, optimizedGoals, optimizationOptions)
               && rebalanceByMovingLeaderReplicasIn(broker, clusterModel, optimizedGoals, optimizationOptions)) {
      _brokerIdsUnderBalanceLowerLimit.add(broker.id());
      LOG.debug("Failed to sufficiently increase leader replica count in broker {}. Leader replicas: {}.",
                broker.id(), broker.leaderReplicas().size());
    }
  }

  private boolean rebalanceByMovingLeadershipOut(Broker broker,
                                                 ClusterModel clusterModel,
                                                 Set<Goal> optimizedGoals,
                                                 OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    if (!clusterModel.deadBrokers().isEmpty()) {
      return true;
    }
    // If the source broker is excluded for replica move, set its upper limit to 0.
    int balanceUpperLimitForSourceBroker = isExcludedForReplicaMove(broker) ? 0 : _balanceUpperLimit;
    int numLeaderReplicas = broker.leaderReplicas().size();
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    boolean fastMode = optimizationOptions.fastMode();
    for (Replica leader : new HashSet<>(broker.leaderReplicas())) {
      if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
        LOG.debug("Move leadership out timeout in fast mode for broker {}.", broker.id());
        break;
      }
      if (excludedTopics.contains(leader.topicPartition().topic())) {
        continue;
      }

      Set<Broker> candidateBrokers = clusterModel.partition(leader.topicPartition()).partitionBrokers().stream()
                                                 .filter(b -> b != broker && !b.replica(leader.topicPartition()).isCurrentOffline())
                                                 .collect(Collectors.toSet());
      Broker b = maybeApplyBalancingAction(clusterModel,
                                           leader,
                                           candidateBrokers,
                                           ActionType.LEADERSHIP_MOVEMENT,
                                           optimizedGoals,
                                           optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (--numLeaderReplicas <= balanceUpperLimitForSourceBroker) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean rebalanceByMovingLeadershipIn(Broker broker,
                                                ClusterModel clusterModel,
                                                Set<Goal> optimizedGoals,
                                                OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    if (!clusterModel.deadBrokers().isEmpty() || optimizationOptions.excludedBrokersForLeadership().contains(broker.id())) {
      return true;
    }

    int numLeaderReplicas = broker.leaderReplicas().size();
    Set<Broker> candidateBrokers = Collections.singleton(broker);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    boolean fastMode = optimizationOptions.fastMode();
    for (Replica replica : broker.replicas()) {
      if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
        LOG.debug("Move leadership in timeout in fast mode for broker {}.", broker.id());
        break;
      }
      if (replica.isLeader() || replica.isCurrentOffline() || excludedTopics.contains(replica.topicPartition().topic())) {
        continue;
      }
      Broker b = maybeApplyBalancingAction(clusterModel,
                                           clusterModel.partition(replica.topicPartition()).leader(),
                                           candidateBrokers,
                                           ActionType.LEADERSHIP_MOVEMENT,
                                           optimizedGoals,
                                           optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (++numLeaderReplicas >= _balanceLowerLimit) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean rebalanceByMovingReplicasOut(Broker broker,
                                               ClusterModel clusterModel,
                                               Set<Goal> optimizedGoals,
                                               OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers;
    if (_fixOfflineReplicasOnly) {
      candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.replicas().size())
                                                 .thenComparingInt(Broker::id));
      candidateBrokers.addAll(clusterModel.aliveBrokers());
    } else {
      candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.leaderReplicas().size())
                                                 .thenComparingInt(Broker::id));
      candidateBrokers.addAll(clusterModel.aliveBrokers()
                      .stream()
                      .filter(b -> b.leaderReplicas().size() < _balanceUpperLimit)
                      .collect(Collectors.toSet()));
    }

    int balanceUpperLimit = _fixOfflineReplicasOnly ? 0 : _balanceUpperLimit;
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    String replicaSortName = replicaSortName(this, false, !_fixOfflineReplicasOnly);
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectLeaders(), !_fixOfflineReplicasOnly)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectOfflineReplicas(), _fixOfflineReplicasOnly)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     (!_fixOfflineReplicasOnly && !clusterModel.selfHealingEligibleReplicas().isEmpty())
                                                     || optimizationOptions.onlyMoveImmigrantReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .trackSortedReplicasFor(replicaSortName, broker);
    SortedSet<Replica> candidateReplicas = broker.trackedSortedReplicas(replicaSortName).sortedReplicas(true);
    int numReplicas = candidateReplicas.size();
    boolean fastMode = optimizationOptions.fastMode();
    for (Replica replica : candidateReplicas) {
      if (fastMode && !replica.isCurrentOffline() && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
        LOG.debug("Move replicas out timeout in fast mode for broker {}.", broker.id());
        break;
      }
      Broker b = maybeApplyBalancingAction(clusterModel,
                                           replica,
                                           candidateBrokers,
                                           ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                           optimizedGoals,
                                           optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (--numReplicas <= balanceUpperLimit) {
          broker.untrackSortedReplicas(replicaSortName);
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (b.leaderReplicas().size() < _balanceUpperLimit || _fixOfflineReplicasOnly) {
          candidateBrokers.add(b);
        }
      }
    }
    broker.untrackSortedReplicas(replicaSortName);
    return true;
  }

  private boolean rebalanceByMovingLeaderReplicasIn(Broker broker,
                                                    ClusterModel clusterModel,
                                                    Set<Goal> optimizedGoals,
                                                    OptimizationOptions optimizationOptions) {
    long moveStartTimeMs = System.currentTimeMillis();
    if (optimizationOptions.excludedBrokersForLeadership().contains(broker.id())) {
      return true;
    }

    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      int result = Integer.compare(b2.leaderReplicas().size(), b1.leaderReplicas().size());
      return result == 0 ? Integer.compare(b1.id(), b2.id()) : result;
    });

    for (Broker aliveBroker : clusterModel.aliveBrokers()) {
      if (aliveBroker.leaderReplicas().size() > _balanceLowerLimit) {
        eligibleBrokers.add(aliveBroker);
      }
    }
    List<Broker> candidateBrokers = Collections.singletonList(broker);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    String replicaSortName = replicaSortName(this, false, true);
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     !clusterModel.brokenBrokers().isEmpty() || onlyMoveImmigrantReplicas)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .trackSortedReplicasFor(replicaSortName, clusterModel);
    int numLeaderReplicas = broker.leaderReplicas().size();
    boolean fastMode = optimizationOptions.fastMode();
    while (!eligibleBrokers.isEmpty()) {
      if (fastMode && remainingTimeMs(_balancingConstraint.fastModePerBrokerMoveTimeoutMs(), moveStartTimeMs) <= 0) {
        LOG.debug("Move leaders in timeout in fast mode for broker {}.", broker.id());
        break;
      }
      Broker sourceBroker = eligibleBrokers.poll();
      for (Replica replica : sourceBroker.trackedSortedReplicas(replicaSortName).sortedReplicas(true)) {
        Broker b = maybeApplyBalancingAction(clusterModel,
                                             replica,
                                             candidateBrokers,
                                             ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                             optimizedGoals,
                                             optimizationOptions);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (++numLeaderReplicas >= _balanceLowerLimit) {
            clusterModel.untrackSortedReplicas(replicaSortName);
            return false;
          }
          // If the source broker has a lower number of leader replicas than the next broker in the eligible broker
          // queue, we reenqueue the source broker and switch to the next broker.
          if (!eligibleBrokers.isEmpty() && sourceBroker.leaderReplicas().size() < eligibleBrokers.peek().leaderReplicas().size()) {
            eligibleBrokers.add(sourceBroker);
            break;
          }
        }
      }
    }
    clusterModel.untrackSortedReplicas(replicaSortName);
    return true;
  }

  private class LeaderReplicaDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;
    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Standard deviation of number of leader replicas over alive brokers in the current must be less than the pre-optimized stats.
      double stDev1 = stats1.leaderReplicaStats().get(Statistic.ST_DEV).doubleValue();
      double stDev2 = stats2.leaderReplicaStats().get(Statistic.ST_DEV).doubleValue();
      int result = AnalyzerUtils.compare(stDev2, stDev1, AnalyzerUtils.EPSILON);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Leader Replica Distribution] post-"
                                                     + "optimization:%.3f pre-optimization:%.3f", name(), stDev1, stDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
