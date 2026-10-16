/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * SOFT GOAL: Generate proposals to keep the outbound network utilization on a broker such that even if all partitions
 * within the broker become the leader, the outbound network utilization would not exceed the corresponding broker
 * capacity threshold. This goal can be interpreted as keeping the potential outbound network utilization under a threshold.
 */
public class PotentialNwOutGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(PotentialNwOutGoal.class);

  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the potential outbound network limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;

  /**
   * Empty constructor for Potential Network Outbound Goal.
   */
  public PotentialNwOutGoal() {
  }

  /**
   * Package private for unit test.
   */
  PotentialNwOutGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  /**
   * Check whether given action is acceptable by this goal. Action is acceptable by this goal if it satisfies
   * either of the following:
   * (1) it is a leadership movement,
   * (2) it is an acceptable replica relocation (i.e. move or swap): {@link #isReplicaRelocationAcceptable(BalancingAction, ClusterModel)}
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    return switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT ->
        // it is a leadership movement,
        ACCEPT;
      case INTER_BROKER_REPLICA_SWAP, INTER_BROKER_REPLICA_MOVEMENT ->
        isReplicaRelocationAcceptable(action, clusterModel);
      default ->
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    };
  }

  /**
   * Check whether the given replica relocation (i.e. move or swap) is acceptable by this goal. Replica relocation is
   * acceptable if it satisfies either of the following:
   *
   * (1) it satisfies {@link #selfSatisfied},
   * (2) replica movement does not make the potential nw outbound goal on destination broker more than the source.
   * (3) replica swap does not make the potential nw outbound goal on source or destination broker more than the max of
   * the initial value on brokers.
   *
   * @param action Replica relocation action (i.e. move or swap) to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  private ActionAcceptance isReplicaRelocationAcceptable(BalancingAction action, ClusterModel clusterModel) {
    if (selfSatisfied(clusterModel, action)) {
      // it satisfies {@link #selfSatisfied},
      return ACCEPT;
    }

    Replica replica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    double destinationBrokerUtilization =
        clusterModel.potentialLeadershipLoadFor(clusterModel.broker(action.destinationBrokerId()).id()).expectedUtilizationFor(Resource.NW_OUT);
    double sourceBrokerUtilization = clusterModel.potentialLeadershipLoadFor(replica.broker().id()).expectedUtilizationFor(Resource.NW_OUT);
    double sourceReplicaUtilization = clusterModel.partition(replica.topicPartition()).leader().load().expectedUtilizationFor(Resource.NW_OUT);
    double maxUtilization = Math.max(destinationBrokerUtilization, sourceBrokerUtilization);

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        double destinationReplicaUtilization = clusterModel.partition(action.destinationTopicPartition())
            .leader().load().expectedUtilizationFor(Resource.NW_OUT);
        // Check source broker potential NW_OUT violation.
        if (sourceBrokerUtilization + destinationReplicaUtilization - sourceReplicaUtilization > maxUtilization) {
          return REPLICA_REJECT;
        }
        return destinationBrokerUtilization + sourceReplicaUtilization - destinationReplicaUtilization <= maxUtilization
               ? ACCEPT : REPLICA_REJECT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        return destinationBrokerUtilization + sourceReplicaUtilization <= maxUtilization ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new PotentialNwOutGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(Math.max(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING,
                                                      _numWindows / DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING),
                                             _minMonitoredPartitionPercentage, false);
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    // Balance the broken brokers (i.e. dead or has bad disks) if any, otherwise balance all brokers.
    SortedSet<Broker> brokenBrokers = clusterModel.brokersHavingOfflineReplicasOnBadDisks();
    if (!brokenBrokers.isEmpty()) {
      brokenBrokers.addAll(clusterModel.deadBrokers());
    } else {
      brokenBrokers = clusterModel.deadBrokers();
    }

    return brokenBrokers.isEmpty() ? clusterModel.brokers() : brokenBrokers;
  }

  /**
   * Check if the movement of potential outbound network utilization from the given source replica to given
   * destination broker is acceptable for this goal. The action is unacceptable if both of the following conditions
   * are met: (1) transfer of replica makes the potential network outbound utilization of the destination broker go
   * out of its allowed capacity, (2) broker containing the source replica is alive. For a swap action, this
   * consideration is bidirectional.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return {@code true} if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    ActionType actionType = action.balancingAction();
    Broker sourceBroker = sourceReplica.broker();
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly && sourceReplica.isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    double destinationBrokerUtilization = clusterModel.potentialLeadershipLoadFor(destinationBroker.id()).expectedUtilizationFor(Resource.NW_OUT);
    double destinationCapacity = destinationBroker.capacityFor(Resource.NW_OUT) * _balancingConstraint.capacityThreshold(Resource.NW_OUT);
    double sourceReplicaUtilization = clusterModel.partition(sourceReplica.topicPartition()).leader().load()
                                                  .expectedUtilizationFor(Resource.NW_OUT);

    if (actionType != ActionType.INTER_BROKER_REPLICA_SWAP) {
      // Check whether replica or leadership transfer leads to violation of capacity limit requirement.
      return destinationCapacity >= destinationBrokerUtilization + sourceReplicaUtilization;
    }

    // Ensure that the destination capacity of self-satisfied for action type swap is not violated.
    double destinationReplicaUtilization = clusterModel.partition(action.destinationTopicPartition()).leader().load()
                                                       .expectedUtilizationFor(Resource.NW_OUT);
    if (destinationCapacity < destinationBrokerUtilization + sourceReplicaUtilization - destinationReplicaUtilization) {
      // Destination capacity would be violated due to swap.
      return false;
    }

    // Ensure that the source capacity of self-satisfied for action type swap is not violated.
    double sourceBrokerUtilization = clusterModel.potentialLeadershipLoadFor(sourceBroker.id()).expectedUtilizationFor(Resource.NW_OUT);
    double sourceCapacity = sourceBroker.capacityFor(Resource.NW_OUT) * _balancingConstraint.capacityThreshold(Resource.NW_OUT);
    return sourceCapacity >= sourceBrokerUtilization + destinationReplicaUtilization - sourceReplicaUtilization;
  }

  /**
   * Set the flag which indicates whether the self healing failed to relocate all offline replicas away from dead brokers
   * or the corresponding dead disks in its initial attempt. Since self healing has not been executed yet, this flag is false.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    // While proposals exclude the excludedTopics, the potential nw_out still considers replicas of the excludedTopics.
    _fixOfflineReplicasOnly = false;

    // Filter out some replicas based on optimization options.
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.warn("Ignoring potential network outbound limit to move offline replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
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
    double capacityThreshold = _balancingConstraint.capacityThreshold(Resource.NW_OUT);
    double capacityLimit = broker.capacityFor(Resource.NW_OUT) * capacityThreshold;
    boolean estimatedMaxPossibleNwOutOverLimit = !broker.replicas().isEmpty()
                                                 && clusterModel.potentialLeadershipLoadFor(broker.id())
                                                                .expectedUtilizationFor(Resource.NW_OUT) > capacityLimit;
    if (!estimatedMaxPossibleNwOutOverLimit && !(_fixOfflineReplicasOnly && !broker.currentOfflineReplicas().isEmpty())) {
      // Estimated max possible utilization in broker is under the limit and there is no offline replica on broker.
      return;
    }
    // Get candidate brokers
    Set<Broker> candidateBrokers = _fixOfflineReplicasOnly ? clusterModel.aliveBrokers() : brokersUnderEstimatedMaxPossibleNwOut(clusterModel);
    // Attempt to move replicas to eligible brokers until either the estimated max possible network out
    // limit requirement is satisfied for the broker or all replicas are checked.
    for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
      // Find the eligible brokers that this replica is allowed to move. Unless the target broker would go
      // over the potential outbound network capacity the movement will be successful.
      List<Broker> eligibleBrokers = new ArrayList<>(candidateBrokers);
      eligibleBrokers.removeAll(clusterModel.partition(replica.topicPartition()).partitionBrokers());
      eligibleBrokers.sort((b1, b2) -> Double.compare(b2.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_OUT),
                                                      b1.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_OUT)));
      Broker destinationBroker =
          maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                    optimizedGoals, optimizationOptions);
      if (destinationBroker != null) {
        int destinationBrokerId = destinationBroker.id();
        // Check if broker capacity limit is satisfied now.
        estimatedMaxPossibleNwOutOverLimit = !broker.replicas().isEmpty() && clusterModel.potentialLeadershipLoadFor(broker.id())
                                                                                         .expectedUtilizationFor(Resource.NW_OUT) > capacityLimit;
        if (!estimatedMaxPossibleNwOutOverLimit && !(_fixOfflineReplicasOnly && !broker.currentOfflineReplicas().isEmpty())) {
          break;
        }

        if (!_fixOfflineReplicasOnly) {
          // Update brokersUnderEstimatedMaxPossibleNwOut (for destination broker).
          double updatedDestBrokerPotentialNwOut =
              clusterModel.potentialLeadershipLoadFor(destinationBrokerId).expectedUtilizationFor(Resource.NW_OUT);
          double destCapacityLimit = destinationBroker.capacityFor(Resource.NW_OUT) * capacityThreshold;
          if (updatedDestBrokerPotentialNwOut > destCapacityLimit) {
            candidateBrokers.remove(clusterModel.broker(destinationBrokerId));
          }
        }
      }
    }
    if (estimatedMaxPossibleNwOutOverLimit) {
      // Utilization is above the max possible limit after all replicas in the source broker were checked.
      LOG.warn("Violated estimated max possible network out limit for broker id:{} limit:{} utilization:{}.",
          broker.id(), capacityLimit, clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT));
      _succeeded = false;
    }
  }

  /**
   * Get a set of brokers that for which the utilization of the estimated maximum possible network outbound
   * utilization is under the outbound network capacity limit.
   *
   * @param clusterModel The state of the cluster.
   * @return A set of brokers that for which the utilization of the estimated maximum possible network outbound
   * utilization is under the outbound network capacity limit.
   */
  private Set<Broker> brokersUnderEstimatedMaxPossibleNwOut(ClusterModel clusterModel) {
    Set<Broker> brokersUnderEstimatedMaxPossibleNwOut = new HashSet<>();
    double capacityThreshold = _balancingConstraint.capacityThreshold(Resource.NW_OUT);

    for (Broker aliveBroker : clusterModel.aliveBrokers()) {
      double capacityLimit = aliveBroker.capacityFor(Resource.NW_OUT) * capacityThreshold;
      if (clusterModel.potentialLeadershipLoadFor(aliveBroker.id()).expectedUtilizationFor(Resource.NW_OUT) < capacityLimit) {
        brokersUnderEstimatedMaxPossibleNwOut.add(aliveBroker);
      }
    }
    return brokersUnderEstimatedMaxPossibleNwOut;
  }

  private class PotentialNwOutGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of brokers under potential nw out in the current cannot be more than the pre-optimized stats.
      int stat1 = stats1.numBrokersUnderPotentialNwOut();
      int stat2 = stats2.numBrokersUnderPotentialNwOut();
      int result = Integer.compare(stat1, stat2);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Number of brokers under potential NwOut] "
                                                     + "post-optimization:%d pre-optimization:%d", name(), stat1, stat2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }
}
