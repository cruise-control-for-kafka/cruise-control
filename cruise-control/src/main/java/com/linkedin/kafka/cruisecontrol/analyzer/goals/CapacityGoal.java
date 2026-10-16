/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.sortReplicasInAscendingOrderByBrokerResourceUtilization;


/**
 * HARD GOAL: Generate leadership and replica movement proposals to push the load on brokers and/or hosts under the
 * capacity limit.
 */
public abstract class CapacityGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(CapacityGoal.class);

  /**
   * Constructor for Capacity Goal.
   */
  public CapacityGoal() {

  }

  /**
   * Package private for unit test.
   */
  CapacityGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  protected abstract Resource resource();

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): Capacity.
   *
   * ## Leadership Movement: impacts only (1) network outbound and (2) CPU resources (See
   * {@link DiskCapacityGoal#actionAcceptance(BalancingAction, ClusterModel)} and
   * {@link NetworkInboundCapacityGoal#actionAcceptance(BalancingAction, ClusterModel)}).
   *   (1) Check if leadership NW_OUT movement is acceptable: NW_OUT movement carries all of leader's NW_OUT load.
   *   (2) Check if leadership CPU movement is acceptable: In reality, CPU movement carries only a fraction of
   * leader's CPU load.
   * To optimize CC performance, we avoid calculation of the expected leadership CPU utilization, and assume that
   * if (action.balancingAction() == ActionType.LEADERSHIP_MOVEMENT &amp;&amp; resource() == Resource.CPU),
   * then the expected leadership CPU utilization would be the full CPU utilization of the leader.
   * <p>
   * ## Replica Movement: impacts any resource.
   * ## Replica Swap: impacts any resource.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());

    return switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP -> {
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        yield isSwapAcceptableForCapacity(sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
      }
      case INTER_BROKER_REPLICA_MOVEMENT, LEADERSHIP_MOVEMENT ->
        isMovementAcceptableForCapacity(sourceReplica, destinationBroker) ? ACCEPT : REPLICA_REJECT;
      default ->
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    };
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, _minMonitoredPartitionPercentage, true);
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to broken broker replicas in case of self-healing.
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
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    // To optimize CC performance, we avoid calculation of the expected leadership CPU utilization, and assume that
    // if (action.balancingAction() == ActionType.LEADERSHIP_MOVEMENT && resource() == Resource.CPU),
    // then the expected leadership CPU utilization would be the full CPU utilization of the leader.
    return isMovementAcceptableForCapacity(sourceReplica, destinationBroker);
  }

  /**
   * Sanity checks: Existing total load on cluster is less than the limiting capacity
   * determined by the total capacity of alive cluster multiplied by the capacity threshold.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Sanity Check -- i.e. not enough resources.
    Load recentClusterLoad = clusterModel.load();

    // While proposals exclude the excludedTopics, the existingUtilization still considers replicas of the excludedTopics.
    double existingUtilization = recentClusterLoad.expectedUtilizationFor(resource());
    double capacity = clusterModel.capacityWithAllowedReplicaMovesFor(resource(), optimizationOptions);
    double allowedCapacity = capacity * _balancingConstraint.capacityThreshold(resource());

    if (allowedCapacity < existingUtilization) {
      Set<Integer> brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
      if (brokersAllowedReplicaMove.isEmpty()) {
        // Handle the case when all alive brokers are excluded from replica moves.
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numBrokers(clusterModel.maxReplicationFactor()).build();
        throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()), recommendation);
      }

      // Identify a typical broker capacity to be used in recommendations in case the cluster is under-provisioned.
      int typicalBrokerId = brokersAllowedReplicaMove.iterator().next();
      double typicalCapacity = clusterModel.broker(typicalBrokerId).capacityFor(resource());
      double missingCapacity = existingUtilization - allowedCapacity;
      int numBrokersToAdd = (int) Math.ceil(missingCapacity / (typicalCapacity * _balancingConstraint.capacityThreshold(resource())));

      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(numBrokersToAdd).typicalBrokerCapacity(typicalCapacity).typicalBrokerId(typicalBrokerId).resource(resource()).build();
      throw new OptimizationFailureException(
          String.format("[%s] Insufficient capacity for %s (Utilization %.2f, Allowed Capacity %.2f, Threshold: %.2f).", name(), resource(),
                        existingUtilization, allowedCapacity, _balancingConstraint.capacityThreshold(resource())), recommendation);
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    // Sort all replicas for each broker based on resource utilization.
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(), onlyMoveImmigrantReplicas)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeOfflineReplicas(),
                                                    !clusterModel.selfHealingEligibleReplicas().isEmpty())
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(), !onlyMoveImmigrantReplicas)
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(resource().name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);

    // Sort leader replicas for each broker based on resource utilization.
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(), onlyMoveImmigrantReplicas)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .maybeAddPriorityFunc(ReplicaSortFunctionFactory.prioritizeImmigrants(), !onlyMoveImmigrantReplicas)
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(resource().name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, true), clusterModel);
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing / self-healing the resource, confirm that the utilization is under
   * the capacity and finish.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // Ensure the resource utilization is under capacity limit.
    // While proposals exclude the excludedTopics, the utilization still considers replicas of the excludedTopics.
    ensureUtilizationUnderCapacity(clusterModel);
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  /**
   * Ensure that for the resource, the utilization is under the capacity of the host/broker-level.
   * {@link Resource#isBrokerResource()} and {@link Resource#isHostResource()} determines the level of checks this
   * function performs.
   * @param clusterModel Cluster model.
   */
  private void ensureUtilizationUnderCapacity(ClusterModel clusterModel) throws OptimizationFailureException {
    Resource resource = resource();
    double capacityThreshold = _balancingConstraint.capacityThreshold(resource);

    for (Broker broker : clusterModel.brokers()) {
      // Host-level violation check.
      if (resource.isHostResource()) {
        double utilization = broker.host().load().expectedUtilizationFor(resource);
        double capacityLimit = broker.host().capacityFor(resource) * capacityThreshold;

        if (!broker.host().replicas().isEmpty() && utilization > capacityLimit) {
          // The utilization of the host for the resource is over the capacity limit.
          ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
              .numBrokers(1).resource(resource()).build();
          throw new OptimizationFailureException(String.format("[%s] %s utilization for host %s (%.2f) is above capacity limit (%.2f).",
                                                               name(), resource, broker.host().name(), utilization, capacityLimit), recommendation);
        }
      }
      // Broker-level violation check.
      if (resource.isBrokerResource()) {
        double utilization = broker.load().expectedUtilizationFor(resource);
        double capacityLimit = broker.capacityFor(resource) * capacityThreshold;

        if (!broker.replicas().isEmpty() && utilization > capacityLimit) {
          // The utilization of the broker for the resource is over the capacity limit.
          ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
              .numBrokers(1).resource(resource()).build();
          throw new OptimizationFailureException(String.format("[%s] %s utilization for broker %d (%.2f) is above capacity limit (%.2f).",
                                                               name(), resource, broker.id(), utilization, capacityLimit), recommendation);
        }
      }
    }
  }

  /**
   * (1) REBALANCE BY LEADERSHIP MOVEMENT:
   * Perform leadership movement to ensure that the load on brokers and/or hosts (see {@link Resource#isHostResource()}
   * and {@link Resource#isBrokerResource()}) for the outbound network load and CPU is under the capacity limit.
   *
   * <p>
   * (2) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement for the given resource to ensure rebalance: The load on brokers and/or
   * hosts (see {@link Resource#isHostResource()} and {@link Resource#isBrokerResource()}) for the given resource is
   * under the capacity limit.
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
                                    OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    Resource currentResource = resource();
    double capacityThreshold = _balancingConstraint.capacityThreshold(currentResource);
    double brokerCapacityLimit = broker.capacityFor(currentResource) * capacityThreshold;
    double hostCapacityLimit = broker.host().capacityFor(currentResource) * capacityThreshold;

    boolean isUtilizationOverLimit =
        isUtilizationOverLimit(broker, currentResource, brokerCapacityLimit, hostCapacityLimit);
    if (!isUtilizationOverLimit && broker.currentOfflineReplicas().isEmpty()) {
      // (1) The utilization of source broker and/or host for the current resource is already under the capacity limit,
      // and (2) there are no offline replicas on the broker.
      return;
    }

    // First try REBALANCE BY LEADERSHIP MOVEMENT:
    if (currentResource == Resource.NW_OUT || currentResource == Resource.CPU) {
      // Sort replicas by descending order of preference to relocate. Preference is based on resource cost.
      // Only leaders in the source broker are sorted.
      // Note that if the replica is offline, it cannot currently be a leader.
      for (Replica leader : broker.trackedSortedReplicas(replicaSortName(this, true, true)).sortedReplicas(true)) {
        // Get online followers of this leader and sort them in ascending order by their broker resource utilization.
        List<Replica> onlineFollowers = clusterModel.partition(leader.topicPartition()).onlineFollowers();
        sortReplicasInAscendingOrderByBrokerResourceUtilization(onlineFollowers, currentResource);
        List<Broker> eligibleBrokers = onlineFollowers.stream().map(Replica::broker).toList();

        Broker b = maybeApplyBalancingAction(clusterModel, leader, eligibleBrokers,
                                             ActionType.LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions);
        if (b == null) {
          LOG.debug("Failed to move leader replica {} to any other brokers in {}", leader, eligibleBrokers);
        }
        isUtilizationOverLimit =
            isUtilizationOverLimit(broker, currentResource, brokerCapacityLimit, hostCapacityLimit);
        // Broker utilization has successfully been reduced under the capacity limit for the current resource.
        if (!isUtilizationOverLimit) {
          break;
        }
      }
    }

    // If leader movement did not work, move replicas.
    if (isUtilizationOverLimit || !broker.currentOfflineReplicas().isEmpty()) {
      // Get sorted alive brokers under host and/or broker capacity limit (depending on the current resource).
      List<Broker> sortedAliveBrokersUnderCapacityLimit =
          clusterModel.sortedAliveBrokersUnderThreshold(currentResource, capacityThreshold);

      // Move replicas that are sorted in descending order of preference to relocate (preference is based on
      // utilization) until the source broker utilization gets under the capacity limit. If the capacity limit cannot
      // be satisfied, throw an exception.
      for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true)) {
        // Unless the target broker would go over the host- and/or broker-level capacity,
        // the movement will be successful.
        Broker b = maybeApplyBalancingAction(clusterModel, replica, sortedAliveBrokersUnderCapacityLimit,
                                             ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
        if (b == null) {
          LOG.debug("Failed to move replica {} to any broker in {}", replica, sortedAliveBrokersUnderCapacityLimit);
        }
        // If capacity limit was not satisfied before, check if it is satisfied now.
        isUtilizationOverLimit =
            isUtilizationOverLimit(broker, currentResource, brokerCapacityLimit, hostCapacityLimit);
        // (1) Broker utilization must successfully be reduced under the capacity limit for the current resource.
        // and (2) there should be no offline replicas on the broker.
        if (!isUtilizationOverLimit && broker.currentOfflineReplicas().isEmpty()) {
          break;
        }
      }
    }

    // Ensure that the requirements of the capacity goal are satisfied after the balance.
    postSanityCheck(isUtilizationOverLimit, broker, brokerCapacityLimit, hostCapacityLimit);
  }

  private void postSanityCheck(boolean utilizationOverLimit, Broker broker, double brokerCapacityLimit, double hostCapacityLimit)
      throws OptimizationFailureException {
    // 1. Capacity violation check -- note that this check also ensures that no replica resides on dead brokers.
    if (utilizationOverLimit) {
      Resource currentResource = resource();
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(1).resource(currentResource).build();
      if (!currentResource.isHostResource()) {
        // Utilization is above the capacity limit after all replicas in the given source broker were checked.
        throw new OptimizationFailureException(String.format("[%s] Utilization (%.2f) of broker %d violated capacity limit (%.2f) for resource %s.",
                                                             name(), broker.load().expectedUtilizationFor(currentResource), broker.id(),
                                                             brokerCapacityLimit, currentResource), recommendation);
      } else {
        throw new OptimizationFailureException(String.format("[%s] Utilization (%.2f) of host %s violated capacity limit (%.2f) for resource %s.",
                                                             name(), broker.host().load().expectedUtilizationFor(currentResource),
                                                             broker.host().name(), hostCapacityLimit, currentResource), recommendation);
      }
    }
    // 2. Ensure that no offline replicas remain in the broker.
    if (!broker.currentOfflineReplicas().isEmpty()) {
      ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
          .numBrokers(1).resource(resource()).build();
      throw new OptimizationFailureException(String.format("[%s] Cannot remove offline replicas from broker %d.", name(), broker.id()),
                                             recommendation);
    }
  }

  /**
   * Check whether the combined replica utilization for the given resource within the given (1) broker and (2) the
   * corresponding host are above the given capacity limits.
   * See {@link Resource#isHostResource()} and {@link Resource#isBrokerResource()} to determine whether host- and/or
   * broker-level capacity is relevant for the given resource.
   *
   * @param broker Broker to be checked for capacity limit violation.
   * @param resource Resource to be checked for capacity limit violation.
   * @param brokerCapacityLimit Capacity limit for the broker.
   * @param hostCapacityLimit Capacity limit for the host.
   * @return {@code true} if utilization is over the limit, {@code false} otherwise.
   */
  private boolean isUtilizationOverLimit(Broker broker,
                                         Resource resource,
                                         double brokerCapacityLimit,
                                         double hostCapacityLimit) {
    // Host-level violation check.
    if (!broker.host().replicas().isEmpty() && resource.isHostResource()) {
      double utilization = broker.host().load().expectedUtilizationFor(resource);
      if (utilization > hostCapacityLimit) {
        // The utilization of the host for the resource is over the capacity limit.
        return true;
      }
    }
    // Broker-level violation check.
    if (!broker.replicas().isEmpty() && resource.isBrokerResource()) {
      double utilization = broker.load().expectedUtilizationFor(resource);
      return utilization > brokerCapacityLimit;
    }
    return false;
  }

  /**
   * Check whether the movement of utilization for the current resource from the given source replica to given
   * destination broker is acceptable for this goal.
   *
   * @param sourceReplica     Source replica.
   * @param destinationBroker Destination broker.
   * @return {@code true} if movement of utilization for the given resource from the given source replica to given
   * destination broker is acceptable for this goal, {@code false} otherwise.
   */
  private boolean isMovementAcceptableForCapacity(Replica sourceReplica, Broker destinationBroker) {
    // The action is unacceptable if the movement of replica or leadership makes the utilization of the destination
    // broker (or destination host for a host-resource) go out of alive capacity for the given resource.
    double replicaUtilization = sourceReplica.load().expectedUtilizationFor(resource());
    return isUtilizationUnderLimitAfterAddingLoad(destinationBroker, replicaUtilization);
  }

  /**
   * Check whether the swap for the current resource between source and destination replicas is acceptable for this goal.
   *
   * @param sourceReplica Source replica.
   * @param destinationReplica Destination replica.
   * @return {@code true} if the swap for the current resource between source and destination replicas is acceptable for this
   * goal, {@code false} otherwise
   */
  private boolean isSwapAcceptableForCapacity(Replica sourceReplica, Replica destinationReplica) {
    double sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(resource());
    double destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(resource());

    double sourceUtilizationDelta = destinationReplicaUtilization - sourceReplicaUtilization;
    return sourceUtilizationDelta > 0 ? isUtilizationUnderLimitAfterAddingLoad(sourceReplica.broker(), sourceUtilizationDelta)
                                      : isUtilizationUnderLimitAfterAddingLoad(destinationReplica.broker(),
                                                                                - sourceUtilizationDelta);
  }

  /**
   * Check whether the additional load on the destination makes the host (for host resources) or broker (for broker
   * resources) go out of the capacity limit.
   *
   * @param destinationBroker Destination broker.
   * @param replicaUtilization Replica utilization for the given resource.
   * @return {@code true} if utilization is equal or above the capacity limit, {@code false} otherwise.
   */
  private boolean isUtilizationUnderLimitAfterAddingLoad(Broker destinationBroker, double replicaUtilization) {
    Resource resource = resource();
    double capacityThreshold = _balancingConstraint.capacityThreshold(resource);

    // Host-level violation check.
    if (resource.isHostResource()) {
      double utilization = destinationBroker.host().load().expectedUtilizationFor(resource);
      double capacityLimit = destinationBroker.host().capacityFor(resource) * capacityThreshold;

      if (utilization + replicaUtilization >= capacityLimit) {
        // The utilization of the host for the resource is over the capacity limit.
        return false;
      }
    }
    // Broker-level violation check.
    if (resource.isBrokerResource()) {
      double utilization = destinationBroker.load().expectedUtilizationFor(resource);
      double capacityLimit = destinationBroker.capacityFor(resource) * capacityThreshold;

      return utilization + replicaUtilization < capacityLimit;
    }
    // Utilization would be under the limit after adding the load to the destination broker.
    return true;
  }
}
