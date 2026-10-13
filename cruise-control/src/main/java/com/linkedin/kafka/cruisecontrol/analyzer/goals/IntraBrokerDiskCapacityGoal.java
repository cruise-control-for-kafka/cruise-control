/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;

/**
 * Class for achieving the following hard goal:
 * HARD GOAL: Generate replica movement proposals between disks of the same broker to push the load on each disk of broker
 * under the capacity limit.
 */
public class IntraBrokerDiskCapacityGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerDiskCapacityGoal.class);
  private static final int MIN_NUM_VALID_WINDOWS = 1;
  private static final Resource RESOURCE = Resource.DISK;
  // This is only used for the remove disks endpoint
  private final boolean _shouldEmptyZeroCapacityDisks;

  /**
   * Constructor for Capacity Goal.
   */
  public IntraBrokerDiskCapacityGoal() {
    _shouldEmptyZeroCapacityDisks = false;
  }

  /**
   * Constructor for Intra Broker Disk Capacity Goal.
   *
   * @param shouldEmptyZeroCapacityDisks specifies if the goal should move all replicas from disks with 0 capacity
   */
  public IntraBrokerDiskCapacityGoal(boolean shouldEmptyZeroCapacityDisks) {
      _shouldEmptyZeroCapacityDisks = shouldEmptyZeroCapacityDisks;
  }

  /**
   * Package private for unit test.
   */
  IntraBrokerDiskCapacityGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
    _shouldEmptyZeroCapacityDisks = false;
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  @Override
  public boolean isIntraBrokerGoal() {
    return true;
  }

  /**
   * Sanity checks: For each alive broker in the cluster, the load for {@link Resource#DISK} less than the limiting capacity
   * determined by the total capacity of alive disks multiplied by the capacity threshold.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    // While proposals exclude the excludedTopics, the existingUtilization still considers replicas of the excludedTopics.
    for (Broker broker : clusterModel.aliveBrokers()) {
      double existingUtilization = broker.load().expectedUtilizationFor(RESOURCE);
      double allowedCapacity = broker.capacityFor(RESOURCE) * _balancingConstraint.capacityThreshold(RESOURCE);
      if (allowedCapacity < existingUtilization) {
        double requiredCapacity = existingUtilization / _balancingConstraint.capacityThreshold(RESOURCE);
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
            .numBrokers(1).totalCapacity(requiredCapacity).build();
        throw new OptimizationFailureException(String.format("[%s] Insufficient disk capacity at broker %d (Utilization %.2f, Allowed "
                                                             + "Capacity %.2f).", name(), broker.id(), existingUtilization, allowedCapacity),
                                               recommendation);
      }
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Sort all the replicas for each disk based on disk utilization. Replicas of excluded topics are still moved off the disks
    // marked for removal, which must be emptied.
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopicsAndDisksToRemove(
                                                         excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(RESOURCE.name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);
  }

  /**
   * Get brokers in the cluster so that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   * Note this goal moves replica between disks within broker, therefore it is unable to heal dead broker.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   *         they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return new TreeSet<>(clusterModel.aliveBrokers());
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. For this goal:
   * ## Leadership Movement: always accept.
   * ## Replica Movement/Swap: accept if action will not make disk load exceed disk capacity limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // Currently disk-granularity goals do not work with broker-granularity goals.
    if (action.sourceBrokerLogdir() == null || action.destinationBrokerLogdir() == null) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " does not support balancing action not "
                                         + "specifying logdir.");
    }
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Disk destinationDisk = clusterModel.broker(action.destinationBrokerId()).disk(action.destinationBrokerLogdir());

    return switch (action.balancingAction()) {
      case INTRA_BROKER_REPLICA_SWAP -> {
        Replica destinationReplica = clusterModel.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition());
        yield isSwapAcceptableForCapacity(sourceReplica, destinationReplica) ? ACCEPT : REPLICA_REJECT;
      }
      case INTRA_BROKER_REPLICA_MOVEMENT ->
        isMovementAcceptableForCapacity(sourceReplica, destinationDisk) ? ACCEPT : REPLICA_REJECT;
      case LEADERSHIP_MOVEMENT -> ACCEPT;
      default ->
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    };
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Disk destinationDisk = clusterModel.broker(action.destinationBrokerId()).disk(action.destinationBrokerLogdir());
    boolean shouldMoveReplica;
    if (_shouldEmptyZeroCapacityDisks) {
      // should also move replicas with 0 expected disk utilization
      shouldMoveReplica = sourceReplica.load().expectedUtilizationFor(RESOURCE) >= 0;
    } else {
      // In general CC tries to reduce number of replica move. Since empty replicas does not affect balance, CC tries to avoid moving them.
      shouldMoveReplica = sourceReplica.load().expectedUtilizationFor(RESOURCE) > 0;
    }
    return shouldMoveReplica && isMovementAcceptableForCapacity(sourceReplica, destinationDisk);
  }

  /**
   * Perform optimization via replica movement cross disks on broker to ensure balance: The load on each alive disk
   * is under the disk's the capacity limit.
   * Note the optimization from this goal cannot be applied to offline replicas because Kafka does not support moving
   * replicas on bad disks to good disks within the same broker.
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
    LOG.debug("balancing broker {}, optimized goals = {}.", broker, optimizedGoals);

    // Get alive disk over capacity limit.
    List<Disk> disksOverUtilized = broker.disks().stream().filter(Disk::isAlive)
                                         .filter(this::isUtilizationOverLimit).toList();
    if (disksOverUtilized.isEmpty()) {
      return;
    }
    List<Disk> candidateDisks = new ArrayList<>(broker.disks());
    candidateDisks.removeAll(disksOverUtilized);
    candidateDisks.sort((disk1, disk2) -> {
      double allowanceForDisk1 = disk1.capacity() * _balancingConstraint.capacityThreshold(RESOURCE) - disk1.utilization();
      double allowanceForDisk2 = disk2.capacity() * _balancingConstraint.capacityThreshold(RESOURCE) - disk2.utilization();
      return Double.compare(allowanceForDisk2, allowanceForDisk1);
    });

    for (Disk disk : disksOverUtilized) {
      for (Replica replica : disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true)) {
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, candidateDisks, optimizedGoals);
        if (d == null) {
          LOG.debug("Failed to move replica {} to any disk {} in broker {}", replica, candidateDisks, replica.broker());
        }
        if (!isUtilizationOverLimit(disk)) {
          break;
        }
      }
    }
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing the resource, confirm that the utilization is under the capacity and finish.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    for (Broker broker : brokersToBalance(clusterModel)) {
      for (Disk disk : broker.disks()) {
        if (disk.isAlive() && isUtilizationOverLimit(disk)) {
          // The utilization of the host for the resource is over the capacity limit.
          double requiredCapacity = disk.utilization() / _balancingConstraint.capacityThreshold(RESOURCE);
          ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED)
              .numDisks(1).totalCapacity(requiredCapacity).build();
          throw new OptimizationFailureException(String.format("[%s] Utilization (%.2f) for disk %s on broker %d is above capacity limit.",
                                                               name(), disk.utilization(), disk, broker.id()), recommendation);
        }
        if (_shouldEmptyZeroCapacityDisks && disk.capacity() == 0 && !disk.replicas().isEmpty()) {
          throw new OptimizationFailureException(String.format("[%s] Could not move all replicas from disk %s on broker %d",
                                                               name(), disk.logDir(), broker.id()));
        }
      }
    }
    finish();
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS, _minMonitoredPartitionPercentage, true);
  }

  /**
   * Check whether the combined replica utilization is above the given disk capacity limits.
   * If _shouldEmptyZeroCapacityDisks is true, the disk utilization is over limit only if it is greater than 0.
   *
   * @param disk Disk to be checked for capacity limit violation.
   * @return {@code true} if utilization is over the limit, {@code false} otherwise.
   */
  private boolean isUtilizationOverLimit(Disk disk) {
    if (_shouldEmptyZeroCapacityDisks && disk.capacity() == 0) {
      return disk.utilization() > 0 || !disk.replicas().isEmpty();
    }
    return disk.utilization() > disk.capacity() * _balancingConstraint.capacityThreshold(RESOURCE);
  }

  /**
   * Check whether the movement of source replica to destination disk is acceptable for this goal.
   *
   * @param sourceReplica   Source replica.
   * @param destinationDisk Destination disk.
   * @return {@code true} if movement is acceptable for this goal, {@code false} otherwise.
   */
  private boolean isMovementAcceptableForCapacity(Replica sourceReplica, Disk destinationDisk) {
    double replicaUtilization = sourceReplica.load().expectedUtilizationFor(RESOURCE);
    return isUtilizationUnderLimitAfterAddingLoad(destinationDisk, replicaUtilization);
  }

  /**
   * Check whether the swap between source and destination replicas is acceptable for this goal.
   *
   * @param sourceReplica Source replica.
   * @param destinationReplica Destination replica.
   * @return {@code true} if the swap for the current resource between source and destination replicas is acceptable for this
   *         goal, {@code false} otherwise.
   */
  private boolean isSwapAcceptableForCapacity(Replica sourceReplica, Replica destinationReplica) {
    double sourceReplicaUtilization = sourceReplica.load().expectedUtilizationFor(RESOURCE);
    double destinationReplicaUtilization = destinationReplica.load().expectedUtilizationFor(RESOURCE);
    double sourceUtilizationDelta = destinationReplicaUtilization - sourceReplicaUtilization;
    return sourceUtilizationDelta > 0 ? isUtilizationUnderLimitAfterAddingLoad(sourceReplica.disk(), sourceUtilizationDelta)
                                      : isUtilizationUnderLimitAfterAddingLoad(destinationReplica.disk(), - sourceUtilizationDelta);
  }

  /**
   * Check whether the additional load on the destination disk makes the disk go out of the capacity limit.
   *
   * @param destinationDisk Destination disk.
   * @param utilizationToAdd Utilization to add.
   * @return {@code true} if utilization is less than the capacity limit, {@code false} otherwise.
   */
  private boolean isUtilizationUnderLimitAfterAddingLoad(Disk destinationDisk, double utilizationToAdd) {
    double capacityLimit = destinationDisk.capacity() * _balancingConstraint.capacityThreshold(RESOURCE);
    return destinationDisk.utilization() + utilizationToAdd < capacityLimit;
  }
}
