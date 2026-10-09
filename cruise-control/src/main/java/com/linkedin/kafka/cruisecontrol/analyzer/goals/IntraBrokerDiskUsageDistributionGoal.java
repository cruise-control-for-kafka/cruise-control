/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.averageDiskUtilizationPercentage;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.diskUtilizationPercentage;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static java.lang.Math.abs;


/**
 * Class for achieving the following soft goal:
 * SOFT GOAL: For each broker rebalance disk usage to push each disk's utilization percentage within range around the
 *  utilization percentage of the whole broker.
 */
public class IntraBrokerDiskUsageDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerDiskUsageDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private static final long PER_DISK_SWAP_TIMEOUT_MS = 500L;
  private static final Resource RESOURCE = Resource.DISK;
  private final Map<Broker, Double> _balanceUpperThresholdByBroker;
  private final Map<Broker, Double> _balanceLowerThresholdByBroker;

  /**
   * Constructor for Resource Distribution Goal.
   */
  public IntraBrokerDiskUsageDistributionGoal() {
    super();
    _balanceLowerThresholdByBroker = new HashMap<>();
    _balanceUpperThresholdByBroker = new HashMap<>();
  }

  /**
   * Package private for unit test.
   */
  IntraBrokerDiskUsageDistributionGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
    _balanceLowerThresholdByBroker = new HashMap<>();
    _balanceUpperThresholdByBroker = new HashMap<>();
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Initialize the utilization thresholds.
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    double balancePercentageWithMargin = (_balancingConstraint.resourceBalancePercentage(RESOURCE) - 1) * BALANCE_MARGIN;
    for (Broker broker : brokersToBalance(clusterModel)) {
      double averageDiskUtilization = averageDiskUtilizationPercentage(broker);
      _balanceUpperThresholdByBroker.put(broker, averageDiskUtilization * (1 + balancePercentageWithMargin));
      _balanceLowerThresholdByBroker.put(broker, averageDiskUtilization * Math.max(0, (1 - balancePercentageWithMargin)));
    }

    // Sort all the replicas for each disk based on disk utilization.
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(RESOURCE.name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.sortByMetricGroupValue(RESOURCE.name()))
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing the resource, check whether there are disks whose utilization percentage is
   * out of range, finish and mark optimization status accordingly.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    List<String> disksAboveBalanceUpperLimit = new ArrayList<>();
    List<String> disksBelowBalanceLowerLimit = new ArrayList<>();
    for (Broker broker : brokersToBalance(clusterModel)) {
      double upperLimit = _balanceUpperThresholdByBroker.get(broker);
      double lowerLimit = _balanceLowerThresholdByBroker.get(broker);
      for (Disk disk : broker.disks()) {
        if (disk.isAlive()) {
          if (diskUtilizationPercentage(disk) > upperLimit) {
            disksAboveBalanceUpperLimit.add(broker.id() + ":" + disk.logDir());
          }
          if (diskUtilizationPercentage(disk) < lowerLimit) {
            disksBelowBalanceLowerLimit.add(broker.id() + ":" + disk.logDir());
          }
        }
      }
    }
    if (!disksAboveBalanceUpperLimit.isEmpty()) {
      LOG.warn("Disks {} are above balance upper limit after optimization.", disksAboveBalanceUpperLimit);
      _succeeded = false;
    }
    if (!disksBelowBalanceLowerLimit.isEmpty()) {
      LOG.warn("Disks {} are below balance lower limit after optimization.", disksBelowBalanceLowerLimit);
      _succeeded = false;
    }
    finish();
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
   * Check whether given action is acceptable by this goal. An action is acceptable by this goal if it satisfies the
   * following:
   * (1) If source and destination disks were within the limit before the action, the corresponding limits cannot be
   * violated after the action.
   * (2) The action cannot increase the utilization difference between disks.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    double sourceUtilizationDelta = sourceUtilizationDelta(action, clusterModel);
    Broker broker = clusterModel.broker(action.sourceBrokerId());
    Disk sourceDisk = broker.disk(action.sourceBrokerLogdir());
    Disk destinationDisk = broker.disk(action.destinationBrokerLogdir());
    if (sourceUtilizationDelta == 0) {
      // No change in terms of load.
      return ACCEPT;
    }
    if (isChangeViolatingLimit(sourceUtilizationDelta, sourceDisk, destinationDisk)) {
      return REPLICA_REJECT;
    }
    return isGettingMoreBalanced(sourceDisk, destinationDisk, sourceUtilizationDelta) ? ACCEPT : REPLICA_REJECT;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    double sourceUtilizationDelta = sourceUtilizationDelta(action, clusterModel);
    return sourceUtilizationDelta != 0 && actionAcceptance(action, clusterModel) == ACCEPT;
  }

  private double sourceUtilizationDelta(BalancingAction action, ClusterModel clusterModel) {
    // Currently disk-granularity goals do not work with broker-granularity goals.
    if (action.sourceBrokerLogdir() == null || action.destinationBrokerLogdir() == null) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " does not support balancing action not "
                                         + "specifying logdir.");
    }

    Broker broker = clusterModel.broker(action.sourceBrokerId());
    Replica sourceReplica = broker.replica(action.topicPartition());
    switch (action.balancingAction()) {
      case INTRA_BROKER_REPLICA_SWAP:
        Replica destinationReplica = broker.replica(action.destinationTopicPartition());
        return destinationReplica.load().expectedUtilizationFor(RESOURCE)
               - sourceReplica.load().expectedUtilizationFor(RESOURCE);
      case LEADERSHIP_MOVEMENT:
        return 0;
      case INTRA_BROKER_REPLICA_MOVEMENT:
        return - sourceReplica.load().expectedUtilizationFor(RESOURCE);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isChangeViolatingLimit(double sourceUtilizationDelta, Disk sourceDisk, Disk destinationDisk) {
    double balanceUpperThreshold = _balanceUpperThresholdByBroker.get(sourceDisk.broker());
    double balanceLowerThreshold = _balanceLowerThresholdByBroker.get(sourceDisk.broker());
    double sourceDiskAllowance = sourceUtilizationDelta > 0 ? sourceDisk.capacity() * balanceUpperThreshold - sourceDisk.utilization()
                                                            : sourceDisk.utilization() - sourceDisk.capacity() * balanceLowerThreshold;
    double destinationDiskAllowance =
        sourceUtilizationDelta > 0 ? destinationDisk.utilization() - destinationDisk.capacity() * balanceLowerThreshold
                                   : destinationDisk.capacity() * balanceUpperThreshold - destinationDisk.utilization();
    if ((sourceDiskAllowance >= 0 && sourceDiskAllowance < abs(sourceUtilizationDelta))
        || (destinationDiskAllowance >= 0 && destinationDiskAllowance < abs(sourceUtilizationDelta))) {
      return true;
    }
    return false;
  }

  private boolean isGettingMoreBalanced(Disk sourceDisk, Disk destinationDisk, double sourceUtilizationDelta) {
    double prevDiff = diskUtilizationPercentage(sourceDisk) - diskUtilizationPercentage(destinationDisk);
    double nextDiff = prevDiff + sourceUtilizationDelta / sourceDisk.capacity() + sourceUtilizationDelta / destinationDisk.capacity();
    return abs(nextDiff) < abs(prevDiff);
  }

  /**
   * (1) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement between disks to ensure balance: The load on disks are within range.
   * (2) REBALANCE BY REPLICA SWAP:
   * Swap replicas to ensure balance without violating optimized goal requirements.
   * Note the optimization from this goal cannot be applied to offline replicas because Kafka does not support moving
   * replicas on bad disks to good disks within the same broker.
   *
   * @param broker              Broker to be balanced.
   * @param clusterModel        The state of the cluster.
   * @param optimizedGoals      Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    double upperLimit = _balanceUpperThresholdByBroker.get(broker);
    double lowerLimit = _balanceLowerThresholdByBroker.get(broker);
    LOG.debug("balancing broker {}", broker);
    LOG.debug("List of broker disks is {}.", broker.disks());
    for (Disk disk : broker.disks()) {
      if (!disk.isAlive()) {
        continue;
      }
      if (diskUtilizationPercentage(disk) > upperLimit) {
        if (rebalanceByMovingLoadOut(disk, clusterModel, optimizedGoals, optimizationOptions)) {
          rebalanceBySwappingLoadOut(disk, clusterModel, optimizedGoals, optimizationOptions);
        }
      }
      if (diskUtilizationPercentage(disk) < lowerLimit) {
        if (rebalanceByMovingLoadIn(disk, clusterModel, optimizedGoals, optimizationOptions)) {
          rebalanceBySwappingLoadIn(disk, clusterModel, optimizedGoals, optimizationOptions);
        }
      }
    }
  }

  /**
   * Try to balance the underloaded disk by moving in replicas from other disks of the same broker.
   *
   * @param disk                 The disk to balance.
   * @param clusterModel         The current cluster model.
   * @param optimizedGoals       Optimized goals.
   * @param optimizationOptions  Options to take into account during optimization -- e.g. excluded topics.
   * @return {@code true} if the disk to balance is still underloaded, {@code false} otherwise.
   */
  private boolean rebalanceByMovingLoadIn(Disk disk,
                                          ClusterModel clusterModel,
                                          Set<Goal> optimizedGoals,
                                          OptimizationOptions optimizationOptions) {
    Broker broker = disk.broker();
    double brokerUtilization = averageDiskUtilizationPercentage(broker);

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Double.compare(diskUtilizationPercentage(d2), diskUtilizationPercentage(d1)));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try moving load from -- sorted in the order of trial (descending load).
      if (candidateDisk.isAlive() && diskUtilizationPercentage(candidateDisk) > brokerUtilization) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Iterator<Replica> iterator = candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true).iterator();
          iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(disk), optimizedGoals);
        // Only need to check status if the action is taken. This will also handle the case that the source disk
        // has nothing to move in. In that case we will never re-enqueue that source disk.
        if (d != null) {
          if (diskUtilizationPercentage(disk) > _balanceLowerThresholdByBroker.get(broker)) {
            return false;
          }
          iterator.remove();
          // If the source disk has a lower utilization than the next disk in the candidate disk priority queue,
          // we re-enqueue the source disk and switch to the next disk.
          if (!candidateDiskPQ.isEmpty() && diskUtilizationPercentage(candidateDisk) < diskUtilizationPercentage(candidateDiskPQ.peek())) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * Try to balance the overloaded disk by moving out replicas to other disks of the same broker.
   *
   * @param disk                 The disk to balance.
   * @param clusterModel         The current cluster model.
   * @param optimizedGoals       Optimized goals.
   * @param optimizationOptions  Options to take into account during optimization -- e.g. excluded topics.
   * @return {@code true} if the disk to balance is still overloaded, {@code false} otherwise.
   */
  private boolean rebalanceByMovingLoadOut(Disk disk,
                                           ClusterModel clusterModel,
                                           Set<Goal> optimizedGoals,
                                           OptimizationOptions optimizationOptions) {
    Broker broker = disk.broker();
    double brokerUtilization = averageDiskUtilizationPercentage(broker);
    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingDouble(GoalUtils::diskUtilizationPercentage));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try moving load to -- sorted in the order of trial (ascending load).
      if (candidateDisk.isAlive() && diskUtilizationPercentage(candidateDisk) < brokerUtilization) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Iterator<Replica> iterator = disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true).iterator();
          iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(candidateDisk), optimizedGoals);
        // Only need to check status if the action is taken. This will also handle the case that no replica can be
        // move to destination disk. In that case we will never re-enqueue that destination disk.
        if (d != null) {
          if (diskUtilizationPercentage(disk) < _balanceUpperThresholdByBroker.get(broker)) {
            return false;
          }
          iterator.remove();
          // If the destination disk has a higher utilization than the next disk in the candidate disk priority queue,
          // we re-enqueue the destination disk and switch to the next disk.
          if (!candidateDiskPQ.isEmpty() && diskUtilizationPercentage(candidateDisk) > diskUtilizationPercentage(candidateDiskPQ.peek())) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * Try to balance the overloaded disk by swapping its replicas with replicas from other disks of the same broker.
   *
   * @param disk                 The disk to balance.
   * @param clusterModel         The current cluster model.
   * @param optimizedGoals       Optimized goals.
   * @param optimizationOptions  Options to take into account during optimization -- e.g. excluded topics.
   */
  private void rebalanceBySwappingLoadOut(Disk disk,
                                         ClusterModel clusterModel,
                                         Set<Goal> optimizedGoals,
                                         OptimizationOptions optimizationOptions) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingDouble(GoalUtils::diskUtilizationPercentage));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try to swap replica with -- sorted in the order of trial (ascending load).
      if (candidateDisk.isAlive() && diskUtilizationPercentage(candidateDisk) < _balanceUpperThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Replica sourceReplica : disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(false)) {
        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel,
                                                         sourceReplica,
                                                         candidateDisk.trackedSortedReplicas(replicaSortName(this, false, false))
                                                                      .sortedReplicas(false),
                                                         optimizedGoals);
        if (swappedIn != null) {
          if (diskUtilizationPercentage(disk) < _balanceUpperThresholdByBroker.get(broker)) {
            // Successfully balanced this broker by swapping in.
            return;
          }
          break;
        }
      }
      if (remainingPerDiskSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap load out timeout for disk {}.", disk.logDir());
        break;
      }
      if (diskUtilizationPercentage(candidateDisk) < _balanceUpperThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  /**
   * Try to balance the underloaded disk by swapping its replicas with replicas from other disks of the same broker.
   *
   * @param disk                 The disk to balance.
   * @param clusterModel         The current cluster model.
   * @param optimizedGoals       Optimized goals.
   * @param optimizationOptions  Options to take into account during optimization -- e.g. excluded topics.
   */
  private void rebalanceBySwappingLoadIn(Disk disk,
                                        ClusterModel clusterModel,
                                        Set<Goal> optimizedGoals,
                                        OptimizationOptions optimizationOptions) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Double.compare(diskUtilizationPercentage(d2), diskUtilizationPercentage(d1)));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try to swap replica with -- sorted in the order of trial (descending load).
      if (candidateDisk.isAlive() && diskUtilizationPercentage(candidateDisk) > _balanceLowerThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Replica sourceReplica : disk.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(false)) {
        // Try swapping the source with the candidate replicas. Get the swapped in replica if successful, null otherwise.
        Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel,
                                                         sourceReplica,
                                                         candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false))
                                                                      .sortedReplicas(false),
                                                         optimizedGoals);
        if (swappedIn != null) {
          if (diskUtilizationPercentage(disk) > _balanceLowerThresholdByBroker.get(broker)) {
            // Successfully balanced this broker by swapping in.
            return;
          }
          break;
        }
      }
      if (remainingPerDiskSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap load out timeout for disk {}.", disk.logDir());
        break;
      }
      if (diskUtilizationPercentage(candidateDisk) > _balanceLowerThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  /**
   * Get the remaining per disk swap time in milliseconds based on the given swap start time.
   *
   * @param swapStartTimeMs Per disk swap start time in milliseconds.
   * @return Remaining per disk swap time in milliseconds.
   */
  private long remainingPerDiskSwapTimeMs(long swapStartTimeMs) {
    return PER_DISK_SWAP_TIMEOUT_MS - (System.currentTimeMillis() - swapStartTimeMs);
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ClusterModelStatsComparator() {
      @Override
      public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
        if (stats1.numUnbalancedDisks() > stats2.numUnbalancedDisks()
            || stats1.diskUtilizationStandardDeviation() > stats2.diskUtilizationStandardDeviation()) {
          return -1;
        }
        return 1;
      }

      @Override
      public String explainLastComparison() {
        return null;
      }
    };
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(_numWindows, _minMonitoredPartitionPercentage, false);
  }
}
