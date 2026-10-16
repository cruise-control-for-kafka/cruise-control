/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils.EPSILON;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.diskIORate;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaDiskIORate;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static java.lang.Math.abs;


/**
 * Class for achieving the following soft goal:
 * SOFT GOAL: For each broker rebalance the estimated disk I/O rate to push each disk's I/O rate within range around the
 *  average disk I/O rate of the alive disks on the broker.
 *
 * <p>The disk I/O rate of a replica is estimated from its network load, see {@link GoalUtils#replicaDiskIORate}:
 * <ul>
 *   <li>Inbound network rate approximates disk writes. Every produced byte is written to the disk of the leader and of
 *   each follower replica, hence it counts for both leader and follower replicas.</li>
 *   <li>Outbound network rate approximates disk reads (i.e. consumer and replication fetches). It counts for leader
 *   replicas only and is weighted by
 *   {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#INTRA_BROKER_DISK_IO_READ_WEIGHT_CONFIG},
 *   because reads may be served from the page cache.</li>
 * </ul>
 * Disks within the same broker are assumed to have the same I/O capability. The balance threshold is determined by
 * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#DISK_BALANCE_THRESHOLD_CONFIG}.
 */
public class IntraBrokerDiskIORateDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerDiskIORateDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private static final long PER_DISK_SWAP_TIMEOUT_MS = 500L;
  private final Map<Broker, Double> _balanceUpperThresholdByBroker;
  private final Map<Broker, Double> _balanceLowerThresholdByBroker;
  private final Map<Broker, Double> _averageDiskIORateByBroker;
  // Estimated disk I/O rate of alive disks, which is kept up to date with the actions applied by this goal.
  private final Map<Disk, Double> _diskIORateByDisk;
  private double _readWeight;

  /**
   * Constructor for Intra-Broker Disk I/O Rate Distribution Goal.
   */
  public IntraBrokerDiskIORateDistributionGoal() {
    super();
    _balanceLowerThresholdByBroker = new HashMap<>();
    _balanceUpperThresholdByBroker = new HashMap<>();
    _averageDiskIORateByBroker = new HashMap<>();
    _diskIORateByDisk = new HashMap<>();
  }

  /**
   * Package private for unit test.
   */
  IntraBrokerDiskIORateDistributionGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Initialize the disk I/O rate thresholds.
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    _readWeight = _balancingConstraint.diskIOReadWeight();
    _balanceUpperThresholdByBroker.clear();
    _balanceLowerThresholdByBroker.clear();
    _averageDiskIORateByBroker.clear();
    _diskIORateByDisk.clear();
    double balancePercentageWithMargin = (_balancingConstraint.resourceBalancePercentage(Resource.DISK) - 1) * BALANCE_MARGIN;
    for (Broker broker : brokersToBalance(clusterModel)) {
      double totalDiskIORate = 0;
      int numAliveDisks = 0;
      for (Disk disk : broker.disks()) {
        if (disk.isAlive()) {
          double rate = diskIORate(disk, _readWeight);
          _diskIORateByDisk.put(disk, rate);
          totalDiskIORate += rate;
          numAliveDisks++;
        }
      }
      double averageDiskIORate = numAliveDisks > 0 ? totalDiskIORate / numAliveDisks : 0;
      _averageDiskIORateByBroker.put(broker, averageDiskIORate);
      _balanceUpperThresholdByBroker.put(broker, averageDiskIORate * (1 + balancePercentageWithMargin));
      _balanceLowerThresholdByBroker.put(broker, averageDiskIORate * Math.max(0, (1 - balancePercentageWithMargin)));
    }

    // Sort all the replicas for each disk based on estimated disk I/O rate.
    double readWeight = _readWeight;
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(replica -> -replicaDiskIORate(replica, readWeight))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(replica -> replicaDiskIORate(replica, readWeight))
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing the disk I/O rate, check whether there are disks whose I/O rate is
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
          if (cachedDiskIORate(disk) > upperLimit) {
            disksAboveBalanceUpperLimit.add(broker.id() + ":" + disk.logDir());
          }
          if (cachedDiskIORate(disk) < lowerLimit) {
            disksBelowBalanceLowerLimit.add(broker.id() + ":" + disk.logDir());
          }
        }
      }
    }
    if (!disksAboveBalanceUpperLimit.isEmpty()) {
      LOG.warn("Disks {} are above balance upper limit of disk I/O rate after optimization.", disksAboveBalanceUpperLimit);
      _succeeded = false;
    }
    if (!disksBelowBalanceLowerLimit.isEmpty()) {
      LOG.warn("Disks {} are below balance lower limit of disk I/O rate after optimization.", disksBelowBalanceLowerLimit);
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
   * (2) Otherwise, the action cannot increase the disk I/O rate difference between disks.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   *         {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    double sourceIORateDelta = sourceIORateDelta(action, clusterModel);
    if (sourceIORateDelta == 0) {
      // No change in terms of disk I/O rate.
      return ACCEPT;
    }
    Broker broker = clusterModel.broker(action.sourceBrokerId());
    if (!_balanceUpperThresholdByBroker.containsKey(broker)) {
      // The broker was not balanced by this goal.
      return ACCEPT;
    }
    // Other goals may have relocated replicas since this goal was optimized, hence compute the current disk I/O rates.
    double sourceDiskIORate = diskIORate(broker.disk(action.sourceBrokerLogdir()), _readWeight);
    double destinationDiskIORate = diskIORate(broker.disk(action.destinationBrokerLogdir()), _readWeight);
    double sourceDiskAllowance = sourceDiskAllowance(sourceIORateDelta, broker, sourceDiskIORate);
    double destinationDiskAllowance = destinationDiskAllowance(sourceIORateDelta, broker, destinationDiskIORate);
    if (isChangeViolatingLimit(sourceIORateDelta, sourceDiskAllowance, destinationDiskAllowance)) {
      return REPLICA_REJECT;
    }
    if (sourceDiskAllowance >= 0 && destinationDiskAllowance >= 0) {
      // Both disks remain within the limit after the action.
      return ACCEPT;
    }
    return isGettingMoreBalanced(sourceDiskIORate, destinationDiskIORate, sourceIORateDelta) ? ACCEPT : REPLICA_REJECT;
  }

  /**
   * An action is self-satisfied if it changes the disk I/O rates without violating the limits and makes the disks more balanced.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return {@code true} if the action is self-satisfied, {@code false} otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    double sourceIORateDelta = sourceIORateDelta(action, clusterModel);
    if (sourceIORateDelta == 0) {
      return false;
    }
    Broker broker = clusterModel.broker(action.sourceBrokerId());
    double sourceDiskIORate = cachedDiskIORate(broker.disk(action.sourceBrokerLogdir()));
    double destinationDiskIORate = cachedDiskIORate(broker.disk(action.destinationBrokerLogdir()));
    return !isChangeViolatingLimit(sourceIORateDelta,
                                   sourceDiskAllowance(sourceIORateDelta, broker, sourceDiskIORate),
                                   destinationDiskAllowance(sourceIORateDelta, broker, destinationDiskIORate))
           && isGettingMoreBalanced(sourceDiskIORate, destinationDiskIORate, sourceIORateDelta);
  }

  /**
   * @param action Balancing action.
   * @param clusterModel The state of the cluster.
   * @return The change of estimated disk I/O rate on the source disk if the given action is applied.
   */
  private double sourceIORateDelta(BalancingAction action, ClusterModel clusterModel) {
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
        return replicaDiskIORate(destinationReplica, _readWeight) - replicaDiskIORate(sourceReplica, _readWeight);
      case LEADERSHIP_MOVEMENT:
        return 0;
      case INTRA_BROKER_REPLICA_MOVEMENT:
        return -replicaDiskIORate(sourceReplica, _readWeight);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  /**
   * @param sourceIORateDelta The change of estimated disk I/O rate on the source disk.
   * @param broker The broker of the disks.
   * @param sourceDiskIORate The estimated disk I/O rate of the source disk.
   * @return The disk I/O rate that can be added to (if the delta is positive) or removed from (otherwise) the source disk
   * without violating the corresponding limit, or a negative value if the source disk already violates that limit.
   */
  private double sourceDiskAllowance(double sourceIORateDelta, Broker broker, double sourceDiskIORate) {
    return sourceIORateDelta > 0 ? _balanceUpperThresholdByBroker.get(broker) - sourceDiskIORate
                                 : sourceDiskIORate - _balanceLowerThresholdByBroker.get(broker);
  }

  /**
   * @param sourceIORateDelta The change of estimated disk I/O rate on the source disk.
   * @param broker The broker of the disks.
   * @param destinationDiskIORate The estimated disk I/O rate of the destination disk.
   * @return The disk I/O rate that can be removed from (if the delta is positive) or added to (otherwise) the destination
   * disk without violating the corresponding limit, or a negative value if the destination disk already violates that limit.
   */
  private double destinationDiskAllowance(double sourceIORateDelta, Broker broker, double destinationDiskIORate) {
    return sourceIORateDelta > 0 ? destinationDiskIORate - _balanceLowerThresholdByBroker.get(broker)
                                 : _balanceUpperThresholdByBroker.get(broker) - destinationDiskIORate;
  }

  private static boolean isChangeViolatingLimit(double sourceIORateDelta, double sourceDiskAllowance, double destinationDiskAllowance) {
    return (sourceDiskAllowance >= 0 && sourceDiskAllowance < abs(sourceIORateDelta))
           || (destinationDiskAllowance >= 0 && destinationDiskAllowance < abs(sourceIORateDelta));
  }

  private static boolean isGettingMoreBalanced(double sourceDiskIORate, double destinationDiskIORate, double sourceIORateDelta) {
    double prevDiff = sourceDiskIORate - destinationDiskIORate;
    double nextDiff = prevDiff + 2 * sourceIORateDelta;
    return abs(nextDiff) < abs(prevDiff);
  }

  private double cachedDiskIORate(Disk disk) {
    return _diskIORateByDisk.get(disk);
  }

  /**
   * Update the cached disk I/O rates after an action applied by this goal.
   *
   * @param sourceDisk Source disk of the action.
   * @param destinationDisk Destination disk of the action.
   * @param sourceIORateDelta The change of estimated disk I/O rate on the source disk.
   */
  private void updateCachedDiskIORate(Disk sourceDisk, Disk destinationDisk, double sourceIORateDelta) {
    _diskIORateByDisk.merge(sourceDisk, sourceIORateDelta, Double::sum);
    _diskIORateByDisk.merge(destinationDisk, -sourceIORateDelta, Double::sum);
  }

  /**
   * (1) REBALANCE BY REPLICA MOVEMENT:
   * Perform optimization via replica movement between disks to ensure balance: The disk I/O rates are within range.
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
    if (broker.disks().stream().filter(Disk::isAlive).count() < 2) {
      // Nothing to balance within a broker having fewer than two alive disks.
      return;
    }
    double upperLimit = _balanceUpperThresholdByBroker.get(broker);
    double lowerLimit = _balanceLowerThresholdByBroker.get(broker);
    for (Disk disk : broker.disks()) {
      if (!disk.isAlive()) {
        continue;
      }
      if (cachedDiskIORate(disk) > upperLimit) {
        if (rebalanceByMovingLoadOut(disk, clusterModel, optimizedGoals)) {
          rebalanceBySwappingLoadOut(disk, clusterModel, optimizedGoals);
        }
      }
      if (cachedDiskIORate(disk) < lowerLimit) {
        if (rebalanceByMovingLoadIn(disk, clusterModel, optimizedGoals)) {
          rebalanceBySwappingLoadIn(disk, clusterModel, optimizedGoals);
        }
      }
    }
  }

  /**
   * Try to move the given replica to the given candidate disk, and update the cached disk I/O rates if succeeded.
   *
   * @param clusterModel   The current cluster model.
   * @param replica        Replica to move.
   * @param candidateDisk  Candidate destination disk.
   * @param optimizedGoals Optimized goals.
   * @return {@code true} if the replica is moved, {@code false} otherwise.
   */
  private boolean maybeMoveReplica(ClusterModel clusterModel, Replica replica, Disk candidateDisk, Set<Goal> optimizedGoals) {
    Disk sourceDisk = replica.disk();
    Disk destinationDisk = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(candidateDisk), optimizedGoals);
    if (destinationDisk == null) {
      return false;
    }
    updateCachedDiskIORate(sourceDisk, destinationDisk, -replicaDiskIORate(replica, _readWeight));
    return true;
  }

  /**
   * Try to swap the given source replica with one of the candidate replicas, and update the cached disk I/O rates if succeeded.
   *
   * @param clusterModel      The current cluster model.
   * @param sourceReplica     Replica to be swapped.
   * @param candidateDisk     The disk hosting the candidate replicas.
   * @param candidateReplicas Candidate replicas to swap with the source replica in the order of attempts to swap.
   * @param optimizedGoals    Optimized goals.
   * @return {@code true} if the replicas are swapped, {@code false} otherwise.
   */
  private boolean maybeSwapReplica(ClusterModel clusterModel,
                                   Replica sourceReplica,
                                   Disk candidateDisk,
                                   SortedSet<Replica> candidateReplicas,
                                   Set<Goal> optimizedGoals) {
    Disk sourceDisk = sourceReplica.disk();
    Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel, sourceReplica, candidateReplicas, optimizedGoals);
    if (swappedIn == null) {
      return false;
    }
    updateCachedDiskIORate(sourceDisk, candidateDisk,
                           replicaDiskIORate(swappedIn, _readWeight) - replicaDiskIORate(sourceReplica, _readWeight));
    return true;
  }

  /**
   * Try to balance the disk with low I/O rate by moving in replicas from other disks of the same broker.
   *
   * @param disk            The disk to balance.
   * @param clusterModel    The current cluster model.
   * @param optimizedGoals  Optimized goals.
   * @return {@code true} if the disk to balance is still below the balance lower limit, {@code false} otherwise.
   */
  private boolean rebalanceByMovingLoadIn(Disk disk, ClusterModel clusterModel, Set<Goal> optimizedGoals) {
    Broker broker = disk.broker();
    double brokerAverage = _averageDiskIORateByBroker.get(broker);

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Double.compare(cachedDiskIORate(d2), cachedDiskIORate(d1)));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try moving load from -- sorted in the order of trial (descending load).
      if (candidateDisk.isAlive() && cachedDiskIORate(candidateDisk) > brokerAverage) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Iterator<Replica> iterator = candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true).iterator();
          iterator.hasNext(); ) {
        Replica replica = iterator.next();
        // Only need to check status if the action is taken. This will also handle the case that the source disk
        // has nothing to move in. In that case we will never re-enqueue that source disk.
        if (maybeMoveReplica(clusterModel, replica, disk, optimizedGoals)) {
          if (cachedDiskIORate(disk) > _balanceLowerThresholdByBroker.get(broker)) {
            return false;
          }
          iterator.remove();
          // If the source disk has a lower I/O rate than the next disk in the candidate disk priority queue,
          // we re-enqueue the source disk and switch to the next disk.
          if (!candidateDiskPQ.isEmpty() && cachedDiskIORate(candidateDisk) < cachedDiskIORate(candidateDiskPQ.peek())) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * Try to balance the disk with high I/O rate by moving out replicas to other disks of the same broker.
   *
   * @param disk            The disk to balance.
   * @param clusterModel    The current cluster model.
   * @param optimizedGoals  Optimized goals.
   * @return {@code true} if the disk to balance is still above the balance upper limit, {@code false} otherwise.
   */
  private boolean rebalanceByMovingLoadOut(Disk disk, ClusterModel clusterModel, Set<Goal> optimizedGoals) {
    Broker broker = disk.broker();
    double brokerAverage = _averageDiskIORateByBroker.get(broker);
    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingDouble(this::cachedDiskIORate));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try moving load to -- sorted in the order of trial (ascending load).
      if (candidateDisk.isAlive() && cachedDiskIORate(candidateDisk) < brokerAverage) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Iterator<Replica> iterator = disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true).iterator();
          iterator.hasNext(); ) {
        Replica replica = iterator.next();
        // Only need to check status if the action is taken. This will also handle the case that no replica can be
        // move to destination disk. In that case we will never re-enqueue that destination disk.
        if (maybeMoveReplica(clusterModel, replica, candidateDisk, optimizedGoals)) {
          if (cachedDiskIORate(disk) < _balanceUpperThresholdByBroker.get(broker)) {
            return false;
          }
          iterator.remove();
          // If the destination disk has a higher I/O rate than the next disk in the candidate disk priority queue,
          // we re-enqueue the destination disk and switch to the next disk.
          if (!candidateDiskPQ.isEmpty() && cachedDiskIORate(candidateDisk) > cachedDiskIORate(candidateDiskPQ.peek())) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
    return true;
  }

  /**
   * Try to balance the disk with high I/O rate by swapping its replicas with replicas from other disks of the same broker.
   *
   * @param disk            The disk to balance.
   * @param clusterModel    The current cluster model.
   * @param optimizedGoals  Optimized goals.
   */
  private void rebalanceBySwappingLoadOut(Disk disk, ClusterModel clusterModel, Set<Goal> optimizedGoals) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingDouble(this::cachedDiskIORate));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try to swap replica with -- sorted in the order of trial (ascending load).
      if (candidateDisk.isAlive() && cachedDiskIORate(candidateDisk) < _balanceUpperThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Replica sourceReplica : disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(false)) {
        // Try swapping the source with the candidate replicas.
        if (maybeSwapReplica(clusterModel, sourceReplica, candidateDisk,
                             candidateDisk.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(false),
                             optimizedGoals)) {
          if (cachedDiskIORate(disk) < _balanceUpperThresholdByBroker.get(broker)) {
            // Successfully balanced this disk by swapping in.
            return;
          }
          break;
        }
      }
      if (remainingPerDiskSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap load out timeout for disk {}.", disk.logDir());
        break;
      }
      if (cachedDiskIORate(candidateDisk) < _balanceUpperThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  /**
   * Try to balance the disk with low I/O rate by swapping its replicas with replicas from other disks of the same broker.
   *
   * @param disk            The disk to balance.
   * @param clusterModel    The current cluster model.
   * @param optimizedGoals  Optimized goals.
   */
  private void rebalanceBySwappingLoadIn(Disk disk, ClusterModel clusterModel, Set<Goal> optimizedGoals) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Double.compare(cachedDiskIORate(d2), cachedDiskIORate(d1)));
    for (Disk candidateDisk : broker.disks()) {
      // Get candidate disk on broker to try to swap replica with -- sorted in the order of trial (descending load).
      if (candidateDisk.isAlive() && cachedDiskIORate(candidateDisk) > _balanceLowerThresholdByBroker.get(broker)) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Replica sourceReplica : disk.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(false)) {
        // Try swapping the source with the candidate replicas.
        if (maybeSwapReplica(clusterModel, sourceReplica, candidateDisk,
                             candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(false),
                             optimizedGoals)) {
          if (cachedDiskIORate(disk) > _balanceLowerThresholdByBroker.get(broker)) {
            // Successfully balanced this disk by swapping in.
            return;
          }
          break;
        }
      }
      if (remainingPerDiskSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap load in timeout for disk {}.", disk.logDir());
        break;
      }
      if (cachedDiskIORate(candidateDisk) > _balanceLowerThresholdByBroker.get(broker)) {
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
  private static long remainingPerDiskSwapTimeMs(long swapStartTimeMs) {
    return PER_DISK_SWAP_TIMEOUT_MS - (System.currentTimeMillis() - swapStartTimeMs);
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ClusterModelStatsComparator() {
      private String _reasonForLastNegativeResult;

      @Override
      public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
        // Tolerate floating point errors in the aggregated standard deviation.
        if (stats1.diskIORateStDev() > stats2.diskIORateStDev() + EPSILON) {
          _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Disk I/O Rate] post-optimization:%.3f "
                                                       + "pre-optimization:%.3f", name(), stats1.diskIORateStDev(),
                                                       stats2.diskIORateStDev());
          return -1;
        }
        return 1;
      }

      @Override
      public String explainLastComparison() {
        return _reasonForLastNegativeResult;
      }
    };
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(_numWindows, _minMonitoredPartitionPercentage, false);
  }
}
