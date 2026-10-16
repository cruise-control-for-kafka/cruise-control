/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.rackaware.RackAwareGoalRackIdMapper;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.HARD_GOALS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.ADMIN;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REBALANCE;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DESTINATION_BROKER_IDS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXCLUDED_TOPICS_PARAM;


/**
 * A util class for goals.
 */
public final class GoalUtils {
  // Intra-broker goals require a minimum number of valid windows to start self-healing. Goals that rely on topology or
  // just the latest window, require at least MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING valid windows. Whereas, if goals
  // rely on historical resource utilization, then they require at least Math.max(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING,
  // number-of-windows / DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING) valid windows for starting self-healing.
  public static final int MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING = 1;
  public static final int DENOMINATOR_FOR_MIN_VALID_WINDOWS_FOR_SELF_HEALING = 14;
  private static final double DEAD_BROKER_UTILIZATION = 1.0;
  private static final double DEAD_DISK_UTILIZATION = 1.0;
  private static final String KAFKA_CRUISE_CONTROL_OBJECT_CONFIG = "kafka.cruise.control.object";

  private GoalUtils() {

  }

  /**
   * @param config The configurations for Cruise Control.
   * @return Configured {@link RackAwareGoalRackIdMapper} instance
   */

  public static RackAwareGoalRackIdMapper getRackAwareGoalRackIdMapper(KafkaCruiseControlConfig config) {
    return config.getConfiguredInstance(AnalyzerConfig.RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS_CONFIG,
                                        RackAwareGoalRackIdMapper.class);
  }

  /**
   * @param configs The configs used to configure {@link Goal} instance.
   * @return Configured {@link RackAwareGoalRackIdMapper} instance
   */
  public static RackAwareGoalRackIdMapper getRackAwareGoalRackIdMapper(Map<String, ?> configs) {
    return getRackAwareGoalRackIdMapper(new KafkaCruiseControlConfig(configs));
  }

  /**
   * Get the remaining time in milliseconds based on the given timout and start time.
   *
   * @param timeoutMs Timeout in milliseconds.
   * @param startTimeMs Start time in milliseconds.
   * @return Remaining time in milliseconds.
   */
  protected static long remainingTimeMs(long timeoutMs, long startTimeMs) {
    return timeoutMs - (System.currentTimeMillis() - startTimeMs);
  }

  /**
   * A convenience {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal.ClusterModelStatsComparator} for typical hard goals.
   */
  public static class HardGoalStatsComparator implements Goal.ClusterModelStatsComparator {
    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Stats are irrelevant to a hard goal. The optimization would already fail if the goal requirements are not met.
      return 0;
    }

    @Override
    public String explainLastComparison() {
      return null;
    }
  }

  /**
   * Filter out the given excluded brokers from the original brokers (if needed). If the user explicitly specified the
   * eligible destination brokers, and the action is not leadership movement, then retain only the brokers in the
   * requested destination brokers. Otherwise, if the action is:
   * <ul>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#LEADERSHIP_MOVEMENT}, then brokers excluded for
   * leadership are not eligible.</li>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT} for a leader replica,
   * then brokers excluded for leadership are not eligible.</li>
   * </ul>
   *
   * Note that this function supports only the above actions.
   *
   * @param originalBrokers Original list of brokers to be filtered.
   * @param optimizationOptions Options to take into account while filtering out brokers.
   * @param replica Replica affected from the action.
   * @param action Action that affects the given replica.
   */
  public static void filterOutBrokersExcludedForLeadership(List<Broker> originalBrokers,
                                                           OptimizationOptions optimizationOptions,
                                                           Replica replica,
                                                           ActionType action) {
    Set<Integer> requestedDestinationBrokerIds = optimizationOptions.requestedDestinationBrokerIds();
    if (requestedDestinationBrokerIds.isEmpty() || action == ActionType.LEADERSHIP_MOVEMENT) {
      Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForLeadership();
      if (!excludedBrokers.isEmpty() && (action == ActionType.LEADERSHIP_MOVEMENT || replica.isLeader())) {
        originalBrokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
      }
    } else {
      // Retain only the brokers that are in the requested destination brokers.
      originalBrokers.removeIf(b -> !requestedDestinationBrokerIds.contains(b.id()));
    }
  }

  /**
   * Filter out the given excluded brokers from the original brokers (if needed). If the user explicitly specified the
   * eligible destination brokers, and the action is not leadership movement, then retain only the brokers in the
   * requested destination brokers. Otherwise, if the action is:
   * <ul>
   * <li>{@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT}, then brokers
   * excluded for replica move are not eligible.</li>
   * </ul>
   *
   * @param originalBrokers Original list of brokers to be filtered.
   * @param optimizationOptions Options to take into account while filtering out brokers.
   * @param action Action that affects the given replica.
   */
  public static void filterOutBrokersExcludedForReplicaMove(List<Broker> originalBrokers,
                                                            OptimizationOptions optimizationOptions,
                                                            ActionType action) {
    Set<Integer> requestedDestinationBrokerIds = optimizationOptions.requestedDestinationBrokerIds();
    if (requestedDestinationBrokerIds.isEmpty()) {
      Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForReplicaMove();
      if (!excludedBrokers.isEmpty() && action == ActionType.INTER_BROKER_REPLICA_MOVEMENT) {
        originalBrokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
      }
    } else if (action != ActionType.LEADERSHIP_MOVEMENT) {
      // Retain only the brokers that are in the requested destination brokers.
      originalBrokers.removeIf(b -> !requestedDestinationBrokerIds.contains(b.id()));
    }
  }

  /**
   * Filter the given candidate brokers in the given clusterModel to retrieve the eligible ones for execution of a
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#INTER_BROKER_REPLICA_MOVEMENT} or
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#LEADERSHIP_MOVEMENT} action for the given replica.
   *
   * Invariant-1: If there are new brokers, an eligible candidate that triggers an action must be a new broker.
   * Invariant-2: Brokers excluded for leadership if exclusion applies to the given replica for the given action.
   *
   * @param clusterModel The state of the cluster.
   * @param replica  Replica to check for action eligibility.
   * @param candidates Candidate brokers among which the eligible ones will be selected.
   * @param action Action that affects the given replica.
   * @param optimizationOptions Options to take into account while applying the given action.
   * @return List of eligible brokers with a fixed order.
   */
  public static List<Broker> eligibleBrokers(ClusterModel clusterModel,
                                             Replica replica,
                                             Collection<Broker> candidates,
                                             ActionType action,
                                             OptimizationOptions optimizationOptions) {
    List<Broker> eligibleBrokers = new ArrayList<>(candidates);
    filterOutBrokersExcludedForLeadership(eligibleBrokers, optimizationOptions, replica, action);
    filterOutBrokersExcludedForReplicaMove(eligibleBrokers, optimizationOptions, action);
    if (!optimizationOptions.requestedDestinationBrokerIds().isEmpty()) {
      return eligibleBrokers;
    }

    if (clusterModel.newBrokers().isEmpty()) {
      return eligibleBrokers;
    }

    // When there are new brokers, we should only allow the replicas/leadership to be moved to the new brokers.
    return eligibleBrokers.stream().filter(b -> b.isNew() || b == replica.originalBroker()).collect(Collectors.toList());
  }

  /**
   * Check whether the proposed inter-broker action is legit. An action is legit if it is:
   * (1) a replica movement across brokers, the destination broker does not have a replica of the same partition and is
   * allowed to have a replica from the partition
   * (2) a leadership movement, the replica is a leader and the destination broker has a follower of the same partition.
   *
   * @param replica Replica that is affected from the given action type.
   * @param destinationBroker Destination broker.
   * @param clusterModel Cluster model.
   * @param actionType Action type.
   * @return {@code true} if the move is legit, {@code false} otherwise.
   */
  public static boolean legitMove(Replica replica,
                                  Broker destinationBroker,
                                  ClusterModel clusterModel,
                                  ActionType actionType) {
    switch (actionType) {
      case INTER_BROKER_REPLICA_MOVEMENT:
        return clusterModel.partition(replica.topicPartition()).canAssignReplicaToBroker(destinationBroker)
               && destinationBroker.replica(replica.topicPartition()) == null;
      case LEADERSHIP_MOVEMENT:
        return replica.isLeader() && destinationBroker.replica(replica.topicPartition()) != null;
      default:
        return false;
    }
  }

  /**
   * Check whether the proposed intra-broker action is legit. An action is legit if it is a replica movement across the
   * disks of the same broker, and the destination disk is alive.
   *
   * @param replica Replica that is affected from the given action type.
   * @param destinationDisk Destination disk.
   * @param actionType Action type.
   * @return {@code true} if the move is legit, {@code false} otherwise.
   */
  public static boolean legitMoveBetweenDisks(Replica replica,
                                              Disk destinationDisk,
                                              ActionType actionType) {
    return actionType == ActionType.INTRA_BROKER_REPLICA_MOVEMENT
           && destinationDisk != null
           && destinationDisk.broker() == replica.broker()
           && destinationDisk.isAlive();
  }

  /**
   * Get eligible replicas among the given candidate replicas for the proposed swap operation of the source replica.
   * Invariant-1: No replica is eligible if the candidate broker is excluded for leadership and the source replica is the leader.
   * Invariant-2: No replica is eligible if the candidate broker is excluded for replica move.
   *
   * @param clusterModel The state of the cluster.
   * @param sourceReplica Source replica for intended swap operation.
   * @param candidateReplicas Candidate replicas (from the same candidate broker) to swap with the source replica in the order
   *                          of attempts to swap.
   * @param optimizationOptions Options to take into account while applying the given action.
   * @return Eligible replicas for swap.
   */
  public static SortedSet<Replica> eligibleReplicasForSwap(ClusterModel clusterModel,
                                                           Replica sourceReplica,
                                                           SortedSet<Replica> candidateReplicas,
                                                           OptimizationOptions optimizationOptions) {
    if (candidateReplicas.isEmpty()) {
      return candidateReplicas;
    }
    Broker destinationBroker = candidateReplicas.first().broker();
    if (optimizationOptions.excludedBrokersForLeadership().contains(destinationBroker.id())
        && !sourceReplica.isOriginalOffline()
        && sourceReplica.isLeader()) {
      return Collections.emptySortedSet();
    }
    if (optimizationOptions.excludedBrokersForReplicaMove().contains(destinationBroker.id())
        && !sourceReplica.isOriginalOffline()) {
      return Collections.emptySortedSet();
    }

    // CASE#1: All candidate replicas are eligible if any of the following is true:
    // (1) there are no new brokers in the cluster,
    // (2) the given candidate set contains no replicas,
    // (3) the intended swap is between replicas of new brokers,
    // (4) the intended swap is between a replica on a new broker, which originally was in the destination broker, and
    // any replica in the destination broker.
    Broker sourceBroker = sourceReplica.broker();

    if (clusterModel.newBrokers().isEmpty()
        || (sourceBroker.isNew() && (destinationBroker.isNew() || sourceReplica.originalBroker() == destinationBroker))) {
      return candidateReplicas;
    }

    // CASE#2: A subset of candidate replicas might be eligible if only the destination broker is a new broker and it
    // contains replicas that were originally in the source broker.
    if (destinationBroker.isNew()) {
      candidateReplicas.removeIf(replica -> replica.originalBroker() != sourceBroker);
      return candidateReplicas;
    }

    // CASE#3: No swap is possible between old brokers when there are new brokers in the cluster.
    return Collections.emptySortedSet();
  }

  /**
   * Checks the replicas that are supposed to be moved away from the dead brokers or broken disks have been moved.
   * If there are still replicas on the dead brokers or broken disks, throws an exception.
   * @param clusterModel the cluster model to check.
   * @param goalName Goal name for which the sanity check is executed.
   * @throws OptimizationFailureException when there are still replicas on the dead brokers or on broken disks.
   */
  public static void ensureNoOfflineReplicas(ClusterModel clusterModel, String goalName)
      throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a decommissioned broker or on broken disk.
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      if (replica.isCurrentOffline()) {
        ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build();
        Broker broker = replica.broker();
        throw new OptimizationFailureException(String.format("[%s] Cannot remove %s from %s broker %d (has %d replicas).", goalName,
                                                             replica, broker.state(), broker.id(), broker.replicas().size()), recommendation);
      }
    }
  }

  /**
   * Checks for the broker with broken disk, the partitions of the replicas used to be on its broken disk does not have
   * any replica on this broker.
   * @param clusterModel the cluster model to check.
   * @param goalName Goal name for which the sanity check is executed.
   * @throws OptimizationFailureException when there are replicas hosted by broker with broken disk which belongs to the
   * same partition as the replica used to be hosted on broken disks
   */
  public static void ensureReplicasMoveOffBrokersWithBadDisks(ClusterModel clusterModel, String goalName)
      throws OptimizationFailureException {
    for (Broker broker : clusterModel.brokersWithBadDisks()) {
      for (Replica replica : broker.replicas()) {
        if (!clusterModel.partition(replica.topicPartition()).canAssignReplicaToBroker(broker)) {
          ProvisionRecommendation recommendation = new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build();
          throw new OptimizationFailureException(String.format("[%s] A replica of %s was moved back to broker %d with broken disk.", goalName,
                                                               clusterModel.partition(replica.topicPartition()), replica.broker().id()),
                                                 recommendation);
        }
      }
    }
  }

  /**
   * Get a filtered set of leaders from the given broker based on given filtering requirements.
   *
   * @param broker Broker whose replicas will be filters.
   * @param immigrantsOnly {@code true} if replicas should be filtered to ensure that they contain only the immigrants.
   * @return A filtered set of leaders from the given broker based on given filtering requirements.
   */
  public static Set<Replica> filterLeaders(Broker broker, boolean immigrantsOnly) {
    Set<Replica> filteredLeaders;
    if (immigrantsOnly) {
      filteredLeaders = new HashSet<>(broker.immigrantReplicas());
      filteredLeaders.removeIf(replica -> !replica.isLeader());
    } else {
      filteredLeaders = broker.leaderReplicas();
    }
    return filteredLeaders;
  }

  /**
   * Get the utilization of the broker for the given resource, or {@link #DEAD_BROKER_UTILIZATION} if the broker is dead.
   * The utilization of an alive broker corresponds to the ratio of its expected utilization to its capacity.
   *
   * @param broker Broker for which the resource utilization percentage has been queried.
   * @param resource Resource for the utilization percentage.
   * @return Utilization of the broker for the given resource as a double in [0.0,1.0].
   */
  public static double utilization(Broker broker, Resource resource) {
    double brokerCapacity = broker.capacityFor(resource);
    return brokerCapacity > 0 ? broker.load().expectedUtilizationFor(resource) / brokerCapacity : DEAD_BROKER_UTILIZATION;
  }

  /**
   * Get the latest average utilization percentage of all the alive disks on the broker.
   *
   * @param broker Broker for which the average disk utilization percentage has been queried.
   * @return Latest average utilization percentage of all the alive disks on the broker.
   */
  public static double averageDiskUtilizationPercentage(Broker broker) {
    double totalAliveDiskCapacity = 0;
    double totalAliveDiskUtilization = 0;
    for (Disk disk : broker.disks()) {
      if (disk.isAlive()) {
        totalAliveDiskCapacity += disk.capacity();
        totalAliveDiskUtilization += disk.utilization();
      }
    }
    return totalAliveDiskCapacity > 0 ? totalAliveDiskUtilization / totalAliveDiskCapacity : DEAD_BROKER_UTILIZATION;
  }

  /**
   * Get the latest utilization percentage of the disk, or {@link #DEAD_DISK_UTILIZATION} if the disk is dead.
   *
   * @param disk Disk to query.
   * @return Latest utilization percentage of the disk.
   */
  public static double diskUtilizationPercentage(Disk disk) {
    double diskCapacity = disk.capacity();
    return diskCapacity > 0 ? disk.utilization() / diskCapacity : DEAD_DISK_UTILIZATION;
  }

  /**
   * Get the estimated disk I/O rate of the given replica. The inbound network rate approximates disk writes, which apply
   * to both leader and follower replicas. The outbound network rate approximates disk reads, which apply to leader
   * replicas only (i.e. follower replicas have no outbound network load in the cluster model).
   *
   * @param replica Replica to query.
   * @param readWeight Weight of the outbound network rate (i.e. reads) in the estimation.
   * @return Estimated disk I/O rate of the replica, i.e. inbound network rate + readWeight * outbound network rate.
   */
  public static double replicaDiskIORate(Replica replica, double readWeight) {
    return replica.load().expectedUtilizationFor(Resource.NW_IN)
           + readWeight * replica.load().expectedUtilizationFor(Resource.NW_OUT);
  }

  /**
   * Get the estimated disk I/O rate of the given disk, i.e. the sum of estimated disk I/O rates of its replicas.
   *
   * @param disk Disk to query.
   * @param readWeight Weight of the outbound network rate (i.e. reads) in the estimation.
   * @return Estimated disk I/O rate of the disk.
   * @see #replicaDiskIORate(Replica, double)
   */
  public static double diskIORate(Disk disk, double readWeight) {
    double diskIORate = 0;
    for (Replica replica : disk.replicas()) {
      diskIORate += replicaDiskIORate(replica, readWeight);
    }
    return diskIORate;
  }

  /**
   * Sort replicas in ascending order of resource quantity present in the broker that they reside in terms of the
   * requested resource.
   *
   * @param replicas A list of replicas to be sorted by the amount of resources that their broker contains.
   * @param resource Resource for which the given replicas will be sorted.
   */
  public static void sortReplicasInAscendingOrderByBrokerResourceUtilization(List<Replica> replicas, Resource resource) {
    replicas.sort((r1, r2) -> {
      double expectedBrokerLoad1 = r1.broker().load().expectedUtilizationFor(resource);
      double expectedBrokerLoad2 = r2.broker().load().expectedUtilizationFor(resource);
      int result = Double.compare(expectedBrokerLoad1, expectedBrokerLoad2);
      return result == 0 ? Integer.compare(r1.broker().id(), r2.broker().id()) : result;
    });
  }

  /**
   * Generate the name of {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas}, which can be used to retrieve sorted
   * replicas in {@link ClusterModel}/{@link Broker}/{@link Disk}.
   *
   * @param goal The {@link Goal} which requests for new {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas}.
   * @param reverse Whether the {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas} tracks replicas in descending order or not.
   * @param leaderOnly Whether the {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas} tracks only leader replicas or all replicas.
   * @return A descriptive name for the {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas}.
   */
  public static String replicaSortName(Goal goal, boolean reverse, boolean leaderOnly) {
    return String.format("%s%s%s", goal.name(), reverse ? "-REVERSE" : "", leaderOnly ? "-LEADER" : "");
  }

  /**
   * Get the set of topics to rebalance for a soft goal.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics to exclude from rebalance.
   * @return Topics of self-healing eligible replicas if the given cluster model has any, all topics except the given
   * excluded topics otherwise.
   */
  public static Set<String> topicsToRebalance(ClusterModel clusterModel, Set<String> excludedTopics) {
    Set<String> topicsToRebalance;
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      topicsToRebalance = new HashSet<>();
      for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
        topicsToRebalance.add(replica.topicPartition().topic());
      }
    } else {
      topicsToRebalance = new HashSet<>(clusterModel.topics());
      topicsToRebalance.removeAll(excludedTopics);
    }

    return topicsToRebalance;
  }

  /**
   * Retain the subset of the given replicas that are currently offline and reside in the given broker.
   *
   * @param broker Broker, whose current offline replicas will be retrieved.
   * @param replicas Replicas, whose subset will be retained in a new set.
   * @return A new set of replicas that are currently offline and reside in the given broker.
   */
  public static Set<Replica> retainCurrentOfflineBrokerReplicas(Broker broker, Collection<Replica> replicas) {
    Set<Replica> offlineReplicas = new HashSet<>(replicas);
    offlineReplicas.retainAll(broker.currentOfflineReplicas());

    return offlineReplicas;
  }

  /**
   * Retrieve alive broker ids that are not excluded for replica moves.
   * Returns a hashset to provide constant time lookup guaranteed by a HashSet.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to retrieve excluded brokers for replica move
   * @return Alive broker ids that are not excluded for replica moves.
   */
  public static HashSet<Integer> aliveBrokersNotExcludedForReplicaMove(ClusterModel clusterModel,
                                                                       OptimizationOptions optimizationOptions) {
    Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForReplicaMove();
    return clusterModel.aliveBrokers()
                       .stream()
                       .map(Broker::id)
                       .filter(id -> !excludedBrokers.contains(id))
                       .collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Whenever appropriate, provide a message that is intended to help with the mitigation of of optimization failures
   * with the given optimization options.
   *
   * @param optimizationOptions Options to take into account during optimization.
   * @return A message that is intended to help with the mitigation of of optimization failures.
   */
  public static String mitigationForOptimizationFailures(OptimizationOptions optimizationOptions) {
    StringBuilder sb = new StringBuilder();
    int mitigationId = 0;

    if (optimizationOptions.onlyMoveImmigrantReplicas()) {
      sb.append(String.format("[%d] The optimization is limited to replicas to be added to/removed from brokers. Potential "
                              + "mitigation: First, rebalance the cluster using %s endpoint with a superset of "
                              + "hard-goals defined via %s config.", ++mitigationId, REBALANCE, HARD_GOALS_CONFIG));
    }
    if (!optimizationOptions.requestedDestinationBrokerIds().isEmpty()) {
      sb.append(String.format("[%d] The destination brokers are limited to %s. Potential mitigation: Relax the constraint "
                              + "on destination brokers using %s parameter.", ++mitigationId,
                              optimizationOptions.requestedDestinationBrokerIds(), DESTINATION_BROKER_IDS_PARAM));
    }
    if (!optimizationOptions.excludedBrokersForReplicaMove().isEmpty()) {
      sb.append(String.format("[%d] The following brokers are excluded from replica moves %s. Potential mitigation:"
                              + " Drop excluded brokers using %s endpoint.", ++mitigationId,
                              optimizationOptions.excludedBrokersForReplicaMove(), ADMIN));
    }
    if (!optimizationOptions.excludedBrokersForLeadership().isEmpty()) {
      sb.append(String.format("[%d] The following brokers are excluded from leadership moves %s. Potential mitigation:"
                              + " Drop brokers from exclusion for leadership move using %s endpoint.", ++mitigationId,
                              optimizationOptions.excludedBrokersForReplicaMove(), ADMIN));
    }
    if (!optimizationOptions.excludedTopics().isEmpty()) {
      sb.append(String.format("[%d] There are %d topics excluded from replica move. Potential mitigation: Remove selected "
                              + "topics from exclusion using %s parameter.", ++mitigationId, optimizationOptions.excludedTopics().size(),
                              EXCLUDED_TOPICS_PARAM));
    }

    if (sb.length() > 0) {
      sb.append(" Then, re-run your original request.");
    }

    return sb.toString();
  }

  /**
   * Compute the utilization upper/lower threshold in percent for the given type of resource.
   *  There are two cases described below (note that this description demonstrates the general idea):
   *
   *    1. Low utilization case (the average utilization percentage is not greater than the low utilization threshold
   *      Threshold lower bound = zero
   *      Threshold upper bound = the low utilization threshold multiplied with the balance margin
   *
   *    2. Not low utilization case
   *      Threshold lower bound = average utilization * (1 - balance percentage * balance margin)
   *      Threshold upper bound = average utilization * (1 + balance percentage * balance margin)
   *
   * @param avgUtilizationPercentage Average cluster utilization that excludes the capacity of brokers excluded for replica moves.
   * @param resource {@link Resource}
   * @param balancingConstraint Balancing constraint
   * @param isTriggeredByGoalViolation whether this computation is triggered by goal violation.
   * @param balanceMargin resource distribution goal balance margin
   * @param isLowerThreshold whether this method calculates resource utilization threshold upper bound or lower bound
   * @return The utilization upper/lower threshold in percent for the given type of resource
   */
  public static double computeResourceUtilizationBalanceThreshold(double avgUtilizationPercentage,
                                                                  Resource resource,
                                                                  BalancingConstraint balancingConstraint,
                                                                  boolean isTriggeredByGoalViolation,
                                                                  double balanceMargin,
                                                                  boolean isLowerThreshold) {

    if (balanceMargin >= 1) {
      throw new IllegalArgumentException("Balance margin must be less than 1.0. Got: " + balanceMargin);
    }
    boolean isLowUtilization = avgUtilizationPercentage <= balancingConstraint.lowUtilizationThreshold(resource);

    if (isLowerThreshold) {
      if (isLowUtilization) {
        return 0.0;
      }
      double balancePercentageWithMargin =
          balancePercentageWithMargin(isTriggeredByGoalViolation, balancingConstraint, resource, balanceMargin);
      return avgUtilizationPercentage * Math.max(0, (1 - balancePercentageWithMargin));

    } else {
      double balancePercentageWithMargin =
          balancePercentageWithMargin(isTriggeredByGoalViolation, balancingConstraint, resource, balanceMargin);
      double resourceBalanceThreshold = avgUtilizationPercentage * (1 + balancePercentageWithMargin);
      if (isLowUtilization) {
        return Math.max(resourceBalanceThreshold, balancingConstraint.lowUtilizationThreshold(resource) * balanceMargin);
      }
      return resourceBalanceThreshold;
    }
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   *
   * @param isTriggeredByGoalViolation whether this computation is triggered by goal violation.
   * @param balancingConstraint Balancing constraint
   * @param resource Resource for which the balance percentage with margin gets calculated
   * @param balanceMargin resource distribution goal balance margin
   * @return The rebalance threshold with a margin.
   */
  private static double balancePercentageWithMargin(boolean isTriggeredByGoalViolation,
                                                    BalancingConstraint balancingConstraint,
                                                    Resource resource,
                                                    double balanceMargin) {

    double balancePercentage = balancingConstraint.resourceBalancePercentage(resource);
    if (isTriggeredByGoalViolation) {
      // Make the balance threshold percentage more lenient in this case
      balancePercentage *= balancingConstraint.goalViolationDistributionThresholdMultiplier();
    }
    return (balancePercentage - 1) * balanceMargin;
  }

  /**
   * This method validates that provision response is {@link ProvisionStatus#OVER_PROVISIONED} only if:
   * <ul>
   *   <li>Number of alive brokers is larger than or equal to the value of {@link AnalyzerConfig#OVERPROVISIONED_MIN_BROKERS_CONFIG}.</li>
   *   <li>Expected number of alive brokers after the provisioning will still be larger than or equal to the maximum replication factor.</li>
   * </ul>
   * if any of the above conditions is violated then provision response is set to {@link ProvisionStatus#RIGHT_SIZED}
   *
   * @param provisionResponse Provision response after goal is optimization
   * @param clusterModel Cluster usage model
   * @param overprovisionedMinBrokers value of the {@link AnalyzerConfig#OVERPROVISIONED_MIN_BROKERS_CONFIG}
   * @return Validated provision response
   * @throws IllegalArgumentException when provision status is {@link ProvisionStatus#OVER_PROVISIONED}
   * and goal doesn't have exactly one recommendation
   */
  public static ProvisionResponse validateProvisionResponse(ProvisionResponse provisionResponse, ClusterModel clusterModel,
                                                            int overprovisionedMinBrokers) {
    if (provisionResponse.status() != ProvisionStatus.OVER_PROVISIONED) {
      return provisionResponse;
    }
    // ensure that a cluster is not identified as over provisioned unless it has the minimum required number of alive brokers
    if (clusterModel.aliveBrokers().size() < overprovisionedMinBrokers) {
      return new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    }
    // when status is OVER_PROVISIONED goal is expected to have exactly 1 recommendation
    if (provisionResponse.recommendationByRecommender().size() != 1) {
      throw new IllegalArgumentException(String.format("Expected to have exactly 1 provision recommendation, but got: %d",
                                                       provisionResponse.recommendationByRecommender().size()));
    }
    String goalName = provisionResponse.recommendationByRecommender().keySet().iterator().next();
    int numBrokersToDrop = provisionResponse.recommendationByRecommender().values().iterator().next().numBrokers();
    // this variable indicates the maximum number of brokers that can be safely dropped.
    int maxAllowedNumBrokersToDrop = clusterModel.aliveBrokers().size() - clusterModel.maxReplicationFactor();
    if (numBrokersToDrop <= maxAllowedNumBrokersToDrop) {
      // return provision response as it is if the recommended number of brokers to drop is smaller or equal to the max allowed number of
      // brokers that can be safely dropped
      return provisionResponse;
    } else if (maxAllowedNumBrokersToDrop > 0) {
      // change the recommended number of brokers to drop if the original recommendation can not be safely satisfied
      ProvisionRecommendation recommendation =
          new ProvisionRecommendation.Builder(ProvisionStatus.OVER_PROVISIONED).numBrokers(maxAllowedNumBrokersToDrop).build();
      return new ProvisionResponse(ProvisionStatus.OVER_PROVISIONED, recommendation, goalName);
    } else {
      // change provision response status to RIGHT_SIZED if none of the brokers can be safely dropped
      return new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    }
  }
}
