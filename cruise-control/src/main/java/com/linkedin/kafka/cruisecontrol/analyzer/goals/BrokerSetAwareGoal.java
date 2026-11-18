/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.kafka.cruisecontrol.config.ReplicaToBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.exception.ReplicaToBrokerSetMappingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolutionHelper;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * HARD GOAL: Generate proposals to provide brokerSet-aware replica distribution.
 *
 * <p>
 * BrokerSets are defined as a group of Brokers within a cluster. With BrokerSets, a topic can be placed within the boundary of a BrokerSet.
 * BrokerSets help reduce blast radius and provide a layer of isolation within a Kafka Cluster. BrokerSets can expand their boundaries or shrink.
 * </p>
 * <p>
 * Assuming a case where RF is 3, the distribution of topic-partitions can look like this:
 * </p>
 *
 * <pre>
 * BrokerSet RED                | BrokerSet BLUE              | BrokerSet GREEN
 * -------------------------------------------------------------------------------
 * Broker1  t1p1-0,  t2p1-2     | Broker4 t3p1-1              | Broker7
 * Broker2  t2p1-0,  t1p1-1     | Broker5 t3p1-2              | Broker8
 * Broker3  t1p1-2,  t2p1-1     | Broker6 t3p1-0              | Broker9
 * </pre>
 *
 * <p>
 * There could also be some topics that do not follow the BrokerSet boundary.
 * Such topics should be excluded from movements using the {topics.excluded.from.partition.movement} property
 * </p>
 * <p>
 * Alternatively, a {@link ReplicaToBrokerSetMappingPolicy} may return {@code null} for a replica, which leaves that replica
 * unconstrained: this goal does not move it while it is online, does not veto moves of it, and does not check its placement.
 * An offline unconstrained replica is moved to any alive broker. This lets a policy such as
 * {@link com.linkedin.kafka.cruisecontrol.config.TopicPatternBrokerSetMappingPolicy} pin only selected topics to broker sets.
 * </p>
 * <p>
 * Supported Optimization Options are:
 * </p>
 * <pre>
 * {@link OptimizationOptions#excludedTopics()} -&gt; Handled by {@link BrokerSetAwareGoal}
 * {@link OptimizationOptions#excludedBrokersForReplicaMove()} -&gt; Handled by {@link AbstractGoal}
 * {@link OptimizationOptions#requestedDestinationBrokerIds()} -&gt; Handled by {@link AbstractGoal}
 * {@link OptimizationOptions#onlyMoveImmigrantReplicas()} -&gt; Handled by {@link BrokerSetAwareGoal}
 * </pre>
 */
public class BrokerSetAwareGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerSetAwareGoal.class);
  private BrokerSetResolutionHelper _brokerSetResolutionHelper;
  private Map<String, Set<Integer>> _brokersByBrokerSet;
  private ReplicaToBrokerSetMappingPolicy _replicaToBrokerSetMappingPolicy;
  private Set<String> _excludedTopics;
  private Set<String> _mustHaveTopicLeadersPerBroker;

  /**
   * Constructor for Broker Set Aware Goal.
   */
  public BrokerSetAwareGoal() {
  }

  /**
   * Package private for unit test.
   */
  BrokerSetAwareGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  /**
   * Initialize states that this goal requires. e.g. -
   * <ol>
   * <li>Run sanity checks regarding hard goals requirements like all topics exclusion or all brokers exclusion.</li>
   * <li>Create sorting/filtering of replicas in cluster model for this goal.</li>
   * <li>Initialize BrokerSet data store.</li>
   * </ol>
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   *
   * @param clusterModel        The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    // This is used to identify brokers not excluded for replica moves.
    final HashSet<Integer> brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      ProvisionRecommendation recommendation =
          new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(clusterModel.maxReplicationFactor()).build();
      throw new OptimizationFailureException(String.format("[%s] All alive brokers are excluded from replica moves.", name()),
                                             recommendation);
    }

    // Whether the {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas} tracks replicas in descending order or not.
    boolean tracksReplicasInReverseOrder = false;
    // Whether the {@link com.linkedin.kafka.cruisecontrol.model.SortedReplicas} tracks only leader replicas or all replicas.
    boolean tracksOnlyLeaderReplicas = false;

    _mustHaveTopicLeadersPerBroker = Collections.unmodifiableSet(
        Utils.getTopicNamesMatchedWithPattern(_balancingConstraint.topicsWithMinLeadersPerBrokerPattern(), clusterModel::topics));
    _excludedTopics = Stream.of(
        _mustHaveTopicLeadersPerBroker, optimizationOptions.excludedTopics()).flatMap(Set::stream).collect(Collectors.toUnmodifiableSet());

    /*
    Replicas using selection/priority/score functions can be filtered/ordered/scored to be picked for movement during balancing act.
    If the Optimization Options requires movement of only immigrant replicas, then we pick immigrant replicas.
    Similarly, if excluded topics are specified, then for optimizing efficiently, then we select only included topics' replicas.
    The reason for picking the below selection functions is to abide by the Optimization Options.
    Other functions like selecting leaders etc are not relevant for BrokerSetAwareGoal.
    */
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     optimizationOptions.onlyMoveImmigrantReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(_excludedTopics),
                                                     !_excludedTopics.isEmpty())
                              .trackSortedReplicasFor(replicaSortName(this, tracksReplicasInReverseOrder, tracksOnlyLeaderReplicas),
                                                      clusterModel);

    BrokerSetResolver brokerSetResolver = _balancingConstraint.brokerSetResolver();
    _brokerSetResolutionHelper = new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);
    _brokersByBrokerSet = _brokerSetResolutionHelper.brokersByBrokerSetId();
    _replicaToBrokerSetMappingPolicy = _balancingConstraint.replicaToBrokerSetMappingPolicy();
    maybeWarnPinnedTopicsWithMinLeadersPerBroker(clusterModel);
  }

  /**
   * Topics that must have a minimum number of leaders per broker are excluded from this goal. Warn once per optimization if
   * the mapping policy binds any of these topics to a broker set, since that binding is not enforced.
   *
   * @param clusterModel The state of the cluster.
   */
  private void maybeWarnPinnedTopicsWithMinLeadersPerBroker(ClusterModel clusterModel) {
    if (_mustHaveTopicLeadersPerBroker.isEmpty()) {
      return;
    }
    Map<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();
    Set<String> pinnedTopics = new TreeSet<>();
    for (String topic : _mustHaveTopicLeadersPerBroker) {
      List<Partition> partitions = partitionsByTopic.get(topic);
      if (partitions == null || partitions.isEmpty()) {
        continue;
      }
      try {
        if (_replicaToBrokerSetMappingPolicy.brokerSetIdForReplica(partitions.get(0).replicas().get(0), clusterModel,
                                                                   _brokerSetResolutionHelper) != null) {
          pinnedTopics.add(topic);
        }
      } catch (BrokerSetResolutionException | ReplicaToBrokerSetMappingException e) {
        LOG.debug("[{}] Failed to map topic {} to a broker set.", name(), topic, e);
      }
    }
    if (!pinnedTopics.isEmpty()) {
      LOG.warn("[{}] Topics {} are mapped to a broker set but are excluded from this goal because they match {}.", name(), pinnedTopics,
               AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG);
    }
  }

  /**
   * Update goal state.
   * Sanity check: After completion of balancing / self-healing, confirm that replicas of each topic reside fully in a
   * broker set.
   *
   * @param clusterModel        The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    // Sanity check to confirm that the final distribution is broker set aware.
    ensureBrokerSetAware(clusterModel, optimizationOptions);
    finish();
  }

  /**
   * Sanity check to confirm that the final distribution is brokerSet aware. Only replicas that this goal was allowed to move are
   * checked, i.e. replicas of topics that are not excluded and, if only immigrant replicas can be moved, immigrant replicas.
   * <ul>
   *   <li>A topic whose checked replicas are all unconstrained (mapped to no broker set) is skipped.</li>
   *   <li>A topic whose checked replicas are all constrained must reside on the brokers of a single broker set.</li>
   *   <li>Each constrained replica must reside on a broker of the broker set that it is mapped to.</li>
   * </ul>
   *
   * @param clusterModel        The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  private void ensureBrokerSetAware(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    for (Map.Entry<String, List<Partition>> partitionsByTopic : clusterModel.getPartitionsByTopic().entrySet()) {
      String topicName = partitionsByTopic.getKey();
      if (_excludedTopics.contains(topicName)) {
        continue;
      }
      Map<Replica, String> brokerSetIdByConstrainedReplica = new LinkedHashMap<>();
      Set<Integer> allBrokersForTopic = new HashSet<>();
      boolean hasUnconstrainedReplica = false;
      for (Partition partition : partitionsByTopic.getValue()) {
        for (Replica replica : partition.replicas()) {
          if (onlyMoveImmigrantReplicas && !replica.isImmigrant()) {
            continue;
          }
          allBrokersForTopic.add(replica.broker().id());
          String brokerSetId = _replicaToBrokerSetMappingPolicy.brokerSetIdForReplica(replica, clusterModel, _brokerSetResolutionHelper);
          if (brokerSetId == null) {
            hasUnconstrainedReplica = true;
          } else {
            brokerSetIdByConstrainedReplica.put(replica, brokerSetId);
          }
        }
      }
      if (brokerSetIdByConstrainedReplica.isEmpty()) {
        continue;
      }
      // Checks if a topic's brokers do not all live in a single brokerSet
      if (!hasUnconstrainedReplica
          && _brokersByBrokerSet.values().stream().noneMatch(brokerSetBrokers -> brokerSetBrokers.containsAll(allBrokersForTopic))) {
        throw new OptimizationFailureException(
            String.format("[%s] Topic %s is not brokerSet-aware. brokers (%s).", name(), topicName, allBrokersForTopic));
      }
      // Checks if each constrained replica lives in the brokerSet that it is mapped to
      for (Map.Entry<Replica, String> entry : brokerSetIdByConstrainedReplica.entrySet()) {
        Replica replica = entry.getKey();
        Set<Integer> brokerSetBrokers = _brokersByBrokerSet.get(entry.getValue());
        if (brokerSetBrokers == null || !brokerSetBrokers.contains(replica.broker().id())) {
          throw new OptimizationFailureException(
              String.format("[%s] Replica %s of topic %s is not on a broker of its brokerSet %s (%s).", name(), replica, topicName,
                            entry.getValue(), brokerSetBrokers));
        }
      }
    }
  }

  /**
   * brokerSet-awareness violations can be resolved with replica movements.
   *
   * @param broker              Broker to be balanced.
   * @param clusterModel        The state of the cluster.
   * @param optimizedGoals      Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);

    String currentBrokerSetId = _brokerSetResolutionHelper.brokerSetId(broker.id());

    for (Replica replica : broker.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(true)) {
      String eligibleBrokerSetIdForReplica =
          _replicaToBrokerSetMappingPolicy.brokerSetIdForReplica(replica, clusterModel, _brokerSetResolutionHelper);
      boolean isOffline = !broker.isAlive() || replica.isCurrentOffline();

      if (eligibleBrokerSetIdForReplica == null) {
        LOG.trace("Replica {} is not bound to any brokerSet.", replica);
        // An unconstrained replica is moved only if it is offline, in which case it can move to any alive broker
        if (isOffline && maybeApplyBalancingAction(clusterModel, replica, clusterModel.aliveBrokers(),
                                                   ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
          ProvisionRecommendation recommendation =
              new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(1).build();
          throw new OptimizationFailureException(
              String.format("[%s] Cannot move offline replica %s, which is not bound to any brokerSet, to an alive broker.", name(),
                            replica), recommendation);
        }
        continue;
      }

      // If the re-balancing action is performed within the broker set boundary, we are good
      // Also, if the topics that are configured to have leaders on each broker within the cluster, then those topics won't be rebalanced
      if (!isOffline && eligibleBrokerSetIdForReplica.equals(currentBrokerSetId)) {
        continue;
      }
      // If the brokerSet awareness condition is violated. Move replica to an eligible broker
      Set<Broker> eligibleBrokers = clusterModel.aliveBrokers()
                                                .stream()
                                                .filter(b -> _brokersByBrokerSet.get(eligibleBrokerSetIdForReplica).contains(b.id()))
                                                .collect(Collectors.toSet());
      if (maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals,
                                    optimizationOptions) == null) {
        // If balancing action can not be applied, provide recommendation to add new Brokers.
        ProvisionRecommendation recommendation =
            new ProvisionRecommendation.Builder(ProvisionStatus.UNDER_PROVISIONED).numBrokers(clusterModel.maxReplicationFactor()).build();
        throw new OptimizationFailureException(
            String.format("[%s] Cannot move replica %s to %s on brokerSet %s", name(), replica, eligibleBrokers,
                          eligibleBrokerSetIdForReplica), recommendation);
      }
    }
  }

  /**
   * Check whether the given action is acceptable by this goal. The following actions are acceptable:
   * <ul>
   *   <li>All leadership moves</li>
   *   <li>Replica moves that do not violate {@link #doesReplicaMoveViolateActionAcceptance(ClusterModel, Replica, Broker)}</li>
   *   <li>Swaps that do not violate {@link #doesReplicaMoveViolateActionAcceptance(ClusterModel, Replica, Broker)}
   *   in both direction</li>
   * </ul>
   *
   * @param action       Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#BROKER_REJECT} if the action is rejected due to violating brokerSet awareness in the destination
   * broker after moving source replica to destination broker, {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // This block is introduced to accept any offline replicas moved by MinTopicLeadersPerBrokerGoal.
    // If MinTopicLeadersPerBrokerGoal used with BrokerSetAwareGoal, the topics that are required to have a minimum leader per broker by
    // MinTopicLeadersPerBrokerGoal, should be excluded from BrokerSetAwareGoal.
    if (_mustHaveTopicLeadersPerBroker.contains(action.topic())) {
      return ACCEPT;
    }
    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case INTER_BROKER_REPLICA_SWAP:
        if (doesReplicaMoveViolateActionAcceptance(clusterModel,
                                                   clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition()),
                                                   clusterModel.broker(action.destinationBrokerId()))) {
          return BROKER_REJECT;
        }
        // SWAP Actions are performed by distribution goals to swap distinct partitions replicas to swap load in or out
        // We would need to check both ways in swap cases that original brokerSet of both replicas matches the brokerSet of destination broker
        if (action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP
            && doesReplicaMoveViolateActionAcceptance(clusterModel,
                                                      clusterModel.broker(action.destinationBrokerId())
                                                                  .replica(action.destinationTopicPartition()),
                                                      clusterModel.broker(action.sourceBrokerId()))) {
          return REPLICA_REJECT;
        }
        return ACCEPT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  protected boolean doesReplicaMoveViolateActionAcceptance(ClusterModel clusterModel, Replica sourceReplica, Broker destinationBroker) {
    // Destination broker cannot be in any other brokerSet.
    // A replica's current broker set id can be different from the expected broker id defined by replica broker set mapping policy
    try {
      String expectedBrokerSetId =
          _replicaToBrokerSetMappingPolicy.brokerSetIdForReplica(sourceReplica, clusterModel, _brokerSetResolutionHelper);
      // A replica that is not bound to any broker set can move anywhere
      if (expectedBrokerSetId == null) {
        return false;
      }
      String destinationBrokerSetId = _brokerSetResolutionHelper.brokerSetId(destinationBroker.id());

      return !expectedBrokerSetId.equals(destinationBrokerSetId);
    } catch (BrokerSetResolutionException | ReplicaToBrokerSetMappingException e) {
      LOG.error(String.format("[%s] Replica move violated action acceptance", name()), e);
      return true;
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    // Stats are irrelevant to a hard goal. The optimization would already fail if the goal requirements are not met.
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // BrokerSet-awareness does not need load information at all
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  /**
   * While attempting to apply balancing action during re-balance,
   * check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action       Action containing information about potential modification to the given cluster model.
   * @return {@code true} if requirements of this goal are not violated if this action is applied to the given cluster state,
   * {@code false} otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    return true;
  }
}
