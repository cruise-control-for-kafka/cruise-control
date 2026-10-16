/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test fails for
 * (a) self healing if there are replicas on dead brokers or broken disks after self healing.
 * (b) adding a new broker causes the replicas to move between old brokers.
 * (c) rebalance if rebalance causes a worse goal state. See {@link #executeGoalsFor} for details of pass / fail status.
 * <p>
 * Test is called from:
 * (1) {@link RandomClusterTest} with different clusters (fixed goals).
 * (2) {@link RandomGoalTest} with different goals (fixed cluster).
 * (3) {@link DeterministicClusterTest} with different balancing constraints with deterministic clusters.
 * (4) {@link RandomSelfHealingTest} with dead brokers.
 */
final class OptimizationVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizationVerifier.class);

  private OptimizationVerifier() {

  }

  /**
   * See {@link #executeGoalsFor(BalancingConstraint, ClusterModel, List, Collection, List, boolean)}.
   *
   * @param constraint         Balancing constraint for the given cluster.
   * @param clusterModel       The state of the cluster.
   * @param goalNameByPriority Name of goals by the order of execution priority.
   * @param verifications      The verifications to make after the optimization.
   * @return Pass / fail status of a test.
   */
  static boolean executeGoalsFor(BalancingConstraint constraint,
                                 ClusterModel clusterModel,
                                 List<String> goalNameByPriority,
                                 List<Verification> verifications) throws Exception {
    return executeGoalsFor(constraint, clusterModel, goalNameByPriority, Collections.emptySet(), verifications);
  }

  /**
   * See {@link #executeGoalsFor(BalancingConstraint, ClusterModel, List, Collection, List, boolean)}.
   *
   * @param constraint         Balancing constraint for the given cluster.
   * @param clusterModel       The state of the cluster.
   * @param goalNameByPriority Name of goals by the order of execution priority.
   * @param excludedTopics     The excluded topics.
   * @param verifications      The verifications to make after the optimization.
   * @return Pass / fail status of a test.
   */
  static boolean executeGoalsFor(BalancingConstraint constraint,
                                 ClusterModel clusterModel,
                                 List<String> goalNameByPriority,
                                 Collection<String> excludedTopics,
                                 List<Verification> verifications) throws Exception {
    return executeGoalsFor(constraint, clusterModel, goalNameByPriority, excludedTopics, verifications, false);
  }

  /**
   * Execute given goals in the given cluster enforcing the given constraint. Return pass / fail status of a test.
   * A test fails if:
   * 1) Rebalance: During the optimization process, optimization of a goal leads to a worse cluster state (in terms of
   * the requirements of the same goal) than the cluster state just before starting the optimization.
   * 2) Self Healing: There are offline replicas on dead brokers or on broken disks of brokers after self healing or soft
   * goal (if any) moves non-immigrant online replicas.
   * 3) Adding a new broker causes the replicas to move among old brokers.
   *
   * @param constraint         Balancing constraint for the given cluster.
   * @param clusterModel       The state of the cluster.
   * @param goalNameByPriority Name of goals by the order of execution priority.
   * @param excludedTopics     The excluded topics.
   * @param verifications      The verifications to make after the optimization.
   * @param separateHardGoalsAndSoftGoals Whether separate hard goals and soft goals and apply to the cluster in two passes.
   * @return Pass / fail status of a test.
   */
  @SuppressWarnings("unchecked")
  static boolean executeGoalsFor(BalancingConstraint constraint,
                                 ClusterModel clusterModel,
                                 List<String> goalNameByPriority,
                                 Collection<String> excludedTopics,
                                 List<Verification> verifications,
                                 boolean separateHardGoalsAndSoftGoals) throws Exception {
    // Get the initial stats from the cluster.
    ClusterModelStats preOptimizedStats = clusterModel.getClusterStats(constraint);

    // Set goals by their priority.
    List<Goal> goalByPriority = new ArrayList<>(goalNameByPriority.size());
    for (String goalClassName : goalNameByPriority) {
      Class<? extends Goal> goalClass = (Class<? extends Goal>) Class.forName(goalClassName);
      try {
        Constructor<? extends Goal> constructor = goalClass.getDeclaredConstructor(BalancingConstraint.class);
        constructor.setAccessible(true);
        goalByPriority.add(constructor.newInstance(constraint));
      } catch (NoSuchMethodException badConstructor) {
        //Try default constructor
        goalByPriority.add(goalClass.newInstance());
      }
    }

    // Generate the goalOptimizer and optimize given goals.
    long startTime = System.currentTimeMillis();
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    StringJoiner stringJoiner = new StringJoiner(",");
    excludedTopics.forEach(stringJoiner::add);
    props.setProperty(AnalyzerConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG, stringJoiner.toString());
    GoalOptimizer goalOptimizer = new GoalOptimizer(new KafkaCruiseControlConfig(constraint.setProps(props)),
                                                    null,
                                                    Time.SYSTEM,
                                                    new MetricRegistry(),
                                                    EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));

    List<Goal> goalsOfFirstPass;
    OptimizerResult resultOfFirstPass;
    List<Goal> goalsOfSecondPass = null;
    OptimizerResult resultOfSecondPass = null;

    if (!separateHardGoalsAndSoftGoals) {
      goalsOfFirstPass = goalByPriority;
      resultOfFirstPass = goalOptimizer.optimizations(clusterModel, goalsOfFirstPass, new OperationProgress());
    } else {
      // First optimize with all hard goals if there is any.
      goalsOfFirstPass = goalByPriority.stream().filter(Goal::isHardGoal).collect(Collectors.toList());
      resultOfFirstPass = goalsOfFirstPass.isEmpty() ? null
                                                     : goalOptimizer.optimizations(clusterModel, goalsOfFirstPass, new OperationProgress());

      // If provided goal list contains any soft goals, then optimize with all goals.
      // In this iteration, all the hard goals will not generate any new proposal since they are already satisfied in previous
      // iteration (otherwise an exception has already been thrown). The optimize result of this iteration will only contains
      // proposals generated by soft goals.
      goalsOfSecondPass = goalByPriority.stream().filter(g -> !g.isHardGoal()).collect(Collectors.toList());
      resultOfSecondPass = goalsOfSecondPass.isEmpty() ? null
                                                       : goalOptimizer.optimizations(clusterModel, goalByPriority, new OperationProgress());
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Took {} ms to execute {} to generate {} proposals.", System.currentTimeMillis() - startTime,
                goalByPriority,
                (resultOfFirstPass == null ? 0 : resultOfFirstPass.goalProposals().size())
                + (resultOfSecondPass == null ? 0 : resultOfSecondPass.goalProposals().size()));
    }

    for (Verification verification : verifications) {
      switch (verification) {
        case GOAL_VIOLATION:
          if ((resultOfFirstPass != null && !verifyGoalViolations(resultOfFirstPass))
              || (resultOfSecondPass != null && !verifyGoalViolations(resultOfSecondPass))) {
            return false;
          }
          break;
        case NEW_BROKERS:
          if (!clusterModel.newBrokers().isEmpty() && !verifyNewBrokers(clusterModel, constraint)) {
            return false;
          }
          break;
        case BROKEN_BROKERS:
          Set<Replica> originalOfflineReplicas = clusterModel.selfHealingEligibleReplicas();
          if (!clusterModel.deadBrokers().isEmpty()) {
            if (!verifyBrokenBrokers(clusterModel)) {
              return false;
            }
            // Ensure that soft goals only move offline replicas or immigrant replicas.
            if (resultOfFirstPass != null
                && !verifySoftGoalReplicaMovements(resultOfFirstPass, null, originalOfflineReplicas, goalsOfFirstPass)) {
              return false;
            }
            if (resultOfSecondPass != null
                && !verifySoftGoalReplicaMovements(resultOfSecondPass, resultOfFirstPass, originalOfflineReplicas, goalsOfSecondPass)) {
              return false;
            }
          }
          break;
        case REGRESSION:
          if (clusterModel.selfHealingEligibleReplicas().isEmpty()
              && ((resultOfFirstPass != null && !verifyRegression(resultOfFirstPass, preOptimizedStats))
              || (resultOfSecondPass != null && !verifyRegression(resultOfSecondPass, preOptimizedStats)))) {
            return false;
          }
          break;
        default:
          throw new IllegalStateException("Invalid verification " + verification);
      }
    }
    return true;
  }

  private static boolean verifyGoalViolations(OptimizerResult optimizerResult) {
    // Check if there are still goals violated after the optimization.
    if (!optimizerResult.violatedGoalsAfterOptimization().isEmpty()) {
      LOG.error("Failed to optimize goal {}", optimizerResult.violatedGoalsAfterOptimization());
      System.out.println(optimizerResult.clusterModelStats().toString());
      return false;
    } else {
      return true;
    }
  }

  private static boolean verifyBrokenBrokers(ClusterModel clusterModel) {
    for (Broker deadBroker : clusterModel.deadBrokers()) {
      if (!deadBroker.replicas().isEmpty()) {
        LOG.error("Failed to move {} replicas on dead broker {} to other brokers.", deadBroker.replicas().size(),
                  deadBroker.id());
        return false;
      }
    }
    Set<Broker> brokersHavingOfflineReplicasOnBadDisks = clusterModel.brokersHavingOfflineReplicasOnBadDisks();
    if (!brokersHavingOfflineReplicasOnBadDisks.isEmpty()) {
      for (Broker brokerHavingOfflineReplicasOnBadDisks : brokersHavingOfflineReplicasOnBadDisks) {
        LOG.error("Failed to move offline replicas from broker with bad disk {}.",
                  brokerHavingOfflineReplicasOnBadDisks.id());
      }
      return false;
    }
    return true;
  }

  private static boolean verifySoftGoalReplicaMovements(OptimizerResult result,
                                                        OptimizerResult resultOfPreviousPass,
                                                        Set<Replica> offlineReplicas,
                                                        List<Goal> goalByPriority) {
    // If there is any hard goal, skip the check since hard goal is allowed to move non-immigrant online replicas in
    // self-healing cluster.
    if (goalByPriority.stream().anyMatch(Goal::isHardGoal)) {
      return true;
    }

    // Populate offline replicas.
    Map<TopicPartition, Set<Integer>> offlineReplicasByTopicPartition = new HashMap<>();
    offlineReplicas.forEach(r -> {
      offlineReplicasByTopicPartition.putIfAbsent(r.topicPartition(), new HashSet<>());
      offlineReplicasByTopicPartition.get(r.topicPartition()).add(r.originalBroker().id());
    });

    // Populate replicas moved in previous pass, these replicas will be treated as immigrant replicas when soft goals calculating
    // execution proposals.
    Map<TopicPartition, Set<ReplicaPlacementInfo>> replicasMovedInPreviousPass = new HashMap<>();
    if (resultOfPreviousPass != null) {
      resultOfPreviousPass.goalProposals().forEach(p -> replicasMovedInPreviousPass.put(p.topicPartition(), p.replicasToAdd()));
    }

    for (ExecutionProposal p : result.goalProposals()) {
      for (ReplicaPlacementInfo r : p.replicasToRemove()) {
        // Soft goals should only generate proposals that
        // (1) move offline replicas from dead brokers/disks
        // (2) move immigrant replicas from alive brokers
        if (!(offlineReplicasByTopicPartition.containsKey(p.topicPartition())
            && offlineReplicasByTopicPartition.get(p.topicPartition()).contains(r.brokerId()))
            && (!replicasMovedInPreviousPass.containsKey(p.topicPartition())
            || !replicasMovedInPreviousPass.get(p.topicPartition()).contains(r))) {
          LOG.error("Self-healing replica movement generated by soft goals must be limited to only moving offline or immigrant replicas."
                    + " Proposal violated: {}, goals used: {}).", p, goalByPriority);
          return false;
        }
      }
    }
    return true;
  }

  private static boolean verifyNewBrokers(ClusterModel clusterModel, BalancingConstraint constraint) {
    for (Broker broker : clusterModel.aliveBrokers()) {
      if (!broker.isNew()) {
        for (Replica replica : broker.replicas()) {
          if (replica.originalBroker() != broker) {
            LOG.error("Broker {} is not a new broker but has received new replicas", broker.id());
            return false;
          }
        }
      }
    }
    for (Broker broker : clusterModel.newBrokers()) {
      // We can only check the disk resource.
      Resource r = Resource.DISK;
      double utilizationLowerThreshold =
          clusterModel.load().expectedUtilizationFor(r) / clusterModel.capacityFor(r) * (2 - constraint.resourceBalancePercentage(r));
      double brokerUtilization = broker.load().expectedUtilizationFor(r) / broker.capacityFor(r);
      if (brokerUtilization < utilizationLowerThreshold) {
        LOG.error("Broker {} is still underutilized for resource {}. Broker utilization is {}, the "
                  + "lower threshold is {}", broker, r, brokerUtilization, utilizationLowerThreshold);
        return false;
      }
    }
    return true;
  }

  private static boolean verifyRegression(OptimizerResult optimizerResult,
                                          ClusterModelStats preOptimizationStats) {
    // Check whether test has failed for rebalance: fails if rebalance caused a worse goal state after rebalance.
    Map<String, ClusterModelStats> statsByGoalName = optimizerResult.statsByGoalName();
    Map<String, Goal.ClusterModelStatsComparator> clusterModelStatsComparatorByGoalName
        = optimizerResult.clusterModelStatsComparatorByGoalName();
    ClusterModelStats preStats = preOptimizationStats;
    for (Map.Entry<String, ClusterModelStats> entry : statsByGoalName.entrySet()) {
      Goal.ClusterModelStatsComparator comparator = clusterModelStatsComparatorByGoalName.get(entry.getKey());
      boolean success = comparator.compare(entry.getValue(), preStats) >= 0;
      if (!success) {
        LOG.error("Failed goal comparison {}. {}", entry.getKey(), comparator.explainLastComparison());
        return false;
      }
      preStats = entry.getValue();
    }
    return true;
  }

  enum Verification {
    GOAL_VIOLATION, BROKEN_BROKERS, NEW_BROKERS, REGRESSION,
  }
}
