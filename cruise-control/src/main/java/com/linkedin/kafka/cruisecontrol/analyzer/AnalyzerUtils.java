/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.RawAndDerivedResource;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;


/**
 * A util class for Analyzer.
 */
public final class AnalyzerUtils {
  public static final double EPSILON = 1E-5;
  private AnalyzerUtils() {

  }

  /**
   * Get the diff represented by the set of balancing proposals to move from initial to final distribution. This method also
   * performs a sanity check for each proposal to ensure that topic partition's replication factor does not change.
   *
   * @param initialReplicaDistribution Initial distribution of replicas over the cluster.
   * @param initialLeaderDistribution Initial distribution of the leaders.
   * @param optimizedClusterModel The optimized cluster model.
   * @return The diff represented by the set of balancing proposals to move from initial to final distribution.
   */
  public static Set<ExecutionProposal> getDiff(Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution,
                                               Map<TopicPartition, ReplicaPlacementInfo> initialLeaderDistribution,
                                               ClusterModel optimizedClusterModel) {
    return getDiff(initialReplicaDistribution, initialLeaderDistribution, optimizedClusterModel, false);
  }

  /**
   * Get the diff represented by the set of balancing proposals to move from initial to final distribution.
   *
   * @param initialReplicaDistribution Initial distribution of replicas over the cluster.
   * @param initialLeaderDistribution Initial distribution of the leaders.
   * @param optimizedClusterModel The optimized cluster model.
   * @param skipReplicationFactorChangeCheck Whether skip sanity check of topic partition's replication factor change before
   *                                         and after optimization.
   * @return The diff represented by the set of balancing proposals to move from initial to final distribution.
   */
  public static Set<ExecutionProposal> getDiff(Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution,
                                               Map<TopicPartition, ReplicaPlacementInfo> initialLeaderDistribution,
                                               ClusterModel optimizedClusterModel,
                                               boolean skipReplicationFactorChangeCheck) {
    Map<TopicPartition, List<ReplicaPlacementInfo>> finalReplicaDistribution = optimizedClusterModel.getReplicaDistribution();
    sanityCheckReplicaDistribution(initialReplicaDistribution, finalReplicaDistribution, skipReplicationFactorChangeCheck);

    // Generate a set of execution proposals to represent the diff between initial and final distribution.
    Set<ExecutionProposal> diff = new HashSet<>();
    for (Map.Entry<TopicPartition, List<ReplicaPlacementInfo>> entry : initialReplicaDistribution.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<ReplicaPlacementInfo> initialReplicas = entry.getValue();
      List<ReplicaPlacementInfo> finalReplicas = finalReplicaDistribution.get(tp);
      Replica finalLeader = optimizedClusterModel.partition(tp).leader();
      ReplicaPlacementInfo finalLeaderPlacementInfo = new ReplicaPlacementInfo(finalLeader.broker().id(),
                                                                               finalLeader.disk() == null ? null : finalLeader.disk().logDir());
      // The partition has no change.
      if (finalReplicas.equals(initialReplicas) && initialLeaderDistribution.get(tp).equals(finalLeaderPlacementInfo)) {
        continue;
      }
      // We need to adjust the final broker list order to ensure the final leader is the first replica.
      if (finalLeaderPlacementInfo != finalReplicas.get(0)) {
        int leaderPos = finalReplicas.indexOf(finalLeaderPlacementInfo);
        finalReplicas.set(leaderPos, finalReplicas.get(0));
        finalReplicas.set(0, finalLeaderPlacementInfo);
      }
      double partitionSize = optimizedClusterModel.partition(tp).leader().load().expectedUtilizationFor(Resource.DISK);
      diff.add(new ExecutionProposal(tp, (int) partitionSize, initialLeaderDistribution.get(tp), initialReplicas, finalReplicas));
    }
    return diff;
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>Initial and final replica distribution have exactly the same partitions.</li>
   *   <li>(Optional) Replication factor of the initial and final replicas in distributions for each partition is the same.</li>
   * </ul>
   *
   * @param initialReplicaDistribution Initial distribution of replicas over the cluster.
   * @param finalReplicaDistribution The final distribution of replicas in the cluster at the point of call.
   * @param skipReplicationFactorChangeCheck Whether skip sanity check of topic partition's replication factor change before
   *                                         and after optimization.
   */
  private static void sanityCheckReplicaDistribution(Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution,
                                                     Map<TopicPartition, List<ReplicaPlacementInfo>> finalReplicaDistribution,
                                                     boolean skipReplicationFactorChangeCheck) {
    // Sanity check to make sure that given distributions contain the same replicas.
    if (!initialReplicaDistribution.keySet().equals(finalReplicaDistribution.keySet())) {
      throw new IllegalArgumentException("Attempt to diff distributions with different partitions.");
    }
    if (!skipReplicationFactorChangeCheck) {
      initialReplicaDistribution.forEach((tp, initialReplicas) -> {
        if (finalReplicaDistribution.get(tp).size() != initialReplicas.size()) {
          throw new IllegalArgumentException("Attempt to diff distributions with modified replication factor.");
        }
      });
    }
  }

  /**
   * Get whether there is any diff represented by a set of balancing proposals to move from the initial to final distribution.
   *
   * @param initialReplicaDistribution Initial distribution of replicas over the cluster.
   * @param initialLeaderDistribution Initial distribution of the leaders.
   * @param optimizedClusterModel The optimized cluster model.
   * @return The diff represented by the set of balancing proposals to move from initial to final distribution.
   */
  public static boolean hasDiff(Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution,
                                Map<TopicPartition, ReplicaPlacementInfo> initialLeaderDistribution,
                                ClusterModel optimizedClusterModel) {
    Map<TopicPartition, List<ReplicaPlacementInfo>> finalReplicaDistribution = optimizedClusterModel.getReplicaDistribution();
    sanityCheckReplicaDistribution(initialReplicaDistribution, finalReplicaDistribution, false);

    boolean hasDiff = false;
    for (Map.Entry<TopicPartition, List<ReplicaPlacementInfo>> entry : initialReplicaDistribution.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<ReplicaPlacementInfo> initialReplicas = entry.getValue();
      List<ReplicaPlacementInfo> finalReplicas = finalReplicaDistribution.get(tp);

      if (!finalReplicas.equals(initialReplicas)) {
        hasDiff = true;
        break;
      } else {
        Replica finalLeader = optimizedClusterModel.partition(tp).leader();
        ReplicaPlacementInfo finalLeaderPlacementInfo = new ReplicaPlacementInfo(finalLeader.broker().id(),
                                                                                 finalLeader.disk() == null ? null : finalLeader.disk().logDir());
        if (!initialLeaderDistribution.get(tp).equals(finalLeaderPlacementInfo)) {
          hasDiff = true;
          break;
        }
        // The partition has no change.
      }
    }
    return hasDiff;
  }

  /**
   * Check whether the given proposal is acceptable for all of the given optimized goals.
   *
   * @param optimizedGoals Optimized goals to check whether they accept the given proposal.
   * @param proposal       Proposal to be checked for acceptance.
   * @param clusterModel   The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the given proposal is acceptable for all the given optimized goals, the
   * reject flag (e.g. {@link ActionAcceptance#REPLICA_REJECT}, {@link ActionAcceptance#BROKER_REJECT}) otherwise.
   */
  public static ActionAcceptance isProposalAcceptableForOptimizedGoals(Set<Goal> optimizedGoals,
                                                                       BalancingAction proposal,
                                                                       ClusterModel clusterModel) {
    for (Goal optimizedGoal : optimizedGoals) {
      ActionAcceptance actionAcceptance = optimizedGoal.actionAcceptance(proposal, clusterModel);
      if (actionAcceptance != ACCEPT) {
        return actionAcceptance;
      }
    }
    return ACCEPT;
  }

  /**
   * Compare the given values. Return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   *
   * @param d1 The first {@code double} to compare.
   * @param d2 The second {@code double} to compare.
   * @param resource the resource the current comparison is for.
   * @return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   */
  public static int compare(double d1, double d2, Resource resource) {
    double epsilon = resource.epsilon(d1, d2);
    return compare(d1, d2, epsilon);
  }

  /**
   * Compare the given values. Return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   *
   * @param d1 The first {@code double} to compare.
   * @param d2 The second {@code double} to compare.
   * @param epsilon Epsilon to be used in comparing doubles.
   * @return 1 if first &gt; second, -1 if first &lt; second, 0 otherwise.
   */
  public static int compare(double d1, double d2, double epsilon) {
    if (d2 - d1 > epsilon) {
      // Second value is larger than the first value.
      return -1;
    }
    if (d1 - d2 > epsilon) {
      // First value is larger than the second value.
      return 1;
    }
    // Given values are approximately equal.
    return 0;
  }

  /**
   * @param config The configurations for Cruise Control.
   * @return The list of default goals sorted by highest to lowest default priority.
   */
  public static List<Goal> getDefaultGoalsByPriority(KafkaCruiseControlConfig config) {
    return config.getConfiguredInstances(AnalyzerConfig.DEFAULT_GOALS_CONFIG, Goal.class);
  }

  /**
   * @param config The configurations for Cruise Control.
   * @return The list of intra broker goals sorted by highest to lowest default priority.
   */
  public static List<Goal> getIntraBrokerGoalsByPriority(KafkaCruiseControlConfig config) {
    return config.getConfiguredInstances(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, Goal.class);
  }

  /**
   * @param config The configurations for Cruise Control.
   * @return A goal map with goal name as the keys.
   */
  public static Map<String, Goal> getCaseInsensitiveGoalsByName(KafkaCruiseControlConfig config) {
    List<Goal> goals = config.getConfiguredInstances(AnalyzerConfig.GOALS_CONFIG, Goal.class);
    goals.addAll(config.getConfiguredInstances(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, Goal.class));
    Map<String, Goal> caseInsensitiveGoalsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (Goal goal: goals) {
      caseInsensitiveGoalsByName.put(goal.name(), goal);
    }
    return caseInsensitiveGoalsByName;
  }

  /**
   * Test if two clusters are significantly different in the metrics we look at for balancing.
   *
   * @param orig the utilization matrix from the original cluster
   * @param optimized the utilization matrix from the optimized cluster
   * @return The P value that the various derived resources come from the same probability distribution. The probability
   * that the null hypothesis is correct.
   */
  public static double[] testDifference(double[][] orig, double[][] optimized) {
    int nResources = RawAndDerivedResource.values().length;
    if (orig.length != nResources) {
      throw new IllegalArgumentException("orig must have number of rows equal to RawAndDerivedResource.");
    }
    if (optimized.length != nResources) {
      throw new IllegalArgumentException("optimized must have number of rows equal to RawAndDerivedResource.");
    }
    if (orig[0].length != optimized[0].length) {
      throw new IllegalArgumentException("The number of brokers must be the same.");
    }

    double[] pValues = new double[orig.length];

    //TODO:  For small N we want to do statistical bootstrapping (not the same as bootstrapping data).
    for (int resourceIndex = 0; resourceIndex < nResources; resourceIndex++) {
      RandomGenerator rng = new MersenneTwister(0x5d11121018463324L);
      KolmogorovSmirnovTest kolmogorovSmirnovTest = new KolmogorovSmirnovTest(rng);
      pValues[resourceIndex] =
          kolmogorovSmirnovTest.kolmogorovSmirnovTest(orig[resourceIndex], optimized[resourceIndex]);
    }

    return pValues;
  }
}
