/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;

/**
 * The strategy, which moves at most one replica of each partition at a time.
 * <p>
 * Without this strategy, an inter-broker replica movement submits the final replica list of a partition at once. For
 * example, moving a partition from brokers {@code [0, 1, 2]} to {@code [3, 4, 5]} makes the partition temporarily live
 * on six brokers, and its leader serves five replication streams at the same time. With this strategy, such a movement
 * is split into sequential steps, each of which adds exactly one new replica:
 * {@code [0, 1, 2] -> [0, 1, 3] -> [0, 3, 4] -> [3, 4, 5]}.
 * <p>
 * In each intermediate step, one replica that is no longer needed is removed from the tail of the replica list, and the
 * new replica is appended to it. The old leader is removed only in the last step, which also sets the exact final
 * replica order, so the partition keeps its leader until its final replica set is reached.
 * <p>
 * This strategy does not change the execution order of the tasks, and can be chained with other strategies. Note that
 * each step is tracked as a separate inter-broker partition movement task.
 */
public class OneReplicaPerPartitionMovementStrategy extends AbstractReplicaMovementStrategy {

  @Override
  public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
    return (task1, task2) -> PRIORITIZE_NONE;
  }

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return taskComparator(new StrategyOptions.Builder(cluster).build());
  }

  /**
   * Get the name of this strategy. Name of a strategy provides an identification for the strategy in human readable format.
   */
  @Override
  public String name() {
    return OneReplicaPerPartitionMovementStrategy.class.getSimpleName();
  }

  /**
   * @param strategy The (possibly chained) replica movement strategy to check.
   * @return {@code true} if the given strategy is or contains {@link OneReplicaPerPartitionMovementStrategy}, {@code false} otherwise.
   */
  public static boolean isEnabled(ReplicaMovementStrategy strategy) {
    return strategy != null
           && Arrays.asList(strategy.name().split(",")).contains(OneReplicaPerPartitionMovementStrategy.class.getSimpleName());
  }

  /**
   * Split the given proposal into sequential proposals, each of which adds exactly one new replica to the partition.
   * <ul>
   *   <li>Each intermediate step removes one replica that is not in the new replica list (if any remains), starting from
   *   the tail of the old replica list and never the old leader, then appends one new replica.</li>
   *   <li>The last step moves the partition to the exact new replica list of the given proposal.</li>
   *   <li>All steps keep the old leader of the given proposal, since it is removed (if at all) only in the last step.</li>
   * </ul>
   * A proposal that adds at most one new replica is returned as is.
   *
   * @param proposal The execution proposal to split.
   * @return The execution proposals to execute in the given order.
   */
  public static List<ExecutionProposal> splitProposal(ExecutionProposal proposal) {
    Set<Integer> oldBrokers = proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet());
    Set<Integer> newBrokers = proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet());
    List<ReplicaPlacementInfo> replicasToAdd = proposal.newReplicas().stream()
                                                       .filter(r -> !oldBrokers.contains(r.brokerId()))
                                                       .collect(Collectors.toList());
    if (replicasToAdd.size() <= 1) {
      return Collections.singletonList(proposal);
    }

    // Replicas to remove before the last step, from the tail of the old replica list. The old leader is removed in the last step.
    int oldLeaderId = proposal.oldLeader().brokerId();
    Deque<ReplicaPlacementInfo> replicasToRemove = new ArrayDeque<>();
    for (ReplicaPlacementInfo replica : proposal.oldReplicas()) {
      if (!newBrokers.contains(replica.brokerId()) && replica.brokerId() != oldLeaderId) {
        replicasToRemove.push(replica);
      }
    }

    List<ExecutionProposal> steps = new ArrayList<>(replicasToAdd.size());
    List<ReplicaPlacementInfo> currentReplicas = proposal.oldReplicas();
    for (ReplicaPlacementInfo replicaToAdd : replicasToAdd.subList(0, replicasToAdd.size() - 1)) {
      List<ReplicaPlacementInfo> nextReplicas = new ArrayList<>(currentReplicas);
      if (!replicasToRemove.isEmpty()) {
        nextReplicas.remove(replicasToRemove.pop());
      }
      nextReplicas.add(replicaToAdd);
      steps.add(new ExecutionProposal(proposal.topicPartition(), proposal.partitionSize(), proposal.oldLeader(),
                                      currentReplicas, nextReplicas));
      currentReplicas = nextReplicas;
    }
    steps.add(new ExecutionProposal(proposal.topicPartition(), proposal.partitionSize(), proposal.oldLeader(),
                                    currentReplicas, proposal.newReplicas()));
    return steps;
  }
}
