/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link OneReplicaPerPartitionMovementStrategy}.
 */
public class OneReplicaPerPartitionMovementStrategyTest {
  private static final TopicPartition TP = new TopicPartition(TOPIC0, 0);
  private static final long PARTITION_SIZE = 10L;

  @Test
  public void testSplitReplacingAllReplicas() {
    verifySplit(0, List.of(0, 1, 2), List.of(3, 4, 5), List.of(List.of(0, 1, 3), List.of(0, 3, 4), List.of(3, 4, 5)));
  }

  @Test
  public void testSplitRemovesOldLeaderLast() {
    verifySplit(0, List.of(1, 0, 2), List.of(3, 4, 5), List.of(List.of(1, 0, 3), List.of(0, 3, 4), List.of(3, 4, 5)));
  }

  @Test
  public void testSplitKeepingOldLeader() {
    verifySplit(0, List.of(0, 1, 2), List.of(0, 3, 4), List.of(List.of(0, 1, 3), List.of(0, 3, 4)));
  }

  @Test
  public void testSplitWithReplicationFactorIncrease() {
    verifySplit(0, List.of(0), List.of(1, 2), List.of(List.of(0, 1), List.of(1, 2)));
    verifySplit(0, List.of(0, 1), List.of(2, 3, 4), List.of(List.of(0, 2), List.of(0, 2, 3), List.of(2, 3, 4)));
  }

  @Test
  public void testSplitWithReplicationFactorDecrease() {
    verifySplit(0, List.of(0, 1, 2, 3), List.of(4, 5), List.of(List.of(0, 1, 2, 4), List.of(4, 5)));
  }

  @Test
  public void testNoSplitWithAtMostOneNewReplica() {
    for (ExecutionProposal proposal : List.of(proposal(0, List.of(0, 1, 2), List.of(0, 1, 3)),
                                              proposal(0, List.of(0, 1, 2), List.of(3, 1, 2)),
                                              proposal(0, List.of(0, 1, 2), List.of(1, 0, 2)),
                                              proposal(0, List.of(0, 1, 2), List.of(1, 2)))) {
      List<ExecutionProposal> steps = OneReplicaPerPartitionMovementStrategy.splitProposal(proposal);
      assertEquals(1, steps.size());
      assertSame(proposal, steps.get(0));
    }
  }

  @Test
  public void testIsEnabled() {
    assertTrue(OneReplicaPerPartitionMovementStrategy.isEnabled(new OneReplicaPerPartitionMovementStrategy()));
    assertTrue(OneReplicaPerPartitionMovementStrategy.isEnabled(
        new OneReplicaPerPartitionMovementStrategy().chainBaseReplicaMovementStrategyIfAbsent()));
    assertTrue(OneReplicaPerPartitionMovementStrategy.isEnabled(
        new PrioritizeLargeReplicaMovementStrategy().chain(new OneReplicaPerPartitionMovementStrategy())
                                                    .chain(new PostponeUrpReplicaMovementStrategy())));
    assertFalse(OneReplicaPerPartitionMovementStrategy.isEnabled(null));
    assertFalse(OneReplicaPerPartitionMovementStrategy.isEnabled(new BaseReplicaMovementStrategy()));
    assertFalse(OneReplicaPerPartitionMovementStrategy.isEnabled(
        new PrioritizeLargeReplicaMovementStrategy().chainBaseReplicaMovementStrategyIfAbsent()));
  }

  private static void verifySplit(int oldLeader, List<Integer> oldReplicas, List<Integer> newReplicas,
                                  List<List<Integer>> expectedStepReplicas) {
    ExecutionProposal proposal = proposal(oldLeader, oldReplicas, newReplicas);
    List<ExecutionProposal> steps = OneReplicaPerPartitionMovementStrategy.splitProposal(proposal);

    List<List<Integer>> stepReplicas = new ArrayList<>();
    List<ReplicaPlacementInfo> previousReplicas = proposal.oldReplicas();
    for (ExecutionProposal step : steps) {
      assertEquals(TP, step.topicPartition());
      assertEquals(proposal.oldLeader(), step.oldLeader());
      assertEquals(previousReplicas, step.oldReplicas());
      assertEquals(1, step.replicasToAdd().size());
      assertEquals(PARTITION_SIZE, step.dataToMoveInMB());
      stepReplicas.add(brokerIds(step.newReplicas()));
      previousReplicas = step.newReplicas();
    }
    assertEquals(expectedStepReplicas, stepReplicas);
    assertEquals(proposal.newReplicas(), previousReplicas);
  }

  private static ExecutionProposal proposal(int oldLeader, List<Integer> oldReplicas, List<Integer> newReplicas) {
    return new ExecutionProposal(TP, PARTITION_SIZE, new ReplicaPlacementInfo(oldLeader), replicas(oldReplicas), replicas(newReplicas));
  }

  private static List<ReplicaPlacementInfo> replicas(List<Integer> brokerIds) {
    return brokerIds.stream().map(ReplicaPlacementInfo::new).collect(Collectors.toList());
  }

  private static List<Integer> brokerIds(List<ReplicaPlacementInfo> replicas) {
    return replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList());
  }
}
