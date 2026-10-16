/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencySummary;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutionConcurrencySummaryTest {
  private final Map<Integer, Integer> _interBrokerConcurrencyMap = Map.of(0, 5, 1, 6, 2, 7);
  private final Map<Integer, Integer> _intraBrokerConcurrencyMap = Map.of(0, 10, 1, 11, 2, 12);
  private final Map<Integer, Integer> _leadershipBrokerConcurrencyMap = Map.of(0, 100, 1, 200, 2, 300);

  @Test
  public void testInterBrokerMinMaxAvgConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap, 257);

    assertEquals(7, summary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertEquals(5, summary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertEquals(6, summary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 0.01);
  }

  @Test
  public void testIntraBrokerMinMaxAvgConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap, 257);

    assertEquals(12, summary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertEquals(10, summary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertEquals(11, summary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 0.01);
  }

  @Test
  public void testLeadershipMinMaxAvgConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap, 257);

    assertEquals(300, summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertEquals(100, summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertEquals(200, summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER), 0.01);
    assertEquals(257, summary.getClusterLeadershipMovementConcurrency());
  }

  @Test
  public void testClusterLeadershipConcurrency() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap, 257);

    assertEquals(257, summary.getClusterLeadershipMovementConcurrency());
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_CLUSTER));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_CLUSTER));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_CLUSTER));
  }

  @Test
  public void testInvalidConcurrencySummaryShouldReturnZeroForMinMaxAvg() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        false, _interBrokerConcurrencyMap, _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap, 257);

    assertEquals(0, summary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertEquals(0, summary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertEquals(0, summary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA), 0.01);
    assertEquals(0, summary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertEquals(0, summary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertEquals(0, summary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA), 0.01);
    assertEquals(0, summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertEquals(0, summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertEquals(0, summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER), 0.01);
    assertEquals(0, summary.getClusterLeadershipMovementConcurrency());
  }

  @Test
  public void testShouldThrowExceptionIfConcurrencyMapNotFullyPopulated() {
    ExecutionConcurrencySummary summary = new ExecutionConcurrencySummary(
        true, Collections.emptyMap(), _intraBrokerConcurrencyMap, _leadershipBrokerConcurrencyMap, 257);
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
    assertThrows(IllegalArgumentException.class, () -> summary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertThrows(IllegalArgumentException.class, () -> summary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertThrows(IllegalArgumentException.class, () -> summary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
    assertEquals(257, summary.getClusterLeadershipMovementConcurrency());
  }
}
