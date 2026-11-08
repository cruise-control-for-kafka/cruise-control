/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.BYTES_IN_MB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecutionTaskTest {
  private static final long START_MS = 1_000_000L;
  private static final long EXECUTION_TIME_ALERTING_THRESHOLD_MS = TimeUnit.SECONDS.toMillis(90);
  private static final double RATE_ALERTING_THRESHOLD_IN_MB_PER_SEC = 0.1;
  // The estimated size of a 500 GB partition moved at the minimum rate takes ~59 days to trigger the estimate-based alert.
  private static final long PARTITION_SIZE_IN_MB = 500L * 1024L;
  private static final long ESTIMATE_BASED_ALERTING_THRESHOLD_MS = Math.round(PARTITION_SIZE_IN_MB / RATE_ALERTING_THRESHOLD_IN_MB_PER_SEC * 1000);

  private static ExecutionTask inProgressInterBrokerReplicaTask() {
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0), PARTITION_SIZE_IN_MB, new ReplicaPlacementInfo(0),
                                                       Collections.singletonList(new ReplicaPlacementInfo(0)),
                                                       Collections.singletonList(new ReplicaPlacementInfo(1)));
    ExecutionTask task = new ExecutionTask(0L, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ESTIMATE_BASED_ALERTING_THRESHOLD_MS);
    task.inProgress(START_MS);
    return task;
  }

  private static List<ExecutionTask> maybeReportExecutionTooSlow(ExecutionTask task, long nowMs) {
    List<ExecutionTask> tasksToReport = new ArrayList<>();
    task.maybeReportExecutionTooSlow(nowMs, EXECUTION_TIME_ALERTING_THRESHOLD_MS, RATE_ALERTING_THRESHOLD_IN_MB_PER_SEC, tasksToReport);
    return tasksToReport;
  }

  @Test
  public void testReportStuckDataMovement() {
    ExecutionTask task = inProgressInterBrokerReplicaTask();
    task.updateDataMovementProgress(100L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 30_000L);
    // No data is moved in a minute (e.g. the replication throttle is too low or the source replica is unavailable).
    task.updateDataMovementProgress(100L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 90_000L);
    assertEquals(0.0, task.dataMovementRateInMBPerSec(), 1e-9);

    // Not reported before the execution time alerting threshold.
    assertTrue(maybeReportExecutionTooSlow(task, START_MS + EXECUTION_TIME_ALERTING_THRESHOLD_MS).isEmpty());
    // Reported right after the execution time alerting threshold -- rather than after ~59 days based on the estimated size.
    assertEquals(Collections.singletonList(task), maybeReportExecutionTooSlow(task, START_MS + EXECUTION_TIME_ALERTING_THRESHOLD_MS + 1));
    // Reported only once.
    assertTrue(maybeReportExecutionTooSlow(task, START_MS + 2 * EXECUTION_TIME_ALERTING_THRESHOLD_MS).isEmpty());
  }

  @Test
  public void testDoNotReportProgressingDataMovement() {
    ExecutionTask task = inProgressInterBrokerReplicaTask();
    task.updateDataMovementProgress(100L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 30_000L);
    // 60 MB are moved in a minute.
    task.updateDataMovementProgress(160L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 90_000L);
    assertEquals(1.0, task.dataMovementRateInMBPerSec(), 1e-9);
    assertTrue(maybeReportExecutionTooSlow(task, START_MS + EXECUTION_TIME_ALERTING_THRESHOLD_MS + 1).isEmpty());
  }

  @Test
  public void testDoNotReportDataMovementThatMovedAllData() {
    ExecutionTask task = inProgressInterBrokerReplicaTask();
    // All data is moved (e.g. the new replica is about to join the ISR), hence no more data movement is expected.
    task.updateDataMovementProgress(PARTITION_SIZE_IN_MB * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 30_000L);
    task.updateDataMovementProgress(PARTITION_SIZE_IN_MB * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 90_000L);
    assertTrue(maybeReportExecutionTooSlow(task, START_MS + EXECUTION_TIME_ALERTING_THRESHOLD_MS + 1).isEmpty());
  }

  @Test
  public void testFallBackToEstimateBasedAlertWithoutDataMovementRate() {
    ExecutionTask task = inProgressInterBrokerReplicaTask();
    // The data movement progress is observed only once, so the data movement rate is unknown.
    task.updateDataMovementProgress(0L, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 30_000L);
    assertTrue(maybeReportExecutionTooSlow(task, START_MS + EXECUTION_TIME_ALERTING_THRESHOLD_MS + 1).isEmpty());
    assertEquals(Collections.singletonList(task), maybeReportExecutionTooSlow(task, START_MS + ESTIMATE_BASED_ALERTING_THRESHOLD_MS + 1));
  }

  @Test
  public void testDataMovementRateIgnoresShrinkingReplica() {
    ExecutionTask task = inProgressInterBrokerReplicaTask();
    task.updateDataMovementProgress(100L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS);
    // The replica being created shrinks (e.g. due to retention), which is not a negative data movement rate.
    task.updateDataMovementProgress(90L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB, START_MS + 10_000L);
    assertEquals(0.0, task.dataMovementRateInMBPerSec(), 1e-9);
    assertEquals(Arrays.asList(90L * BYTES_IN_MB, PARTITION_SIZE_IN_MB * BYTES_IN_MB),
                 Arrays.asList(task.movedDataInBytes(), task.totalDataToMoveInBytes()));
  }
}
