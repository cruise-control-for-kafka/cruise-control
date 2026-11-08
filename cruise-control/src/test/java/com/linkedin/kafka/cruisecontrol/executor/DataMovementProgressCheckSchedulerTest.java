/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataMovementProgressCheckSchedulerTest {
  private static final long CHECK_INTERVAL_MS = 60_000L;
  private static final long START_MS = 1_000_000L;
  private static final List<ExecutionTask> TASKS = Arrays.asList(task(0), task(1));

  private static ExecutionTask task(int partition) {
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", partition), 10, new ReplicaPlacementInfo(0),
                                                       Collections.singletonList(new ReplicaPlacementInfo(0)),
                                                       Collections.singletonList(new ReplicaPlacementInfo(1)));
    return new ExecutionTask(partition, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 100L);
  }

  private static void maybeCheck(DataMovementProgressCheckScheduler scheduler, long nowMs, List<ExecutionTask> tasks,
                                 List<List<ExecutionTask>> checkedTasks) {
    scheduler.maybeCheck(nowMs, tasks, checkedTasks::add);
  }

  @Test
  public void testCheckAtMostOncePerInterval() {
    DataMovementProgressCheckScheduler scheduler = new DataMovementProgressCheckScheduler(true, CHECK_INTERVAL_MS);
    List<List<ExecutionTask>> checkedTasks = new ArrayList<>();
    maybeCheck(scheduler, START_MS, TASKS, checkedTasks);
    // Even if the check failed to update the progress of some tasks, they are not retried before the next interval.
    maybeCheck(scheduler, START_MS + 5_000L, TASKS, checkedTasks);
    maybeCheck(scheduler, START_MS + CHECK_INTERVAL_MS - 1, TASKS, checkedTasks);
    maybeCheck(scheduler, START_MS + CHECK_INTERVAL_MS, TASKS, checkedTasks);
    assertEquals(Arrays.asList(TASKS, TASKS), checkedTasks);
  }

  @Test
  public void testCheckAsSoonAsTasksAreInProgressAfterReset() {
    DataMovementProgressCheckScheduler scheduler = new DataMovementProgressCheckScheduler(true, CHECK_INTERVAL_MS);
    List<List<ExecutionTask>> checkedTasks = new ArrayList<>();
    maybeCheck(scheduler, START_MS, TASKS, checkedTasks);
    // A new replica movement phase starts.
    scheduler.reset();
    // No in-progress task yet: the check remains due.
    maybeCheck(scheduler, START_MS + 1_000L, Collections.emptyList(), checkedTasks);
    maybeCheck(scheduler, START_MS + 2_000L, TASKS, checkedTasks);
    assertEquals(Arrays.asList(TASKS, TASKS), checkedTasks);
  }

  @Test
  public void testNoCheckIfDisabled() {
    DataMovementProgressCheckScheduler scheduler = new DataMovementProgressCheckScheduler(false, CHECK_INTERVAL_MS);
    List<List<ExecutionTask>> checkedTasks = new ArrayList<>();
    maybeCheck(scheduler, START_MS, TASKS, checkedTasks);
    assertTrue(checkedTasks.isEmpty());
  }
}
