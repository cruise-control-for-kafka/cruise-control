/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.EXECUTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTER_BROKER_DATA_COMPLETED_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTER_BROKER_DATA_FINISHED_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTER_BROKER_DATA_IN_EXECUTION_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTER_BROKER_DATA_REMAINING_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTER_BROKER_DATA_TOTAL_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTRA_BROKER_DATA_COMPLETED_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTRA_BROKER_DATA_FINISHED_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTRA_BROKER_DATA_IN_EXECUTION_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTRA_BROKER_DATA_REMAINING_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_INTRA_BROKER_DATA_TOTAL_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.GAUGE_ONGOING_EXECUTION_DURATION_MS;
import static org.junit.Assert.assertEquals;


public class ExecutionTaskTrackerTest {
  private static final long EXECUTION_ALERTING_THRESHOLD_MS = 1000L;
  private static final String TOPIC = "topic";
  private MetricRegistry _metricRegistry;
  private MockTime _time;
  private ExecutionTaskTracker _tracker;

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    _metricRegistry = new MetricRegistry();
    _time = new MockTime();
    _tracker = new ExecutionTaskTracker(_metricRegistry, _time);
  }

  @Test
  public void testInterBrokerDataMovementGauges() {
    ExecutionTask task0 = interBrokerTask(0, 10);
    ExecutionTask task1 = interBrokerTask(1, 20);
    _tracker.addTasksToTrace(Arrays.asList(task0, task1), INTER_BROKER_REPLICA_ACTION);
    assertInterBrokerDataMovementInMB(30, 0, 0, 0, 30);

    _tracker.markTaskState(task0, ExecutionTaskState.IN_PROGRESS);
    assertInterBrokerDataMovementInMB(30, 0, 0, 10, 20);
    _tracker.markTaskState(task1, ExecutionTaskState.IN_PROGRESS);
    assertInterBrokerDataMovementInMB(30, 0, 0, 30, 0);
    _tracker.markTaskState(task0, ExecutionTaskState.COMPLETED);
    assertInterBrokerDataMovementInMB(30, 10, 10, 20, 0);
    // The data of an aborting task remains in execution until the task is aborted or dead.
    _tracker.markTaskState(task1, ExecutionTaskState.ABORTING);
    assertInterBrokerDataMovementInMB(30, 10, 10, 20, 0);
    // The data of a dead task is finished, but not completed.
    _tracker.markTaskState(task1, ExecutionTaskState.DEAD);
    assertInterBrokerDataMovementInMB(30, 30, 10, 0, 0);
    assertIntraBrokerDataMovementInMB(0, 0, 0, 0, 0);

    _tracker.clear();
    assertInterBrokerDataMovementInMB(0, 0, 0, 0, 0);
  }

  @Test
  public void testIntraBrokerDataMovementGauges() {
    ExecutionTask task0 = intraBrokerTask(0, 5);
    ExecutionTask task1 = intraBrokerTask(1, 7);
    _tracker.addTasksToTrace(Arrays.asList(task0, task1), INTRA_BROKER_REPLICA_ACTION);
    assertIntraBrokerDataMovementInMB(12, 0, 0, 0, 12);

    _tracker.markTaskState(task0, ExecutionTaskState.IN_PROGRESS);
    assertIntraBrokerDataMovementInMB(12, 0, 0, 5, 7);
    _tracker.markTaskState(task0, ExecutionTaskState.COMPLETED);
    assertIntraBrokerDataMovementInMB(12, 5, 5, 0, 7);
    _tracker.markTaskState(task1, ExecutionTaskState.IN_PROGRESS);
    _tracker.markTaskState(task1, ExecutionTaskState.ABORTING);
    assertIntraBrokerDataMovementInMB(12, 5, 5, 7, 0);
    // The data of an aborted task is finished, but not completed.
    _tracker.markTaskState(task1, ExecutionTaskState.ABORTED);
    assertIntraBrokerDataMovementInMB(12, 12, 5, 0, 0);
    assertInterBrokerDataMovementInMB(0, 0, 0, 0, 0);

    _tracker.clear();
    assertIntraBrokerDataMovementInMB(0, 0, 0, 0, 0);
  }

  @Test
  public void testNoRemainingDataToMoveUponStopRequest() {
    ExecutionTask interBrokerTask0 = interBrokerTask(0, 10);
    ExecutionTask interBrokerTask1 = interBrokerTask(1, 20);
    _tracker.addTasksToTrace(Arrays.asList(interBrokerTask0, interBrokerTask1), INTER_BROKER_REPLICA_ACTION);
    _tracker.addTasksToTrace(Collections.singletonList(intraBrokerTask(2, 5)), INTRA_BROKER_REPLICA_ACTION);
    _tracker.markTaskState(interBrokerTask0, ExecutionTaskState.IN_PROGRESS);

    // Pending tasks are not executed once a stop is requested.
    _tracker.setStopRequested();
    assertInterBrokerDataMovementInMB(30, 0, 0, 10, 0);
    assertIntraBrokerDataMovementInMB(5, 0, 0, 0, 0);
    assertEquals(0, numPendingTasks(ExecutionTaskTracker.INTER_BROKER_REPLICA_ACTION));
    _tracker.markTaskState(interBrokerTask0, ExecutionTaskState.COMPLETED);
    assertInterBrokerDataMovementInMB(30, 10, 10, 0, 0);

    _tracker.clear();
    _tracker.addTasksToTrace(Collections.singletonList(interBrokerTask1), INTER_BROKER_REPLICA_ACTION);
    assertInterBrokerDataMovementInMB(20, 0, 0, 0, 20);
  }

  @Test
  public void testStopRequestedWithoutOngoingExecutionDoesNotApplyToNextExecution() {
    // E.g. stop_proposal_execution while there is no ongoing execution. The executor resets its stop signal when the next
    // execution starts, so that execution runs its pending tasks, and the sensors must report them.
    _tracker.setStopRequested();
    ExecutionTask interBrokerTask0 = interBrokerTask(0, 10);
    ExecutionTask interBrokerTask1 = interBrokerTask(1, 20);
    _tracker.addTasksToTrace(Arrays.asList(interBrokerTask0, interBrokerTask1), INTER_BROKER_REPLICA_ACTION);
    _tracker.addTasksToTrace(Collections.singletonList(intraBrokerTask(2, 5)), INTRA_BROKER_REPLICA_ACTION);
    assertInterBrokerDataMovementInMB(30, 0, 0, 0, 30);
    assertIntraBrokerDataMovementInMB(5, 0, 0, 0, 5);
    assertEquals(2, numPendingTasks(ExecutionTaskTracker.INTER_BROKER_REPLICA_ACTION));
    assertEquals(1, numPendingTasks(ExecutionTaskTracker.INTRA_BROKER_REPLICA_ACTION));

    // A stop requested during the execution applies to it.
    _tracker.markTaskState(interBrokerTask0, ExecutionTaskState.IN_PROGRESS);
    _tracker.setStopRequested();
    assertInterBrokerDataMovementInMB(30, 0, 0, 10, 0);
    assertEquals(0, numPendingTasks(ExecutionTaskTracker.INTER_BROKER_REPLICA_ACTION));
  }

  @Test
  public void testOngoingExecutionDurationGauge() {
    assertEquals(0L, gaugeValue(GAUGE_ONGOING_EXECUTION_DURATION_MS));
    _time.sleep(100L);
    assertEquals(0L, gaugeValue(GAUGE_ONGOING_EXECUTION_DURATION_MS));

    _tracker.addTasksToTrace(Collections.singletonList(interBrokerTask(0, 10)), INTER_BROKER_REPLICA_ACTION);
    _time.sleep(500L);
    // Tasks of other types are added to the same execution, hence they do not reset the start time of the execution.
    _tracker.addTasksToTrace(Collections.singletonList(intraBrokerTask(1, 5)), INTRA_BROKER_REPLICA_ACTION);
    _time.sleep(250L);
    assertEquals(750L, gaugeValue(GAUGE_ONGOING_EXECUTION_DURATION_MS));

    _tracker.clear();
    assertEquals(0L, gaugeValue(GAUGE_ONGOING_EXECUTION_DURATION_MS));
    _tracker.addTasksToTrace(Collections.emptyList(), INTER_BROKER_REPLICA_ACTION);
    _time.sleep(300L);
    assertEquals(300L, gaugeValue(GAUGE_ONGOING_EXECUTION_DURATION_MS));
  }

  private static ExecutionTask interBrokerTask(int partition, long partitionSizeInMB) {
    ReplicaPlacementInfo r0 = new ReplicaPlacementInfo(0);
    ReplicaPlacementInfo r1 = new ReplicaPlacementInfo(1);
    ReplicaPlacementInfo r2 = new ReplicaPlacementInfo(2);
    // Move the follower replica from broker 0 to broker 1, which moves the partition data once.
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC, partition), partitionSizeInMB, r2,
                                                       Arrays.asList(r0, r2), Arrays.asList(r2, r1));
    return new ExecutionTask(partition, proposal, INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private static ExecutionTask intraBrokerTask(int partition, long partitionSizeInMB) {
    ReplicaPlacementInfo oldReplica = new ReplicaPlacementInfo(0, "/logdir0");
    ReplicaPlacementInfo newReplica = new ReplicaPlacementInfo(0, "/logdir1");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC, partition), partitionSizeInMB, oldReplica,
                                                       Collections.singletonList(oldReplica), Collections.singletonList(newReplica));
    return new ExecutionTask(partition, proposal, 0, INTRA_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private void assertInterBrokerDataMovementInMB(long total, long finished, long completed, long inExecution, long remaining) {
    assertEquals(total, gaugeValue(GAUGE_INTER_BROKER_DATA_TOTAL_MB));
    assertEquals(finished, gaugeValue(GAUGE_INTER_BROKER_DATA_FINISHED_MB));
    assertEquals(completed, gaugeValue(GAUGE_INTER_BROKER_DATA_COMPLETED_MB));
    assertEquals(inExecution, gaugeValue(GAUGE_INTER_BROKER_DATA_IN_EXECUTION_MB));
    assertEquals(remaining, gaugeValue(GAUGE_INTER_BROKER_DATA_REMAINING_MB));
  }

  private void assertIntraBrokerDataMovementInMB(long total, long finished, long completed, long inExecution, long remaining) {
    assertEquals(total, gaugeValue(GAUGE_INTRA_BROKER_DATA_TOTAL_MB));
    assertEquals(finished, gaugeValue(GAUGE_INTRA_BROKER_DATA_FINISHED_MB));
    assertEquals(completed, gaugeValue(GAUGE_INTRA_BROKER_DATA_COMPLETED_MB));
    assertEquals(inExecution, gaugeValue(GAUGE_INTRA_BROKER_DATA_IN_EXECUTION_MB));
    assertEquals(remaining, gaugeValue(GAUGE_INTRA_BROKER_DATA_REMAINING_MB));
  }

  private long gaugeValue(String name) {
    Gauge<?> gauge = _metricRegistry.getGauges().get(MetricRegistry.name(EXECUTOR_SENSOR, name));
    return (Long) gauge.getValue();
  }

  private int numPendingTasks(String taskType) {
    Gauge<?> gauge = _metricRegistry.getGauges().get(MetricRegistry.name(EXECUTOR_SENSOR, taskType + "-" + ExecutionTaskTracker.PENDING));
    return (Integer) gauge.getValue();
  }
}
