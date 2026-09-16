/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.Time;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.EXECUTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;

/**
 * A class for tracking the (1) dead tasks, (2) aborting/aborted tasks, (3) in progress tasks, and (4) pending tasks.
 *
 * This class is not thread-safe.
 */
public class ExecutionTaskTracker {
  private final Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> _tasksByType;
  private long _remainingInterBrokerDataToMoveInMB;
  private long _remainingIntraBrokerDataToMoveInMB;
  private long _inExecutionInterBrokerDataMovementInMB;
  private long _inExecutionIntraBrokerDataMovementInMB;
  private long _finishedInterBrokerDataMovementInMB;
  private long _finishedIntraBrokerDataMovementInMB;
  private boolean _isKafkaAssignerMode;
  private final Time _time;
  private volatile boolean _stopRequested;
  private final Meter _interBrokerPartitionMovementRateMeter;
  private final Meter _intraBrokerPartitionMovementRateMeter;
  private final Meter _leadershipMovementRateMeter;
  private final Meter _partitionDataMovementRateMeter;

  public static final String INTER_BROKER_REPLICA_ACTION = "replica-action";
  public static final String INTRA_BROKER_REPLICA_ACTION = "intra-broker-replica-action";
  public static final String LEADERSHIP_ACTION = "leadership-action";
  public static final String IN_PROGRESS = "in-progress";
  public static final String PENDING = "pending";
  public static final String ABORTING = "aborting";
  public static final String ABORTED = "aborted";
  public static final String DEAD = "dead";
  public static final String COMPLETED = "completed";
  public static final String GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE = "ongoing-execution-kafka_assigner";
  public static final String GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE = "ongoing-execution-non_kafka_assigner";
  public static final String METER_INTER_BROKER_PARTITION_MOVEMENT_RATE = "inter-broker-partition-movement-rate";
  public static final String METER_INTRA_BROKER_PARTITION_MOVEMENT_RATE = "intra-broker-partition-movement-rate";
  public static final String METER_LEADERSHIP_MOVEMENT_RATE = "leadership-movement-rate";
  public static final String METER_PARTITION_DATA_MOVEMENT_RATE = "partition-data-movement-rate-MB";
  public static final String GAUGE_ONGOING_EXECUTION_TOTAL_DATA_TO_MOVE = "ongoing-execution-inter_broker_data_movement_total_bytes";
  public static final String GAUGE_ONGOING_EXECUTION_FINISHED_DATA_TO_MOVE = "ongoing-execution-inter_broker_data_movement_completed_bytes";
  public static final String GAUGE_ONGOING_EXECUTION_DATA_MOVEMENT = "ongoing-execution-inter_broker_data_movement_moving_bytes";
  public static final String GAUGE_ONGOING_EXECUTION_REMAINING_DATA_TO_MOVE = "ongoing-execution-inter_broker_data_movement_remaining_bytes";

  ExecutionTaskTracker(MetricRegistry dropwizardMetricRegistry, Time time) {
    List<ExecutionTaskState> states = ExecutionTaskState.cachedValues();
    List<TaskType> taskTypes = TaskType.cachedValues();
    _tasksByType = new HashMap<>();
    for (TaskType type : taskTypes) {
      Map<ExecutionTaskState, Set<ExecutionTask>> taskMap = new HashMap<>();
      for (ExecutionTaskState state : states) {
        taskMap.put(state, new HashSet<>());
      }
      _tasksByType.put(type, taskMap);
    }
    _remainingInterBrokerDataToMoveInMB = 0L;
    _remainingIntraBrokerDataToMoveInMB = 0L;
    _inExecutionInterBrokerDataMovementInMB = 0L;
    _inExecutionIntraBrokerDataMovementInMB = 0L;
    _finishedInterBrokerDataMovementInMB = 0L;
    _finishedIntraBrokerDataMovementInMB = 0L;
    _isKafkaAssignerMode = false;
    _time = time;
    _stopRequested = false;
    _interBrokerPartitionMovementRateMeter = new Meter();
    _intraBrokerPartitionMovementRateMeter = new Meter();
    _leadershipMovementRateMeter = new Meter();
    _partitionDataMovementRateMeter = new Meter();

    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);

    // Register meter sensors.
    registerMeterSensors(dropwizardMetricRegistry);
  }

  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    for (TaskType type : TaskType.cachedValues()) {
      for (ExecutionTaskState state : ExecutionTaskState.cachedValues()) {
        String typeString = type == TaskType.INTER_BROKER_REPLICA_ACTION
                            ? INTER_BROKER_REPLICA_ACTION : type == TaskType.INTRA_BROKER_REPLICA_ACTION
                                                            ? INTRA_BROKER_REPLICA_ACTION : LEADERSHIP_ACTION;
        String stateString = state == ExecutionTaskState.PENDING
                             ? PENDING : state == ExecutionTaskState.IN_PROGRESS
                                         ? IN_PROGRESS : state == ExecutionTaskState.ABORTING
                                                         ? ABORTING : state == ExecutionTaskState.ABORTED
                                                                      ? ABORTED : state == ExecutionTaskState.COMPLETED
                                                                                  ? COMPLETED : DEAD;
        dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, typeString + "-" + stateString),
                                          (Gauge<Integer>) () -> (state == ExecutionTaskState.PENDING && _stopRequested)
                                                                 ? 0 : _tasksByType.get(type).get(state).size());
      }
    }
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) () -> _isKafkaAssignerMode
                                                             && !inExecutionTasks(TaskType.cachedValues()).isEmpty() ? 1 : 0);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) () -> !_isKafkaAssignerMode
                                                             && !inExecutionTasks(TaskType.cachedValues()).isEmpty() ? 1 : 0);

     // expose rebalance metrics
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_TOTAL_DATA_TO_MOVE),
            (Gauge<Long>) () -> ( finishedInterBrokerDataMovementInMB()
                    + inExecutionInterBrokerDataMovementInMB()
                    + remainingInterBrokerDataToMoveInMB() ) * 1024 * 1024 );
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_FINISHED_DATA_TO_MOVE),
            (Gauge<Long>) () -> finishedInterBrokerDataMovementInMB() * 1024 * 1024 );
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_DATA_MOVEMENT),
            (Gauge<Long>) () -> inExecutionInterBrokerDataMovementInMB() * 1024 * 1024 );
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_REMAINING_DATA_TO_MOVE),
            (Gauge<Long>) () -> remainingInterBrokerDataToMoveInMB() * 1024 * 1024 );
  }

  private void registerMeterSensors(MetricRegistry dropwizardMetricRegistry) {
    dropwizardMetricRegistry.register(
        MetricRegistry.name(EXECUTOR_SENSOR, METER_INTER_BROKER_PARTITION_MOVEMENT_RATE), _interBrokerPartitionMovementRateMeter);
    dropwizardMetricRegistry.register(
        MetricRegistry.name(EXECUTOR_SENSOR, METER_INTRA_BROKER_PARTITION_MOVEMENT_RATE), _intraBrokerPartitionMovementRateMeter);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_LEADERSHIP_MOVEMENT_RATE), _leadershipMovementRateMeter);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_PARTITION_DATA_MOVEMENT_RATE), _partitionDataMovementRateMeter);
  }
  /**
   * Update the execution state of the task.
   *
   * @param task The task to update.
   * @param newState New execution state of the task.
   */
  public void markTaskState(ExecutionTask task, ExecutionTaskState newState) {
    _tasksByType.get(task.type()).get(task.state()).remove(task);
    switch (newState) {
      case PENDING:
        // Let it go.
        break;
      case IN_PROGRESS:
        task.inProgress(_time.milliseconds());
        updateDataMovement(task);
        break;
      case ABORTING:
        task.abort();
        break;
      case ABORTED:
        task.aborted(_time.milliseconds());
        updateDataMovement(task);
        break;
      case COMPLETED:
        markTaskExecutionRateMeter(task);
        task.completed(_time.milliseconds());
        updateDataMovement(task);
        break;
      case DEAD:
        task.kill(_time.milliseconds());
        updateDataMovement(task);
        break;
      default:
        break;
    }
    _tasksByType.get(task.type()).get(newState).add(task);
  }

  private void markTaskExecutionRateMeter(ExecutionTask task) {
    switch (task.type()) {
      case INTER_BROKER_REPLICA_ACTION:
        _interBrokerPartitionMovementRateMeter.mark();
        break;
      case INTRA_BROKER_REPLICA_ACTION:
        _intraBrokerPartitionMovementRateMeter.mark();
        break;
      case LEADER_ACTION:
        _leadershipMovementRateMeter.mark();
        break;
      default:
        break;
    }
  }

  private void updateDataMovement(ExecutionTask task) {
    long dataToMove = task.type() == TaskType.INTRA_BROKER_REPLICA_ACTION
                      ? task.proposal().intraBrokerDataToMoveInMB() : task.type() == TaskType.INTER_BROKER_REPLICA_ACTION
                                                                      ? task.proposal().interBrokerDataToMoveInMB() : 0;
    if (task.state() == ExecutionTaskState.IN_PROGRESS) {
      if (task.type() == TaskType.INTRA_BROKER_REPLICA_ACTION) {
        _remainingIntraBrokerDataToMoveInMB -= dataToMove;
        _inExecutionIntraBrokerDataMovementInMB += dataToMove;
      } else if (task.type() == TaskType.INTER_BROKER_REPLICA_ACTION) {
        _remainingInterBrokerDataToMoveInMB -= dataToMove;
        _inExecutionInterBrokerDataMovementInMB += dataToMove;
      }
    } else if (task.state() == ExecutionTaskState.ABORTED
               || task.state() == ExecutionTaskState.DEAD
               || task.state() == ExecutionTaskState.COMPLETED) {
      _partitionDataMovementRateMeter.mark(dataToMove);
      if (task.type() == TaskType.INTRA_BROKER_REPLICA_ACTION) {
        _inExecutionIntraBrokerDataMovementInMB -= dataToMove;
        _finishedIntraBrokerDataMovementInMB += dataToMove;
      } else if (task.type() == TaskType.INTER_BROKER_REPLICA_ACTION) {
        _inExecutionInterBrokerDataMovementInMB -= dataToMove;
        _finishedInterBrokerDataMovementInMB += dataToMove;
      }
    }
  }

  /**
   * Add new tasks to ExecutionTaskTracker to trace their execution.
   * Tasks are added homogeneously -- all tasks have the same task type.
   *
   * @param tasks    New tasks to add.
   * @param taskType Task type of new tasks.
   */
  public void addTasksToTrace(Collection<ExecutionTask> tasks, TaskType taskType) {
    _tasksByType.get(taskType).get(ExecutionTaskState.PENDING).addAll(tasks);
    if (taskType == TaskType.INTER_BROKER_REPLICA_ACTION) {
      _remainingInterBrokerDataToMoveInMB += tasks.stream().mapToLong(t -> t.proposal().interBrokerDataToMoveInMB()).sum();
    } else if (taskType == TaskType.INTRA_BROKER_REPLICA_ACTION) {
      _remainingIntraBrokerDataToMoveInMB += tasks.stream().mapToLong(t -> t.proposal().intraBrokerDataToMoveInMB()).sum();
    }
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   */
  public void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Get the statistic of task execution state.
   *
   * @return The statistic of task execution state.
   */
  private Map<TaskType, Map<ExecutionTaskState, Integer>> taskStat() {
    Map<TaskType, Map<ExecutionTaskState, Integer>> taskStatMap = new HashMap<>();
    for (TaskType type : TaskType.cachedValues()) {
      taskStatMap.put(type, new HashMap<>());
      _tasksByType.get(type).forEach((k, v) -> taskStatMap.get(type).put(k, v.size()));
    }
    return taskStatMap;
  }

  /**
   * Get a filtered list of tasks of different {@link TaskType} and in different {@link ExecutionTaskState}.
   *
   * @param taskTypesToGetFullList  Task types to return complete list of tasks.
   * @return                        A filtered list of tasks.
   */
  private Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> filteredTasksByState(Set<TaskType> taskTypesToGetFullList) {
    Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> tasksByState = new HashMap<>();
    for (TaskType type : taskTypesToGetFullList) {
      tasksByState.put(type, new HashMap<>());
      _tasksByType.get(type).forEach((k, v) -> tasksByState.get(type).put(k, new HashSet<>(v)));
    }
    return tasksByState;
  }

  /**
   * Clear the replica action and leader action tasks.
   */
  public void clear() {
    _tasksByType.values().forEach(m -> m.values().forEach(Set::clear));
    _remainingInterBrokerDataToMoveInMB = 0L;
    _remainingIntraBrokerDataToMoveInMB = 0L;
    _inExecutionInterBrokerDataMovementInMB = 0L;
    _inExecutionIntraBrokerDataMovementInMB = 0L;
    _finishedInterBrokerDataMovementInMB = 0L;
    _finishedIntraBrokerDataMovementInMB = 0L;
    _stopRequested = false;
  }

  public void setStopRequested() {
    _stopRequested = true;
  }

  // Internal query APIs.
  public int numRemainingInterBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.PENDING).size();
  }

  public long remainingInterBrokerDataToMoveInMB() {
    return _remainingInterBrokerDataToMoveInMB;
  }

  /**
   * @return Number of finished inter broker partition movements, which is the sum of completed, dead, and aborted tasks.
   */
  public int numFinishedInterBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.COMPLETED).size()
           + _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.DEAD).size()
           + _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.ABORTED).size();
  }

  public long finishedInterBrokerDataMovementInMB() {
    return _finishedInterBrokerDataMovementInMB;
  }

  /**
   * Get tasks in execution with the given task type.
   *
   * @param types Task type.
   * @return Tasks that are in progress or aborting with the given task type.
   */
  public Set<ExecutionTask> inExecutionTasks(Collection<TaskType> types) {
    Set<ExecutionTask> inExecutionTasks = new HashSet<>();
    for (TaskType type : types) {
      inExecutionTasks.addAll(_tasksByType.get(type).get(ExecutionTaskState.IN_PROGRESS));
      inExecutionTasks.addAll(_tasksByType.get(type).get(ExecutionTaskState.ABORTING));
    }
    return inExecutionTasks;
  }

  public long inExecutionInterBrokerDataMovementInMB() {
    return _inExecutionInterBrokerDataMovementInMB;
  }

  public int numRemainingLeadershipMovements() {
    return _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.PENDING).size();
  }

  /**
   * @return Number of finished leadership movements, which is the sum of completed, dead, and aborted tasks.
   */
  public int numFinishedLeadershipMovements() {
    return _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.COMPLETED).size()
           + _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.DEAD).size()
           + _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.ABORTED).size();
  }

  public int numRemainingIntraBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.PENDING).size();
  }

  public long remainingIntraBrokerDataToMoveInMB() {
    return _remainingIntraBrokerDataToMoveInMB;
  }

  /**
   * @return Number of finished intra broker partition movements, which is the sum of completed, dead, and aborted tasks.
   */
  public int numFinishedIntraBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.COMPLETED).size()
           + _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.DEAD).size()
           + _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.ABORTED).size();
  }

  public long finishedIntraBrokerDataToMoveInMB() {
    return _finishedIntraBrokerDataMovementInMB;
  }

  public long inExecutionIntraBrokerDataMovementInMB() {
    return _inExecutionIntraBrokerDataMovementInMB;
  }

  /**
   * Get execution tasks summary.
   *
   * @param taskTypesToGetFullList Task types to return complete list of tasks.
   * @return Execution tasks summary.
   */
  public ExecutionTasksSummary getExecutionTasksSummary(Set<TaskType> taskTypesToGetFullList) {
    return new ExecutionTasksSummary(_finishedInterBrokerDataMovementInMB,
                                     _finishedIntraBrokerDataMovementInMB,
                                     _inExecutionInterBrokerDataMovementInMB,
                                     _inExecutionIntraBrokerDataMovementInMB,
                                     _remainingInterBrokerDataToMoveInMB,
                                     _remainingIntraBrokerDataToMoveInMB,
                                     taskStat(),
                                     filteredTasksByState(taskTypesToGetFullList)
    );
  }

  public static class ExecutionTasksSummary {
    private final long _finishedInterBrokerDataMovementInMB;
    private final long _finishedIntraBrokerDataMovementInMB;
    private final long _inExecutionInterBrokerDataMovementInMB;
    private final long _inExecutionIntraBrokerDataMovementInMB;
    private final long _remainingInterBrokerDataToMoveInMB;
    private final long _remainingIntraBrokerDataToMoveInMB;
    private final Map<TaskType, Map<ExecutionTaskState, Integer>> _taskStat;
    private final Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> _filteredTasksByState;

    ExecutionTasksSummary(long finishedInterBrokerDataMovementInMB,
                          long finishedIntraBrokerDataMovementInMB,
                          long inExecutionInterBrokerDataMovementInMB,
                          long inExecutionIntraBrokerDataMovementInMB,
                          long remainingInterBrokerDataToMoveInMB,
                          long remainingIntraBrokerDataToMoveInMB,
                          Map<TaskType, Map<ExecutionTaskState, Integer>> taskStat,
                          Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> filteredTasksByState) {
      _finishedInterBrokerDataMovementInMB = finishedInterBrokerDataMovementInMB;
      _finishedIntraBrokerDataMovementInMB = finishedIntraBrokerDataMovementInMB;
      _inExecutionInterBrokerDataMovementInMB = inExecutionInterBrokerDataMovementInMB;
      _inExecutionIntraBrokerDataMovementInMB = inExecutionIntraBrokerDataMovementInMB;
      _remainingInterBrokerDataToMoveInMB = remainingInterBrokerDataToMoveInMB;
      _remainingIntraBrokerDataToMoveInMB = remainingIntraBrokerDataToMoveInMB;
      _taskStat = taskStat;
      _filteredTasksByState = filteredTasksByState;
    }

    public long finishedInterBrokerDataMovementInMB() {
      return _finishedInterBrokerDataMovementInMB;
    }

    public long finishedIntraBrokerDataMovementInMB() {
      return _finishedIntraBrokerDataMovementInMB;
    }

    public long inExecutionInterBrokerDataMovementInMB() {
      return _inExecutionInterBrokerDataMovementInMB;
    }

    public long inExecutionIntraBrokerDataMovementInMB() {
      return _inExecutionIntraBrokerDataMovementInMB;
    }

    public long remainingInterBrokerDataToMoveInMB() {
      return _remainingInterBrokerDataToMoveInMB;
    }

    public long remainingIntraBrokerDataToMoveInMB() {
      return _remainingIntraBrokerDataToMoveInMB;
    }

    public Map<TaskType, Map<ExecutionTaskState, Integer>> taskStat() {
      return Collections.unmodifiableMap(_taskStat);
    }

    public Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> filteredTasksByState() {
      return Collections.unmodifiableMap(_filteredTasksByState);
    }
  }
}
