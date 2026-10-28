/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * The persisted state of a single {@link ExecutionTask}. The proposal of the task is persisted separately (once per
 * partition) in {@link PersistedExecutionState#proposals()}.
 */
public final class PersistedTask {
  public static final int NO_BROKER_ID = -1;
  private final ExecutionTask.TaskType _type;
  private final TopicPartition _topicPartition;
  private final int _brokerId;
  private final ExecutionTaskState _state;
  private final long _startTimeMs;
  private final long _endTimeMs;

  /**
   * @param type Type of the task.
   * @param topicPartition Partition of the task.
   * @param brokerId Broker of an intra-broker replica task, or {@link #NO_BROKER_ID} for other task types.
   * @param state State of the task.
   * @param startTimeMs Time the task started, or -1 if not started.
   * @param endTimeMs Time the task finished, or -1 if not finished.
   */
  public PersistedTask(ExecutionTask.TaskType type,
                       TopicPartition topicPartition,
                       int brokerId,
                       ExecutionTaskState state,
                       long startTimeMs,
                       long endTimeMs) {
    _type = validateNotNull(type, "Task type cannot be null.");
    _topicPartition = validateNotNull(topicPartition, "Topic partition cannot be null.");
    _brokerId = brokerId;
    _state = validateNotNull(state, "Task state cannot be null.");
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
  }

  /**
   * @param task Execution task to persist.
   * @return The persisted state of the given task.
   */
  public static PersistedTask of(ExecutionTask task) {
    return new PersistedTask(task.type(), task.proposal().topicPartition(), task.brokerId(), task.state(), task.startTimeMs(),
                             task.endTimeMs());
  }

  public ExecutionTask.TaskType type() {
    return _type;
  }

  public TopicPartition topicPartition() {
    return _topicPartition;
  }

  public int brokerId() {
    return _brokerId;
  }

  public ExecutionTaskState state() {
    return _state;
  }

  public long startTimeMs() {
    return _startTimeMs;
  }

  public long endTimeMs() {
    return _endTimeMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistedTask)) {
      return false;
    }
    PersistedTask that = (PersistedTask) o;
    return _brokerId == that._brokerId && _startTimeMs == that._startTimeMs && _endTimeMs == that._endTimeMs
           && _type == that._type && _topicPartition.equals(that._topicPartition) && _state == that._state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _topicPartition, _brokerId, _state, _startTimeMs, _endTimeMs);
  }

  @Override
  public String toString() {
    return String.format("{%s, %s%s, %s}", _type, _topicPartition, _brokerId == NO_BROKER_ID ? "" : ", broker " + _brokerId, _state);
  }
}
