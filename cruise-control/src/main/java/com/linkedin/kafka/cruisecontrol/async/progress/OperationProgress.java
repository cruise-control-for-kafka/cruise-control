/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jakarta.servlet.http.HttpSession;


/**
 * A class to track the progress of a task. This class is used to allow different users to trigger
 * an endpoint which may take a while for Cruise Control to respond, e.g. getting a complicated proposal.
 * Cruise Control will use {@link HttpSession} to keep track the progress of such requests and
 * report the progress to the users.
 */
@JsonResponseClass
public class OperationProgress {
  @JsonResponseField
  protected static final String OPERATION = "operation";
  @JsonResponseField
  protected static final String OPERATION_PROGRESS = "operationProgress";
  private boolean _mutable = true;
  private List<OperationStep> _steps = new ArrayList<>();
  private List<Long> _startTimes = new ArrayList<>();
  private final String _operation;

  public OperationProgress() {
    this("");
  }

  public OperationProgress(String operation) {
    _operation = operation;
  }

  /**
   * Add a {@link OperationStep} to the progress.
   * @param step the operation step to add.
   */
  public synchronized void addStep(OperationStep step) {
    ensureMutable();
    _steps.add(step);
    _startTimes.add(System.currentTimeMillis());
  }

  /**
   * Refer this operation progress to another one. This is useful when multiple operations are waiting for the
   * same background task to finish.
   *
   * Once this OperationProgress is referring to another OperationProgress, this OperationProgress becomes immutable
   * to avoid accidental change of the referred OperationProgress.
   *
   * @param other the other operation progress to refer to.
   */
  public void refer(OperationProgress other) {
    // ensure the integrity and avoid dead lock.
    List<OperationStep> steps;
    List<Long> startTimes;
    synchronized (other) {
      steps = other._steps;
      startTimes = other._startTimes;
    }
    synchronized (this) {
      ensureMutable();
      this._steps = steps;
      this._startTimes = startTimes;
      this._mutable = false;
    }
  }

  /**
   * @return The list of operation steps in this operation progress.
   */
  public synchronized List<OperationStep> progress() {
    return Collections.unmodifiableList(_steps);
  }

  /**
   * Clear the progress.
   */
  public synchronized void clear() {
    this._mutable = true;
    _steps.clear();
    _startTimes.clear();
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _steps.size(); i++) {
      OperationStep step = _steps.get(i);
      long time = (i == _steps.size() - 1 ? System.currentTimeMillis() : _startTimes.get(i + 1)) - _startTimes.get(i);
      sb.append(new StepProgress(step, time));
    }
    return sb.toString();
  }

  /**
   * @return the up-to-date total execution time on the progress
   */
  public synchronized long getCurrentTotalExecutionTimeMs() {
    if (_startTimes.isEmpty()) {
      return 0;
    } else {
      long currentTime = System.currentTimeMillis();
      long startTime = _startTimes.get(0);
      return currentTime - startTime;
    }
  }

  /**
   * @return the name of the operation
   */
  public String getOperation() {
    return _operation;
  }

  /**
   * @return The map describing the progress of the operation.
   */
  public Map<String, Object> getJsonStructure() {
    return Map.of(OPERATION, _operation, OPERATION_PROGRESS, getProgress());
  }

  private synchronized Object[] getProgress() {
    Object[] progressArray = new Object[_steps.size()];
    for (int i = 0; i < _steps.size(); i++) {
      OperationStep step = _steps.get(i);
      long time = (i == _steps.size() - 1 ? System.currentTimeMillis() : _startTimes.get(i + 1)) - _startTimes.get(i);
      progressArray[i] = new StepProgress(step, time).getJsonStructure();
    }
    return progressArray;
  }

  private void ensureMutable() {
    if (!_mutable) {
      throw new IllegalStateException("Cannot change this operation progress because it is immutable.");
    }
  }
}
