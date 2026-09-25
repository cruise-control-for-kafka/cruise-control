/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.LinearRegressionModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import org.apache.commons.math3.linear.SingularMatrixException;

import java.util.*;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.IN_PROGRESS_STATES;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class CruiseControlState extends AbstractCruiseControlResponse {
  protected static final String INTER_BROKER_PARTITION_MOVEMENTS = "inter-broker partition movements";
  protected static final String INTRA_BROKER_PARTITION_MOVEMENTS = "intra-broker partition movements";
  protected static final String LEADERSHIP_MOVEMENTS = "leadership movements";
  @JsonResponseField(required = false)
  protected static final String MONITOR_STATE = "MonitorState";
  @JsonResponseField(required = false)
  protected static final String EXECUTOR_STATE = "ExecutorState";
  @JsonResponseField(required = false)
  protected static final String ANALYZER_STATE = "AnalyzerState";
  @JsonResponseField(required = false)
  protected static final String ANOMALY_DETECTOR_STATE = "AnomalyDetectorState";
  protected ExecutorState _executorState;
  protected LoadMonitorState _monitorState;
  protected AnalyzerState _analyzerState;
  protected AnomalyDetectorState _anomalyDetectorState;

  public CruiseControlState(ExecutorState executionState,
                            LoadMonitorState monitorState,
                            AnalyzerState analyzerState,
                            AnomalyDetectorState anomalyDetectorState,
                            KafkaCruiseControlConfig config) {
    super(config);
    _executorState = executionState;
    _monitorState = monitorState;
    _analyzerState = analyzerState;
    _anomalyDetectorState = anomalyDetectorState;
  }

  protected String getJsonString(CruiseControlParameters parameters) {
    Gson gson = new Gson();
    Map<String, Object> jsonStructure = getJsonStructure(((CruiseControlStateParameters) parameters).isVerbose());
    jsonStructure.put(VERSION, JSON_VERSION);
    return gson.toJson(jsonStructure);
  }

  /**
   * @param verbose {@code true} if verbose, {@code false} otherwise.
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> cruiseControlState = new HashMap<>();
    if (_monitorState != null) {
      cruiseControlState.put(MONITOR_STATE, _monitorState.getJsonStructure(verbose));
    }
    if (_executorState != null) {
      cruiseControlState.put(EXECUTOR_STATE, _executorState.getJsonStructure(verbose));
    }
    if (_analyzerState != null) {
      cruiseControlState.put(ANALYZER_STATE, _analyzerState.getJsonStructure(verbose));
    }
    if (_anomalyDetectorState != null) {
      cruiseControlState.put(ANOMALY_DETECTOR_STATE, _anomalyDetectorState.getJsonStructure());
    }

    return cruiseControlState;
  }

  protected void writeVerboseMonitorState(StringBuilder sb) {
    if (_monitorState != null) {
      sb.append(String.format("%n%nMonitored Windows [Window End_Time=Data_Completeness]:%n"));

      StringJoiner joiner = new StringJoiner(", ", "{", "}");
      for (Map.Entry<Long, Float> entry : _monitorState.monitoredWindows().entrySet()) {
        joiner.add(String.format("%d=%.3f%%", entry.getKey(), entry.getValue() * 100));
      }
      sb.append(joiner);
    }
  }

  protected void writeVerboseAnalyzerState(StringBuilder sb) {
    if (_analyzerState != null) {
      sb.append(String.format("%n%nGoal Readiness:%n"));
      for (Map.Entry<Goal, Boolean> entry : _analyzerState.readyGoals().entrySet()) {
        Goal goal = entry.getKey();
        sb.append(String.format("%50s, %s, %s%n", goal.getClass().getSimpleName(), goal.clusterModelCompletenessRequirements(),
                                entry.getValue() ? "Ready" : "NotReady"));
      }
    }
  }

  protected void writeVerboseExecutorState(StringBuilder sb) {
    if (_executorState != null) {
      // There is no execution task summary if no execution is in progress.
      if (!IN_PROGRESS_STATES.contains(_executorState.state())) {
        return;
      }
      Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> taskSnapshot = _executorState.executionTasksSummary().filteredTasksByState();
      taskSnapshot.forEach((type, taskMap) -> {
        String taskTypeString = type == TaskType.INTER_BROKER_REPLICA_ACTION
                                ? INTER_BROKER_PARTITION_MOVEMENTS : type == TaskType.INTRA_BROKER_REPLICA_ACTION
                                                                     ? INTRA_BROKER_PARTITION_MOVEMENTS : LEADERSHIP_MOVEMENTS;
        sb.append(String.format("%n%n%s %s:%n",
                                _executorState.state() == ExecutorState.State.STOPPING_EXECUTION ? "Cancelled" : "Pending",
                                taskTypeString));
        for (ExecutionTask task : taskMap.get(ExecutionTaskState.PENDING)) {
          sb.append(String.format("%s%n", task));
        }
        sb.append(String.format("%n%nIn progress %s:%n", taskTypeString));
        for (ExecutionTask task : taskMap.get(ExecutionTaskState.IN_PROGRESS)) {
          sb.append(String.format("%s%n", task));
        }
        sb.append(String.format("%n%nAborting %s:%n", taskTypeString));
        for (ExecutionTask task : taskMap.get(ExecutionTaskState.ABORTING)) {
          sb.append(String.format("%s%n", task));
        }
        sb.append(String.format("%n%nAborted %s:%n", taskTypeString));
        for (ExecutionTask task : taskMap.get(ExecutionTaskState.ABORTED)) {
          sb.append(String.format("%s%n", task));
        }
        sb.append(String.format("%n%nDead %s:%n", taskTypeString));
        for (ExecutionTask task : taskMap.get(ExecutionTaskState.DEAD)) {
          sb.append(String.format("%s%n", task));
        }
      });
    }
  }

  /**
   * Super verbose response is used only for debugging and development purposes.
   * It is not exposed in JSON response, and would be dropped in a future release.
   *
   * @param sb String builder to append super verbose response.
   */
  protected void writeSuperVerbose(StringBuilder sb) {
    if (_monitorState != null) {
      LinearRegressionModelParameters.LinearRegressionModelState linearRegressionModelState;
      try {
        linearRegressionModelState = _monitorState.detailTrainingProgress();
      } catch (SingularMatrixException e) {
        sb.append(String.format("%n%nFailed to calculate Linear Regression Model State with error message:%n%s", e.getMessage()));
        return;
      }
      sb.append(String.format("%n%nLinear Regression Model State:%n%s", linearRegressionModelState));
    }
  }

  protected String getPlaintext(CruiseControlParameters parameters) {
    boolean verbose = ((CruiseControlStateParameters) parameters).isVerbose();
    boolean superVerbose = ((CruiseControlStateParameters) parameters).isSuperVerbose();

    StringBuilder sb = new StringBuilder();
    sb.append(_monitorState != null ? String.format("MonitorState: %s%n", _monitorState) : "");
    sb.append(_executorState == null ? "" : String.format("ExecutorState: %s%n", _executorState.getPlaintext()));
    sb.append(_analyzerState != null ? String.format("AnalyzerState: %s%n", _analyzerState) : "");
    sb.append(_anomalyDetectorState != null ? String.format("AnomalyDetectorState: %s%n", _anomalyDetectorState) : "");

    if (verbose || superVerbose) {
      writeVerboseMonitorState(sb);
      writeVerboseAnalyzerState(sb);
      writeVerboseExecutorState(sb);
      if (superVerbose) {
        writeSuperVerbose(sb);
      }
    }

    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString(parameters) : getPlaintext(parameters);
    // Discard irrelevant response.
    _executorState = null;
    _monitorState = null;
    _analyzerState = null;
    _anomalyDetectorState = null;
  }

  public enum SubState {
    ANALYZER, MONITOR, EXECUTOR, ANOMALY_DETECTOR;

    protected static final List<SubState> CACHED_VALUES = List.of(values());

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<SubState> cachedValues() {
      return Collections.unmodifiableList(CACHED_VALUES);
    }
  }
}
