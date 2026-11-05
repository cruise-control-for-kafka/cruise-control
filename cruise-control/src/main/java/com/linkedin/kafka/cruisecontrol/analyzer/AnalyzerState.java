/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;


/**
 * The state for the analyzer.
 */
@JsonResponseClass
public class AnalyzerState {
  @JsonResponseField
  @Schema(type = "boolean")
  private static final String IS_PROPOSAL_READY = "isProposalReady";
  @JsonResponseField
  @ArraySchema(schema = @Schema(type = "string"))
  private static final String READY_GOALS = "readyGoals";
  @JsonResponseField(required = false)
  @ArraySchema(schema = @Schema(ref = "#/components/schemas/GoalReadinessRecord"))
  private static final String GOAL_READINESS = "goalReadiness";
  private static final String READY = "ready";
  private static final String NOT_READY = "notReady";
  private final boolean _isProposalReady;
  private final Map<Goal, Boolean> _readyGoals;

  /**
   * @param isProposalReady {@code true} if the goal optimizer has valid cached proposals for optimization with the default goals.
   * @param readyGoals Goals that are ready for self-healing.
   */
  public AnalyzerState(boolean isProposalReady, Map<Goal, Boolean> readyGoals) {
    _isProposalReady = isProposalReady;
    _readyGoals = readyGoals;
  }

  /**
   * @return {@code true} if the proposal is ready, {@code false} otherwise.
   */
  public boolean proposalReady() {
    return _isProposalReady;
  }

  /**
   * @return A map of ready goals.
   */
  public Map<Goal, Boolean> readyGoals() {
    return Collections.unmodifiableMap(_readyGoals);
  }

  /**
   * @param verbose {@code true} if verbose, {@code false} otherwise.
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure(boolean verbose) {
    Map<String, Object> analyzerState = new HashMap<>();
    Set<String> readyGoalNames = new HashSet<>();
    for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
      if (entry.getValue()) {
        readyGoalNames.add(entry.getKey().name());
      }
    }
    analyzerState.put(IS_PROPOSAL_READY, _isProposalReady);
    analyzerState.put(READY_GOALS, readyGoalNames);
    if (verbose) {
      List<Object> goalReadinessList = new ArrayList<>(_readyGoals.size());
      for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
        Goal goal = entry.getKey();
        String goalReadyStatus = entry.getValue() ? READY : NOT_READY;
        GoalReadinessRecord goalReadinessRecord = new GoalReadinessRecord(goal, goalReadyStatus);
        goalReadinessList.add(goalReadinessRecord.getJsonStructure());
      }
      analyzerState.put(GOAL_READINESS, goalReadinessList);
    }
    return analyzerState;
  }

  @Override
  public String toString() {
    Set<String> readyGoalNames = new HashSet<>();
    for (Map.Entry<Goal, Boolean> entry : _readyGoals.entrySet()) {
      if (entry.getValue()) {
        readyGoalNames.add(entry.getKey().getClass().getSimpleName());
      }
    }
    return String.format("{%s: %s, %s: %s}", IS_PROPOSAL_READY, _isProposalReady, READY_GOALS, readyGoalNames);
  }
}
