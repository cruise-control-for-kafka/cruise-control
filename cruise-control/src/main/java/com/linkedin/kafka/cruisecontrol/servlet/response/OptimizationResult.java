/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddedOrRemovedBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.KafkaOptimizationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class OptimizationResult extends AbstractCruiseControlResponse {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizationResult.class);
  @JsonResponseField
  protected static final String SUMMARY = "summary";
  @JsonResponseField(required = false)
  protected static final String PROPOSALS = "proposals";
  @JsonResponseField
  protected static final String GOAL_SUMMARY = "goalSummary";
  @JsonResponseField
  protected static final String LOAD_AFTER_OPTIMIZATION = "loadAfterOptimization";
  @JsonResponseField(required = false)
  protected static final String LOAD_BEFORE_OPTIMIZATION = "loadBeforeOptimization";
  protected OptimizerResult _optimizerResult;
  protected String _cachedJsonResponse;
  protected String _cachedPlaintextResponse;

  public OptimizationResult(OptimizerResult optimizerResult, KafkaCruiseControlConfig config) {
    super(config);
    _optimizerResult = optimizerResult;
    _cachedJsonResponse = null;
    _cachedPlaintextResponse = null;
  }

  public OptimizerResult optimizerResult() {
    return _optimizerResult;
  }

  /**
   * @return JSON response if cached, null otherwise.
   */
  public String cachedJsonResponse() {
    return _cachedJsonResponse;
  }

  /**
   * @return Plaintext response if cached, null otherwise.
   */
  public String cachedPlaintextResponse() {
    return _cachedPlaintextResponse;
  }

  protected String getPlaintextPretext(CruiseControlParameters parameters) {
    switch ((CruiseControlEndPoint) parameters.endPoint()) {
      case ADD_BROKER:
        return String.format("%n%nCluster load after adding broker %s:%n", ((AddedOrRemovedBrokerParameters) parameters).brokerIds());
      case REMOVE_BROKER:
        return String.format("%n%nCluster load after removing broker %s:%n", ((AddedOrRemovedBrokerParameters) parameters).brokerIds());
      case FIX_OFFLINE_REPLICAS:
        return String.format("%n%nCluster load after fixing offline replicas:%n");
      case PROPOSALS:
        return String.format("%n%nOptimized load:%n");
      case REBALANCE:
        return String.format("%n%nCluster load after rebalance:%n");
      case DEMOTE_BROKER:
        return String.format("%n%nCluster load after demoting broker %s:%n", ((DemoteBrokerParameters) parameters).brokerIds());
      case TOPIC_CONFIGURATION:
        return String.format("%n%nCluster load after updating replication factor of topics %s%n",
                             _optimizerResult.topicsWithReplicationFactorChange());
      case REMOVE_DISKS:
        return String.format("%n%nCluster load after removing disks %s:%n", ((RemoveDisksParameters) parameters).brokerIdAndLogdirs());
      default:
        LOG.error("Unrecognized endpoint.");
        return "Unrecognized endpoint.";
    }
  }

  protected String getPlaintext(boolean isVerbose, String pretext) {
    StringBuilder sb = new StringBuilder();
    if (isVerbose) {
      sb.append(_optimizerResult.goalProposals().toString());
    }

    writeProposalSummary(sb);
    if (isVerbose) {
      sb.append(String.format("%n%nCurrent load:%n%s", _optimizerResult.brokerStatsBeforeOptimization().toString()));
    }

    sb.append(String.format("%s%s", pretext, _optimizerResult.brokerStatsAfterOptimization().toString()));
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    boolean isVerbose = ((KafkaOptimizationParameters) parameters).isVerbose();
    _cachedResponse = parameters.json() ? getJsonString(isVerbose) : getPlaintext(isVerbose, getPlaintextPretext(parameters));
    if (parameters.json()) {
      _cachedJsonResponse = _cachedResponse;
    } else {
      _cachedPlaintextResponse = _cachedResponse;
    }
    // Discard irrelevant response.
    _optimizerResult = null;
  }

  /**
   * Keeps the JSON and plaintext response and discards the optimizer result.
   */
  public void discardIrrelevantAndCacheJsonAndPlaintext() {
    if (_optimizerResult != null) {
      _cachedJsonResponse = getJsonString(false);
      _cachedPlaintextResponse = getPlaintext(false, String.format("%n%nCluster load after self-healing:%n"));
      // Discard irrelevant response.
      _optimizerResult = null;
    }
  }

  protected String getJsonString(boolean isVerbose) {
    Map<String, Object> optimizationResult = new HashMap<>();
    if (isVerbose) {
      optimizationResult.put(PROPOSALS, _optimizerResult.goalProposals().stream()
                                                        .map(ExecutionProposal::getJsonStructure).collect(Collectors.toSet()));
      optimizationResult.put(LOAD_BEFORE_OPTIMIZATION, _optimizerResult.brokerStatsBeforeOptimization().getJsonStructure());
    }

    optimizationResult.put(SUMMARY, _optimizerResult.getProposalSummaryForJson());
    List<Map<String, Object>> goalSummary = new ArrayList<>();
    for (String goalName : _optimizerResult.statsByGoalName().keySet()) {
      goalSummary.add(new GoalStatus(goalName).getJsonStructure());
    }
    optimizationResult.put(GOAL_SUMMARY, goalSummary);
    optimizationResult.put(LOAD_AFTER_OPTIMIZATION, _optimizerResult.brokerStatsAfterOptimization().getJsonStructure());
    optimizationResult.put(VERSION, JSON_VERSION);
    Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();

    return gson.toJson(optimizationResult);
  }

  protected void writeProposalSummary(StringBuilder sb) {
    sb.append(_optimizerResult.getProposalSummary());
    for (Map.Entry<String, ClusterModelStats> entry : _optimizerResult.statsByGoalName().entrySet()) {
      String goalName = entry.getKey();
      sb.append(String.format("%n%n[%6d ms] Stats for %s(%s):%n", _optimizerResult.optimizationDuration(goalName).toMillis(), goalName,
                              _optimizerResult.goalResultDescription(goalName)));
      sb.append(entry.getValue().toString());
    }
  }

  @JsonResponseClass
  protected class GoalStatus {
    @JsonResponseField
    protected static final String GOAL = "goal";
    @JsonResponseField
    protected static final String STATUS = "status";
    @JsonResponseField
    protected static final String CLUSTER_MODEL_STATS = "clusterModelStats";
    @JsonResponseField
    protected static final String OPTIMIZATION_TIME_MS = "optimizationTimeMs";
    protected String _goalName;

    GoalStatus(String goalName) {
      _goalName = goalName;
    }

    protected Map<String, Object> getJsonStructure() {
      return Map.of(GOAL, _goalName, STATUS, _optimizerResult.goalResultDescription(_goalName),
                    CLUSTER_MODEL_STATS, _optimizerResult.statsByGoalName().get(_goalName).getJsonStructure(),
                    OPTIMIZATION_TIME_MS, _optimizerResult.optimizationDuration(_goalName).toMillis());
    }
  }
}
