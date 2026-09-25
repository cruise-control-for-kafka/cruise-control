/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;

import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class AdminResult extends AbstractCruiseControlResponse {
  @JsonResponseField(required = false)
  protected static final String SELF_HEALING_ENABLED_BEFORE = "selfHealingEnabledBefore";
  @JsonResponseField(required = false)
  protected static final String SELF_HEALING_ENABLED_AFTER = "selfHealingEnabledAfter";
  @JsonResponseField(required = false)
  protected static final String ONGOING_CONCURRENCY_CHANGE_REQUEST = "ongoingConcurrencyChangeRequest";
  @JsonResponseField(required = false)
  protected static final String DROP_RECENT_BROKERS_REQUEST = "dropRecentBrokersRequest";
  @JsonResponseField(required = false)
  protected static final String CONCURRENCY_ADJUSTER_ENABLED_BEFORE = "concurrencyAdjusterEnabledBefore";
  @JsonResponseField(required = false)
  protected static final String CONCURRENCY_ADJUSTER_ENABLED_AFTER = "concurrencyAdjusterEnabledAfter";
  @JsonResponseField(required = false)
  protected static final String MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_REQUEST = "minIsrBasedConcurrencyAdjustmentRequest";
  protected final Map<AnomalyType, Boolean> _selfHealingEnabledBefore;
  protected final Map<AnomalyType, Boolean> _selfHealingEnabledAfter;
  protected String _ongoingConcurrencyChangeRequest;
  protected String _dropRecentBrokersRequest;
  protected final Map<ConcurrencyType, Boolean> _concurrencyAdjusterEnabledBefore;
  protected final Map<ConcurrencyType, Boolean> _concurrencyAdjusterEnabledAfter;
  protected String _minIsrBasedConcurrencyAdjustmentRequest;

  public AdminResult(Map<AnomalyType, Boolean> selfHealingEnabledBefore,
                     Map<AnomalyType, Boolean> selfHealingEnabledAfter,
                     String ongoingConcurrencyChangeRequest,
                     String dropRecentBrokersRequest,
                     Map<ConcurrencyType, Boolean> concurrencyAdjusterEnabledBefore,
                     Map<ConcurrencyType, Boolean> concurrencyAdjusterEnabledAfter,
                     String minIsrBasedConcurrencyAdjustmentRequest,
                     KafkaCruiseControlConfig config) {
    super(config);
    _selfHealingEnabledBefore = selfHealingEnabledBefore;
    _selfHealingEnabledAfter = selfHealingEnabledAfter;
    _ongoingConcurrencyChangeRequest = ongoingConcurrencyChangeRequest;
    _dropRecentBrokersRequest = dropRecentBrokersRequest;
    _concurrencyAdjusterEnabledBefore = concurrencyAdjusterEnabledBefore;
    _concurrencyAdjusterEnabledAfter = concurrencyAdjusterEnabledAfter;
    _minIsrBasedConcurrencyAdjustmentRequest = minIsrBasedConcurrencyAdjustmentRequest;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
    // Discard irrelevant response.
    _selfHealingEnabledBefore.clear();
    _selfHealingEnabledAfter.clear();
    _ongoingConcurrencyChangeRequest = null;
    _dropRecentBrokersRequest = null;
    _concurrencyAdjusterEnabledBefore.clear();
    _concurrencyAdjusterEnabledAfter.clear();
    _minIsrBasedConcurrencyAdjustmentRequest = null;
  }

  protected String getJsonString() {
    // Set initial capacity to max possible capacity to avoid rehashing.
    Map<String, Object> jsonStructure = new HashMap<>();
    if (!_selfHealingEnabledBefore.isEmpty()) {
      jsonStructure.put(SELF_HEALING_ENABLED_BEFORE, _selfHealingEnabledBefore);
      jsonStructure.put(SELF_HEALING_ENABLED_AFTER, _selfHealingEnabledAfter);
    }
    if (_ongoingConcurrencyChangeRequest != null && !_ongoingConcurrencyChangeRequest.isEmpty()) {
      jsonStructure.put(ONGOING_CONCURRENCY_CHANGE_REQUEST, _ongoingConcurrencyChangeRequest);
    }
    if (_dropRecentBrokersRequest != null && !_dropRecentBrokersRequest.isEmpty()) {
      jsonStructure.put(DROP_RECENT_BROKERS_REQUEST, _dropRecentBrokersRequest);
    }
    if (!_concurrencyAdjusterEnabledBefore.isEmpty()) {
      jsonStructure.put(CONCURRENCY_ADJUSTER_ENABLED_BEFORE, _concurrencyAdjusterEnabledBefore);
      jsonStructure.put(CONCURRENCY_ADJUSTER_ENABLED_AFTER, _concurrencyAdjusterEnabledAfter);
    }
    if (_minIsrBasedConcurrencyAdjustmentRequest != null && !_minIsrBasedConcurrencyAdjustmentRequest.isEmpty()) {
      jsonStructure.put(MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_REQUEST, _minIsrBasedConcurrencyAdjustmentRequest);
    }
    jsonStructure.put(VERSION, JSON_VERSION);
    return new Gson().toJson(jsonStructure);
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("{%n"));
    if (!_selfHealingEnabledBefore.isEmpty()) {
      sb.append(String.format("%s: %s, %s: %s%n", SELF_HEALING_ENABLED_BEFORE, _selfHealingEnabledBefore,
                              SELF_HEALING_ENABLED_AFTER, _selfHealingEnabledAfter));
    }
    if (_ongoingConcurrencyChangeRequest != null && !_ongoingConcurrencyChangeRequest.isEmpty()) {
      sb.append(String.format("%s: %s%n", ONGOING_CONCURRENCY_CHANGE_REQUEST, _ongoingConcurrencyChangeRequest));
    }
    if (_dropRecentBrokersRequest != null && !_dropRecentBrokersRequest.isEmpty()) {
      sb.append(String.format("%s: %s%n", DROP_RECENT_BROKERS_REQUEST, _dropRecentBrokersRequest));
    }
    if (!_concurrencyAdjusterEnabledBefore.isEmpty()) {
      sb.append(String.format("%s: %s, %s: %s%n", CONCURRENCY_ADJUSTER_ENABLED_BEFORE, _concurrencyAdjusterEnabledBefore,
                              CONCURRENCY_ADJUSTER_ENABLED_AFTER, _concurrencyAdjusterEnabledAfter));
    }
    if (_minIsrBasedConcurrencyAdjustmentRequest != null && !_minIsrBasedConcurrencyAdjustmentRequest.isEmpty()) {
      sb.append(String.format("%s: %s%n", MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_REQUEST, _minIsrBasedConcurrencyAdjustmentRequest));
    }
    sb.append("}");

    return sb.toString();
  }
}
