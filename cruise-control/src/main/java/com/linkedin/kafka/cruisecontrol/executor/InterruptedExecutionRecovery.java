/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionState;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;


/**
 * What happened to the latest execution that was interrupted by a Cruise Control restart -- reported in the executor state
 * (see {@link ExecutorState#recovery()}) from the startup until the next execution starts, so that operators can tell whether
 * Cruise Control resumed it, is cleaning it up, or why it did not resume it.
 */
@JsonResponseClass
final class InterruptedExecutionRecovery {
  @JsonResponseField
  private static final String UUID = "uuid";
  @JsonResponseField
  private static final String REASON = "reason";
  @JsonResponseField
  private static final String TRIGGERED_BY_USER_REQUEST = "triggeredByUserRequest";
  @JsonResponseField
  private static final String START_TIME = "startTime";
  @JsonResponseField
  private static final String INTERRUPTED_TIME = "interruptedTime";
  @JsonResponseField
  private static final String RESUME_COUNT = "resumeCount";
  @JsonResponseField
  private static final String STATUS = "status";
  @JsonResponseField(required = false)
  private static final String REASON_NOT_RESUMED = "reasonNotResumed";
  @JsonResponseField(required = false)
  private static final String DIVERGENCES = "divergences";
  @JsonResponseField(required = false)
  private static final String TASKS_BY_RECOVERY_ACTION = "tasksByRecoveryAction";
  @JsonResponseField(required = false)
  private static final String IN_FLIGHT_PARTITION_MOVEMENTS = "inFlightPartitionMovements";
  @JsonResponseField(required = false)
  private static final String CLEANUP_DEADLINE = "cleanupDeadline";
  @JsonResponseField(required = false)
  private static final String KEPT_THROTTLES = "keptThrottles";
  @JsonResponseField(required = false)
  private static final String ERROR = "error";

  /**
   * The status of the recovery.
   */
  enum Status {
    /** Deciding whether to resume the interrupted execution. */
    DECIDING,
    /** The interrupted execution has been resumed -- see the executor state for its progress. */
    RESUMED,
    /** Waiting for the partition reassignments that the interrupted execution started to finish. */
    WAITING_FOR_IN_FLIGHT_MOVEMENTS,
    /** Retrying to reach the Kafka cluster to clean up the interrupted execution. */
    WAITING_FOR_KAFKA,
    /** Cancelling the partition reassignments that the interrupted execution started -- due to a stop request or a timeout. */
    CANCELLING_IN_FLIGHT_MOVEMENTS,
    /** Removing the replication throttles that the interrupted execution left behind. */
    REMOVING_THROTTLES,
    /** The interrupted execution has been cleaned up. */
    CLEANED_UP,
    /** The cleanup failed -- it is retried upon the next restart. */
    FAILED,
    /** Cruise Control is shutting down before the cleanup finished -- it is retried upon the next restart. */
    RETAINED_FOR_RESTART
  }

  private static final int MAX_LISTED_ITEMS = 20;
  private final String _uuid;
  private final String _reason;
  private final boolean _triggeredByUserRequest;
  private final long _startTimeMs;
  private final long _lastUpdateTimeMs;
  private final int _resumeCount;
  private Status _status;
  private String _reasonNotResumed;
  private List<String> _divergences;
  private Map<String, Integer> _tasksByAction;
  private List<String> _inFlightMovements;
  private long _cleanupDeadlineMs;
  private List<String> _keptThrottles;
  private String _error;

  InterruptedExecutionRecovery(PersistedExecutionState state) {
    _uuid = state.uuid();
    _reason = state.reason();
    _triggeredByUserRequest = state.triggeredByUserRequest();
    _startTimeMs = state.startTimeMs();
    _lastUpdateTimeMs = state.lastUpdateTimeMs();
    _resumeCount = state.resumeCount();
    _divergences = Collections.emptyList();
    _inFlightMovements = Collections.emptyList();
    _keptThrottles = Collections.emptyList();
    _cleanupDeadlineMs = -1L;
    _status = Status.DECIDING;
  }

  String uuid() {
    return _uuid;
  }

  synchronized Status status() {
    return _status;
  }

  synchronized void setStatus(Status status) {
    _status = status;
  }

  synchronized void setReasonNotResumed(String reasonNotResumed) {
    _reasonNotResumed = reasonNotResumed;
  }

  synchronized List<String> divergences() {
    return _divergences;
  }

  synchronized void setDivergences(List<String> divergences) {
    _divergences = Collections.unmodifiableList(new ArrayList<>(divergences));
  }

  synchronized void setTasksByAction(Map<ExecutionRecoveryUtils.RecoveryAction, Integer> tasksByAction) {
    Map<String, Integer> byName = new LinkedHashMap<>();
    tasksByAction.forEach((action, count) -> byName.put(action.name(), count));
    _tasksByAction = byName;
  }

  synchronized void setInFlightMovements(Collection<TopicPartition> inFlightMovements) {
    _inFlightMovements = new ArrayList<>(new TreeSet<>(inFlightMovements.stream().map(TopicPartition::toString).collect(Collectors.toSet())));
  }

  synchronized void setCleanupDeadlineMs(long cleanupDeadlineMs) {
    _cleanupDeadlineMs = cleanupDeadlineMs;
  }

  synchronized void setKeptThrottles(List<String> keptThrottles) {
    _keptThrottles = Collections.unmodifiableList(new ArrayList<>(keptThrottles));
  }

  synchronized List<String> keptThrottles() {
    return _keptThrottles;
  }

  synchronized void setFailed(String error) {
    _status = Status.FAILED;
    _error = error;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  synchronized Map<String, Object> getJsonStructure() {
    Map<String, Object> json = new LinkedHashMap<>();
    json.put(UUID, _uuid);
    json.put(REASON, _reason);
    json.put(TRIGGERED_BY_USER_REQUEST, _triggeredByUserRequest);
    json.put(START_TIME, utcDateFor(_startTimeMs));
    json.put(INTERRUPTED_TIME, utcDateFor(_lastUpdateTimeMs));
    json.put(RESUME_COUNT, _resumeCount);
    json.put(STATUS, _status.name());
    if (_reasonNotResumed != null) {
      json.put(REASON_NOT_RESUMED, _reasonNotResumed);
    }
    if (!_divergences.isEmpty()) {
      json.put(DIVERGENCES, firstItems(_divergences));
    }
    if (_tasksByAction != null) {
      json.put(TASKS_BY_RECOVERY_ACTION, _tasksByAction);
    }
    if (!_inFlightMovements.isEmpty()) {
      json.put(IN_FLIGHT_PARTITION_MOVEMENTS, firstItems(_inFlightMovements));
    }
    if (_cleanupDeadlineMs > 0) {
      json.put(CLEANUP_DEADLINE, utcDateFor(_cleanupDeadlineMs));
    }
    if (!_keptThrottles.isEmpty()) {
      json.put(KEPT_THROTTLES, firstItems(_keptThrottles));
    }
    if (_error != null) {
      json.put(ERROR, _error);
    }
    return json;
  }

  private static List<String> firstItems(List<String> items) {
    if (items.size() <= MAX_LISTED_ITEMS) {
      return items;
    }
    List<String> firstItems = new ArrayList<>(items.subList(0, MAX_LISTED_ITEMS));
    firstItems.add(String.format("... and %d more", items.size() - MAX_LISTED_ITEMS));
    return firstItems;
  }
}
