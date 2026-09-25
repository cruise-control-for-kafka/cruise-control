/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.*;

/**
 * A class that wraps the execution information of a balancing proposal
 * <p>
 * The task state machine is the following:
 *
 * <pre>
 * PENDING ---&gt; IN_PROGRESS ------------&gt; COMPLETED
 *                  |
 *                  |
 *                  |----&gt; ABORTING ----&gt; ABORTED
 *                  |          |
 *                  |          v
 *                  |-------------------&gt; DEAD
 *
 * A newly created task is in {@code PENDING} state.
 * A {@code PENDING} task becomes {@code IN_PROGRESS} when it is drained from the {@link ExecutionTaskPlanner}
 * An {@code IN_PROGRESS} task becomes {@code COMPLETED} if the execution is done without error.
 * An {@code IN_PROGRESS} task becomes {@code ABORTING} if an error is encountered and the rollback is possible.
 * An {@code IN_PROGRESS} task becomes {@code DEAD} if an error is encountered and the rollback is not possible.
 * An {@code ABORTING} task becomes {@code ABORTED} if the rollback of the original task is successfully done.
 * An {@code ABORTING} task becomes {@code DEAD} if the rollback of the original task encountered an error.
 * </pre>
 */
@JsonResponseClass
public class ExecutionTask implements Comparable<ExecutionTask> {
    @JsonResponseField
    private static final String EXECUTION_ID = "executionId";
    @JsonResponseField
    private static final String TYPE = "type";
    @JsonResponseField
    private static final String STATE = "state";
    @JsonResponseField
    private static final String PROPOSAL = "proposal";
    @JsonResponseField
    private static final String BROKER_ID = "brokerId";
    private static final Map<ExecutionTaskState, Set<ExecutionTaskState>> VALID_TRANSFER = new HashMap<>();
    private final TaskType _type;
    private final long _executionId;
    private final ExecutionProposal _proposal;
    // _brokerId is only relevant for intra-broker replica action, otherwise it will be -1.
    private final int _brokerId;
    private ExecutionTaskState _state;
    private long _startTimeMs;
    private long _endTimeMs;
    private long _alertTimeMs;
    private boolean _slowExecutionReported;

    static {
        VALID_TRANSFER.put(ExecutionTaskState.PENDING, new HashSet<>(Collections.singleton(ExecutionTaskState.IN_PROGRESS)));
        VALID_TRANSFER.put(ExecutionTaskState.IN_PROGRESS,
                new HashSet<>(Arrays.asList(ExecutionTaskState.ABORTING, ExecutionTaskState.DEAD, ExecutionTaskState.COMPLETED)));
        VALID_TRANSFER.put(ExecutionTaskState.ABORTING, new HashSet<>(Arrays.asList(ExecutionTaskState.ABORTED, ExecutionTaskState.DEAD)));
        VALID_TRANSFER.put(ExecutionTaskState.COMPLETED, Collections.emptySet());
        VALID_TRANSFER.put(ExecutionTaskState.DEAD, Collections.emptySet());
        VALID_TRANSFER.put(ExecutionTaskState.ABORTED, Collections.emptySet());
    }

    /**
     * Construct an execution task.
     *
     * @param executionId                  The execution id of the proposal so we can keep track of the task when execute it.
     * @param proposal                     The corresponding balancing proposal of this task.
     * @param brokerId                     The broker to operate on if the task is of type {@link TaskType#INTRA_BROKER_REPLICA_ACTION}.
     * @param type                         The {@link TaskType} of this task.
     * @param executionAlertingThresholdMs The alerting threshold of task execution time. If the execution time exceeds this
     *                                     threshold, {@link #maybeReportExecutionTooSlow(long, List)} will be called.
     */
    public ExecutionTask(long executionId, ExecutionProposal proposal, Integer brokerId, TaskType type, long executionAlertingThresholdMs) {
        if (type != TaskType.INTRA_BROKER_REPLICA_ACTION && brokerId != null) {
            throw new IllegalArgumentException("Broker id is specified for non-intra-broker task.");
        }
        if (executionAlertingThresholdMs <= 0) {
            throw new IllegalArgumentException(String.format("Non-positive execution alerting threshold %d is set for task %d.",
                    executionAlertingThresholdMs, executionId));
        }
        _executionId = executionId;
        _proposal = proposal;
        _brokerId = brokerId == null ? -1 : brokerId;
        _state = ExecutionTaskState.PENDING;
        _type = type;
        _startTimeMs = -1L;
        _endTimeMs = -1L;
        _alertTimeMs = executionAlertingThresholdMs;
        _slowExecutionReported = false;
    }

    public ExecutionTask(long executionId, ExecutionProposal proposal, TaskType type, long executionAlertingThresholdMs) {
        this(executionId, proposal, null, type, executionAlertingThresholdMs);
    }

    /**
     * Check if the state transfer is possible.
     *
     * @param targetState the state to transfer to.
     * @return {@code true} if the transfer is valid, {@code false} otherwise.
     */
    public boolean canTransferToState(ExecutionTaskState targetState) {
        return VALID_TRANSFER.get(_state).contains(targetState);
    }

    /**
     * @return The valid target state to transfer to.
     */
    public Set<ExecutionTaskState> validTargetState() {
        return Collections.unmodifiableSet(VALID_TRANSFER.get(_state));
    }

    /**
     * @return The execution id of this execution task.
     */
    public long executionId() {
        return _executionId;
    }

    /**
     * @return The execution proposal of this execution task.
     */
    public ExecutionProposal proposal() {
        return _proposal;
    }

    /**
     * @return The task type of this execution task.
     */
    public TaskType type() {
        return _type;
    }

    /**
     * @return The state of the task.
     */
    public ExecutionTaskState state() {
        return this._state;
    }

    /**
     * @return The timestamp that the task started.
     */
    public long startTimeMs() {
        return _startTimeMs;
    }

    /**
     * @return The timestamp that the task finishes.
     */
    public long endTimeMs() {
        return _endTimeMs;
    }

    /**
     * @return The broker id for intra-broker replica movement task.
     */
    public int brokerId() {
        return _brokerId;
    }

    /**
     * Mark task in progress.
     *
     * @param now Current system time.
     */
    public void inProgress(long now) {
        ensureValidTransfer(ExecutionTaskState.IN_PROGRESS);
        this._state = ExecutionTaskState.IN_PROGRESS;
        _startTimeMs = now;
        _alertTimeMs += now;
    }

    /**
     * Kill the task.
     *
     * @param now Current system time.
     */
    public void kill(long now) {
        ensureValidTransfer(ExecutionTaskState.DEAD);
        this._state = ExecutionTaskState.DEAD;
        _endTimeMs = now;
    }

    /**
     * Abort the task.
     */
    public void abort() {
        ensureValidTransfer(ExecutionTaskState.ABORTING);
        this._state = ExecutionTaskState.ABORTING;
    }

    /**
     * Change the task state to aborted.
     *
     * @param now Current system time.
     */
    public void aborted(long now) {
        ensureValidTransfer(ExecutionTaskState.ABORTED);
        this._state = ExecutionTaskState.ABORTED;
        _endTimeMs = now;
    }

    /**
     * Change the task state to completed.
     *
     * @param now Current system time.
     */
    public void completed(long now) {
        ensureValidTransfer(ExecutionTaskState.COMPLETED);
        this._state = ExecutionTaskState.COMPLETED;
        _endTimeMs = now;
    }

    /**
     * If the task's execution time exceeds alerting threshold, add the task to the group of tasks for which an alert will
     * be sent out.
     * Note to avoid repetitive alerts for the same task, the task will be added to the group only for the first time the
     * slow execution is detected.
     *
     * @param now           Current system time.
     * @param tasksToReport A list of tasks for which a slow execution alert will be sent out.
     */
    public void maybeReportExecutionTooSlow(long now, List<ExecutionTask> tasksToReport) {
        if (!_slowExecutionReported && (_state == ExecutionTaskState.IN_PROGRESS || _state == ExecutionTaskState.ABORTING) && now > _alertTimeMs) {
            tasksToReport.add(this);
            // Mute the task to prevent sending the same alert repeatedly.
            _slowExecutionReported = true;
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ExecutionTask && _executionId == ((ExecutionTask) o)._executionId;
    }

    @Override
    public int hashCode() {
        return (int) _executionId;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        Map<String, Object> executionStatsMap = new HashMap<>();
        executionStatsMap.put(EXECUTION_ID, _executionId);
        executionStatsMap.put(TYPE, _type);
        executionStatsMap.put(STATE, _state);
        executionStatsMap.put(PROPOSAL, _proposal.getJsonStructure());
        if (_type == TaskType.INTRA_BROKER_REPLICA_ACTION) {
            executionStatsMap.put(BROKER_ID, _brokerId);
        }
        return executionStatsMap;
    }

    private void ensureValidTransfer(ExecutionTaskState targetState) {
        if (!canTransferToState(targetState)) {
            throw new IllegalStateException("Cannot mark a task in " + _state + " to " + targetState + " state. The "
                    + "valid target state are " + validTargetState());
        }
    }

    public enum TaskType {
        INTER_BROKER_REPLICA_ACTION, INTRA_BROKER_REPLICA_ACTION, LEADER_ACTION;

        private static final List<TaskType> CACHED_VALUES = List.of(values());

        /**
         * Use this instead of values() because values() creates a new array each time.
         *
         * @return enumerated values in the same order as values()
         */
        public static List<TaskType> cachedValues() {
            return Collections.unmodifiableList(CACHED_VALUES);
        }
    }

    @Override
    public String toString() {
        switch (_type) {
            case INTRA_BROKER_REPLICA_ACTION:
                return String.format("{EXE_ID: %d, %s(%d), %s, %s}", _executionId, _type, _brokerId, _proposal, _state);
            case INTER_BROKER_REPLICA_ACTION:
            case LEADER_ACTION:
                return String.format("{EXE_ID: %d, %s, %s, %s}", _executionId, _type, _proposal, _state);
            default:
                throw new IllegalStateException("Unknown task type " + _type);
        }
    }

    @Override
    public int compareTo(ExecutionTask o) {
        return Long.compare(this._executionId, o._executionId);
    }
}
