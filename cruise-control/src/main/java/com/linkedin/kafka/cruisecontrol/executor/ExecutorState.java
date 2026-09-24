/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencySummary;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.*;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.*;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;

@JsonResponseClass
public final class ExecutorState {
    @JsonResponseField(required = false)
    private static final String TRIGGERED_USER_TASK_ID = "triggeredUserTaskId";
    @JsonResponseField(required = false)
    private static final String TRIGGERED_SELF_HEALING_TASK_ID = "triggeredSelfHealingTaskId";
    @JsonResponseField(required = false)
    private static final String TRIGGERED_TASK_REASON = "triggeredTaskReason";
    @JsonResponseField
    private static final String STATE = "state";
    @JsonResponseField(required = false)
    private static final String RECENTLY_DEMOTED_BROKERS = "recentlyDemotedBrokers";
    @JsonResponseField(required = false)
    private static final String RECENTLY_REMOVED_BROKERS = "recentlyRemovedBrokers";

    @JsonResponseField(required = false)
    private static final String NUM_TOTAL_LEADERSHIP_MOVEMENTS = "numTotalLeadershipMovements";
    @JsonResponseField(required = false)
    private static final String NUM_PENDING_LEADERSHIP_MOVEMENTS = "numPendingLeadershipMovements";
    @JsonResponseField(required = false)
    private static final String NUM_CANCELLED_LEADERSHIP_MOVEMENTS = "numCancelledLeadershipMovements";
    @JsonResponseField(required = false)
    private static final String NUM_FINISHED_LEADERSHIP_MOVEMENTS = "numFinishedLeadershipMovements";
    @JsonResponseField(required = false)
    private static final String PENDING_LEADERSHIP_MOVEMENT = "pendingLeadershipMovement";
    @JsonResponseField(required = false)
    private static final String CANCELLED_LEADERSHIP_MOVEMENT = "cancelledLeadershipMovement";
    @JsonResponseField(required = false)
    private static final String MAXIMUM_BROKER_LEADER_MOVEMENT_CONCURRENCY = "maximumBrokerLeaderMovementConcurrency";
    @JsonResponseField(required = false)
    private static final String MINIMUM_BROKER_LEADER_MOVEMENT_CONCURRENCY = "minimumBrokerLeaderMovementConcurrency";
    @JsonResponseField(required = false)
    private static final String AVERAGE_BROKER_LEADER_MOVEMENT_CONCURRENCY = "averageBrokerLeaderMovementConcurrency";
    @JsonResponseField(required = false)
    private static final String CLUSTER_LEADER_MOVEMENT_CONCURRENCY = "clusterLeaderMovementConcurrency";
    @JsonResponseField(required = false)
    private static final String NUM_TOTAL_INTER_BROKER_PARTITION_MOVEMENTS = "numTotalPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_PENDING_INTER_BROKER_PARTITION_MOVEMENTS = "numPendingPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_CANCELLED_INTER_BROKER_PARTITION_MOVEMENTS = "numCancelledPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENTS = "numInProgressPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_ABORTING_INTER_BROKER_PARTITION_MOVEMENTS = "abortingPartitions";
    @JsonResponseField(required = false)
    private static final String NUM_FINISHED_INTER_BROKER_PARTITION_MOVEMENTS = "numFinishedPartitionMovements";
    @JsonResponseField(required = false)
    private static final String IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENT = "inProgressPartitionMovement";
    @JsonResponseField(required = false)
    private static final String PENDING_INTER_BROKER_PARTITION_MOVEMENT = "pendingPartitionMovement";
    @JsonResponseField(required = false)
    private static final String CANCELLED_INTER_BROKER_PARTITION_MOVEMENT = "cancelledPartitionMovement";
    @JsonResponseField(required = false)
    private static final String DEAD_INTER_BROKER_PARTITION_MOVEMENT = "deadPartitionMovement";
    @JsonResponseField(required = false)
    private static final String COMPLETED_INTER_BROKER_PARTITION_MOVEMENT = "completedPartitionMovement";
    @JsonResponseField(required = false)
    private static final String ABORTING_INTER_BROKER_PARTITION_MOVEMENT = "abortingPartitionMovement";
    @JsonResponseField(required = false)
    private static final String ABORTED_INTER_BROKER_PARTITION_MOVEMENT = "abortedPartitionMovement";
    @JsonResponseField(required = false)
    private static final String FINISHED_INTER_BROKER_DATA_MOVEMENT = "finishedDataMovement";
    @JsonResponseField(required = false)
    private static final String TOTAL_INTER_BROKER_DATA_TO_MOVE = "totalDataToMove";
    @JsonResponseField(required = false)
    private static final String MAXIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER = "maximumConcurrentPartitionMovementsPerBroker";
    @JsonResponseField(required = false)
    private static final String MINIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER = "minimumConcurrentPartitionMovementsPerBroker";
    @JsonResponseField(required = false)
    private static final String AVERAGE_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER = "averageConcurrentPartitionMovementsPerBroker";

    @JsonResponseField(required = false)
    private static final String NUM_TOTAL_INTRA_BROKER_PARTITION_MOVEMENTS = "numTotalIntraBrokerPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_FINISHED_INTRA_BROKER_PARTITION_MOVEMENTS = "numFinishedIntraBrokerPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENTS = "numInProgressIntraBrokerPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_ABORTING_INTRA_BROKER_PARTITION_MOVEMENTS = "numAbortingIntraBrokerPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_PENDING_INTRA_BROKER_PARTITION_MOVEMENTS = "numPendingIntraBrokerPartitionMovements";
    @JsonResponseField(required = false)
    private static final String NUM_CANCELLED_INTRA_BROKER_PARTITION_MOVEMENTS = "numCancelledIntraBrokerPartitionMovements";
    @JsonResponseField(required = false)
    private static final String IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENT = "inProgressIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String PENDING_INTRA_BROKER_PARTITION_MOVEMENT = "pendingIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String CANCELLED_INTRA_BROKER_PARTITION_MOVEMENT = "cancelledIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String DEAD_INTRA_BROKER_PARTITION_MOVEMENT = "deadIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String COMPLETED_INTRA_BROKER_PARTITION_MOVEMENT = "completedIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String ABORTING_INTRA_BROKER_PARTITION_MOVEMENT = "abortingIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String ABORTED_INTRA_BROKER_PARTITION_MOVEMENT = "abortedIntraBrokerPartitionMovement";
    @JsonResponseField(required = false)
    private static final String FINISHED_INTRA_BROKER_DATA_MOVEMENT = "finishedIntraBrokerDataMovement";
    @JsonResponseField(required = false)
    private static final String TOTAL_INTRA_BROKER_DATA_TO_MOVE = "totalIntraBrokerDataToMove";
    @JsonResponseField(required = false)
    private static final String MAXIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER
            = "maximumConcurrentIntraBrokerPartitionMovementsPerBroker";
    @JsonResponseField(required = false)
    private static final String MINIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER
            = "minimumConcurrentIntraBrokerPartitionMovementsPerBroker";
    @JsonResponseField(required = false)
    private static final String AVERAGE_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER
            = "averageConcurrentIntraBrokerPartitionMovementsPerBroker";

    @JsonResponseField(required = false)
    private static final String ERROR = "error";

    public enum State {
        NO_TASK_IN_PROGRESS,
        STARTING_EXECUTION,
        INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
        INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
        LEADER_MOVEMENT_TASK_IN_PROGRESS,
        STOPPING_EXECUTION,
        INITIALIZING_PROPOSAL_EXECUTION,
        GENERATING_PROPOSALS_FOR_EXECUTION
    }

    public static final Set<State> IN_PROGRESS_STATES = Set.of(State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
            State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
            State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
            State.STOPPING_EXECUTION);
    private final State _state;
    // Execution task statistics to report.
    private final ExecutionTaskTracker.ExecutionTasksSummary _executionTasksSummary;
    // Configs to report.
    private final ExecutionConcurrencySummary _executionConcurrencySummary;
    private final String _uuid;
    private final String _reason;
    private final boolean _isTriggeredByUserRequest;
    private final Set<Integer> _recentlyDemotedBrokers;
    private final Set<Integer> _recentlyRemovedBrokers;

    private ExecutorState(State state,
                          ExecutionTasksSummary executionTasksSummary,
                          ExecutionConcurrencySummary executionConcurrencySummary,
                          String uuid,
                          String reason,
                          Set<Integer> recentlyDemotedBrokers,
                          Set<Integer> recentlyRemovedBrokers,
                          boolean isTriggeredByUserRequest) {
        _state = state;
        _executionTasksSummary = executionTasksSummary;
        _executionConcurrencySummary = executionConcurrencySummary;
        _uuid = uuid;
        _reason = reason;
        _recentlyDemotedBrokers = recentlyDemotedBrokers;
        _recentlyRemovedBrokers = recentlyRemovedBrokers;
        _isTriggeredByUserRequest = isTriggeredByUserRequest;
    }

    /**
     * @param recentlyDemotedBrokers Recently demoted broker IDs.
     * @param recentlyRemovedBrokers Recently removed broker IDs.
     * @return Executor state when no task is in progress.
     */
    public static ExecutorState noTaskInProgress(Set<Integer> recentlyDemotedBrokers,
                                                 Set<Integer> recentlyRemovedBrokers) {
        return new ExecutorState(State.NO_TASK_IN_PROGRESS,
                null,
                null,
                "",
                "",
                recentlyDemotedBrokers,
                recentlyRemovedBrokers,
                false);
    }

    /**
     * @param uuid                     UUID of the current execution.
     * @param reason                   Reason of the current execution.
     * @param recentlyDemotedBrokers   Recently demoted broker IDs.
     * @param recentlyRemovedBrokers   Recently removed broker IDs.
     * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
     * @return Executor state when the execution is starting.
     */
    public static ExecutorState executionStarting(String uuid,
                                                  String reason,
                                                  Set<Integer> recentlyDemotedBrokers,
                                                  Set<Integer> recentlyRemovedBrokers,
                                                  boolean isTriggeredByUserRequest) {
        return new ExecutorState(State.STARTING_EXECUTION,
                null,
                null,
                uuid,
                reason,
                recentlyDemotedBrokers,
                recentlyRemovedBrokers,
                isTriggeredByUserRequest);
    }

    /**
     * @param uuid                     UUID of the current execution.
     * @param reason                   Reason of the current execution.
     * @param recentlyDemotedBrokers   Recently demoted broker IDs.
     * @param recentlyRemovedBrokers   Recently removed broker IDs.
     * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
     * @return Executor state when initializing proposal execution.
     */
    public static ExecutorState initializeProposalExecution(String uuid,
                                                            String reason,
                                                            Set<Integer> recentlyDemotedBrokers,
                                                            Set<Integer> recentlyRemovedBrokers,
                                                            boolean isTriggeredByUserRequest) {
        return new ExecutorState(State.INITIALIZING_PROPOSAL_EXECUTION,
                null,
                null,
                uuid,
                reason,
                recentlyDemotedBrokers,
                recentlyRemovedBrokers,
                isTriggeredByUserRequest);
    }

    /**
     * @param uuid                     UUID of the current execution.
     * @param reason                   Reason of the current execution.
     * @param recentlyDemotedBrokers   Recently demoted broker IDs.
     * @param recentlyRemovedBrokers   Recently removed broker IDs.
     * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
     * @return Executor state when generating proposals for execution.
     */
    public static ExecutorState generatingProposalsForExecution(String uuid,
                                                                String reason,
                                                                Set<Integer> recentlyDemotedBrokers,
                                                                Set<Integer> recentlyRemovedBrokers,
                                                                boolean isTriggeredByUserRequest) {
        return new ExecutorState(State.GENERATING_PROPOSALS_FOR_EXECUTION,
                null,
                null,
                uuid,
                reason,
                recentlyDemotedBrokers,
                recentlyRemovedBrokers,
                isTriggeredByUserRequest);
    }

    /**
     * @param state                       State of executor.
     * @param reason                      Reason of the current execution.
     * @param executionTasksSummary       Summary of the execution tasks.
     * @param executionConcurrencySummary Summary of the execution concurrency
     * @param uuid                        UUID of the current execution.
     * @param recentlyDemotedBrokers      Recently demoted broker IDs.
     * @param recentlyRemovedBrokers      Recently removed broker IDs.
     * @param isTriggeredByUserRequest    Whether the execution is triggered by a user request.
     * @return Executor state when execution is in progress.
     */
    public static ExecutorState operationInProgress(State state,
                                                    ExecutionTasksSummary executionTasksSummary,
                                                    ExecutionConcurrencySummary executionConcurrencySummary,
                                                    String uuid,
                                                    String reason,
                                                    Set<Integer> recentlyDemotedBrokers,
                                                    Set<Integer> recentlyRemovedBrokers,
                                                    boolean isTriggeredByUserRequest) {
        if (!IN_PROGRESS_STATES.contains(state)) {
            throw new IllegalArgumentException(String.format("%s is not an operation-in-progress executor state %s.", state, IN_PROGRESS_STATES));
        }
        return new ExecutorState(state,
                executionTasksSummary,
                executionConcurrencySummary,
                uuid,
                reason,
                recentlyDemotedBrokers,
                recentlyRemovedBrokers,
                isTriggeredByUserRequest);
    }

    /**
     * @return The current state of the executor.
     */
    public State state() {
        return _state;
    }

    public int numTotalMovements(ExecutionTask.TaskType type) {
        return _executionTasksSummary.taskStat().get(type).values().stream().mapToInt(i -> i).sum();
    }

    /**
     * @param type The type of execution task.
     * @return Number of finished movements, which is the sum of dead, completed, and aborted tasks.
     */
    public int numFinishedMovements(ExecutionTask.TaskType type) {
        return _executionTasksSummary.taskStat().get(type).get(ExecutionTaskState.DEAD)
                + _executionTasksSummary.taskStat().get(type).get(ExecutionTaskState.COMPLETED)
                + _executionTasksSummary.taskStat().get(type).get(ExecutionTaskState.ABORTED);
    }

    /**
     * @return Number of MBs for inter broker data to move, which is the sum of in execution, finished, and remaining tasks.
     */
    public long numTotalInterBrokerDataToMove() {
        return _executionTasksSummary.inExecutionInterBrokerDataMovementInMB()
                + _executionTasksSummary.finishedInterBrokerDataMovementInMB()
                + _executionTasksSummary.remainingInterBrokerDataToMoveInMB();
    }

    /**
     * @return Number of MBs for intra broker data to move, which is the sum of in execution, finished, and remaining tasks.
     */
    public long numTotalIntraBrokerDataToMove() {
        return _executionTasksSummary.inExecutionIntraBrokerDataMovementInMB()
                + _executionTasksSummary.finishedIntraBrokerDataMovementInMB()
                + _executionTasksSummary.remainingIntraBrokerDataToMoveInMB();
    }

    public String uuid() {
        return _uuid;
    }

    public Set<Integer> recentlyDemotedBrokers() {
        return _recentlyDemotedBrokers;
    }

    public Set<Integer> recentlyRemovedBrokers() {
        return _recentlyRemovedBrokers;
    }

    public ExecutionTasksSummary executionTasksSummary() {
        return _executionTasksSummary;
    }

    private List<Object> getTaskDetails(ExecutionTask.TaskType type, ExecutionTaskState state) {
        List<Object> taskList = new ArrayList<>();
        for (ExecutionTask task : _executionTasksSummary.filteredTasksByState().get(type).get(state)) {
            taskList.add(task.getJsonStructure());
        }
        return taskList;
    }

    private void populateUuidFieldInJsonStructure(Map<String, Object> execState, String uuid) {
        if (_isTriggeredByUserRequest) {
            execState.put(TRIGGERED_SELF_HEALING_TASK_ID, "");
            execState.put(TRIGGERED_USER_TASK_ID, uuid);
        } else {
            execState.put(TRIGGERED_SELF_HEALING_TASK_ID, uuid);
            execState.put(TRIGGERED_USER_TASK_ID, "");
        }
    }

    /**
     * @param verbose {@code true} if verbose, {@code false} otherwise.
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure(boolean verbose) {
        Map<String, Object> execState = new HashMap<>();
        execState.put(STATE, _state);
        if (_recentlyDemotedBrokers != null && !_recentlyDemotedBrokers.isEmpty()) {
            execState.put(RECENTLY_DEMOTED_BROKERS, _recentlyDemotedBrokers);
        }
        if (_recentlyRemovedBrokers != null && !_recentlyRemovedBrokers.isEmpty()) {
            execState.put(RECENTLY_REMOVED_BROKERS, _recentlyRemovedBrokers);
        }
        Map<ExecutionTaskState, Integer> interBrokerPartitionMovementStats;
        Map<ExecutionTaskState, Integer> intraBrokerPartitionMovementStats;
        switch (_state) {
            case NO_TASK_IN_PROGRESS:
                break;
            case STARTING_EXECUTION:
            case INITIALIZING_PROPOSAL_EXECUTION:
            case GENERATING_PROPOSALS_FOR_EXECUTION:
                populateUuidFieldInJsonStructure(execState, _uuid);
                execState.put(TRIGGERED_TASK_REASON, _reason);
                break;
            case LEADER_MOVEMENT_TASK_IN_PROGRESS:
                populateUuidFieldInJsonStructure(execState, _uuid);
                execState.put(TRIGGERED_TASK_REASON, _reason);
                execState.put(MAXIMUM_BROKER_LEADER_MOVEMENT_CONCURRENCY, _executionConcurrencySummary.getMaxExecutionConcurrency(
                        ConcurrencyType.LEADERSHIP_BROKER));
                execState.put(MINIMUM_BROKER_LEADER_MOVEMENT_CONCURRENCY,
                        _executionConcurrencySummary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
                execState.put(AVERAGE_BROKER_LEADER_MOVEMENT_CONCURRENCY,
                        _executionConcurrencySummary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
                execState.put(CLUSTER_LEADER_MOVEMENT_CONCURRENCY, _executionConcurrencySummary.getClusterLeadershipMovementConcurrency());
                execState.put(NUM_PENDING_LEADERSHIP_MOVEMENTS, _executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTaskState.PENDING));
                execState.put(NUM_FINISHED_LEADERSHIP_MOVEMENTS, numFinishedMovements(LEADER_ACTION));
                execState.put(NUM_TOTAL_LEADERSHIP_MOVEMENTS, numTotalMovements(LEADER_ACTION));
                if (verbose) {
                    execState.put(PENDING_LEADERSHIP_MOVEMENT, getTaskDetails(LEADER_ACTION, ExecutionTaskState.PENDING));
                }
                break;
            case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
                interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
                populateUuidFieldInJsonStructure(execState, _uuid);
                execState.put(TRIGGERED_TASK_REASON, _reason);
                execState.put(MAXIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
                execState.put(MINIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
                execState.put(AVERAGE_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
                execState.put(NUM_IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS));
                execState.put(NUM_ABORTING_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING));
                execState.put(NUM_PENDING_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING));
                execState.put(NUM_FINISHED_INTER_BROKER_PARTITION_MOVEMENTS, numFinishedMovements(INTER_BROKER_REPLICA_ACTION));
                execState.put(NUM_TOTAL_INTER_BROKER_PARTITION_MOVEMENTS, numTotalMovements(INTER_BROKER_REPLICA_ACTION));
                execState.put(FINISHED_INTER_BROKER_DATA_MOVEMENT, _executionTasksSummary.finishedInterBrokerDataMovementInMB());
                execState.put(TOTAL_INTER_BROKER_DATA_TO_MOVE, numTotalInterBrokerDataToMove());
                if (verbose) {
                    execState.put(IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS));
                    execState.put(PENDING_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING));
                    execState.put(ABORTING_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.ABORTING));
                    execState.put(ABORTED_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.ABORTED));
                    execState.put(DEAD_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.DEAD));
                    execState.put(COMPLETED_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.COMPLETED));
                }
                break;
            case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
                intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
                populateUuidFieldInJsonStructure(execState, _uuid);
                execState.put(TRIGGERED_TASK_REASON, _reason);
                execState.put(MAXIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
                execState.put(MINIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
                execState.put(AVERAGE_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
                execState.put(NUM_IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS));
                execState.put(NUM_ABORTING_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING));
                execState.put(NUM_PENDING_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING));
                execState.put(NUM_FINISHED_INTRA_BROKER_PARTITION_MOVEMENTS, numFinishedMovements(INTRA_BROKER_REPLICA_ACTION));
                execState.put(NUM_TOTAL_INTRA_BROKER_PARTITION_MOVEMENTS, numTotalMovements(INTRA_BROKER_REPLICA_ACTION));
                execState.put(FINISHED_INTRA_BROKER_DATA_MOVEMENT, _executionTasksSummary.finishedIntraBrokerDataMovementInMB());
                execState.put(TOTAL_INTRA_BROKER_DATA_TO_MOVE, numTotalIntraBrokerDataToMove());
                if (verbose) {
                    execState.put(IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS));
                    execState.put(PENDING_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING));
                    execState.put(ABORTING_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.ABORTING));
                    execState.put(ABORTED_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.ABORTED));
                    execState.put(DEAD_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.DEAD));
                    execState.put(COMPLETED_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.COMPLETED));
                }
                break;
            case STOPPING_EXECUTION:
                interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
                intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
                populateUuidFieldInJsonStructure(execState, _uuid);
                execState.put(TRIGGERED_TASK_REASON, _reason);
                execState.put(MAXIMUM_CONCURRENT_INTER_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA));
                execState.put(MAXIMUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PER_BROKER,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA));
                execState.put(MAXIMUM_BROKER_LEADER_MOVEMENT_CONCURRENCY,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER));
                execState.put(CLUSTER_LEADER_MOVEMENT_CONCURRENCY, _executionConcurrencySummary.getClusterLeadershipMovementConcurrency());
                execState.put(NUM_CANCELLED_LEADERSHIP_MOVEMENTS, _executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTaskState.PENDING));
                execState.put(NUM_IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS));
                execState.put(NUM_ABORTING_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING));
                execState.put(NUM_CANCELLED_INTER_BROKER_PARTITION_MOVEMENTS, interBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING));
                execState.put(NUM_IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS));
                execState.put(NUM_ABORTING_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING));
                execState.put(NUM_CANCELLED_INTRA_BROKER_PARTITION_MOVEMENTS, intraBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING));
                if (verbose) {
                    execState.put(CANCELLED_LEADERSHIP_MOVEMENT, getTaskDetails(LEADER_ACTION, ExecutionTaskState.PENDING));
                    execState.put(CANCELLED_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING));
                    execState.put(IN_PROGRESS_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS));
                    execState.put(ABORTING_INTER_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.ABORTING));
                    execState.put(CANCELLED_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING));
                    execState.put(IN_PROGRESS_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS));
                    execState.put(ABORTING_INTRA_BROKER_PARTITION_MOVEMENT, getTaskDetails(INTRA_BROKER_REPLICA_ACTION, ExecutionTaskState.ABORTING));
                }
                break;
            default:
                execState.clear();
                execState.put(ERROR, "ILLEGAL_STATE_EXCEPTION");
                break;
        }
        return execState;
    }

    /**
     * @return Plaintext response for the executor state.
     */
    public String getPlaintext() {
        String recentlyDemotedBrokers = (_recentlyDemotedBrokers != null && !_recentlyDemotedBrokers.isEmpty())
                ? String.format(", %s: %s", RECENTLY_DEMOTED_BROKERS, _recentlyDemotedBrokers) : "";
        String recentlyRemovedBrokers = (_recentlyRemovedBrokers != null && !_recentlyRemovedBrokers.isEmpty())
                ? String.format(", %s: %s", RECENTLY_REMOVED_BROKERS, _recentlyRemovedBrokers) : "";
        Map<ExecutionTaskState, Integer> interBrokerPartitionMovementStats;
        Map<ExecutionTaskState, Integer> intraBrokerPartitionMovementStats;

        switch (_state) {
            case NO_TASK_IN_PROGRESS:
                return String.format("{%s: %s%s%s}", STATE, _state, recentlyDemotedBrokers, recentlyRemovedBrokers);
            case STARTING_EXECUTION:
            case INITIALIZING_PROPOSAL_EXECUTION:
            case GENERATING_PROPOSALS_FOR_EXECUTION:
                return String.format("{%s: %s, %s: %s, %s: %s%s%s}", STATE, _state,
                        _isTriggeredByUserRequest ? TRIGGERED_USER_TASK_ID : TRIGGERED_SELF_HEALING_TASK_ID,
                        _uuid, TRIGGERED_TASK_REASON, _reason, recentlyDemotedBrokers, recentlyRemovedBrokers);
            case LEADER_MOVEMENT_TASK_IN_PROGRESS:
                return String.format("{%s: %s, finished(%d)/total(%d) leadership movements,"
                                + " max/min/avg broker leadership movement concurrency: %d/%d/%.2f, "
                                + "cluster leadership movement concurrency: %d, "
                                + "%s: %s, %s: %s%s%s}",
                        STATE, _state, numFinishedMovements(LEADER_ACTION), numTotalMovements(LEADER_ACTION),
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER),
                        _executionConcurrencySummary.getMinExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER),
                        _executionConcurrencySummary.getAvgExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER),
                        _executionConcurrencySummary.getClusterLeadershipMovementConcurrency(),
                        _isTriggeredByUserRequest ? TRIGGERED_USER_TASK_ID : TRIGGERED_SELF_HEALING_TASK_ID,
                        _uuid, TRIGGERED_TASK_REASON, _reason, recentlyDemotedBrokers, recentlyRemovedBrokers);
            case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:

                interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
                long finishedInterBrokerDataMovementInMB = _executionTasksSummary.finishedInterBrokerDataMovementInMB();
                long numTotalInterBrokerDataToMove = numTotalInterBrokerDataToMove();
                double finishedInterBrokerDataPercent = 0.0;
                if (numTotalInterBrokerDataToMove != 0L) {
                    finishedInterBrokerDataPercent = ((double) finishedInterBrokerDataMovementInMB) / numTotalInterBrokerDataToMove * 100;
                }

                return String.format("{%s: %s, pending(%d)/in-progress(%d)/aborting(%d)/finished(%d)/total(%d) inter-broker partition movements,"
                                + " completed(%d)/total(%d) bytes in MBs: %.2f%%, max/min/avg concurrent inter-broker partition movements per-broker:"
                                + " %d/%d/%.2f, %s: %s, %s: %s%s%s}",
                        STATE, _state,
                        interBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING),
                        interBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS),
                        interBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING),
                        numFinishedMovements(INTER_BROKER_REPLICA_ACTION),
                        numTotalMovements(INTER_BROKER_REPLICA_ACTION),
                        finishedInterBrokerDataMovementInMB,
                        numTotalInterBrokerDataToMove,
                        finishedInterBrokerDataPercent,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA),
                        _executionConcurrencySummary.getMinExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA),
                        _executionConcurrencySummary.getAvgExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA),
                        _isTriggeredByUserRequest ? TRIGGERED_USER_TASK_ID : TRIGGERED_SELF_HEALING_TASK_ID, _uuid,
                        TRIGGERED_TASK_REASON, _reason, recentlyDemotedBrokers, recentlyRemovedBrokers);
            case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
                intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
                long finishedIntraBrokerDataMovementInMB = _executionTasksSummary.finishedIntraBrokerDataMovementInMB();
                long numTotalIntraBrokerDataToMove = numTotalIntraBrokerDataToMove();
                double finishedIntraBrokerDataPercent = 0.0;
                if (numTotalIntraBrokerDataToMove != 0L) {
                    finishedIntraBrokerDataPercent = ((double) finishedIntraBrokerDataMovementInMB) / numTotalIntraBrokerDataToMove * 100;
                }

                return String.format("{%s: %s, pending(%d)/in-progress(%d)/aborting(%d)/finished(%d)/total(%d) intra-broker partition movements,"
                                + "completed(%d)/total(%d) bytes in MBs: %.2f%%, max/min/avg concurrent intra-broker partition movements per-broker:"
                                + "%d/%d/%.2f, %s: %s, %s: %s%s%s}",
                        STATE, _state,
                        intraBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING),
                        intraBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS),
                        intraBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING),
                        numFinishedMovements(INTRA_BROKER_REPLICA_ACTION),
                        numTotalMovements(INTRA_BROKER_REPLICA_ACTION),
                        finishedIntraBrokerDataMovementInMB,
                        numTotalIntraBrokerDataToMove,
                        finishedIntraBrokerDataPercent,
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA),
                        _executionConcurrencySummary.getMinExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA),
                        _executionConcurrencySummary.getAvgExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA),
                        _isTriggeredByUserRequest ? TRIGGERED_USER_TASK_ID : TRIGGERED_SELF_HEALING_TASK_ID, _uuid,
                        TRIGGERED_TASK_REASON, _reason, recentlyDemotedBrokers, recentlyRemovedBrokers);
            case STOPPING_EXECUTION:
                interBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTER_BROKER_REPLICA_ACTION);
                intraBrokerPartitionMovementStats = _executionTasksSummary.taskStat().get(INTRA_BROKER_REPLICA_ACTION);
                return String.format("{%s: %s, cancelled(%d)/in-progress(%d)/aborting(%d)/total(%d) intra-broker partition movements,"
                                + "cancelled(%d)/in-progress(%d)/aborting(%d)/total(%d) inter-broker partition movements movements,"
                                + "cancelled(%d)/total(%d) leadership movements, maximum concurrent intra-broker partition movements per-broker: %d, "
                                + "maximum concurrent inter-broker partition movements per-broker: %d, "
                                + "maximum broker leadership movement concurrency: %d, "
                                + "cluster leadership movements concurrency: %d, "
                                + "%s: %s, %s: %s%s%s}",
                        STATE, _state,
                        intraBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING),
                        intraBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS),
                        intraBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING),
                        numTotalMovements(INTRA_BROKER_REPLICA_ACTION),
                        interBrokerPartitionMovementStats.get(ExecutionTaskState.PENDING),
                        interBrokerPartitionMovementStats.get(ExecutionTaskState.IN_PROGRESS),
                        interBrokerPartitionMovementStats.get(ExecutionTaskState.ABORTING),
                        numTotalMovements(INTER_BROKER_REPLICA_ACTION),
                        _executionTasksSummary.taskStat().get(LEADER_ACTION).get(ExecutionTaskState.PENDING),
                        numTotalMovements(LEADER_ACTION),
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTER_BROKER_REPLICA),
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.INTRA_BROKER_REPLICA),
                        _executionConcurrencySummary.getMaxExecutionConcurrency(ConcurrencyType.LEADERSHIP_BROKER),
                        _executionConcurrencySummary.getClusterLeadershipMovementConcurrency(),
                        _isTriggeredByUserRequest ? TRIGGERED_USER_TASK_ID : TRIGGERED_SELF_HEALING_TASK_ID, _uuid,
                        TRIGGERED_TASK_REASON, _reason, recentlyDemotedBrokers, recentlyRemovedBrokers);
            default:
                throw new IllegalStateException("This should never happen");
        }
    }
}
