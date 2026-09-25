/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.kafka.common.Cluster;


/**
 * An interface to enable customization of replica movement strategies.
 */
public interface ReplicaMovementStrategy {

    /**
     * Determine the execution order for replica movement tasks based on a customized strategy.
     *
     * @param replicaMovementTasks The replica movement tasks to be executed.
     * @param strategyOptions      Strategy options to be used during application of a replica movement strategy.
     * @return Ordered set of tasks to be executed for each broker.
     */
    Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> replicaMovementTasks, StrategyOptions strategyOptions);

    /**
     * Determine the execution order for replica movement tasks based on a customized strategy.
     *
     * @param replicaMovementTasks The replica movement tasks to be executed.
     * @param cluster              The current cluster state.
     * @return Ordered set of tasks to be executed for each broker.
     * @deprecated Will be removed in a future release -- please use {@link #applyStrategy(Set, StrategyOptions)}.
     */
    @Deprecated
    Map<Integer, SortedSet<ExecutionTask>> applyStrategy(Set<ExecutionTask> replicaMovementTasks, Cluster cluster);

    /**
     * Chain with another replica movement strategy to create a composite strategy.The returned strategy should use a combined rule
     * of two strategies in determining the task execution order.
     *
     * @param strategy The other replica movement strategy.
     * @return The composite replica movement strategy.
     */
    ReplicaMovementStrategy chain(ReplicaMovementStrategy strategy);

    /**
     * Unless the custom strategies are already chained with {@link BaseReplicaMovementStrategy},
     * chain the generated composite strategy with {@link BaseReplicaMovementStrategy} in the end to ensure the returned strategy can always
     * determine the order of two tasks.
     *
     * @return The replica movement strategy which is guaranteed to have {@link BaseReplicaMovementStrategy}.
     */
    ReplicaMovementStrategy chainBaseReplicaMovementStrategyIfAbsent();

    /**
     * Generate a comparator for replica movement task which incorporate the strategy to apply. The "smaller" task will have
     * higher execution priority.
     *
     * @param strategyOptions Strategy options to be used while comparing the tasks.
     * @return The comparator of task.
     */
    Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions);

    /**
     * Generate a comparator for replica movement task which incorporate the strategy to apply. The "smaller" task will have
     * higher execution priority.
     *
     * @param cluster The current cluster state.
     * @return The comparator of task.
     * @deprecated Will be removed in a future release -- please use {@link #taskComparator(StrategyOptions)}.
     */
    @Deprecated
    Comparator<ExecutionTask> taskComparator(Cluster cluster);

    /**
     * @return The name of this strategy. Name of a strategy provides an identification for the strategy in human readable format.
     */
    String name();
}
