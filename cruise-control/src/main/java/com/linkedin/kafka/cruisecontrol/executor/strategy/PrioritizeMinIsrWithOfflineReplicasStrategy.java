/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionUtils;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;


/**
 * The strategy, which tries to move replicas of (At/Under)MinISR partitions with offline replicas.
 * When multiple brokers are offline, (At/Under)MinISR partitions are at a higher risk -- i.e. further failures can make them unavailable
 * for produce and consume. This strategy aims to help fixing such partitions faster.
 */
public class PrioritizeMinIsrWithOfflineReplicasStrategy extends AbstractReplicaMovementStrategy {

    /**
     * <ul>
     *   <li>If both tasks are moving either an AtMinISR or an UnderMinISR partition, then they have the same priority</li>
     *   <li>If neither task is moving an (At/Under)MinISR partition, then they have the same priority</li>
     *   <li>If a task is moving an UnderMinISR partition, but the other task is not, then task with UnderMinISR partition is prioritized</li>
     *   <li>If neither task is moving an UnderMinISR partition, then the task moving an AtMinISR partition (if any) is prioritized</li>
     * </ul>
     *
     * @param strategyOptions Strategy options to be used while comparing the tasks.
     * @return The comparator of task.
     */
    @Override
    public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
        Comparator<PartitionInfo> comparator = Comparator.comparing(PartitionInfo::topic).thenComparingInt(PartitionInfo::partition);
        Set<PartitionInfo> atMinIsr = new TreeSet<>(comparator);
        Set<PartitionInfo> underMinIsr = new TreeSet<>(comparator);
        ExecutionUtils.populateMinIsrState(strategyOptions.cluster(), strategyOptions.minIsrWithTimeByTopic(), underMinIsr, atMinIsr, true);

        return (task1, task2) -> {
            boolean task1IsUnderMinISR = isTaskInSet(task1, underMinIsr);
            boolean task2IsUnderMinISR = isTaskInSet(task2, underMinIsr);
            boolean task1IsAtMinISR = isTaskInSet(task1, atMinIsr);
            boolean task2IsAtMinISR = isTaskInSet(task2, atMinIsr);

            if (task1IsUnderMinISR) {
                // task1 is UnderMinISR. Unless task2 is also UnderMinISR, task1 is prioritized.
                return task2IsUnderMinISR ? PRIORITIZE_NONE : PRIORITIZE_TASK_1;
            } else {
                // task1 is not UnderMinISR. Unless task2 is UnderMinISR (i.e. prioritized), the rest of the comparison is based on AtMinISR status.
                return task2IsUnderMinISR ? PRIORITIZE_TASK_2 : task1IsAtMinISR ? task2IsAtMinISR ? PRIORITIZE_NONE : PRIORITIZE_TASK_1
                        : task2IsAtMinISR ? PRIORITIZE_TASK_2 : PRIORITIZE_NONE;
            }
        };
    }

    @Override
    public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
        return taskComparator(new StrategyOptions.Builder(cluster).build());
    }

    @Override
    public String name() {
        return PrioritizeMinIsrWithOfflineReplicasStrategy.class.getSimpleName();
    }
}
