/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.exception.PartitionNotExistsException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;

import java.util.Comparator;

import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.isPartitionUnderReplicated;

/**
 * The strategy, which tries to first move replicas of partitions which currently are not under replicated.
 */
public class PostponeUrpReplicaMovementStrategy extends AbstractReplicaMovementStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PostponeUrpReplicaMovementStrategy.class);

    @Override
    public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
        return (task1, task2) -> {
            boolean isTask1PartitionUnderReplicated = false;
            boolean isTask2PartitionUnderReplicated = false;
            boolean task1PartitionExists = false;
            boolean task2PartitionExists = false;
            try {
                isTask1PartitionUnderReplicated = isPartitionUnderReplicated(strategyOptions.cluster(),
                        task1.proposal().topicPartition());
                task1PartitionExists = true;
            } catch (PartitionNotExistsException e) {
                LOG.warn("Task {} skipped comparison since partition {} does not exist in cluster.",
                        task1, task1.proposal().topicPartition(), e);
            }
            try {
                isTask2PartitionUnderReplicated = isPartitionUnderReplicated(strategyOptions.cluster(),
                        task2.proposal().topicPartition());
                task2PartitionExists = true;
            } catch (PartitionNotExistsException e) {
                LOG.warn("Task {} skipped comparison since partition {} does not exist in cluster.",
                        task2, task2.proposal().topicPartition(), e);
            }

            if (task1PartitionExists && task2PartitionExists) {
                return isTask1PartitionUnderReplicated
                        ? (isTask2PartitionUnderReplicated ? PRIORITIZE_NONE : PRIORITIZE_TASK_2)
                        : (isTask2PartitionUnderReplicated ? PRIORITIZE_TASK_1 : PRIORITIZE_NONE);
            } else if (task1PartitionExists) {
                return isTask1PartitionUnderReplicated ? PRIORITIZE_NONE : PRIORITIZE_TASK_1;
            } else if (task2PartitionExists) {
                return isTask2PartitionUnderReplicated ? PRIORITIZE_NONE : PRIORITIZE_TASK_2;
            } else {
                return PRIORITIZE_NONE;
            }
        };
    }

    @Override
    public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
        return taskComparator(new StrategyOptions.Builder(cluster).build());
    }

    /**
     * Get the name of this strategy. Name of a strategy provides an identification for the strategy in human readable format.
     */
    @Override
    public String name() {
        return PostponeUrpReplicaMovementStrategy.class.getSimpleName();
    }
}
