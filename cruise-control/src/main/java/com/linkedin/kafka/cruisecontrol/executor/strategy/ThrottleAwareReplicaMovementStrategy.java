/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.kafka.common.Cluster;

/**
 * The strategy, which starts inter-broker replica movements only within the replication throttle of their brokers, to avoid
 * movements whose combined write rate exceeds the throttle of a broker, which never catch up and stall the execution.
 * <p>
 * When this strategy is part of the (possibly chained) replica movement strategy of an execution, and the execution has a
 * replication throttle, the executor starts a movement only if the replication throttle of its source (i.e. the current leader
 * of the partition) and destination brokers can carry the write rates of the movements in progress on them plus the write rate
 * of the partition, all increased by {@code replica.movement.throttle.write.rate.margin}, leaving a share of the throttle to
 * catch up. A partition with a write rate higher than the throttle starts alone once no movement is in progress on its brokers
 * rather than wait forever, but only once no movement that fits can start, so that it never holds up the movements that fit,
 * whatever the order of tasks. The write rate of a partition is its leader bytes in rate in the current metrics window, or its
 * last known rate if the window has no sample yet.
 * <p>
 * This strategy does not change the order of tasks, which is determined by the other chained strategies.
 */
public class ThrottleAwareReplicaMovementStrategy extends AbstractReplicaMovementStrategy {

  @Override
  public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
    return (task1, task2) -> PRIORITIZE_NONE;
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
    return ThrottleAwareReplicaMovementStrategy.class.getSimpleName();
  }

  /**
   * @param strategy The (possibly chained) replica movement strategy to check.
   * @return {@code true} if the given strategy is or contains {@link ThrottleAwareReplicaMovementStrategy}, {@code false} otherwise.
   */
  public static boolean isEnabledIn(ReplicaMovementStrategy strategy) {
    return strategy != null && Arrays.asList(strategy.name().split(","))
                                     .contains(ThrottleAwareReplicaMovementStrategy.class.getSimpleName());
  }
}
