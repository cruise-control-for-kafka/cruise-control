/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;

/**
 * The replication throttle budget of brokers for inter-broker replica movements.
 * <p>
 * The replication throttle of a broker is shared by all throttled replica movements in progress on that broker: the leader
 * throttle by the movements whose leader (i.e. source) is the broker, the follower throttle by the movements that add a replica
 * to the broker. A movement does not finish if the throttle cannot carry the write rates of the partitions moving through
 * the broker. This class tracks the write rate (in bytes per second) of the movements on each side of each broker:
 * <ul>
 *   <li>Leader side of the source broker (i.e. the current leader of the partition): the write rate of the partition times the
 *   number of brokers fetching the partition from it.</li>
 *   <li>Follower side of each broker fetching the partition, i.e. each broker to add a replica to, except the source broker:
 *   the write rate of the partition.</li>
 * </ul>
 * A partition without a known write rate is considered to have a write rate of 0.
 * <p>
 * This class is not thread safe.
 */
class ReplicationThrottleBudget {
  private final Map<TopicPartition, Double> _writeRateByPartition;
  private final Function<ExecutionTask, Integer> _currentLeaderByTask;
  private final Function<Integer, Long> _leaderThrottleByBrokerId;
  private final Function<Integer, Long> _followerThrottleByBrokerId;
  private final double _margin;
  private final Map<Integer, Double> _leaderUsageByBrokerId;
  private final Map<Integer, Double> _followerUsageByBrokerId;

  /**
   * @param inProgressTasks The inter-broker replica movement tasks in progress. Tasks of other types or states are ignored.
   * @param writeRateByPartition The write rate of partitions in bytes per second.
   * @param currentLeaderByTask The current leader of the partition of a task, which the brokers to add a replica to fetch the
   *                            partition from, or {@code null} if unknown, in which case its old leader is used.
   * @param leaderThrottleByBrokerId The leader replication throttle of a broker in bytes per second, {@code null} for no throttle.
   * @param followerThrottleByBrokerId The follower replication throttle of a broker in bytes per second, {@code null} for no throttle.
   * @param margin The fraction by which the write rates on a broker side are increased to leave a margin to the throttle.
   */
  ReplicationThrottleBudget(Collection<ExecutionTask> inProgressTasks,
                            Map<TopicPartition, Double> writeRateByPartition,
                            Function<ExecutionTask, Integer> currentLeaderByTask,
                            Function<Integer, Long> leaderThrottleByBrokerId,
                            Function<Integer, Long> followerThrottleByBrokerId,
                            double margin) {
    _writeRateByPartition = writeRateByPartition;
    _currentLeaderByTask = currentLeaderByTask;
    _leaderThrottleByBrokerId = leaderThrottleByBrokerId;
    _followerThrottleByBrokerId = followerThrottleByBrokerId;
    _margin = margin;
    _leaderUsageByBrokerId = new HashMap<>();
    _followerUsageByBrokerId = new HashMap<>();
    for (ExecutionTask task : inProgressTasks) {
      if (task.type() == INTER_BROKER_REPLICA_ACTION && task.state() == ExecutionTaskState.IN_PROGRESS) {
        reserve(task);
      }
    }
  }

  /**
   * A task fits if, on each side of each broker it involves, either the throttle can carry the write rate in use plus the write
   * rate of the task, both increased by the margin, or no write rate is in use. The latter lets a partition with a write rate
   * higher than the throttle start alone rather than wait forever.
   *
   * @param task Inter-broker replica movement task.
   * @return {@code true} if the given task fits in the replication throttle budget, {@code false} otherwise.
   */
  boolean fits(ExecutionTask task) {
    return fits(task, true);
  }

  /**
   * A task fits if, on each side of each broker it involves, the throttle can carry the write rate in use plus the write rate of
   * the task, both increased by the margin, or, if idle broker sides are allowed, no write rate is in use.
   *
   * @param task Inter-broker replica movement task.
   * @param allowIdleBrokerSides {@code true} to let the task fit on a broker side with no write rate in use even if the throttle
   *                             cannot carry the write rate of the task, {@code false} otherwise.
   * @return {@code true} if the given task fits in the replication throttle budget, {@code false} otherwise.
   */
  boolean fits(ExecutionTask task, boolean allowIdleBrokerSides) {
    double writeRate = writeRate(task);
    int sourceBroker = sourceBroker(task);
    List<Integer> fetchingBrokers = fetchingBrokers(task, sourceBroker);
    if (fetchingBrokers.isEmpty()) {
      return true;
    }
    if (!fitsOnBrokerSide(_leaderUsageByBrokerId, _leaderThrottleByBrokerId, sourceBroker, writeRate * fetchingBrokers.size(),
                          allowIdleBrokerSides)) {
      return false;
    }
    for (int fetchingBroker : fetchingBrokers) {
      if (!fitsOnBrokerSide(_followerUsageByBrokerId, _followerThrottleByBrokerId, fetchingBroker, writeRate, allowIdleBrokerSides)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param task Inter-broker replica movement task.
   * @return {@code true} if the throttle of a broker side involved in the given task cannot carry the write rate of the task
   * increased by the margin even if nothing else is in use, i.e. the movement cannot catch up, {@code false} otherwise.
   */
  boolean exceedsThrottle(ExecutionTask task) {
    return !exceededThrottles(task).isEmpty();
  }

  /**
   * @param task Inter-broker replica movement task.
   * @return A description of the throttles that cannot carry the write rate of the given task increased by the margin even if
   * nothing else is in use, or an empty string if there is no such throttle.
   */
  String exceededThrottles(ExecutionTask task) {
    double writeRate = writeRate(task);
    int sourceBroker = sourceBroker(task);
    List<Integer> fetchingBrokers = fetchingBrokers(task, sourceBroker);
    StringJoiner exceeded = new StringJoiner(", ");
    if (!fetchingBrokers.isEmpty() && exceeds(_leaderThrottleByBrokerId.apply(sourceBroker), writeRate * fetchingBrokers.size())) {
      exceeded.add(String.format("leader throttle %d B/s of broker %d", _leaderThrottleByBrokerId.apply(sourceBroker), sourceBroker));
    }
    for (int fetchingBroker : fetchingBrokers) {
      if (exceeds(_followerThrottleByBrokerId.apply(fetchingBroker), writeRate)) {
        exceeded.add(String.format("follower throttle %d B/s of broker %d", _followerThrottleByBrokerId.apply(fetchingBroker),
                                   fetchingBroker));
      }
    }
    return exceeded.toString();
  }

  /**
   * Reserve the write rate of the given task, so that it is considered to be in use by {@link #fits(ExecutionTask)}.
   *
   * @param task Inter-broker replica movement task.
   */
  void reserve(ExecutionTask task) {
    double writeRate = writeRate(task);
    int sourceBroker = sourceBroker(task);
    List<Integer> fetchingBrokers = fetchingBrokers(task, sourceBroker);
    if (fetchingBrokers.isEmpty()) {
      return;
    }
    _leaderUsageByBrokerId.merge(sourceBroker, writeRate * fetchingBrokers.size(), Double::sum);
    for (int fetchingBroker : fetchingBrokers) {
      _followerUsageByBrokerId.merge(fetchingBroker, writeRate, Double::sum);
    }
  }

  /**
   * @param task Inter-broker replica movement task.
   * @return The write rate of the partition of the given task in bytes per second, 0 if unknown.
   */
  double writeRate(ExecutionTask task) {
    return _writeRateByPartition.getOrDefault(task.proposal().topicPartition(), 0.0);
  }

  /**
   * @param task Inter-broker replica movement task.
   * @return The current leader of the partition of the given task if known, its old leader otherwise.
   */
  private int sourceBroker(ExecutionTask task) {
    Integer currentLeader = _currentLeaderByTask.apply(task);
    return currentLeader != null ? currentLeader : task.proposal().oldLeader().brokerId();
  }

  /**
   * @param task Inter-broker replica movement task.
   * @param sourceBroker The broker that the brokers to add a replica to fetch the partition from.
   * @return The brokers to add a replica to, except the source broker, which does not fetch the partition.
   */
  private static List<Integer> fetchingBrokers(ExecutionTask task, int sourceBroker) {
    List<Integer> fetchingBrokers = new ArrayList<>(task.proposal().replicasToAdd().size());
    for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
      if (replicaToAdd.brokerId() != sourceBroker) {
        fetchingBrokers.add(replicaToAdd.brokerId());
      }
    }
    return fetchingBrokers;
  }

  private boolean fitsOnBrokerSide(Map<Integer, Double> usageByBrokerId,
                                   Function<Integer, Long> throttleByBrokerId,
                                   int brokerId,
                                   double need,
                                   boolean allowIdle) {
    double usage = usageByBrokerId.getOrDefault(brokerId, 0.0);
    if (allowIdle && usage <= 0.0) {
      return true;
    }
    return !exceeds(throttleByBrokerId.apply(brokerId), usage + need);
  }

  private boolean exceeds(Long throttle, double writeRate) {
    return throttle != null && (1 + _margin) * writeRate > throttle;
  }
}
