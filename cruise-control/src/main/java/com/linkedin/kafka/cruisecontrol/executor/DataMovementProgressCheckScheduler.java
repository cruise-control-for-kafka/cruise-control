/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Schedules the checks of the data movement progress of in-progress replica movement tasks, so that they happen at most
 * once per partition movement progress check interval. A failed check (e.g. a broker fails to respond in time) is not
 * retried before the next interval, to avoid putting extra load on the cluster; the tasks whose progress could not be
 * updated keep their last known progress meanwhile.
 *
 * This class is not thread-safe.
 */
class DataMovementProgressCheckScheduler {
  private final boolean _enabled;
  private final long _checkIntervalMs;
  // The time of the last check, or -1 if no check happened since the last reset.
  private long _lastCheckMs;

  /**
   * @param enabled {@code true} if partition movement progress tracking is enabled, {@code false} otherwise.
   * @param checkIntervalMs The minimum interval between two checks.
   */
  DataMovementProgressCheckScheduler(boolean enabled, long checkIntervalMs) {
    _enabled = enabled;
    _checkIntervalMs = checkIntervalMs;
    reset();
  }

  /**
   * Reset the schedule, so that the next opportunity triggers a check -- e.g. at the start of a replica movement phase.
   */
  void reset() {
    _lastCheckMs = -1L;
  }

  /**
   * Check the data movement progress of the given tasks if a check is due.
   *
   * @param nowMs The current time in milliseconds.
   * @param inProgressReplicaTasks In-progress replica movement tasks.
   * @param check A function that checks the data movement progress of the given tasks.
   */
  void maybeCheck(long nowMs, Collection<ExecutionTask> inProgressReplicaTasks, Consumer<List<ExecutionTask>> check) {
    // Without in-progress tasks, the check remains due -- so that it happens as soon as tasks are in progress.
    if (_enabled && !inProgressReplicaTasks.isEmpty() && (_lastCheckMs < 0L || nowMs - _lastCheckMs >= _checkIntervalMs)) {
      check.accept(List.copyOf(inProgressReplicaTasks));
      _lastCheckMs = nowMs;
    }
  }
}
