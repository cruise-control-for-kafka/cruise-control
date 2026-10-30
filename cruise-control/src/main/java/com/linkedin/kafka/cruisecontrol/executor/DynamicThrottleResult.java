/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

/**
 * The outcome of removing a throttle from the ongoing execution. A boolean cannot express it: a removal that does nothing
 * does so for two different reasons, and reporting them with the same sentence is how a response ends up saying "there is no
 * ongoing intra-broker replica movement (executor state: INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS)".
 */
public enum DynamicThrottleResult {
  /** The throttle was removed from the ongoing execution. */
  REMOVED,
  /** There is an ongoing execution of the relevant movement type, but it is not throttled, so there was nothing to remove. */
  NOT_THROTTLED,
  /** There is no ongoing replica movement of the relevant type to remove a throttle from. */
  NO_ONGOING_MOVEMENT
}
