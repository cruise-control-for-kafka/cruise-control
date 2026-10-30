/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * A pluggable store that persists the state of the ongoing execution, so that Cruise Control can recover it after a
 * restart (e.g. clean up leftover replication throttles or resume the remaining tasks).
 *
 * The executor calls {@link #save(PersistedExecutionState)} whenever the state of the ongoing execution changes -- i.e.
 * when the execution starts, before a batch of tasks is submitted to Kafka, and whenever a task finishes. Hence,
 * implementations are expected to be cheap and each save should atomically replace the previously saved state.
 *
 * At most one execution state is stored at a time.
 */
@InterfaceStability.Evolving
public interface ExecutionStateStore extends CruiseControlConfigurable {

  /**
   * Atomically replace the stored execution state with the given state.
   *
   * @param state Execution state to store.
   * @throws IOException If the state cannot be stored.
   */
  void save(PersistedExecutionState state) throws IOException;

  /**
   * @return The stored execution state, or {@link Optional#empty()} if there is no stored execution state.
   * @throws IOException If the stored state cannot be read.
   */
  Optional<PersistedExecutionState> load() throws IOException;

  /**
   * Delete the stored execution state (if any).
   *
   * @throws IOException If the stored state cannot be deleted.
   */
  void delete() throws IOException;

  /**
   * Release any resources held by the store.
   */
  default void close() { }
}
