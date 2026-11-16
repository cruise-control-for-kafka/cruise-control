/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import java.util.Map;
import java.util.Optional;


/**
 * An {@link ExecutionStateStore} that does not persist anything. Using this store disables the recovery of executions
 * interrupted by a Cruise Control restart.
 */
public class NoopExecutionStateStore implements ExecutionStateStore {
  @Override
  public void save(PersistedExecutionState state) { }

  @Override
  public Optional<PersistedExecutionState> load() {
    return Optional.empty();
  }

  @Override
  public void delete() { }

  @Override
  public void configure(Map<String, ?> configs) { }
}
