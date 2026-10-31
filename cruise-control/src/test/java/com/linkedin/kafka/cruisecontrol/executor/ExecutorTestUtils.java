/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public final class ExecutorTestUtils {

  static final String RANDOM_UUID = "random_uuid";
  // A UUID to test the proposal execution to be started with UNKNOWN_UUID, but the executor received RANDOM_UUID.
  static final String UNKNOWN_UUID = "unknown_uuid";
  static final Integer BROKER_ID_PLACEHOLDER = 0;
  static final long EXECUTION_ID_PLACEHOLDER = 0;
  static final long REMOVAL_HISTORY_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(12);
  static final long DEMOTION_HISTORY_RETENTION_TIME_MS = TimeUnit.DAYS.toMillis(1);
  static final long PRODUCE_SIZE_IN_BYTES = 10000L;
  static final long EXECUTION_DEADLINE_MS = TimeUnit.SECONDS.toMillis(120);
  static final long EXECUTION_SHORT_CHECK_MS = 10L;
  static final long EXECUTION_REGULAR_CHECK_MS = 100L;
  static final long LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS = 15000L;
  static final int LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS = 1;
  static final long EXECUTION_ALERTING_THRESHOLD_MS = 100L;

  private ExecutorTestUtils() {
  }

  /**
   * Read a value while another thread holds the monitor of the given object -- e.g. the lock that a throttle helper holds while
   * it alters and verifies broker configs.
   *
   * @param lock The object whose monitor another thread holds during the read.
   * @param read The read.
   * @param <T> The type of the value read.
   * @return The value read.
   * @throws java.util.concurrent.TimeoutException If the read does not return within 10 seconds, i.e. it waits for the monitor.
   */
  static <T> T readWhileLocked(Object lock, Callable<T> read) throws Exception {
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder = new Thread(() -> {
      synchronized (lock) {
        locked.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    holder.start();
    locked.await();
    ExecutorService reader = Executors.newSingleThreadExecutor();
    try {
      return reader.submit(read).get(10, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      holder.join();
      reader.shutdownNow();
    }
  }
}
