/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class KafkaCruiseControlThreadFactory implements ThreadFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlThreadFactory.class);
  private final String _name;
  private final boolean _daemon;
  private final AtomicInteger _id = new AtomicInteger(0);
  private final Logger _logger;

  public KafkaCruiseControlThreadFactory(String name) {
    this(name, true, null);
  }

  public KafkaCruiseControlThreadFactory(String name, boolean daemon, Logger logger) {
    _name = name;
    _daemon = daemon;
    _logger = logger == null ? LOG : logger;
  }

  @Override
  public Thread newThread(@NonNull Runnable r) {
    Thread t = new Thread(r, _name + "-" + _id.getAndIncrement());
    t.setDaemon(_daemon);
    t.setUncaughtExceptionHandler((t1, e) -> _logger.error("Uncaught exception in {}: ", t1.getName(), e));
    return t;
  }
}
