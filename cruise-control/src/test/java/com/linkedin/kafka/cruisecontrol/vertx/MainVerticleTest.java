/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link MainVerticle}
 */
public class MainVerticleTest {

  private Vertx _vertx;
  private AsyncKafkaCruiseControl _mockAsyncKafkaCruiseControl;
  private MetricRegistry _metricRegistry;

  /**
   * Setup test dependencies before each test.
   */
  @Before
  public void setup() {
    _vertx = Vertx.vertx();
    _mockAsyncKafkaCruiseControl = EasyMock.mock(AsyncKafkaCruiseControl.class);
    _metricRegistry = new MetricRegistry();
  }

  /**
   * Teardown test dependencies after each test.
   */
  @After
  public void teardown() {
    if (_vertx != null) {
      CountDownLatch latch = new CountDownLatch(1);
      _vertx.close(ar -> latch.countDown());
      try {
        latch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Test that MainVerticle can be created with required parameters.
   * This verifies that the Jackson dependencies are properly aligned and the class can be instantiated.
   */
  @Test
  public void testMainVerticleCreation() {
    MainVerticle verticle = new MainVerticle(_mockAsyncKafkaCruiseControl, _metricRegistry, 9090, "localhost");
    
    assertNotNull("Verticle should not be null", verticle);
    // Note: getEndPoints() is initialized in start() method, not in constructor
  }

  /**
   * Test that Jackson dependencies are properly available for Vertx usage.
   * This test ensures that the JsonIncludeProperties annotation is available,
   * which was the root cause of issue #2333.
   */
  @Test
  public void testJacksonDependenciesAvailable() {
    try {
      Class.forName("com.fasterxml.jackson.annotation.JsonIncludeProperties");
    } catch (ClassNotFoundException e) {
      fail("Jackson JsonIncludeProperties class should be available: " + e.getMessage());
    }
  }
}
