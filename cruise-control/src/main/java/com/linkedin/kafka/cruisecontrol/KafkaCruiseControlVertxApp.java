/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.vertx.MainVerticle;
import io.vertx.core.Vertx;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This is the main entry point for the Vertx based API.
 */
public class KafkaCruiseControlVertxApp extends KafkaCruiseControlApp {

    protected MainVerticle _verticle;
    private final Vertx _vertx;

    public KafkaCruiseControlVertxApp(KafkaCruiseControlConfig config, Integer port, String hostname) {
        super(config, port, hostname);
        _vertx = Vertx.vertx();
    }

    @Override
    public String serverUrl() {
        try {
            return new URL("http", _hostname, _verticle.getServer().actualPort(), "/").toString();
        } catch (MalformedURLException e) {
            LOG.error("Could not create URL from the given port and host.", e);
        }
        return null;
    }

    @Override
    public void start() {
        CountDownLatch startupLatch = new CountDownLatch(1);
        if (LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.NOT_STARTED
                .equals(_kafkaCruiseControl.getLoadMonitorTaskRunnerState())) {
            _kafkaCruiseControl.startUp();
        }

        _verticle = new MainVerticle(_kafkaCruiseControl, _metricRegistry, _port, _hostname);
        _vertx.deployVerticle(_verticle, event -> {
            if (event.failed()) {
                throw new RuntimeException("Startup failed", event.cause());
            }
            startupLatch.countDown();
        });
        try {
            if (!startupLatch.await(1, TimeUnit.MINUTES)) {
                throw new RuntimeException("Startup timed out");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Startup interrupted", e);
        }
        printStartupInfo();
    }

    @Override
    public void stop() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        _vertx.close(event -> {
            super.stop();
            if (event.failed()) {
                throw new RuntimeException("Shutdown failed", event.cause());
            }
            shutdownLatch.countDown();
        });
        try {
            if (!shutdownLatch.await(1, TimeUnit.MINUTES)) {
                throw new RuntimeException("Shutdown timed out");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Shutdown interrupted", e);
        }
    }
}
