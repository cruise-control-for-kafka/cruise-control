/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.metrics.LegacyObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaCruiseControlApp {
    protected static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlApp.class);
    protected static final String METRIC_DOMAIN = "kafka.cruisecontrol";

    protected final KafkaCruiseControlConfig _config;
    protected final AsyncKafkaCruiseControl _kafkaCruiseControl;
    protected final JmxReporter _jmxReporter;
    protected MetricRegistry _metricRegistry;
    protected Integer _port;
    protected String _hostname;

    KafkaCruiseControlApp(KafkaCruiseControlConfig config, Integer port, String hostname) {
        this._config = config;
        _metricRegistry = new MetricRegistry();
        _jmxReporter = JmxReporter.forRegistry(_metricRegistry).inDomain(METRIC_DOMAIN)
                .createsObjectNamesWith(LegacyObjectNameFactory.getInstance()).build();
        _jmxReporter.start();
        _port = port;
        _hostname = hostname;

        _kafkaCruiseControl = new AsyncKafkaCruiseControl(config, _metricRegistry);

    }

    public String getHostname() {
        return _hostname;
    }

    public int getPort() {
        return _port;
    }

    public void start() throws Exception {
        _kafkaCruiseControl.startUp();
    }

    void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    /**
     * Stops Cruise Control
     */
    public void stop() {
        _kafkaCruiseControl.shutdown();
        _jmxReporter.close();
    }

    public abstract String serverUrl();

    protected void printStartupInfo() {
        boolean corsEnabled = _config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
        boolean vertxEnabled = _config.getBoolean(WebServerConfig.VERTX_ENABLED_CONFIG);
        String webApiUrlPrefix = _config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
        String uiUrlPrefix = _config.getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
        String webDir = _config.getString(WebServerConfig.WEBSERVER_UI_DISKPATH_CONFIG);
        String sessionPath = _config.getString(WebServerConfig.WEBSERVER_SESSION_PATH_CONFIG);
        System.out.println(">> ********************************************* <<");
        System.out.println(">> Application directory            : " + System.getProperty("user.dir"));
        System.out.println(">> REST API available on            : " + webApiUrlPrefix);
        System.out.println(">> Web UI available on              : " + uiUrlPrefix);
        System.out.println(">> Web UI Directory                 : " + webDir);
        System.out.println(">> Cookie prefix path               : " + sessionPath);
        System.out.println(">> Kafka Cruise Control started on  : " + serverUrl());
        System.out.println(">> CORS Enabled ?                   : " + corsEnabled);
        System.out.println(">> Vertx Enabled ?                  : " + vertxEnabled);
        System.out.println(">> ********************************************* <<");
    }

}
