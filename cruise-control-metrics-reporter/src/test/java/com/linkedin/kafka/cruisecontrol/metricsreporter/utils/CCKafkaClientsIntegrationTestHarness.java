/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;


public abstract class CCKafkaClientsIntegrationTestHarness extends CCKafkaIntegrationTestHarness {
  protected static final String TOPIC = "CruiseControlMetricsReporterTest";
  protected CCContainerizedKraftCluster _cluster;
  protected List<Map<Object, Object>> _brokerConfigs;

  @Override
  public void setUp() {
    Properties adminClientProps = new Properties();
    setSecurityConfigs(adminClientProps, "admin");

    _brokerConfigs = buildBrokerConfigs();
    _cluster = new CCContainerizedKraftCluster(2, _brokerConfigs, adminClientProps);
    _cluster.start();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();
  }

  /**
   * Tear down the unit test.
   */
  @After
  public void tearDown() {
    if (_cluster != null) {
      _cluster.close();
    }
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
       "localhost:" + CCContainerizedKraftCluster.CONTAINER_INTERNAL_LISTENER_PORT);
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":PLAINTEXT"));
    props.setProperty(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    props.setProperty(CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
    props.setProperty(KafkaServerConfigs.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.setProperty(KafkaServerConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2");
    return props;
  }

  @javax.annotation.Nonnull
  protected Producer<String, String> createProducer(Properties overrides) {
    Properties props = getProducerProperties(overrides);
    return new KafkaProducer<>(props);
  }

  protected Properties getProducerProperties(Properties overrides) {
    Properties result = new Properties();

    //populate defaults
    result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

    setSecurityConfigs(result, "producer");

    //apply overrides
    if (overrides != null) {
      result.putAll(overrides);
    }

    return result;
  }

  protected void setSecurityConfigs(Properties clientProps, String certAlias) {
    SecurityProtocol protocol = securityProtocol();
    if (protocol == SecurityProtocol.SSL) {
      File trustStoreFile = trustStoreFile();
      if (trustStoreFile == null) {
        throw new AssertionError("ssl set but no trust store provided");
      }
      clientProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.name);
      clientProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
      try {
        clientProps.putAll(CCSslTestUtils.createSslConfig(true, true,
            CCSslTestUtils.ConnectionMode.CLIENT, trustStoreFile, certAlias));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
