/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.metricsreporter.config;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.config.ConfigData;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.PREFIX;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for EnvConfigProvider class
 */
public class EnvConfigProviderTest {

  public static final String CLIENT_ID = "client1";
  public static final String NOT_SUBSTITUTED_CONFIG = "${env:TOPIC}";
  public static final String ENV_CONFIG_PROVIDER_TEST_PROPERTIES = "envConfigProviderTest.properties";
  public static final String WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG = "webserver.ssl.keystore.password";

  @Test
  public void testEnvConfigProvider() throws IOException {
    CruiseControlMetricsReporterConfig configs = CruiseControlMetricsUtils.readConfig(
      Objects.requireNonNull(getClass().getClassLoader().getResource(ENV_CONFIG_PROVIDER_TEST_PROPERTIES)).getPath());

    String actualClientId = configs.getString(PREFIX + CLIENT_ID_CONFIG);
    assertEquals(CLIENT_ID, actualClientId);

    String actualTopic = configs.getString(CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
    assertEquals(NOT_SUBSTITUTED_CONFIG, actualTopic);
  }

  @Test
  public void testExistingEnvReturnsValue() {
    String key = System.getenv().keySet().stream().findFirst().orElse("");
    String expected = System.getenv(key);
    Set<String> set = new HashSet<>();
    set.add(key);
    ConfigData actual;

    try (EnvConfigProvider configProvider = new EnvConfigProvider()) {
      actual = configProvider.get("", set);
    }

    assertEquals(expected, actual.data().get(key));
  }

  @Test
  public void testNonExistingEnvReturnsEmpty() {
    Set<String> set = new HashSet<>();
    set.add("NON_EXISTING_ENV");
    ConfigData actual;

    try (EnvConfigProvider configProvider = new EnvConfigProvider()) {
      actual = configProvider.get("", set);
    }

    assertEquals(0, actual.data().size());
  }

}
