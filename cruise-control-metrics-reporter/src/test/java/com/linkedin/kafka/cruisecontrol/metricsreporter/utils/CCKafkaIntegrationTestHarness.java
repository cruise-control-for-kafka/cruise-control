/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.util.Properties;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.After;
import org.junit.Before;

public abstract class CCKafkaIntegrationTestHarness {
  protected static final String TOPIC = "CruiseControlMetricsReporterTest";
  protected CCContainerizedKraftCluster _cluster;
  protected String _bootstrapUrl;

  /**
   * Creates and starts a containerized KRaft Kafka cluster before each test.
   */
  @Before
  public void setUp() {
    _cluster = new CCContainerizedKraftCluster(clusterSize(), overridingProps(), new Properties());
    _cluster.start();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();
  }

  /**
   * Shuts down the containerized Kafka cluster after each test.
   */
  @After
  public void tearDown() {
    if (_cluster != null) {
      _cluster.close();
    }
  }

  public String bootstrapServers() {
    return _bootstrapUrl;
  }

  protected SecurityProtocol securityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected File trustStoreFile() {
    return null;
  }

  /**
   * Returns the number of brokers in the test cluster.
   * Defaults to 3, the minimum required to maintain KRaft quorum in combined broker/controller mode.
   *
   * @return the number of brokers
   */
  protected int clusterSize() {
    return 3;
  }

  /**
   * Returns properties to override the default Kafka broker configuration for Cruise Control testing.
   *
   * @return broker configurations
   */
  public Properties overridingProps() {
    Properties props = new Properties();
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":PLAINTEXT"));
    props.setProperty(KafkaServerConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, "1");
    props.setProperty(KafkaServerConfigs.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.setProperty(KafkaServerConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2");

    return props;
  }
}
