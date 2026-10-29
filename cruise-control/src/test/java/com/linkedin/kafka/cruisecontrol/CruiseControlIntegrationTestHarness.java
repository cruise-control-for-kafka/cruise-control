/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.KafkaServerConfigs;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.After;
import org.junit.Before;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;

public abstract class CruiseControlIntegrationTestHarness extends CCKafkaIntegrationTestHarness {
  // The containerized Kraft cluster run in dual broker/controller mode requires 3 brokers for reaching
  // quorum when tests shut down brokers during partition moves.
  private static final int NUM_OF_BROKERS = 3;
  protected static final String TOPIC = "CruiseControlMetricsReporterTest";
  protected static final String HOST = "127.0.0.1";
  protected CCContainerizedKraftCluster _cluster;
  protected List<Map<Object, Object>> _brokerConfigs;

  protected KafkaCruiseControlConfig _config;
  protected KafkaCruiseControlApp _app;

  protected static final String LOCALHOST = "localhost";
  private static final int ANY_PORT = 0;

  protected Map<String, Object> withConfigs() {
    return Collections.emptyMap();
  }

  private void setupConfig() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    properties.setProperty(CruiseControlMetricsReporterSampler.METRIC_REPORTER_TOPIC, TOPIC);
    properties.put(KafkaSampleStore.PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__partition_samples");
    properties.put(KafkaSampleStore.BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__broker_samples");
    properties.putAll(withConfigs());
    _config = new KafkaCruiseControlConfig(properties);
  }

  @Override
  @Before
  public void setUp() {
    _brokerConfigs = buildBrokerConfigs();
    _cluster = new CCContainerizedKraftCluster(clusterSize(), _brokerConfigs, new Properties());
    _cluster.start();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();

    setupConfig();
    try {
      _app = KafkaCruiseControlUtils.getCruiseControlApp(_config, ANY_PORT, LOCALHOST);
      _app.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tear down the unit test.
   */
  @Override
  @After
  public void tearDown() {
    if (_app != null) {
      _app.stop();
    }
    if (_cluster != null) {
      _cluster.close();
    }
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
       HOST + ":" + CCContainerizedKraftCluster.CONTAINER_INTERNAL_LISTENER_PORT);
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":PLAINTEXT"));
    props.setProperty(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    props.setProperty(CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
    props.setProperty(KafkaServerConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, "1");
    props.setProperty(KafkaServerConfigs.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.setProperty(KafkaServerConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2");

    return props;
  }

  @Override
  protected int clusterSize() {
    return NUM_OF_BROKERS;
  }
}

