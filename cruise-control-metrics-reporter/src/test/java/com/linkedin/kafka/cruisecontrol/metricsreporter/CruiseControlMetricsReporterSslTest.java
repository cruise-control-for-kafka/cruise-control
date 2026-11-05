/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Assert;

public class CruiseControlMetricsReporterSslTest extends CruiseControlMetricsReporterTest {

  private File _trustStoreFile;

  public CruiseControlMetricsReporterSslTest() {
    super();
    try {
      _trustStoreFile = File.createTempFile("truststore", ".jks");
    } catch (IOException e) {
      Assert.fail("Failed to create trust store");
    }
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();

    // Generate server-side SSL configs (keystore + truststore) for the broker
    try {
      props.putAll(CCSslTestUtils.createSslConfig(false, true,
          CCSslTestUtils.ConnectionMode.SERVER, _trustStoreFile, "server"));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    // Generate client-side SSL configs for the CC metrics reporter and prefix them
    Properties clientSslProps = new Properties();
    setSecurityConfigs(clientSslProps, "producer");
    for (String configName : ProducerConfig.configNames()) {
      Object value = clientSslProps.get(configName);
      if (value != null) {
        props.put(appendPrefix(configName), value);
      }
    }
    props.putAll(super.overridingProps());
    props.put("listener.security.protocol.map", String.join(",",
      CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
      CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":SSL",
      CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":SSL"));
    // The Kafka brokers should use the same key manager algorithm as the host that generates the certs
    props.setProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, KeyManagerFactory.getDefaultAlgorithm());
    return props;
  }

  @Override
  public File trustStoreFile() {
    return _trustStoreFile;
  }

  @Override
  public SecurityProtocol securityProtocol() {
    return SecurityProtocol.SSL;
  }

  private String appendPrefix(Object key) {
    return CruiseControlMetricsReporterConfig.config((String) key);
  }

}
