/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Assert;
import org.junit.Test;


public class AdminClientConfigParserTest {

  @Test
  public void testParseAdminClientRequestTimeoutMs() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    Integer expectedAdminClientTimeoutMs = (int) TimeUnit.SECONDS.toMillis(101);
    properties.put(ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(expectedAdminClientTimeoutMs));
    KafkaCruiseControlConfig cruiseControlConfig = new KafkaCruiseControlConfig(properties);
    Map<String, Object> parsedAdminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(cruiseControlConfig);
    Assert.assertEquals(expectedAdminClientTimeoutMs, parsedAdminClientConfigs.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
  }

  @Test
  public void testParseDefaultAdminClientRequestTimeoutMs() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig cruiseControlConfig = new KafkaCruiseControlConfig(properties);
    Map<String, Object> parsedAdminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(cruiseControlConfig);
    Assert.assertEquals(ExecutorConfig.DEFAULT_ADMIN_CLIENT_REQUEST_TIMEOUT_MS,
                        parsedAdminClientConfigs.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
  }

  @Test
  public void testParseAdminClientPassesThroughUserConfigs() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3");
    properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    properties.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000");
    properties.put(MonitorConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "2000");
    KafkaCruiseControlConfig cruiseControlConfig = new KafkaCruiseControlConfig(properties);
    Map<String, Object> parsedAdminClientConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(cruiseControlConfig);
    AdminClientConfig adminClientConfig = new AdminClientConfig(parsedAdminClientConfigs);
    Assert.assertEquals(List.of("TLSv1.3"), adminClientConfig.getList(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
    Assert.assertEquals("kafka", adminClientConfig.getString(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
    Assert.assertEquals(5000L, (long) adminClientConfig.getLong(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG));
    Assert.assertEquals(2000L, (long) adminClientConfig.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG));
    Assert.assertTrue(AdminClientConfig.configNames().containsAll(parsedAdminClientConfigs.keySet()));
  }
}
