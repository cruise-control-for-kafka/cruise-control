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

package com.linkedin.kafka.cruisecontrol;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.Errors;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.closeAdminClientWithTimeout;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createAdminClient;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createTopic;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.wrapTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class KafkaCruiseControlUtilsTest {
  private static final NewTopic TEST_TOPIC = wrapTopic("mock-topic", 10, (short) 3, TimeUnit.MINUTES.toMillis(10));

  @Test
  public void testCreateTopic() throws ExecutionException, InterruptedException, TimeoutException {
    Admin adminClient = EasyMock.createMock(Admin.class);
    CreateTopicsResult createTopicsResult = EasyMock.createMock(CreateTopicsResult.class);
    KafkaFuture<Void> createTopicsResultFuture = EasyMock.createMock(KafkaFuture.class);
    Map<String, KafkaFuture<Void>> createTopicsResultValues = Collections.singletonMap(TEST_TOPIC.name(), createTopicsResultFuture);

    // 1. Existing topic
    EasyMock.expect(adminClient.createTopics(Collections.singletonList(TEST_TOPIC))).andReturn(createTopicsResult).once();
    EasyMock.expect(createTopicsResult.values()).andReturn(createTopicsResultValues).once();
    EasyMock.expect(createTopicsResultFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS))
            .andThrow(new ExecutionException(Errors.TOPIC_ALREADY_EXISTS.exception()));

    EasyMock.replay(adminClient, createTopicsResult, createTopicsResultFuture);

    assertFalse(createTopic(adminClient, TEST_TOPIC));
    EasyMock.verify(adminClient, createTopicsResult, createTopicsResultFuture);
    EasyMock.reset(adminClient, createTopicsResult, createTopicsResultFuture);

    // 2. New topic
    EasyMock.expect(adminClient.createTopics(Collections.singletonList(TEST_TOPIC))).andReturn(createTopicsResult).once();
    EasyMock.expect(createTopicsResult.values()).andReturn(createTopicsResultValues).once();
    EasyMock.expect(createTopicsResultFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)).andReturn(null).once();

    EasyMock.replay(adminClient, createTopicsResult, createTopicsResultFuture);

    assertTrue(createTopic(adminClient, TEST_TOPIC));
    EasyMock.verify(adminClient, createTopicsResult, createTopicsResultFuture);
  }

  @Test
  public void testCreateAdminClientWithComponentName() {
    Map<String, Object> adminClientConfigs = new HashMap<>();
    adminClientConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    adminClientConfigs.put(AdminClientConfig.CLIENT_ID_CONFIG, "cc");
    Admin adminClient = createAdminClient(adminClientConfigs, "executor");
    try {
      Set<String> clientIds = adminClient.metrics().keySet().stream()
                                         .map(metricName -> metricName.tags().get("client-id"))
                                         .filter(clientId -> clientId != null)
                                         .collect(Collectors.toSet());
      assertEquals(Set.of("cc-admin-executor"), clientIds);
      assertEquals("cc", adminClientConfigs.get(AdminClientConfig.CLIENT_ID_CONFIG));
    } finally {
      closeAdminClientWithTimeout(adminClient);
    }
  }
}
