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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.KAFKA_CRUISE_CONTROL_BASE_PATH;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;

@RunWith(Parameterized.class)
public class BrokerFailureIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int PARTITION_COUNT = 10;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + STATE + "?substates=analyzer&json=true";
  private final Configuration _gsonJsonConfig = KafkaCruiseControlIntegrationTestUtils.createJsonMappingConfig();

  private static final int BROKER_ID_TO_REMOVE = 1;

  private final Boolean _vertxEnabled;

  public BrokerFailureIntegrationTest(Boolean vertxEnabled) {
    this._vertxEnabled = vertxEnabled;
  }

  /**
   * Sets different parameters for test runs.
   * @return Parameters for the test runs.
   */
  @Parameterized.Parameters
  public static Collection<Boolean> data() {
    Boolean[] data = {true, false};
    return Arrays.asList(data);
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = KafkaCruiseControlIntegrationTestUtils.ccConfigOverrides();
    configs.put(AnomalyDetectorConfig.BROKER_FAILURE_DETECTION_INTERVAL_MS_CONFIG, "30000");
    configs.put(SelfHealingNotifier.BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG, "1000");
    configs.put(SelfHealingNotifier.BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG, "1500");
    configs.put(
        TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());

    configs.put(AnalyzerConfig.DEFAULT_GOALS_CONFIG, new StringJoiner(",")
        .add(MinTopicLeadersPerBrokerGoal.class.getName())
        .add(ReplicaCapacityGoal.class.getName())
        .add(ReplicaDistributionGoal.class.getName()).toString());
    
    configs.put(AnalyzerConfig.HARD_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(MinTopicLeadersPerBrokerGoal.class.getName()).toString());
    configs.put(WebServerConfig.VERTX_ENABLED_CONFIG, String.valueOf(_vertxEnabled));
    
    return configs;
  }

  @Test
  public void testBrokerFailure() {
    KafkaCruiseControlIntegrationTestUtils.createTopic(bootstrapServers(),
        new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2));

    waitForMetadataPropagates();

    KafkaCruiseControlIntegrationTestUtils.produceRandomDataToTopic(TOPIC0, 4000, 
        KafkaCruiseControlIntegrationTestUtils.getDefaultProducerProperties(bootstrapServers()));

    waitForProposal();

    // shut down a broker to initiate a self-healing action
    _cluster.shutDownBroker(BROKER_ID_TO_REMOVE);

    waitForSelfHealing();
  }

  private void waitForSelfHealing() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        Integer brokers = JsonPath.<Integer>read(responseMessage, "KafkaBrokerState.Summary.Brokers");
        JSONArray partitionLeadersArray = JsonPath.read(responseMessage,
            "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
        List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
            .read("$.*", new TypeRef<>() { });
        Map<String, String> offlineReplicas = JsonPath.read(responseMessage,
            "$.KafkaBrokerState.OfflineReplicaCountByBrokerId");
        return partitionLeaders.size() == PARTITION_COUNT && brokers == clusterSize() - 1
            && offlineReplicas.isEmpty();
    }, 80, new AssertionError("Topic replicas not fixed after broker removed"));
  }

  private void waitForProposal() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
      return JsonPath.<Boolean>read(responseMessage, "AnalyzerState.isProposalReady");
    }, 100, new AssertionError("No proposals were ready"));
  }

  private void waitForMetadataPropagates() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        JSONArray partitionLeadersArray = JsonPath.read(responseMessage,
            "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
        List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
            .read("$.*", new TypeRef<>() { });
        return partitionLeaders.size() == PARTITION_COUNT;
    }, 80, new AssertionError("Topic partitions not found for " + TOPIC0));
  }

}
