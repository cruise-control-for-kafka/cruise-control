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
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.KAFKA_CRUISE_CONTROL_BASE_PATH;

@RunWith(Parameterized.class)
public class ReplicaCapacityViolationIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int BROKER_ID_TO_ADD = 3;
  private static final int PARTITION_COUNT = 2;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + STATE + "?substates=anomaly_detector&json=true";
  private final Configuration _gsonJsonConfig = KafkaCruiseControlIntegrationTestUtils.createJsonMappingConfig();
  private final Boolean _vertxEnabled;

  public ReplicaCapacityViolationIntegrationTest(Boolean vertxEnabled) {
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
    configs.put(TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "2");

    configs.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "15000");
    configs.put(MonitorConfig.METADATA_MAX_AGE_MS_CONFIG, "15000");
    configs.put(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, "20000");
    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG, "20000");

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

    configs.put(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, "4");
    configs.put(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, "4");
    configs.put(WebServerConfig.VERTX_ENABLED_CONFIG, String.valueOf(_vertxEnabled));
    return configs;
  }

  @Test
  public void testReplicaCapacityViolation() {
    KafkaCruiseControlIntegrationTestUtils.createTopic(bootstrapServers(),
        new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2));

    waitForReplicaCapacityGoalViolation();

    _cluster.addBroker(BROKER_ID_TO_ADD, overridingProps());

    waitForReplicasCreatedOnNewBroker();
  }

  private void waitForReplicasCreatedOnNewBroker() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        Integer replicaCountOnBroker = JsonPath.read(responseMessage, "KafkaBrokerState.ReplicaCountByBrokerId."
          + BROKER_ID_TO_ADD);
        return replicaCountOnBroker > 0;
    }, 80, new AssertionError("No replica found on the new broker"));
  }

  private void waitForReplicaCapacityGoalViolation() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
        JSONArray unfixableGoalsArray = JsonPath.read(responseMessage,
            "$.AnomalyDetectorState.recentGoalViolations[*].unfixableViolatedGoals.[*]");
        List<String> unfixableGoals = JsonPath.parse(unfixableGoalsArray, _gsonJsonConfig)
            .read("$..*", new TypeRef<>() { });
        return unfixableGoals.contains("ReplicaCapacityGoal");
    }, 100, new AssertionError("Replica capacity goal violation not found"));
  }

}

