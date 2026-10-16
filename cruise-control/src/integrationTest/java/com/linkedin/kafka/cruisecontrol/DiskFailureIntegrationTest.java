/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyState;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaTestUtils;
import net.minidev.json.JSONArray;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.KAFKA_CRUISE_CONTROL_BASE_PATH;

@RunWith(Parameterized.class)
public class DiskFailureIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final short TOPIC0_REPLICATION_FACTOR = (short) 2;
  private static final int BROKER_ID_TO_CAUSE_DISK_FAILURE = 2;
  private static final int PARTITION_COUNT = 3;
  private static final int KAFKA_CLUSTER_SIZE = 3;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + STATE + "?substates=anomaly_detector&json=true";
  private static final String CRUISE_CONTROL_ANALYZER_STATE_ENDPOINT =
      KAFKA_CRUISE_CONTROL_BASE_PATH + STATE + "?substates=analyzer&json=true";
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureIntegrationTest.class);
  private final Configuration _gsonJsonConfig = KafkaCruiseControlIntegrationTestUtils.createJsonMappingConfig();
  private final List<Entry<File, File>> _brokerLogDirs = new ArrayList<>(KAFKA_CLUSTER_SIZE);
  private final Boolean _vertxEnabled;

  public DiskFailureIntegrationTest(Boolean vertxEnabled) {
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
  
  @Before
  public void setup() throws Exception {
    super.start();
  }

  @Override
  protected int clusterSize() {
    return KAFKA_CLUSTER_SIZE;
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Override
  public Map<Object, Object> overridingProps() {
    Map<Object, Object> props = KafkaCruiseControlIntegrationTestUtils.createBrokerProps();
    Entry<File, File> logFolders = Map.entry(CCKafkaTestUtils.newTempDir(), CCKafkaTestUtils.newTempDir());
    _brokerLogDirs.add(logFolders);
    props.put(ServerLogConfigs.LOG_DIR_CONFIG, logFolders.getKey().getAbsolutePath() + "," + logFolders.getValue().getAbsolutePath());
    return props;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = KafkaCruiseControlIntegrationTestUtils.ccConfigOverrides();
    configs.put(TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    
    configs.put(AnalyzerConfig.DEFAULT_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaDistributionGoal.class.getName())
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());
    
    configs.put(AnalyzerConfig.HARD_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());
    configs.put(WebServerConfig.VERTX_ENABLED_CONFIG, String.valueOf(_vertxEnabled));
    
    return configs;
  }

  @Test
  public void testDiskFailure() throws IOException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections
        .singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Collections.singleton(new NewTopic(TOPIC0, PARTITION_COUNT, TOPIC0_REPLICATION_FACTOR)));
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    waitForMetadataPropagates();

    KafkaCruiseControlIntegrationTestUtils.produceRandomDataToTopic(TOPIC0, 5, 400, KafkaCruiseControlIntegrationTestUtils
        .getDefaultProducerProperties(bootstrapServers()));

    waitForProposal();
    
    Entry<File, File> entry = _brokerLogDirs.get(BROKER_ID_TO_CAUSE_DISK_FAILURE);
    FileUtils.deleteDirectory(entry.getKey());
    LOG.info("Disk failure injected");
    waitForOfflineReplicaDetection();
    waitForDiskFailureFixStarted();
    waitForOfflineReplicasFixed();
  }

  private void waitForOfflineReplicasFixed() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
      Map<String, String> offlineReplicas = JsonPath.read(responseMessage,
            "$.KafkaBrokerState.OfflineReplicaCountByBrokerId");
      return offlineReplicas.isEmpty();
    }, 100, new AssertionError("There are still offline replica on broker"));
  }

  private void waitForDiskFailureFixStarted() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
      JSONArray diskFailuresArray = JsonPath.read(responseMessage,
            "$.AnomalyDetectorState.recentDiskFailures[?(@.status=='" + AnomalyState.Status.FIX_STARTED + "')].anomalyId");
      return diskFailuresArray.size() == 1;
    }, 100, new AssertionError("Disk failure anomaly not detected"));
  }

  private void waitForOfflineReplicaDetection() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
      Integer offlineReplicas = JsonPath.read(responseMessage,
          "$.KafkaBrokerState.OfflineReplicaCountByBrokerId." + BROKER_ID_TO_CAUSE_DISK_FAILURE);
      return offlineReplicas > 0;
    }, 100, new AssertionError("Offline replicas not detected"));
  }

  private void waitForProposal() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_ANALYZER_STATE_ENDPOINT);
      return JsonPath.<Boolean>read(responseMessage, "AnalyzerState.isProposalReady");
    }, Duration.ofSeconds(200), Duration.ofSeconds(15), new AssertionError("No proposal available"));
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

