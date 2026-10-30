/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.exception.ReplicaToBrokerSetMappingException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.TOPIC_COLD;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.TOPIC_HOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link TopicPatternBrokerSetMappingPolicy}
 */
public class TopicPatternBrokerSetMappingPolicyTest {
  private static final String HOT2 = "hot2";

  @Rule
  public TemporaryFolder _folder = new TemporaryFolder();

  /**
   * Tests that a topic is mapped to the broker set whose topic pattern fully matches it, and other topics are unconstrained.
   */
  @Test
  public void testTopicPatternMapping() throws Exception {
    ClusterModel clusterModel = clusterModelWithHot2();
    BrokerSetResolutionHelper helper = helper(clusterModel, Map.of("fast", Set.of(1, 2, 3), "unmapped", Set.of(0)));
    TopicPatternBrokerSetMappingPolicy policy = policy(resourceFile("testBrokerSetsWithTopicPatterns.json"));

    for (Replica replica : clusterModel.leaderReplicas()) {
      String topic = replica.topicPartition().topic();
      String brokerSetId = policy.brokerSetIdForReplica(replica, clusterModel, helper);
      if (TOPIC_HOT.equals(topic)) {
        assertEquals("fast", brokerSetId);
      } else {
        // "cold" matches no pattern, "hot2" is not a full match of "hot".
        assertNull("Unexpected broker set for topic " + topic, brokerSetId);
      }
    }
  }

  /**
   * Tests that a broker set file without topic patterns leaves every replica unconstrained.
   */
  @Test
  public void testNoTopicPatterns() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    BrokerSetResolutionHelper helper = helper(clusterModel, Map.of("Blue", Set.of(0, 1, 2), "Green", Set.of(3)));
    TopicPatternBrokerSetMappingPolicy policy = policy(resourceFile("testBrokerSets.json"));

    for (Replica replica : clusterModel.leaderReplicas()) {
      assertNull(policy.brokerSetIdForReplica(replica, clusterModel, helper));
    }
  }

  /**
   * Tests that a topic pattern that does not compile fails with an error naming the topic, the broker set and the pattern.
   */
  @Test
  public void testInvalidTopicPattern() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    BrokerSetResolutionHelper helper = helper(clusterModel, Map.of("fast", Set.of(1, 2, 3), "unmapped", Set.of(0)));
    TopicPatternBrokerSetMappingPolicy policy = policy(writeFile(brokerSet("fast", "1, 2, 3", "hot[")));

    ReplicaToBrokerSetMappingException e =
        assertThrows(ReplicaToBrokerSetMappingException.class,
                     () -> policy.brokerSetIdForReplica(replica(clusterModel, TOPIC_COLD), clusterModel, helper));
    assertMessageContains(e, TOPIC_COLD, "fast", "hot[");
  }

  /**
   * Tests that a topic matched by the topic patterns of two broker sets fails with an error naming the topic, the broker sets and
   * the patterns, while a topic matched by one pattern is still mapped.
   */
  @Test
  public void testTopicMatchedByMultipleBrokerSets() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    BrokerSetResolutionHelper helper = helper(clusterModel, Map.of("fast", Set.of(1, 2), "faster", Set.of(3), "unmapped", Set.of(0)));
    TopicPatternBrokerSetMappingPolicy policy =
        policy(writeFile(brokerSet("fast", "1, 2", "hot|cold") + ",\n" + brokerSet("faster", "3", "h.*")));

    ReplicaToBrokerSetMappingException e =
        assertThrows(ReplicaToBrokerSetMappingException.class,
                     () -> policy.brokerSetIdForReplica(replica(clusterModel, TOPIC_HOT), clusterModel, helper));
    assertMessageContains(e, TOPIC_HOT, "fast", "faster", "hot|cold", "h.*");
    assertEquals("fast", policy.brokerSetIdForReplica(replica(clusterModel, TOPIC_COLD), clusterModel, helper));
  }

  /**
   * Tests that a topic matched by the topic pattern of a broker set unknown to the broker set resolution helper fails with an
   * error naming the topic, the broker set and the pattern.
   */
  @Test
  public void testUnknownBrokerSet() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    BrokerSetResolutionHelper helper = helper(clusterModel, Map.of("Blue", Set.of(0, 1, 2, 3)));
    TopicPatternBrokerSetMappingPolicy policy = policy(resourceFile("testBrokerSetsWithTopicPatterns.json"));

    ReplicaToBrokerSetMappingException e =
        assertThrows(ReplicaToBrokerSetMappingException.class,
                     () -> policy.brokerSetIdForReplica(replica(clusterModel, TOPIC_HOT), clusterModel, helper));
    assertMessageContains(e, TOPIC_HOT, "fast", "hot", "Blue");
    assertNull(policy.brokerSetIdForReplica(replica(clusterModel, TOPIC_COLD), clusterModel, helper));
  }

  /**
   * Tests that the file is read once per broker set resolution helper (i.e. per optimization), and re-read for a new helper.
   */
  @Test
  public void testReloadPerBrokerSetResolutionHelper() throws Exception {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    Map<String, Set<Integer>> brokerSets = Map.of("fast", Set.of(1, 2, 3), "unmapped", Set.of(0));
    File file = writeFile(brokerSet("fast", "1, 2, 3", "hot"));
    TopicPatternBrokerSetMappingPolicy policy = policy(file);
    Replica hot = replica(clusterModel, TOPIC_HOT);
    Replica cold = replica(clusterModel, TOPIC_COLD);

    BrokerSetResolutionHelper firstHelper = helper(clusterModel, brokerSets);
    assertEquals("fast", policy.brokerSetIdForReplica(hot, clusterModel, firstHelper));
    assertNull(policy.brokerSetIdForReplica(cold, clusterModel, firstHelper));

    // The same helper does not read the edited file again.
    Files.writeString(file.toPath(), brokerSetsJson(brokerSet("fast", "1, 2, 3", "cold")), StandardCharsets.UTF_8);
    assertEquals("fast", policy.brokerSetIdForReplica(hot, clusterModel, firstHelper));
    assertNull(policy.brokerSetIdForReplica(cold, clusterModel, firstHelper));

    // A new helper reads the edited file.
    BrokerSetResolutionHelper secondHelper = helper(clusterModel, brokerSets);
    assertNull(policy.brokerSetIdForReplica(hot, clusterModel, secondHelper));
    assertEquals("fast", policy.brokerSetIdForReplica(cold, clusterModel, secondHelper));

    // A file that cannot be read fails the optimization, and is not read again by the same helper.
    assertTrue(file.delete());
    BrokerSetResolutionHelper thirdHelper = helper(clusterModel, brokerSets);
    assertThrows(ReplicaToBrokerSetMappingException.class, () -> policy.brokerSetIdForReplica(hot, clusterModel, thirdHelper));
    Files.writeString(file.toPath(), brokerSetsJson(brokerSet("fast", "1, 2, 3", "hot")), StandardCharsets.UTF_8);
    assertThrows(ReplicaToBrokerSetMappingException.class, () -> policy.brokerSetIdForReplica(hot, clusterModel, thirdHelper));
    assertEquals("fast", policy.brokerSetIdForReplica(hot, clusterModel, helper(clusterModel, brokerSets)));
  }

  private static void assertMessageContains(Exception e, String... parts) {
    for (String part : parts) {
      assertTrue(String.format("Message '%s' does not contain '%s'", e.getMessage(), part), e.getMessage().contains(part));
    }
  }

  private static ClusterModel clusterModelWithHot2() {
    ClusterModel clusterModel = DeterministicCluster.brokerSetTopicPatternSatisfiable();
    clusterModel.createReplica(DeterministicCluster.RACK_BY_BROKER_TOPIC_PATTERN.get(1).toString(), 1, new TopicPartition(HOT2, 0), 0, true);
    return clusterModel;
  }

  private static Replica replica(ClusterModel clusterModel, String topic) {
    return clusterModel.partition(new TopicPartition(topic, 0)).leader();
  }

  private static BrokerSetResolutionHelper helper(ClusterModel clusterModel, Map<String, Set<Integer>> brokerSets)
      throws BrokerSetResolutionException {
    BrokerSetResolver brokerSetResolver = EasyMock.createNiceMock(BrokerSetResolver.class);
    EasyMock.expect(brokerSetResolver.brokerIdsByBrokerSetId(BrokerSetResolutionHelper.getRackIdByBrokerIdMapping(clusterModel)))
            .andReturn(brokerSets);
    EasyMock.replay(brokerSetResolver);
    return new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);
  }

  private static TopicPatternBrokerSetMappingPolicy policy(File brokerSetFile) {
    TopicPatternBrokerSetMappingPolicy policy = new TopicPatternBrokerSetMappingPolicy();
    policy.configure(Map.of(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, brokerSetFile.getAbsolutePath()));
    return policy;
  }

  private File resourceFile(String name) {
    return new File(Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getFile());
  }

  private File writeFile(String brokerSets) throws IOException {
    File file = _folder.newFile();
    Files.writeString(file.toPath(), brokerSetsJson(brokerSets), StandardCharsets.UTF_8);
    return file;
  }

  private static String brokerSetsJson(String brokerSets) {
    return String.format("{\"brokerSets\": [%n%s%n]}", brokerSets);
  }

  private static String brokerSet(String brokerSetId, String brokerIds, String topicPattern) {
    return String.format("{\"brokerSetId\": \"%s\", \"brokerIds\": [%s], \"topicPattern\": \"%s\"}", brokerSetId, brokerIds,
                         topicPattern.replace("\\", "\\\\"));
  }
}
