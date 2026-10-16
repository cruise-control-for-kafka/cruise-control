/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.exception.ReplicaToBrokerSetMappingException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Unit test for {@link TopicNameHashBrokerSetMappingPolicy}
 */
public class TopicNameHashBrokerSetMappingPolicyTest {
  private static final TopicNameHashBrokerSetMappingPolicy TOPIC_NAME_HASH_BROKER_SET_MAPPING_POLICY =
      new TopicNameHashBrokerSetMappingPolicy();

  /**
   * Tests if the policy maps topics to a single broker set
   */
  @Test
  public void testSingleBrokerSetMappingPolicy() throws BrokerSetResolutionException, ReplicaToBrokerSetMappingException {
    ClusterModel clusterModel = DeterministicCluster.brokerSetSatisfiable1();
    Map<String, Set<Integer>> testSingleBrokerSetMapping = Collections.singletonMap("BS1", Set.of(0, 1, 2, 3, 4, 5));

    BrokerSetResolver brokerSetResolver = EasyMock.createNiceMock(BrokerSetResolver.class);
    EasyMock.expect(brokerSetResolver.brokerIdsByBrokerSetId(BrokerSetResolutionHelper.getRackIdByBrokerIdMapping(clusterModel)))
            .andReturn(testSingleBrokerSetMapping);
    EasyMock.replay(brokerSetResolver);

    BrokerSetResolutionHelper brokerSetResolutionHelper = new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);

    for (Replica replica : clusterModel.leaderReplicas()) {
      assertEquals("BS1",
                   TOPIC_NAME_HASH_BROKER_SET_MAPPING_POLICY.brokerSetIdForReplica(replica, clusterModel, brokerSetResolutionHelper));
    }
  }

  /**
   * Tests if the policy maps topics to a multiple broker sets
   */
  @Test
  public void testMultipleBrokerSetMappingPolicy() throws BrokerSetResolutionException {
    // Cluster with 4 topics, with names "A", "B", "C", "D"
    ClusterModel clusterModel = DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY);
    Map<String, Set<Integer>> testBrokerSetMapping = Map.of("BS1", Set.of(0), "BS2", Set.of(1, 2));

    BrokerSetResolver brokerSetResolver = EasyMock.createNiceMock(BrokerSetResolver.class);
    EasyMock.expect(brokerSetResolver.brokerIdsByBrokerSetId(BrokerSetResolutionHelper.getRackIdByBrokerIdMapping(clusterModel)))
            .andReturn(testBrokerSetMapping);
    EasyMock.replay(brokerSetResolver);

    BrokerSetResolutionHelper brokerSetResolutionHelper = new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);

    Map<String, String> brokerSetsMappingForTopics = new HashMap<>();

    for (Map.Entry<String, List<Partition>> partitionsByTopic : clusterModel.getPartitionsByTopic().entrySet()) {
      Set<Replica> replicasForTopic = partitionsByTopic.getValue()
                                                       .stream()
                                                       .map(Partition::replicas)
                                                       .flatMap(Collection::stream)
                                                       .collect(Collectors.toSet());
      Set<String> brokerSetIdForTopic = replicasForTopic.stream().map(replica -> {
        try {
          return TOPIC_NAME_HASH_BROKER_SET_MAPPING_POLICY.brokerSetIdForReplica(replica, clusterModel, brokerSetResolutionHelper);
        } catch (ReplicaToBrokerSetMappingException e) {
          // Make lambda happy about unchecked exception
          throw new RuntimeException();
        }
      }).collect(Collectors.toSet());

      // Validate that each topic has one single broker set assigned
      assertEquals(1, brokerSetIdForTopic.size());

      brokerSetsMappingForTopics.put(partitionsByTopic.getKey(), brokerSetIdForTopic.stream().findFirst().get());
    }

    assertEquals("BS2", brokerSetsMappingForTopics.get("A"));
    assertEquals("BS1", brokerSetsMappingForTopics.get("B"));
    assertEquals("BS1", brokerSetsMappingForTopics.get("C"));
    assertEquals("BS1", brokerSetsMappingForTopics.get("D"));
  }

  /**
   * Tests if the policy maps topics to multiple broker sets with more broker sets
   */
  @Test
  public void testMultipleBrokerSetMappingPolicy2() throws BrokerSetResolutionException {
    // Cluster with 4 topics, with names "A", "B", "C", "D"
    ClusterModel clusterModel = DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY);
    Map<String, Set<Integer>> testBrokerSetMapping = Map.of("BS1", Set.of(0), "BS2", Set.of(1), "BS3", Set.of(2));

    BrokerSetResolver brokerSetResolver = EasyMock.createNiceMock(BrokerSetResolver.class);
    EasyMock.expect(brokerSetResolver.brokerIdsByBrokerSetId(BrokerSetResolutionHelper.getRackIdByBrokerIdMapping(clusterModel)))
            .andReturn(testBrokerSetMapping);
    EasyMock.replay(brokerSetResolver);

    BrokerSetResolutionHelper brokerSetResolutionHelper = new BrokerSetResolutionHelper(clusterModel, brokerSetResolver);

    Map<String, String> brokerSetsMappingForTopics = new HashMap<>();

    for (Map.Entry<String, List<Partition>> partitionsByTopic : clusterModel.getPartitionsByTopic().entrySet()) {
      Set<Replica> replicasForTopic = partitionsByTopic.getValue()
                                                       .stream()
                                                       .map(Partition::replicas)
                                                       .flatMap(Collection::stream)
                                                       .collect(Collectors.toSet());
      Set<String> brokerSetIdForTopic = replicasForTopic.stream().map(replica -> {
        try {
          return TOPIC_NAME_HASH_BROKER_SET_MAPPING_POLICY.brokerSetIdForReplica(replica, clusterModel, brokerSetResolutionHelper);
        } catch (ReplicaToBrokerSetMappingException e) {
          // Make lambda happy about unchecked exception
          throw new RuntimeException();
        }
      }).collect(Collectors.toSet());

      // Validate that each topic has one single broker set assigned
      assertEquals(1, brokerSetIdForTopic.size());

      brokerSetsMappingForTopics.put(partitionsByTopic.getKey(), brokerSetIdForTopic.stream().findFirst().get());
    }

    assertEquals("BS2", brokerSetsMappingForTopics.get("A"));
    assertEquals("BS1", brokerSetsMappingForTopics.get("B"));
    assertEquals("BS3", brokerSetsMappingForTopics.get("C"));
    assertEquals("BS3", brokerSetsMappingForTopics.get("D"));
  }
}

