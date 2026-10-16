/*
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

package com.linkedin.kafka.cruisecontrol.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class MetadataAdminClientTest {
  private static final String CLUSTER_ID = "test-cluster-id";
  private static final String TOPIC_0 = "test-topic-0";
  private static final Node NODE_0 = new Node(0, "host0", 9092);
  private static final Node NODE_1 = new Node(1, "host1", 9092);
  private static final Node NODE_2 = new Node(2, "host2", 9092);

  private Admin _mockAdmin;

  /**
   * Setup the unit test.
   */
  @Before
  public void setup() {
    _mockAdmin = EasyMock.mock(Admin.class);
  }

  /**
   * Tear down the unit test.
   */
  @After
  public void tearDown() {
    // Clear the interrupt flag so InterruptedException tests don't leak state to other tests.
    Thread.interrupted();
  }

  private Cluster setupAndFetchCluster(TopicPartitionInfo tpi, Collection<Node> clusterNodes) {
    TopicDescription topicDescription = new TopicDescription(TOPIC_0, false, Collections.singletonList(tpi));
    Set<String> topicNames = Collections.singleton(TOPIC_0);
    Map<String, TopicDescription> topicDescriptionMap = Collections.singletonMap(TOPIC_0, topicDescription);

    ListTopicsResult listTopicsResult = EasyMock.mock(ListTopicsResult.class);
    EasyMock.expect(listTopicsResult.names()).andReturn(KafkaFuture.completedFuture(topicNames));

    DescribeTopicsResult describeTopicsResult = EasyMock.mock(DescribeTopicsResult.class);
    EasyMock.expect(describeTopicsResult.allTopicNames()).andReturn(KafkaFuture.completedFuture(topicDescriptionMap));

    DescribeClusterResult describeClusterResult = EasyMock.mock(DescribeClusterResult.class);
    EasyMock.expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(clusterNodes));
    EasyMock.expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));

    EasyMock.expect(_mockAdmin.listTopics(EasyMock.anyObject(ListTopicsOptions.class))).andReturn(listTopicsResult);
    EasyMock.expect(_mockAdmin.describeTopics(topicNames)).andReturn(describeTopicsResult);
    EasyMock.expect(_mockAdmin.describeCluster()).andReturn(describeClusterResult);

    EasyMock.replay(_mockAdmin, listTopicsResult, describeTopicsResult, describeClusterResult);

    Cluster cluster = new MetadataAdminClient(_mockAdmin).cluster();

    EasyMock.verify(_mockAdmin, listTopicsResult, describeTopicsResult, describeClusterResult);

    return cluster;
  }

  private void clusterFallsBackToCacheOnException(Exception exception, boolean expectInterrupted) throws Exception {
    List<Node> nodes = Arrays.asList(NODE_0, NODE_1);
    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, NODE_0,
      Arrays.asList(NODE_0, NODE_1), Arrays.asList(NODE_0, NODE_1));
    TopicDescription topicDescription = new TopicDescription(TOPIC_0, false, Collections.singletonList(topicPartitionInfo));
    Set<String> topicNames = Collections.singleton(TOPIC_0);
    Map<String, TopicDescription> descriptions = Collections.singletonMap(TOPIC_0, topicDescription);

    ListTopicsResult listTopicsResult1 = EasyMock.mock(ListTopicsResult.class);
    EasyMock.expect(listTopicsResult1.names()).andReturn(KafkaFuture.completedFuture(topicNames));

    ListTopicsResult listTopicsResult2 = EasyMock.mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> failingFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(failingFuture.get()).andThrow(exception);
    EasyMock.expect(listTopicsResult2.names()).andReturn(failingFuture);

    DescribeTopicsResult describeTopicsResult = EasyMock.mock(DescribeTopicsResult.class);
    EasyMock.expect(describeTopicsResult.allTopicNames()).andReturn(KafkaFuture.completedFuture(descriptions));

    DescribeClusterResult describeClusterResult = EasyMock.mock(DescribeClusterResult.class);
    EasyMock.expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(nodes));
    EasyMock.expect(describeClusterResult.clusterId()).andReturn(KafkaFuture.completedFuture(CLUSTER_ID));

    EasyMock.expect(_mockAdmin.listTopics(EasyMock.anyObject(ListTopicsOptions.class)))
      .andReturn(listTopicsResult1)
      .andReturn(listTopicsResult2);
    EasyMock.expect(_mockAdmin.describeTopics(topicNames)).andReturn(describeTopicsResult);
    EasyMock.expect(_mockAdmin.describeCluster()).andReturn(describeClusterResult);

    EasyMock.replay(_mockAdmin, listTopicsResult1, describeTopicsResult, describeClusterResult,
      listTopicsResult2, failingFuture);

    MetadataAdminClient client = new MetadataAdminClient(_mockAdmin);
    Cluster firstCluster = client.cluster();

    Cluster cachedCluster = client.cluster();
    assertSame(firstCluster, cachedCluster);

    if (expectInterrupted) {
      assertTrue(Thread.currentThread().isInterrupted());
    }

    EasyMock.verify(_mockAdmin, listTopicsResult1, describeTopicsResult, describeClusterResult,
      listTopicsResult2, failingFuture);
  }

  private void clusterThrowsOnExceptionWithNoCache(Exception exception, boolean expectInterrupted) throws Exception {
    KafkaFuture<Set<String>> failingFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(failingFuture.get()).andThrow(exception);

    ListTopicsResult listTopicsResult = EasyMock.mock(ListTopicsResult.class);
    EasyMock.expect(listTopicsResult.names()).andReturn(failingFuture);

    EasyMock.expect(_mockAdmin.listTopics(EasyMock.anyObject(ListTopicsOptions.class))).andReturn(listTopicsResult);

    EasyMock.replay(_mockAdmin, listTopicsResult, failingFuture);

    MetadataAdminClient client = new MetadataAdminClient(_mockAdmin);
    RuntimeException ex = assertThrows(RuntimeException.class, client::cluster);
    assertTrue(ex.getMessage().contains("no cached metadata available"));
    assertEquals(exception.getClass(), ex.getCause().getClass());

    if (expectInterrupted) {
      assertTrue(Thread.currentThread().isInterrupted());
    }

    EasyMock.verify(_mockAdmin, listTopicsResult, failingFuture);
  }

  @Test
  public void testCluster() {
    List<Node> nodes = Arrays.asList(NODE_0, NODE_1);
    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, NODE_0, nodes, nodes);

    Cluster cluster = setupAndFetchCluster(topicPartitionInfo, nodes);

    assertNotNull(cluster);
    assertEquals(CLUSTER_ID, cluster.clusterResource().clusterId());
  }

  @Test
  public void testClusterWithOfflineReplicas() {
    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, NODE_0,
      Arrays.asList(NODE_0, NODE_1, NODE_2), Arrays.asList(NODE_0, NODE_1));

    Cluster cluster = setupAndFetchCluster(topicPartitionInfo, Arrays.asList(NODE_0, NODE_1));

    PartitionInfo pi = cluster.partitionsForTopic(TOPIC_0).get(0);
    assertEquals(NODE_0, pi.leader());
    assertEquals(3, pi.replicas().length);
    assertEquals(2, pi.inSyncReplicas().length);
    assertEquals(1, pi.offlineReplicas().length);
    assertEquals(2, pi.offlineReplicas()[0].id());
  }

  @Test
  public void testClusterThrowsOnExecutionExceptionWithNoCache() throws Exception {
    clusterThrowsOnExceptionWithNoCache(
      new ExecutionException(new RuntimeException("connection failed")), false);
  }

  @Test
  public void testClusterThrowsOnInterruptedExceptionWithNoCache() throws Exception {
    clusterThrowsOnExceptionWithNoCache(new InterruptedException("interrupted"), true);
  }

  @Test
  public void testClusterFallsBackToCacheOnExecutionException() throws Exception {
    clusterFallsBackToCacheOnException(
      new ExecutionException(new RuntimeException("connection failed")), false);
  }

  @Test
  public void testClusterFallsBackToCacheOnInterruptedException() throws Exception {
    clusterFallsBackToCacheOnException(new InterruptedException("interrupted"), true);
  }
}
