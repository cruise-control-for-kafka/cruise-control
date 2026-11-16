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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for fetching Kafka cluster metadata using Kafka Admin APIs.
 *
 * This replaces the use of MetadataClient which relies on internal Kafka APIs.
 */
public class MetadataAdminClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataAdminClient.class);

  private final Admin _adminClient;
  private final AtomicInteger _clusterGeneration = new AtomicInteger(0);
  private volatile ClusterAndGeneration _cached;

  /**
   * An atomic snapshot of the cluster metadata and its generation counter.
   */
  public record ClusterAndGeneration(Cluster cluster, int generation) { }

  /**
   * Creates a new MetadataAdminClient.
   *
   * @param adminClient The Admin client to use for fetching cluster metadata.
   */
  public MetadataAdminClient(Admin adminClient) {
    _adminClient = adminClient;
  }

  /**
   * Fetches the current cluster metadata and returns it paired with the generation counter.
   * Falls back to cached metadata if the refresh fails.
   *
   * @return a {@link ClusterAndGeneration} containing the cluster snapshot and its generation
   */
  public ClusterAndGeneration clusterAndGeneration() {
    try {
      Set<String> topicNames = _adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();

      Map<String, TopicDescription> topicDescriptions = _adminClient.describeTopics(topicNames).allTopicNames().get();

      DescribeClusterResult describeResult = _adminClient.describeCluster();
      Collection<Node> nodes = describeResult.nodes().get();
      String clusterId = describeResult.clusterId().get();
      Set<Integer> liveNodeIds = nodes.stream().map(Node::id).collect(Collectors.toSet());

      List<PartitionInfo> partitionInfos = new ArrayList<>();

      for (TopicDescription desc : topicDescriptions.values()) {
        for (TopicPartitionInfo partInfo : desc.partitions()) {

          Node leader = partInfo.leader();
          List<Node> replicaList = partInfo.replicas();

          Node[] replicas = replicaList.toArray(Node[]::new);
          Node[] isr = partInfo.isr().toArray(Node[]::new);

          // TopicPartitionInfo doesn't expose offline replicas so we derive them from live broker set.
          Node[] offlineReplicas = replicaList.stream()
              .filter(r -> !liveNodeIds.contains(r.id()))
              .toArray(Node[]::new);

          partitionInfos.add(new PartitionInfo(desc.name(), partInfo.partition(), leader, replicas, isr, offlineReplicas));
        }
      }

      Cluster newCluster = new Cluster(clusterId, nodes, partitionInfos, Collections.emptySet(), Collections.emptySet());
      Cluster previousCluster = _cached != null ? _cached.cluster() : null;
      if (previousCluster != null && MonitorUtils.metadataChanged(previousCluster, newCluster)) {
        _clusterGeneration.incrementAndGet();
      }
      _cached = new ClusterAndGeneration(newCluster, _clusterGeneration.get());
      return _cached;
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (_cached == null) {
          throw new RuntimeException("Failed to refresh cluster metadata and no cached metadata available", e);
        }
        LOG.warn("Interrupted while fetching cluster metadata, using cached cluster metadata", e);
    } catch (ExecutionException e) {
        if (_cached == null) {
          throw new RuntimeException("Failed to refresh cluster metadata and no cached metadata available", e);
        }
        LOG.warn("ExecutionException while fetching cluster metadata, using cached cluster metadata", e);
    }
    return _cached;
  }

  /**
   * Convenience method that fetches cluster metadata without the generation counter.
   *
   * @return the current {@link Cluster}
   */
  public Cluster cluster() {
    return clusterAndGeneration().cluster();
  }

  /**
   * @return The current cluster generation, which increments when the cluster topology changes.
   */
  public int clusterGeneration() {
    return clusterAndGeneration().generation();
  }
}
