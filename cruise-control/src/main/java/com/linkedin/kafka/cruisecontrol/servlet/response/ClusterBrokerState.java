/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.BrokerSetAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolutionHelper;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;

@JsonResponseClass
public class ClusterBrokerState {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterBrokerState.class);
  @JsonResponseField
  public static final String LEADER_COUNT = "LeaderCountByBrokerId";
  @JsonResponseField
  public static final String OUT_OF_SYNC_COUNT = "OutOfSyncCountByBrokerId";
  @JsonResponseField
  public static final String REPLICA_COUNT = "ReplicaCountByBrokerId";
  @JsonResponseField
  public static final String OFFLINE_REPLICA_COUNT = "OfflineReplicaCountByBrokerId";
  @JsonResponseField
  public static final String IS_CONTROLLER = "IsController";
  @JsonResponseField
  public static final String ONLINE_LOGDIRS = "OnlineLogDirsByBrokerId";
  @JsonResponseField
  public static final String OFFLINE_LOGDIRS = "OfflineLogDirsByBrokerId";
  @JsonResponseField
  public static final String SUMMARY = "Summary";
  @JsonResponseField
  public static final String BROKER_SET = "BrokerSetByBrokerId";
  public static final String TIMED_OUT_LOGDIR_FLAG = "timed_out";
  protected final Map<Integer, Integer> _leaderCountByBrokerId;
  protected final Map<Integer, Integer> _outOfSyncCountByBrokerId;
  protected final Map<Integer, Integer> _replicaCountByBrokerId;
  protected final Map<Integer, Integer> _offlineReplicaCountByBrokerId;
  protected final Map<Integer, Boolean> _isControllerByBrokerId;
  protected final Map<Integer, Set<String>> _onlineLogDirsByBrokerId;
  protected final Map<Integer, Set<String>> _offlineLogDirsByBrokerId;
  protected final Cluster _kafkaCluster;
  protected final AdminClient _adminClient;
  protected final KafkaCruiseControlConfig _config;
  protected Map<Integer, String> _brokerSetIdByBrokerId;

  public ClusterBrokerState(Cluster kafkaCluster, AdminClient adminClient, KafkaCruiseControlConfig config, BrokerSetResolver brokerSetResolver)
      throws ExecutionException, InterruptedException {
    _kafkaCluster = kafkaCluster;
    _adminClient = adminClient;
    _config = config;
    _leaderCountByBrokerId = new TreeMap<>();
    _outOfSyncCountByBrokerId = new TreeMap<>();
    _replicaCountByBrokerId = new TreeMap<>();
    _offlineReplicaCountByBrokerId = new TreeMap<>();
    _isControllerByBrokerId = new TreeMap<>();
    // Gather the broker state.
    populateKafkaBrokerState(_leaderCountByBrokerId,
                             _outOfSyncCountByBrokerId,
                             _replicaCountByBrokerId,
                             _offlineReplicaCountByBrokerId,
                             _isControllerByBrokerId);

    _onlineLogDirsByBrokerId = new TreeMap<>();
    _offlineLogDirsByBrokerId = new TreeMap<>();

    _brokerSetIdByBrokerId = new HashMap<>();

    // Only resolve broker-set if the default goals contain the BrokerSetAwareGoal
    List<String> defaultGoals = config.getList(AnalyzerConfig.DEFAULT_GOALS_CONFIG);
    if (defaultGoals.contains(BrokerSetAwareGoal.class.getName())) {
      try {
        BrokerSetResolutionHelper brokerSetResolutionHelper = new BrokerSetResolutionHelper(kafkaCluster, brokerSetResolver);
        _brokerSetIdByBrokerId.putAll(brokerSetResolutionHelper.brokerSetIdByBrokerId());
      } catch (BrokerSetResolutionException e) {
        LOG.error("Failed to resolve broker set with exception: ", e);
      }
    }

    // Broker LogDirs Summary
    populateKafkaBrokerLogDirState(_onlineLogDirsByBrokerId, _offlineLogDirsByBrokerId, _replicaCountByBrokerId.keySet());
  }

  /**
   * @return response with JSON structure.
   */
  public Map<String, Object> getJsonStructure() {
    return Map.of(LEADER_COUNT, _leaderCountByBrokerId, OUT_OF_SYNC_COUNT, _outOfSyncCountByBrokerId,
                  REPLICA_COUNT, _replicaCountByBrokerId, OFFLINE_REPLICA_COUNT, _offlineReplicaCountByBrokerId,
                  IS_CONTROLLER, _isControllerByBrokerId, ONLINE_LOGDIRS, _onlineLogDirsByBrokerId, OFFLINE_LOGDIRS, _offlineLogDirsByBrokerId,
                  SUMMARY, new ClusterStats(_kafkaCluster.topics().size(), _replicaCountByBrokerId, _leaderCountByBrokerId).getJsonStructure(),
                  BROKER_SET, _brokerSetIdByBrokerId);
  }

  /**
   * Gather the Kafka broker state within the given under leader, out-of-sync, and replica counts.
   *
   * @param leaderCountByBrokerId Leader count by broker id.
   * @param outOfSyncCountByBrokerId Out of sync replica count by broker id.
   * @param replicaCountByBrokerId Replica count by broker id.
   * @param offlineReplicaCountByBrokerId Offline replica count by broker id.
   * @param isControllerByBrokerId Controller information by broker id.
   */
  protected void populateKafkaBrokerState(Map<Integer, Integer> leaderCountByBrokerId,
                                          Map<Integer, Integer> outOfSyncCountByBrokerId,
                                          Map<Integer, Integer> replicaCountByBrokerId,
                                          Map<Integer, Integer> offlineReplicaCountByBrokerId,
                                          Map<Integer, Boolean> isControllerByBrokerId) {
    // Part-1: Gather the states of brokers with replicas.
    for (String topic : _kafkaCluster.topics()) {
      for (PartitionInfo partitionInfo : _kafkaCluster.partitionsForTopic(topic)) {
        if (partitionInfo.leader() == null) {
          continue;
        }
        leaderCountByBrokerId.merge(partitionInfo.leader().id(), 1, Integer::sum);

        Set<Integer> replicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toSet());
        Set<Integer> inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toSet());
        Set<Integer> outOfSyncReplicas = new HashSet<>(replicas);
        outOfSyncReplicas.removeAll(inSyncReplicas);
        Set<Integer> offlineReplicas = Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());

        outOfSyncReplicas.forEach(brokerId -> outOfSyncCountByBrokerId.merge(brokerId, 1, Integer::sum));
        offlineReplicas.forEach(brokerId -> offlineReplicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
        replicas.forEach(brokerId -> replicaCountByBrokerId.merge(brokerId, 1, Integer::sum));
      }
    }
    // Part-2: Gather the states of brokers without replicas.
    for (Node node : _kafkaCluster.nodes()) {
      int nodeId = node.id();
      if (replicaCountByBrokerId.get(nodeId) == null) {
        offlineReplicaCountByBrokerId.put(nodeId, 0);
        replicaCountByBrokerId.put(nodeId, 0);
        outOfSyncCountByBrokerId.put(nodeId, 0);
        leaderCountByBrokerId.put(nodeId, 0);
      }
    }
    // Part-3: Gather controller information.
    replicaCountByBrokerId.keySet().forEach(brokerId -> isControllerByBrokerId.put(brokerId, false));
    Node controller = _kafkaCluster.controller();
    if (controller != null) {
      isControllerByBrokerId.put(controller.id(), true);
    }
  }

  protected void populateKafkaBrokerLogDirState(Map<Integer, Set<String>> onlineLogDirsByBrokerId,
                                                Map<Integer, Set<String>> offlineLogDirsByBrokerId,
                                                Set<Integer> brokers)
      throws ExecutionException, InterruptedException {
    // If the broker does not show up in latest metadata, the broker is dead.
    Set<Integer> aliveBrokers = new HashSet<>();
    _kafkaCluster.nodes().forEach(node -> aliveBrokers.add(node.id()));
    for (Integer broker: brokers) {
      if (!aliveBrokers.contains(broker)) {
        onlineLogDirsByBrokerId.put(broker, Collections.singleton("broker_dead"));
        offlineLogDirsByBrokerId.put(broker, Collections.singleton("broker_dead"));
      }
    }

    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> logDirsByBrokerId
        = _adminClient.describeLogDirs(aliveBrokers).descriptions();
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : logDirsByBrokerId.entrySet()) {
      onlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());
      offlineLogDirsByBrokerId.put(entry.getKey(), new HashSet<>());

      try {
        entry.getValue().get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((key, value) -> {
          if (value.error() == null) {
            onlineLogDirsByBrokerId.get(entry.getKey()).add(key);
          } else {
            offlineLogDirsByBrokerId.get(entry.getKey()).add(key);
          }
        });
      } catch (TimeoutException te) {
        LOG.error("Getting log dir information for broker {} timed out.", entry.getKey());
        onlineLogDirsByBrokerId.get(entry.getKey()).add(TIMED_OUT_LOGDIR_FLAG);
        offlineLogDirsByBrokerId.get(entry.getKey()).add(TIMED_OUT_LOGDIR_FLAG);
      }
    }
  }

  /**
   * Write broker summary
   * @param sb String builder to write the response to.
   */
  public void writeBrokerSummary(StringBuilder sb) {
    // Overall Summary / Stats
    new ClusterStats(_kafkaCluster.topics().size(), _replicaCountByBrokerId, _leaderCountByBrokerId).writeClusterStats(sb);
    // Broker Summary
    String initMessage = "Brokers:";
    sb.append(String.format("%s%n%20s%20s%20s%20s%20s%20s%20s%n", initMessage, "BROKER", "LEADER(S)", "REPLICAS", "OUT-OF-SYNC",
                            "OFFLINE", "IS_CONTROLLER", "BROKER_SET"));

    for (Integer brokerId : _replicaCountByBrokerId.keySet()) {
      sb.append(String.format("%20d%20d%20d%20d%20d%20s%20s%n",
                              brokerId,
                              _leaderCountByBrokerId.getOrDefault(brokerId, 0),
                              _replicaCountByBrokerId.getOrDefault(brokerId, 0),
                              _outOfSyncCountByBrokerId.getOrDefault(brokerId, 0),
                              _offlineReplicaCountByBrokerId.getOrDefault(brokerId, 0),
                              _isControllerByBrokerId.get(brokerId),
                              _brokerSetIdByBrokerId.get(brokerId)));
    }
    // Broker LogDirs Summary
    writeKafkaBrokerLogDirState(sb, _replicaCountByBrokerId.keySet());
  }

  protected void writeKafkaBrokerLogDirState(StringBuilder sb, Set<Integer> brokers) {
    int onlineLogDirsNameLength = _onlineLogDirsByBrokerId.values().stream().mapToInt(dir -> dir.toString().length())
                                                          .max().orElse(20) + 14;
    int offlineLogDirsNameLength = _offlineLogDirsByBrokerId.values().stream().mapToInt(dir -> dir.toString().length())
                                                            .max().orElse(20) + 15;

    String initMessage = "LogDirs of brokers with replicas:";
    sb.append(String.format("%n%s%n%20s%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                            initMessage, "BROKER", "ONLINE-LOGDIRS", "OFFLINE-LOGDIRS"));

    for (int brokerId : brokers) {
      sb.append(String.format("%20d%" + onlineLogDirsNameLength + "s%" + offlineLogDirsNameLength + "s%n",
                              brokerId,
                              _onlineLogDirsByBrokerId.get(brokerId).toString(),
                              _offlineLogDirsByBrokerId.get(brokerId).toString()));
    }
  }
}
