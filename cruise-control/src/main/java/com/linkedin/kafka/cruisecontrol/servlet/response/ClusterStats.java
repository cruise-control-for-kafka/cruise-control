/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import java.util.HashMap;
import java.util.Map;


/**
 * A class to keep selected stats concerning brokers, topics, replicas, and leaders -- per broker topic stats are
 * omitted for simplicity.
 */
@JsonResponseClass
public class ClusterStats {
  @JsonResponseField
  public static final String BROKERS = "Brokers";
  @JsonResponseField
  public static final String TOPICS = "Topics";
  @JsonResponseField
  public static final String REPLICAS = "Replicas";
  @JsonResponseField
  public static final String LEADERS = "Leaders";
  @JsonResponseField
  public static final String AVG_REPLICATION_FACTOR = "AvgReplicationFactor";
  @JsonResponseField
  public static final String AVG_REPLICAS_PER_BROKER = "AvgReplicasPerBroker";
  @JsonResponseField
  public static final String AVG_LEADERS_PER_BROKER = "AvgLeadersPerBroker";
  @JsonResponseField
  public static final String MAX_REPLICAS_PER_BROKER = "MaxReplicasPerBroker";
  @JsonResponseField
  public static final String MAX_LEADERS_PER_BROKER = "MaxLeadersPerBroker";
  @JsonResponseField
  public static final String STD_REPLICAS_PER_BROKER = "StdReplicasPerBroker";
  @JsonResponseField
  public static final String STD_LEADERS_PER_BROKER = "StdLeadersPerBroker";
  protected final int _numBrokers;
  protected final int _numTopics;
  protected final int _numReplicas;
  protected final int _numLeaders;
  protected final double _avgReplicationFactor;
  protected final double _avgReplicasPerBroker;
  protected final double _avgLeadersPerBroker;
  protected final int _maxReplicasPerBroker;
  protected final int _maxLeadersPerBroker;
  protected final double _stdReplicasPerBroker;
  protected final double _stdLeadersPerBroker;

  public ClusterStats(int numTopics, Map<Integer, Integer> replicaCountByBrokerId, Map<Integer, Integer> leaderCountByBrokerId) {
    _numBrokers = replicaCountByBrokerId.size();
    _numTopics = numTopics;
    _numReplicas = replicaCountByBrokerId.values().stream().mapToInt(i -> i).sum();
    _numLeaders = leaderCountByBrokerId.values().stream().mapToInt(i -> i).sum();
    _avgReplicationFactor = _numLeaders == 0 ? 0 : _numReplicas / (double) _numLeaders;

    // Replicas per broker
    _avgReplicasPerBroker = _numReplicas / (double) _numBrokers;
    int maxReplicasPerBroker = 0;
    double replicaVariance = 0.0;
    for (int replicasInBroker : replicaCountByBrokerId.values()) {
      maxReplicasPerBroker = Math.max(maxReplicasPerBroker, replicasInBroker);
      replicaVariance += Math.pow((double) replicasInBroker - _avgReplicasPerBroker, 2) / _numBrokers;
    }
    _maxReplicasPerBroker = maxReplicasPerBroker;
    _stdReplicasPerBroker = Math.sqrt(replicaVariance);

    // Leaders per broker
    _avgLeadersPerBroker = _numLeaders / (double) _numBrokers;
    int maxLeadersPerBroker = 0;
    double leaderVariance = 0.0;
    for (int leadersInBroker : leaderCountByBrokerId.values()) {
      maxLeadersPerBroker = Math.max(maxLeadersPerBroker, leadersInBroker);
      leaderVariance += Math.pow((double) leadersInBroker - _avgLeadersPerBroker, 2) / _numBrokers;
    }
    _maxLeadersPerBroker = maxLeadersPerBroker;
    _stdLeadersPerBroker = Math.sqrt(leaderVariance);
  }

  /**
   * @return response with JSON structure.
   */
  public Map<String, Object> getJsonStructure() {
    // Write the cluster summary.
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(BROKERS, _numBrokers);
    jsonMap.put(TOPICS, _numTopics);
    jsonMap.put(REPLICAS, _numReplicas);
    jsonMap.put(LEADERS, _numLeaders);
    jsonMap.put(AVG_REPLICATION_FACTOR, _avgReplicationFactor);
    jsonMap.put(AVG_REPLICAS_PER_BROKER, _avgReplicasPerBroker);
    jsonMap.put(AVG_LEADERS_PER_BROKER, _avgLeadersPerBroker);
    jsonMap.put(MAX_REPLICAS_PER_BROKER, _maxReplicasPerBroker);
    jsonMap.put(MAX_LEADERS_PER_BROKER, _maxLeadersPerBroker);
    jsonMap.put(STD_REPLICAS_PER_BROKER, _stdReplicasPerBroker);
    jsonMap.put(STD_LEADERS_PER_BROKER, _stdLeadersPerBroker);
    return jsonMap;
  }

  /**
   * Write cluster stats
   * @param sb String builder to write the response to.
   */
  public void writeClusterStats(StringBuilder sb) {
    sb.append(String.format("Summary: The cluster has %d brokers, %d replicas, %d leaders, and %d topics with avg replication factor: %.2f."
                            + " [Leaders/Replicas per broker] avg: %.2f/%.2f max: %d/%d std: %.2f/%.2f%n%n",
                            _numBrokers, _numReplicas, _numLeaders, _numTopics, _avgReplicationFactor, _avgLeadersPerBroker,
                            _avgReplicasPerBroker, _maxLeadersPerBroker, _maxReplicasPerBroker, _stdLeadersPerBroker, _stdReplicasPerBroker));
  }
}
