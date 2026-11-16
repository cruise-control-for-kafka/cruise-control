/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


/**
 * A compact fingerprint of the replica placement (i.e. the replica set of each partition) of the partitions that an execution
 * does not move. It is persisted with the execution state, so that upon recovery Cruise Control can tell whether the replicas
 * outside the execution were moved, or partitions were created or deleted, while Cruise Control was not running -- in which case
 * the interrupted execution, planned for a different cluster, is not resumed.
 *
 * The fingerprint is an order-independent 64-bit hash over the partitions and their replica sets, along with the number of
 * partitions and the number of replicas on each broker -- the latter two tell what changed, the hash tells whether anything did.
 */
public final class ReplicaPlacementFingerprint {
  private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long FNV_PRIME = 0x100000001b3L;
  private static final long GOLDEN_GAMMA = 0x9e3779b97f4a7c15L;
  // Report at most this many brokers whose number of replicas changed.
  private static final int MAX_BROKERS_TO_REPORT = 10;
  private final long _hash;
  private final int _numPartitions;
  private final SortedMap<Integer, Integer> _numReplicasByBroker;

  public ReplicaPlacementFingerprint(long hash, int numPartitions, Map<Integer, Integer> numReplicasByBroker) {
    _hash = hash;
    _numPartitions = numPartitions;
    _numReplicasByBroker = Collections.unmodifiableSortedMap(new TreeMap<>(numReplicasByBroker));
  }

  /**
   * @param cluster The cluster metadata.
   * @param excludedPartitions Partitions to exclude -- i.e. the partitions of the execution.
   * @return The fingerprint of the replica placement of the partitions in the given cluster, except the excluded ones.
   */
  public static ReplicaPlacementFingerprint of(Cluster cluster, Set<TopicPartition> excludedPartitions) {
    long hash = 0L;
    int numPartitions = 0;
    Map<Integer, Integer> numReplicasByBroker = new TreeMap<>();
    for (String topic : cluster.topics()) {
      long topicHash = hashString(topic);
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        if (excludedPartitions.contains(new TopicPartition(topic, partitionInfo.partition()))) {
          continue;
        }
        int[] replicas = Arrays.stream(partitionInfo.replicas()).mapToInt(Node::id).sorted().toArray();
        long partitionHash = mix(topicHash ^ mix(partitionInfo.partition() + GOLDEN_GAMMA));
        for (int replica : replicas) {
          partitionHash = mix(partitionHash ^ (replica + GOLDEN_GAMMA));
          numReplicasByBroker.merge(replica, 1, Integer::sum);
        }
        // A sum is independent of the order in which the partitions are visited.
        hash += partitionHash;
        numPartitions++;
      }
    }
    return new ReplicaPlacementFingerprint(hash, numPartitions, numReplicasByBroker);
  }

  public long hash() {
    return _hash;
  }

  public int numPartitions() {
    return _numPartitions;
  }

  public SortedMap<Integer, Integer> numReplicasByBroker() {
    return _numReplicasByBroker;
  }

  /**
   * @param current The fingerprint of the current replica placement.
   * @return Human-readable differences from the given fingerprint (e.g. "broker 1: 812 -> 905 replicas"), or an empty list if
   * the replica placement has not changed.
   */
  public List<String> differencesTo(ReplicaPlacementFingerprint current) {
    List<String> differences = new ArrayList<>();
    if (equals(current)) {
      return differences;
    }
    if (_numPartitions != current._numPartitions) {
      differences.add(String.format("partitions outside the execution: %d -> %d", _numPartitions, current._numPartitions));
    }
    Set<Integer> brokers = new TreeSet<>(_numReplicasByBroker.keySet());
    brokers.addAll(current._numReplicasByBroker.keySet());
    int numChangedBrokers = 0;
    for (int broker : brokers) {
      int before = _numReplicasByBroker.getOrDefault(broker, 0);
      int after = current._numReplicasByBroker.getOrDefault(broker, 0);
      if (before != after && numChangedBrokers++ < MAX_BROKERS_TO_REPORT) {
        differences.add(String.format("broker %d: %d -> %d replicas outside the execution", broker, before, after));
      }
    }
    if (numChangedBrokers > MAX_BROKERS_TO_REPORT) {
      differences.add(String.format("... and %d more brokers", numChangedBrokers - MAX_BROKERS_TO_REPORT));
    }
    if (differences.isEmpty()) {
      differences.add("replicas outside the execution were moved between brokers");
    }
    return differences;
  }

  /**
   * A 64-bit FNV-1a hash of the given string.
   *
   * @param value String to hash.
   * @return The hash of the given string.
   */
  public static long hashString(String value) {
    long hash = FNV_OFFSET_BASIS;
    for (int i = 0; i < value.length(); i++) {
      hash ^= value.charAt(i);
      hash *= FNV_PRIME;
    }
    return hash;
  }

  /**
   * The finalizer of MurmurHash3 -- spreads the bits of the given value.
   *
   * @param value Value to mix.
   * @return The mixed value.
   */
  public static long mix(long value) {
    long x = value;
    x ^= x >>> 33;
    x *= 0xff51afd7ed558ccdL;
    x ^= x >>> 33;
    x *= 0xc4ceb9fe1a85ec53L;
    x ^= x >>> 33;
    return x;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReplicaPlacementFingerprint)) {
      return false;
    }
    ReplicaPlacementFingerprint that = (ReplicaPlacementFingerprint) o;
    return _hash == that._hash && _numPartitions == that._numPartitions && _numReplicasByBroker.equals(that._numReplicasByBroker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_hash, _numPartitions, _numReplicasByBroker);
  }

  @Override
  public String toString() {
    return String.format("{hash: %d, partitions: %d, replicasByBroker: %s}", _hash, _numPartitions, _numReplicasByBroker);
  }
}
