/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * A class that holds the information of the broker, including its liveness and load for replicas. A broker object is
 * created as part of a rack structure.
 */
public class Broker implements Serializable, Comparable<Broker> {
  private static final double DEAD_BROKER_CAPACITY = -1.0;

  public enum State {
    ALIVE, DEAD, NEW, DEMOTED, BAD_DISKS
  }

  private final int _id;
  private final Host _host;
  private final double[] _brokerCapacity;
  private final Set<Replica> _replicas;
  private final Set<Replica> _leaderReplicas;
  /** A map of cached sorted replicas using different user defined score functions. */
  private final Map<String, SortedReplicas> _sortedReplicas;
  /** Set of immigrant replicas */
  private final Set<Replica> _immigrantReplicas;
  /** Set of offline replicas on broker */
  private final Set<Replica> _currentOfflineReplicas;
  /** A map for tracking topic -&gt; (partitionId -&gt; replica). */
  private final Map<String, Map<Integer, Replica>> _topicReplicas;
  private final Load _load;
  private final Load _leadershipLoadForNwResources;
  private final SortedMap<String, Disk> _diskByLogdir;
  private State _state;

  /**
   * Constructor for Broker class.
   *
   * @param host           The host this broker is on
   * @param id             The id of the broker.
   * @param brokerCapacityInfo Capacity information of the created broker.
   * @param populateReplicaPlacementInfo Whether populate replica placement over disk information or not.
   */
  Broker(Host host, int id, BrokerCapacityInfo brokerCapacityInfo, boolean populateReplicaPlacementInfo) {
    Map<Resource, Double> brokerCapacity = validateNotNull(brokerCapacityInfo.capacity(),
            () -> "Attempt to create broker " + id + " on host " + host.name() + " with null capacity.");
    _host = host;
    _id = id;
    _brokerCapacity = new double[Resource.cachedValues().size()];
    for (Map.Entry<Resource, Double> entry : brokerCapacity.entrySet()) {
      Resource resource = entry.getKey();
      _brokerCapacity[resource.id()] = (resource == Resource.CPU) ? (entry.getValue() * brokerCapacityInfo.numCpuCores())
                                                                  : entry.getValue();
    }

    if (populateReplicaPlacementInfo) {
      _diskByLogdir = new TreeMap<>();
      brokerCapacityInfo.diskCapacityByLogDir().forEach((key, value) -> _diskByLogdir.put(key, new Disk(key, this, value)));
    } else {
      _diskByLogdir = Collections.emptySortedMap();
    }

    _replicas = new HashSet<>();
    _leaderReplicas = new HashSet<>();
    _topicReplicas = new HashMap<>();
    _sortedReplicas = new HashMap<>();
    _immigrantReplicas = new HashSet<>();
    _currentOfflineReplicas = new HashSet<>();
    // Initially broker does not contain any load.
    _load = new Load();
    _leadershipLoadForNwResources = new Load();
    _state = State.ALIVE;
  }

  public Host host() {
    return _host;
  }

  public State state() {
    return _state;
  }

  /**
   * @return Rack of the broker.
   */
  public Rack rack() {
    return _host.rack();
  }

  /**
   * @return Broker Id.
   */
  public int id() {
    return _id;
  }

  /**
   * Get broker capacity for the requested resource.
   *
   * @param resource Resource for which the capacity will be provided.
   * @return If broker is alive, the capacity of the requested resource, {@link #DEAD_BROKER_CAPACITY} otherwise.
   */
  public double capacityFor(Resource resource) {
      return _brokerCapacity[resource.id()];
  }

  /**
   * @return Replicas residing in the broker.
   */
  public Set<Replica> replicas() {
    return Collections.unmodifiableSet(_replicas);
  }

  /**
   * @return All the leader replicas.
   */
  public Set<Replica> leaderReplicas() {
    return Collections.unmodifiableSet(_leaderReplicas);
  }

  /**
   * @return The immigrant replicas (The replicas that have been moved here).
   */
  public Set<Replica> immigrantReplicas() {
    return Collections.unmodifiableSet(_immigrantReplicas);
  }

  /**
   * @return Current offline replicas -- i.e. replicas (1) whose current broker is this broker, and (2) are offline.
   */
  public Set<Replica> currentOfflineReplicas() {
    return _currentOfflineReplicas;
  }

  /**
   * Get the replica if it is in the broker.
   *
   * @param tp Topic partition of the replica.
   * @return Replica if it exists in the broker, null otherwise.
   */
  public Replica replica(TopicPartition tp) {
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(tp.topic());
    if (topicReplicas == null) {
      return null;
    }

    return topicReplicas.get(tp.partition());
  }

  /**
   * Get replicas for topic.
   *
   * @param topic Topic of the requested replicas.
   * @return Replicas in this broker sharing the given topic.
   */
  public Collection<Replica> replicasOfTopicInBroker(String topic) {
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(topic);

    return topicReplicas == null ? Collections.emptySet() : topicReplicas.values();
  }

  /**
   * Get number of replicas from the given topic in this broker.
   *
   * @param topic Topic for which both the leader and follower replica count will be returned.
   * @return The number of replicas from the given topic in this broker.
   */
  public int numReplicasOfTopicInBroker(String topic) {
    Map<Integer, Replica> topicReplicas = _topicReplicas.get(topic);
    return topicReplicas == null ? 0 : topicReplicas.size();
  }

  /**
   * Get number of only leader replicas from the given topic in this broker.
   *
   * @param topicName Topic for which the replica count will be returned.
   * @return The number of leader replicas from the given topic in this broker.
   */
  public int numLeadersFor(String topicName) {
    return (int) replicasOfTopicInBroker(topicName).stream().filter(Replica::isLeader).count();
  }

  /**
   * @return {@code true} if the broker is not dead, {@code false} otherwise.
   */
  public boolean isAlive() {
    return _state != State.DEAD;
  }

  /**
   * @return {@code true} if the broker is using JBOD, {@code false} otherwise
   */
  public boolean isUsingJBOD() {
    return !_diskByLogdir.isEmpty();
  }

  /**
   * @return {@code true} if the broker is a new broker, {@code false} otherwise.
   */
  public boolean isNew() {
    return _state == State.NEW;
  }

  /**
   * @return {@code true} if the broker has been demoted, {@code false} otherwise.
   */
  public boolean isDemoted() {
    return _state == State.DEMOTED;
  }

  /**
   * Check if the broker has bad disks (i.e. is being fixed by removing offline replicas from it).
   * Note that contrary to {@link State#DEAD}, a {@link State#BAD_DISKS} broker might receive replicas from other
   * brokers during a rebalance.
   *
   * @return {@code true} if the broker has bad disks, {@code false} otherwise.
   */
  public boolean hasBadDisks() {
    return _state == State.BAD_DISKS;
  }

  /**
   * @return The broker load of the broker.
   */
  public Load load() {
    return _load;
  }

  /**
   * @return The load for the replicas for which this broker is a leader. This is meaningful for network bytes in, and
   * network bytes out but not meaningful for the other resources.
   */
  public Load leadershipLoadForNwResources() {
    return _leadershipLoadForNwResources;
  }

  /**
   * @return The set of topics in the broker.
   */
  public Set<String> topics() {
    return _topicReplicas.keySet();
  }

  /**
   * Get the tracked sorted replicas using the given sort name.
   *
   * @param sortName the sort name.
   * @return The {@link SortedReplicas} for the given sort name.
   */
  public SortedReplicas trackedSortedReplicas(String sortName) {
    SortedReplicas sortedReplicas = _sortedReplicas.get(sortName);
    if (sortedReplicas == null) {
      throw new IllegalStateException("The sort name " + sortName + "  is not found. Make sure trackSortedReplicas() "
                                      + "has been called for the sort name");
    }
    return sortedReplicas;
  }

  /**
   * Get a comparator for the replicas in the broker. The comparisons performed are:
   * 1. offline replicas have higher priority, i.e. comes before the immigrant and native replicas.
   * 2. immigrant replicas have higher priority compared to the native replicas.
   * 3. sort by partition id.
   *
   * @return A Comparator to compare the replicas for the given topic.
   */
  public Comparator<Replica> replicaComparator() {
    return (r1, r2) -> {
      boolean isR1Offline = _currentOfflineReplicas.contains(r1);
      boolean isR2Offline = _currentOfflineReplicas.contains(r2);

      if (isR1Offline && !isR2Offline) {
        return -1;
      } else if (!isR1Offline && isR2Offline) {
        return 1;
      } else {
        boolean isR1Immigrant = _immigrantReplicas.contains(r1);
        boolean isR2Immigrant = _immigrantReplicas.contains(r2);
        int result = (isR1Immigrant && !isR2Immigrant) ? -1 : ((!isR1Immigrant && isR2Immigrant) ? 1 : 0);

        if (result == 0) {
          if (r1.topicPartition().partition() > r2.topicPartition().partition()) {
            return 1;
          } else if (r1.topicPartition().partition() < r2.topicPartition().partition()) {
            return -1;
          }
        }

        return result;
      }
    };
  }

  /**
   * Set broker alive status. If the broker is not alive, add all of its replicas to current offline replicas.
   *
   * @param newState The new state of the broker.
   */
  void setState(State newState) {
    _state = newState;
    if (!isAlive()) {
      _currentOfflineReplicas.addAll(replicas());
      _diskByLogdir.values().forEach(d -> d.setState(Disk.State.DEAD));
      Resource.cachedValues().forEach(r -> _brokerCapacity[r.id()] = DEAD_BROKER_CAPACITY);
    }
  }

  /**
   * Add replica to the broker.
   *
   * @param replica Replica to be added to the current broker.
   */
  void addReplica(Replica replica) {
    // Add replica to list of all replicas in the broker.
    if (_replicas.contains(replica)) {
      throw new IllegalStateException(String.format("Broker %d already has replica %s", _id, replica.topicPartition()));
    }
    _replicas.add(replica);

    if (replica.originalBroker().id() != _id) {
      _immigrantReplicas.add(replica);
    } else if (replica.isOriginalOffline()) {
      // Current broker is the original broker and the replica resides on an offline disk.
      _currentOfflineReplicas.add(replica);
    }

    // Add topic replica.
    _topicReplicas.computeIfAbsent(replica.topicPartition().topic(), t -> new HashMap<>())
                  .put(replica.topicPartition().partition(), replica);

    // Add leader replica.
    if (replica.isLeader()) {
      _leadershipLoadForNwResources.addLoad(replica.load());
      _leaderReplicas.add(replica);
    }

    // Add replica load to the broker load.
    _load.addLoad(replica.load());
    _sortedReplicas.values().forEach(sr -> sr.add(replica));

    if (replica.disk() != null) {
      _diskByLogdir.get(replica.disk().logDir()).addReplica(replica);
    }
  }

  /**
   * Add a dead disk to the broker.
   * This is used in cluster model initialization. If a disk is dead, the
   * {@link com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver} may not report the disk information,
   * later populating replicas to cluster model will get some offline replicas' disk not found.
   *
   * @param logdir Logdir of the dead disk to be added to the current broker.
   * @return The dead disk that was added to this broker.
   */
  Disk addDeadDisk(String logdir) {
    Disk disk = new Disk(logdir, this, -1);
    _diskByLogdir.put(logdir, disk);
    return disk;
  }

  /**
   * Track the sorted replicas using the given selection/priority/score functions.
   * Selection functions determine whether a replica should be included or not, only replica satisfies all selection functions
   * will be included.
   * Then sort replicas first with priority functions, then with score function (i.e. priority functions are first applied one by one
   * until two replicas are of different priority regards to the current priority function; if all priority are applied and the
   * replicas are unable to be sorted, the score function will be used and replicas will be sorted in ascending order of score).
   * The priority functions are useful to priorities particular types of replicas, e.g leader replicas, immigrant replicas, etc.
   *
   * @param sortName the name of the tracked sorted replicas.
   * @param selectionFuncs A set of selection functions to decide which replica to include in the sort. If it is {@code null}
   *                      or empty, all the replicas are to be included.
   * @param priorityFuncs A list of priority functions to sort the replicas.
   * @param scoreFunc the score function to sort the replicas with the same priority, replicas are sorted in ascending
   *                  order of score.
   */
  void trackSortedReplicas(String sortName,
                           Set<Function<Replica, Boolean>> selectionFuncs,
                           List<Function<Replica, Integer>> priorityFuncs,
                           Function<Replica, Double> scoreFunc) {
    _sortedReplicas.putIfAbsent(sortName, new SortedReplicas(this, selectionFuncs, priorityFuncs, scoreFunc));
    for (Disk disk : _diskByLogdir.values()) {
      disk.trackSortedReplicas(sortName, selectionFuncs, priorityFuncs, scoreFunc);
    }
  }

  /**
   * Untrack the sorted replicas for the given sort name. This helps release memory.
   *
   * @param sortName the name of the tracked sorted replicas.
   */
  public void untrackSortedReplicas(String sortName) {
    _sortedReplicas.remove(sortName);
    for (Disk disk : _diskByLogdir.values()) {
      disk.untrackSortedReplicas(sortName);
    }
  }

  /**
   * Clear all cached sorted replicas. This helps release memory.
   */
  public void clearSortedReplicas() {
    _sortedReplicas.clear();
    for (Disk disk : _diskByLogdir.values()) {
      disk.clearSortedReplicas();
    }
  }

  private void removeSortedReplicas(Replica replica) {
    _sortedReplicas.values().forEach(sr -> {
      sr.remove(replica);
    });
  }

  private void addSortedReplicas(Replica replica) {
    _sortedReplicas.values().forEach(sr -> {
      sr.add(replica);
    });
  }

  /**
   * (1) Make the replica with the given topic partition and brokerId a follower.
   * (2) Remove and get the outbound network load associated with leadership from the given replica.
   * (3) Remove and get the CPU load associated with leadership from the given replica.
   *
   * @param tp TopicPartition of the replica for which the outbound network load will be removed.
   * @return Leadership load by snapshot time.
   */
  AggregatedMetricValues makeFollower(TopicPartition tp) {
    Replica replica = replica(tp);
    _leadershipLoadForNwResources.subtractLoad(replica.load());
    // Remove once before load change for replica
    removeSortedReplicas(replica);

    AggregatedMetricValues leadershipLoadDelta = replica.makeFollower();
    // Remove leadership load from load.
    _load.subtractLoad(leadershipLoadDelta);
    _leaderReplicas.remove(replica);
    addSortedReplicas(replica);
    return leadershipLoadDelta;
  }

  /**
   * (1) Make the replica with the given topic partition and brokerId the leader.
   * (2) Add the outbound network load associated with leadership to the given replica.
   * (3) Add the CPU load associated with leadership.
   *
   * @param tp TopicPartition of the replica for which the outbound network load will be added.
   * @param leadershipLoadDelta Resource to leadership load to be added by snapshot time.
   */
  void makeLeader(TopicPartition tp, AggregatedMetricValues leadershipLoadDelta) {
    Replica replica = replica(tp);
    // Remove once before load change for replica
    removeSortedReplicas(replica);
    replica.makeLeader(leadershipLoadDelta);
    _leadershipLoadForNwResources.addLoad(replica.load());
    // Add leadership load to load.
    _load.addLoad(leadershipLoadDelta);
    _leaderReplicas.add(replica);
    addSortedReplicas(replica);
  }

  /**
   * Get the removed replica from the broker.
   *
   * @param tp Topic partition of the replica to be removed from replicas in the current broker.
   * @return The removed replica or null if the topic partition is not present.
   */
  Replica removeReplica(TopicPartition tp) {
    // Find the index of the replica with the given replica ID and topic name.
    Replica removedReplica = replica(tp);
    if (removedReplica != null) {
      // Remove the replica from the list of replicas.
      _replicas.remove(removedReplica);
      // Remove the load of the removed replica from the load of the broker.
      _load.subtractLoad(removedReplica.load());

      // Remove topic replica.
      Map<Integer, Replica> topicReplicas = _topicReplicas.get(tp.topic());
      if (topicReplicas != null) {
        topicReplicas.remove(tp.partition());
      }
      if (removedReplica.isLeader()) {
        _leadershipLoadForNwResources.subtractLoad(removedReplica.load());
        _leaderReplicas.remove(removedReplica);
      }
      _immigrantReplicas.remove(removedReplica);
      _currentOfflineReplicas.remove(removedReplica);
      _sortedReplicas.values().forEach(sr -> sr.remove(removedReplica));
    }

    return removedReplica;
  }

  /**
   * Move replica between the disks of the broker.
   *
   * @param tp                Topic partition of the replica to be moved.
   * @param sourceLogdir      Log directory of the source disk.
   * @param destinationLogdir Log directory of the destination disk.
   */
  void moveReplicaBetweenDisks(TopicPartition tp, String sourceLogdir, String destinationLogdir) {
    Replica replica = replica(tp);
    _diskByLogdir.get(sourceLogdir).removeReplica(replica);
    _diskByLogdir.get(destinationLogdir).addReplica(replica);
  }

  /**
   * Set the disk state to dead.
   *
   * @param logdir Log directory of the disk.
   * @return Disk capacity due to disk death.
   */
  double markDiskDead(String logdir) {
    Disk disk = _diskByLogdir.get(logdir);
    double diskCapacity = disk.capacity();
    _brokerCapacity[Resource.DISK.id()] -= diskCapacity;
    disk.setState(Disk.State.DEAD);
    disk.replicas().forEach(Replica::markOriginalOffline);
    return diskCapacity;
  }

  /**
   * Clear the content of monitoring data at each replica in the broker.
   */
  void clearLoad() {
    _replicas.forEach(Replica::clearLoad);
  }

  /**
   * Clear all replicas in the broker.
   */
  void clearReplicas() {
    _replicas.clear();
    _leaderReplicas.clear();
    _topicReplicas.clear();
    _immigrantReplicas.clear();
    _currentOfflineReplicas.clear();
    _load.clearLoad();
    _leadershipLoadForNwResources.clearLoad();
  }

  /**
   * Set the load of the replicas. The load will be added to the broker load. Note that this method should only
   * be called once for each replica.
   *
   * @param tp Topic partition that identifies the replica in this broker.
   * @param aggregatedMetricValues The metric values of this topic partition.
   * @param windows The windows list of the aggregated metric values.
   */
  void setReplicaLoad(TopicPartition tp, AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
    Replica replica = replica(tp);
    replica.setMetricValues(aggregatedMetricValues, windows);
    if (replica.disk() != null) {
      replica.disk().addReplicaLoad(replica);
    }
    if (replica.isLeader()) {
      _leadershipLoadForNwResources.addMetricValues(aggregatedMetricValues, windows);
    }
    _load.addMetricValues(aggregatedMetricValues, windows);
  }

  /**
   * Get disk information that corresponds to the logdir.
   *
   * @param logdir The logdir of the disk to query.
   * @return Disk information.
   */
  public Disk disk(String logdir) {
    return _diskByLogdir.get(logdir);
  }

  /**
   * Get all the disks of the broker.
   *
   * @return Collection of disk.
   */
  public Collection<Disk> disks() {
    return _diskByLogdir.values();
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    List<Map<String, Object>> replicaList = new ArrayList<>();
    for (Replica replica : _replicas) {
      replicaList.add(replica.getJsonStructure());
    }
    return Map.of(ModelUtils.BROKER_ID, _id, ModelUtils.BROKER_STATE, _state, ModelUtils.REPLICAS, replicaList);
  }

  /**
   * Get per-logdir disk statistics of the broker.
   *
   * @return The per-logdir disk statistics. This method is relevant only when the {@link ClusterModel} has
   *         been created with a request to populate replica placement info, otherwise returns an empty map.
   */
  public Map<String, DiskStats> diskStats() {
    if (_diskByLogdir.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, DiskStats> diskStatMap = new HashMap<>();
    _diskByLogdir.forEach((k, v) -> diskStatMap.put(k, v.diskStats()));
    return diskStatMap;
  }

  /**
   * Output writing string representation of this class to the stream.
   * @param out the output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    String broker = String.format("<Broker id=\"%d\" state=\"%s\">%n", _id, _state);
    out.write(broker.getBytes(StandardCharsets.UTF_8));
    for (Disk disk : _diskByLogdir.values()) {
      disk.writeTo(out);
    }
    // If information of replica placement over disk is not populated, write replica information to output stream;
    // otherwise disk will write replica information to output stream.
    if (_diskByLogdir.isEmpty()) {
      for (Replica replica : _replicas) {
        replica.writeTo(out);
      }
    }
    out.write("</Broker>%n".getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    return String.format("Broker[id=%d,rack=%s,state=%s,replicaCount=%d,logdirs=%s,leaderCount=%d]",
                         _id, rack().id(), _state, _replicas.size(), _diskByLogdir.keySet(), _leaderReplicas.size());
  }

  /**
   * Compare by broker id.
   */
  @Override
  public int compareTo(Broker o) {
    return Integer.compare(_id, o.id());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Broker broker = (Broker) o;
    return _id == broker._id;
  }

  @Override
  public int hashCode() {
    return _id;
  }
}
