/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.EMPTY_BROKER_CAPACITY;

/**
 * A class that holds the information of the cluster, including topology, liveness and load for racks, brokers and
 * replicas. A cluster object is created either by a load balance request or a self healing request. It is used as
 * the input of the analyzer to generate the proposals for load rebalance.
 */
public class ClusterModel implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterModel.class);
  private static final long serialVersionUID = -6840253566423285966L;
  // Hypothetical broker that indicates the original broker of replicas to be created in the existing cluster model.
  private static final Broker GENESIS_BROKER = new Broker(null, -1, new BrokerCapacityInfo(EMPTY_BROKER_CAPACITY), false);
  private static final OptimizationOptions DEFAULT_OPTIMIZATION_OPTIONS = new OptimizationOptions(Collections.emptySet(),
                                                                                                  Collections.emptySet(),
                                                                                                  Collections.emptySet());

  private final ModelGeneration _generation;
  private final Map<String, Rack> _racksById;
  private final Map<Integer, Rack> _brokerIdToRack;
  private final Map<TopicPartition, Partition> _partitionsByTopicPartition;
  private final Set<Replica> _selfHealingEligibleReplicas;
  private final SortedSet<Broker> _newBrokers;
  private final SortedSet<Broker> _brokersWithBadDisks;
  private final Set<Broker> _aliveBrokers;
  private final SortedSet<Broker> _deadBrokers;
  private final SortedSet<Broker> _brokers;
  private final double _monitoredPartitionsRatio;
  private final double[] _clusterCapacity;
  private final Load _load;
  // An integer to keep track of the maximum replication factor that a partition was ever created with.
  private int _maxReplicationFactor;
  // The replication factor that each topic in the cluster created with ().
  private final Map<String, Integer> _replicationFactorByTopic;
  private final Map<String, Integer> _numReplicasByTopic;
  private final Map<Integer, Load> _potentialLeadershipLoadByBrokerId;
  private int _unknownHostId;
  private final Map<Integer, String> _capacityEstimationInfoByBrokerId;

  /**
   * Constructor for the cluster class. It creates data structures to hold a list of racks, a map for partitions by
   * topic partition, topic replica collocation by topic.
   *
   * @param generation Model generation of the cluster
   * @param monitoredPartitionsRatio Monitored partitions ratio
   */
  public ClusterModel(ModelGeneration generation, double monitoredPartitionsRatio) {
    _generation = generation;
    _racksById = new HashMap<>();
    _brokerIdToRack = new HashMap<>();
    _partitionsByTopicPartition = new HashMap<>();
    // Replicas are added/removed only when broker alive status is set via setState(). Replica contents are
    // automatically updated in case of replica or leadership relocation.
    _selfHealingEligibleReplicas = new HashSet<>();
    // A sorted set of newly added brokers
    _newBrokers = new TreeSet<>();
    // A sorted set of alive brokers with bad disks
    _brokersWithBadDisks = new TreeSet<>();
    // A set of alive brokers
    _aliveBrokers = new HashSet<>();
    // A set of all brokers
    _brokers = new TreeSet<>();
    // A set of dead brokers
    _deadBrokers = new TreeSet<>();
    // Initially cluster does not contain any load.
    _load = new Load();
    _clusterCapacity = new double[Resource.cachedValues().size()];
    _maxReplicationFactor = 1;
    _replicationFactorByTopic = new HashMap<>();
    _numReplicasByTopic = new HashMap<>();
    _potentialLeadershipLoadByBrokerId = new HashMap<>();
    _monitoredPartitionsRatio = monitoredPartitionsRatio;
    _unknownHostId = 0;
    _capacityEstimationInfoByBrokerId = new HashMap<>();
  }

  /**
   * @return The metadata generation for this cluster model.
   */
  public ModelGeneration generation() {
    return _generation;
  }

  /**
   * @return The coverage of this cluster model via monitored partitions ratio, showing how representative the cluster is.
   */
  public double monitoredPartitionsRatio() {
    return _monitoredPartitionsRatio;
  }

  /**
   * Populate the analysis stats with this cluster, given balancing constraint, and optimization options.
   *
   * @param balancingConstraint Balancing constraint.
   * @param optimizationOptions Options to take into account while populating stats.
   * @return Analysis stats with this cluster and given balancing constraint.
   */
  public ClusterModelStats getClusterStats(BalancingConstraint balancingConstraint, OptimizationOptions optimizationOptions) {
    return (new ClusterModelStats()).populate(this, balancingConstraint, optimizationOptions);
  }

  /**
   * Populate the analysis stats with this cluster and given balancing constraint.
   *
   * @param balancingConstraint Balancing constraint.
   * @return Analysis stats with this cluster and given balancing constraint.
   */
  public ClusterModelStats getClusterStats(BalancingConstraint balancingConstraint) {
    return getClusterStats(balancingConstraint, DEFAULT_OPTIMIZATION_OPTIONS);
  }

  /**
   * Get the rack with the rack id if it is found in the cluster; null otherwise.
   *
   * @param rackId ID of the requested rack.
   * @return The rack with the rack id if it is found in the cluster; null otherwise.
   */
  private Rack rack(String rackId) {
    return _racksById.get(rackId);
  }

  /**
   * Get the distribution of replicas in the cluster at the point of call. Replica distribution is represented by the
   * map: topic-partition -&gt; broker-id-of-replicas. broker-id-of-replicas[0] represents the leader's broker id.
   *
   * @return The replica distribution of leader and follower replicas in the cluster at the point of call.
   */
  public Map<TopicPartition, List<ReplicaPlacementInfo>> getReplicaDistribution() {
    Map<TopicPartition, List<ReplicaPlacementInfo>> replicaDistribution = new HashMap<>();

    for (Map.Entry<TopicPartition, Partition> entry : _partitionsByTopicPartition.entrySet()) {
      TopicPartition tp = entry.getKey();
      Partition partition = entry.getValue();
      List<ReplicaPlacementInfo> replicaPlacementInfos
          = partition.replicas().stream().map(r -> r.disk() == null ? new ReplicaPlacementInfo(r.broker().id())
                                                                    : new ReplicaPlacementInfo(r.broker().id(), r.disk().logDir()))
                     .collect(Collectors.toList());
      // Add distribution of replicas in the partition.
      replicaDistribution.put(tp, replicaPlacementInfos);
    }

    return replicaDistribution;
  }

  /**
   * @return Leader broker ids for each partition.
   */
  public Map<TopicPartition, ReplicaPlacementInfo> getLeaderDistribution() {
    Map<TopicPartition, ReplicaPlacementInfo> leaders = new HashMap<>();
    for (Map.Entry<TopicPartition, Partition> entry : _partitionsByTopicPartition.entrySet()) {
      Replica leaderReplica = entry.getValue().leader();
      if (leaderReplica.disk() == null) {
        leaders.put(entry.getKey(), new ReplicaPlacementInfo(leaderReplica.broker().id()));
      } else {
        leaders.put(entry.getKey(), new ReplicaPlacementInfo(leaderReplica.broker().id(), leaderReplica.disk().logDir()));
      }
    }
    return leaders;
  }

  /**
   * @return Replicas eligible for self-healing.
   */
  public Set<Replica> selfHealingEligibleReplicas() {
    return Collections.unmodifiableSet(_selfHealingEligibleReplicas);
  }

  /**
   * @return The recent cluster load information.
   */
  public Load load() {
    return _load;
  }

  /**
   * Get the leadership load for given broker id. Leadership load is the accumulated outbound network load for leader
   * of each replica in a broker. This is the hypothetical maximum that would be realized if the specified broker
   * became the leader of all the replicas it currently owns.
   *
   * @param brokerId Broker id.
   * @return The leadership load for broker.
   */
  public Load potentialLeadershipLoadFor(Integer brokerId) {
    return _potentialLeadershipLoadByBrokerId.get(brokerId);
  }

  /**
   * @return The maximum replication factor of a replica that was added to the cluster before.
   */
  public int maxReplicationFactor() {
    return _maxReplicationFactor;
  }

  /**
   * @return The replication factor that each topic in the cluster created with.
   */
  public Map<String, Integer> replicationFactorByTopic() {
    return Collections.unmodifiableMap(_replicationFactorByTopic);
  }

  /**
   * Get partition of the given replica.
   *
   * @param tp Topic partition of the replica for which the partition is requested.
   * @return Partition of the given replica.
   */
  public Partition partition(TopicPartition tp) {
    return _partitionsByTopicPartition.get(tp);
  }

  /**
   * @return A map of partitions by topic names.
   */
  public SortedMap<String, List<Partition>> getPartitionsByTopic() {
    SortedMap<String, List<Partition>> partitionsByTopic = new TreeMap<>();
    for (String topicName: topics()) {
      partitionsByTopic.put(topicName, new ArrayList<>());
    }
    for (Map.Entry<TopicPartition, Partition> entry: _partitionsByTopicPartition.entrySet()) {
      partitionsByTopic.get(entry.getKey().topic()).add(entry.getValue());
    }
    return partitionsByTopic;
  }

  /**
   * @return All the leader replicas in the cluster.
   */
  public Set<Replica> leaderReplicas() {
    return _partitionsByTopicPartition.values().stream().map(Partition::leader).collect(Collectors.toSet());
  }

  /**
   * Return a map from topic names to the number of leader replicas of each topic
   * @param topics a set of topic names
   * @return A map from topic names to the number of leader replicas of each topic
   */
  public Map<String, Integer> numLeadersPerTopic(Set<String> topics) {
    Map<String, Integer> leaderCountByTopicNames = new HashMap<>();
    for (TopicPartition tp : _partitionsByTopicPartition.keySet()) {
      String topicName = tp.topic();
      if (topics.contains(topicName)) {
        leaderCountByTopicNames.merge(topicName, 1, Integer::sum);
      }
    }
    return leaderCountByTopicNames;
  }

  /**
   * Set the {@link Broker.State liveness state} of the given broker.
   * <ul>
   * <li>All currently offline replicas of a broker are considered to be self healing eligible.</li>
   * <li>A broker with bad disks is also considered as an alive broker.</li>
   * </ul>
   *
   * @param brokerId Id of the broker for which the alive status is set.
   * @param newState The new state of the broker.
   */
  public void setBrokerState(int brokerId, Broker.State newState) {
    Broker broker = broker(brokerId);
    if (broker == null) {
      throw new IllegalArgumentException("Broker " + brokerId + " does not exist.");
    }
    // We need to go through rack so all the cached capacity will be updated.
    broker.rack().setBrokerState(brokerId, newState);
    _selfHealingEligibleReplicas.addAll(broker.currentOfflineReplicas());
    refreshCapacity();
    switch (newState) {
      case DEAD:
        _aliveBrokers.remove(broker);
        _deadBrokers.add(broker);
        _brokersWithBadDisks.remove(broker);
        break;
      case NEW:
        _newBrokers.add(broker);
        // fall through to remove the replicas from selfHealingEligibleReplicas
      case DEMOTED:
        // As of now we still treat demoted brokers as alive brokers.
      case ALIVE:
        _aliveBrokers.add(broker);
        _deadBrokers.remove(broker);
        _brokersWithBadDisks.remove(broker);
        break;
      case BAD_DISKS:
        // We treat brokers with bad disks (i.e. brokers with at least one healthy disk) as alive brokers.
        _aliveBrokers.add(broker);
        _deadBrokers.remove(broker);
        _brokersWithBadDisks.add(broker);
        // Due to the limitation of Kafka JBOD support, if a partition has replica on a broker's broken disk, then we cannot
        // move the other replicas of this partition to this broker.
        for (Replica replica : broker.currentOfflineReplicas()) {
          _partitionsByTopicPartition.get(replica.topicPartition()).addIneligibleBroker(broker);
        }
        break;
      default:
        throw new IllegalArgumentException("Illegal broker state " + newState + " is provided.");
    }
  }

  /**
   * Set the given disk to dead state.
   * This method is for testing only.
   *
   * @param brokerId Id of the broker on which the disk resides.
   * @param logdir   Log directory of the disk.
   */
  void markDiskDead(int brokerId, String logdir) {
    Broker broker = broker(brokerId);
    if (broker == null) {
      throw new IllegalArgumentException("Broker " + brokerId + " does not exist.");
    }
    broker.rack().markDiskDead(brokerId, logdir);
    _selfHealingEligibleReplicas.addAll(broker.currentOfflineReplicas());
    refreshCapacity();
  }

  /**
   * For replica movement across the disks of the same broker.
   *
   * @param tp                Partition Info of the replica to be relocated.
   * @param brokerId          Broker id.
   * @param destinationLogdir Destination logdir.
   */
  public void relocateReplica(TopicPartition tp, int brokerId, String destinationLogdir) {
    Replica replicaToMove = _partitionsByTopicPartition.get(tp).replica(brokerId);
    // Move replica from the source disk to destination disk on the same broker.
    replicaToMove.broker().moveReplicaBetweenDisks(tp, replicaToMove.disk().logDir(), destinationLogdir);
  }

  /**
   * For replica movement across the broker:
   *    (1) Remove the replica from the source broker,
   *    (2) Set the broker of the removed replica as the destination broker,
   *    (3) Add this replica to the destination broker.
   * There is no need to make any modifications to _partitionsByTopicPartition because even after the move,
   * partitions will contain the same replicas.
   *
   * @param tp                      Partition Info of the replica to be relocated.
   * @param sourceBrokerId          Source broker id.
   * @param destinationBrokerId     Destination broker id.
   */
  public void relocateReplica(TopicPartition tp, int sourceBrokerId, int destinationBrokerId) {
    // Removes the replica and related load from the source broker / source rack / cluster.
    Replica replica = removeReplica(sourceBrokerId, tp);
    if (replica == null) {
      throw new IllegalArgumentException("Replica is not in the cluster.");
    }
    // Updates the broker of the removed replica with destination broker.
    replica.setBroker(broker(destinationBrokerId));

    // Add this replica and related load to the destination broker / destination rack / cluster.
    replica.broker().rack().addReplica(replica);
    // Increment the number of replicas per this topic.
    _numReplicasByTopic.merge(tp.topic(), 1, Integer::sum);
    _load.addLoad(replica.load());
    // Add leadership load to the destination replica.
    _potentialLeadershipLoadByBrokerId.get(destinationBrokerId).addLoad(partition(tp).leader().load());
  }

  /**
   * (1) Removes leadership from source replica.
   * (2) Adds this leadership to the destination replica.
   * (3) Transfers the whole outbound network and a fraction of CPU load of source replica to the destination replica.
   * (4) Updates the leader and list of followers of the partition.
   *
   * @param tp      Topic partition of this replica.
   * @param sourceBrokerId      Source broker id.
   * @param destinationBrokerId Destination broker id.
   * @return {@code true} if relocation is successful, {@code false} otherwise.
   */
  public boolean relocateLeadership(TopicPartition tp, int sourceBrokerId, int destinationBrokerId) {
    // Sanity check to see if the source replica is the leader.
    Replica sourceReplica = _partitionsByTopicPartition.get(tp).replica(sourceBrokerId);
    if (!sourceReplica.isLeader()) {
      return false;
    }
    // Sanity check to see if the destination replica is a follower.
    Replica destinationReplica = _partitionsByTopicPartition.get(tp).replica(destinationBrokerId);
    if (destinationReplica.isLeader()) {
      throw new IllegalArgumentException("Cannot relocate leadership of partition " + tp + "from broker "
                                         + sourceBrokerId + " to broker " + destinationBrokerId
                                         + " because the destination replica is a leader.");
    }

    // Transfer the leadership load (whole outbound network and a fraction of CPU load) of source replica to the
    // destination replica.
    // (1) Remove and get the outbound network load and a fraction of CPU load associated with leadership from the
    // given replica.
    // (2) Add the outbound network load and CPU load associated with leadership to the given replica.
    //
    // Remove the load from the source rack.
    Rack rack = broker(sourceBrokerId).rack();
    AggregatedMetricValues leadershipLoadDelta = rack.makeFollower(sourceBrokerId, tp);
    // Add the load to the destination rack.
    rack = broker(destinationBrokerId).rack();
    rack.makeLeader(destinationBrokerId, tp, leadershipLoadDelta);

    // Update the leader and list of followers of the partition.
    Partition partition = _partitionsByTopicPartition.get(tp);
    partition.relocateLeadership(destinationReplica);

    return true;
  }

  /**
   * @return The alive brokers in the cluster.
   */
  public Set<Broker> aliveBrokers() {
    return Collections.unmodifiableSet(_aliveBrokers);
  }

  /**
   * @return The view of dead brokers in the cluster.
   */
  public SortedSet<Broker> deadBrokers() {
    return Collections.unmodifiableSortedSet(_deadBrokers);
  }

  /**
   * @return Broken brokers -- i.e. dead brokers and brokers with bad disk in the cluster.
   */
  public SortedSet<Broker> brokenBrokers() {
    SortedSet<Broker> brokenBrokers = new TreeSet<>(_deadBrokers);
    brokenBrokers.addAll(brokersWithBadDisks());
    return Collections.unmodifiableSortedSet(brokenBrokers);
  }

  /**
   * @return Capacity estimation info by broker id for which there has been an estimation.
   */
  public Map<Integer, String> capacityEstimationInfoByBrokerId() {
    return Collections.unmodifiableMap(_capacityEstimationInfoByBrokerId);
  }

  /**
   * @return The demoted brokers in the cluster.
   */
  public SortedSet<Broker> demotedBrokers() {
    SortedSet<Broker> demotedBrokers = new TreeSet<>();
    for (Rack rack : _racksById.values()) {
      rack.brokers().forEach(b -> {
        if (b.isDemoted()) {
          demotedBrokers.add(b);
        }
      });
    }
    return demotedBrokers;
  }

  /**
   * @return The set of new brokers.
   */
  public SortedSet<Broker> newBrokers() {
    return Collections.unmodifiableSortedSet(_newBrokers);
  }

  /**
   * @return The set of brokers with bad disks -- i.e. for which the offline replicas are being fixed.
   */
  public SortedSet<Broker> brokersWithBadDisks() {
    return Collections.unmodifiableSortedSet(_brokersWithBadDisks);
  }

  /**
   * @return Brokers containing offline replicas residing on bad disks in the current cluster model.
   */
  public SortedSet<Broker> brokersHavingOfflineReplicasOnBadDisks() {
    SortedSet<Broker> brokersWithOfflineReplicasOnBadDisks = _brokersWithBadDisks.isEmpty() ? Collections.emptySortedSet() : new TreeSet<>();
    for (Broker brokerWithBadDisks : _brokersWithBadDisks) {
      if (!brokerWithBadDisks.currentOfflineReplicas().isEmpty()) {
        brokersWithOfflineReplicasOnBadDisks.add(brokerWithBadDisks);
      }
    }

    return brokersWithOfflineReplicasOnBadDisks;
  }

  /**
   * @return {@code true} if at least one rack is alive in the cluster, {@code false} otherwise.
   */
  public boolean isClusterAlive() {
    for (Rack rack : _racksById.values()) {
      if (rack.isRackAlive()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Clear the content of monitoring data at each replica in the cluster.
   * Typically, if a change is detected in topology, this method is called to clear the monitoring data collected with
   * the old topology.
   */
  public void clearLoad() {
    _racksById.values().forEach(Rack::clearLoad);
    _load.clearLoad();
  }

  /**
   * Remove and get removed replica from the cluster.
   *
   * @param brokerId       Id of the broker containing the partition.
   * @param tp Topic partition of the replica to be removed.
   * @return The requested replica if the id exists in the rack and the partition is found in the broker, null
   * otherwise.
   */
  public Replica removeReplica(int brokerId, TopicPartition tp) {
    for (Rack rack : _racksById.values()) {
      // Remove the replica and the associated load from the rack that it resides in.
      Replica removedReplica = rack.removeReplica(brokerId, tp);
      if (removedReplica != null) {
        // Decrement the number of replicas per this topic.
        _numReplicasByTopic.merge(tp.topic(), -1, Integer::sum);
        if (_numReplicasByTopic.get(tp.topic()) == 0) {
          _numReplicasByTopic.remove(tp.topic());
        }
        // Remove the load of the removed replica from the recent load of the cluster.
        _load.subtractLoad(removedReplica.load());
        _potentialLeadershipLoadByBrokerId.get(brokerId).subtractLoad(partition(tp).leader().load());
        // Return the removed replica.
        return removedReplica;
      }
    }
    return null;
  }

  /**
   * @return The unmodifiable view of set of brokers in the cluster.
   */
  public SortedSet<Broker> brokers() {
    return Collections.unmodifiableSortedSet(_brokers);
  }

  /**
   * Get the requested broker in the cluster.
   *
   * @param brokerId Id of the requested broker.
   * @return Requested broker if it is in the cluster, null otherwise.
   */
  public Broker broker(int brokerId) {
    Rack rack = _brokerIdToRack.get(brokerId);
    return rack == null ? null : rack.broker(brokerId);
  }

  /**
   * Ask the cluster model to keep track of the replicas sorted with the given selection functions, priority functions and score function.
   *
   * The selection functions determine which replicas to be included in the sorted replicas. Then the sort will first
   * use the priority functions then the score function. The priority functions allow the caller to prioritize a certain
   * type of replicas, e.g immigrant replicas.
   *
   * It is recommended to use the functions from {@link ReplicaSortFunctionFactory} so the functions can be maintained
   * in a single place.
   *
   * The sorted replica will only be updated in the following cases:
   * 1. A replica is added to or removed from a broker
   * 2. A replica's role has changed from leader to follower, and vice versa.
   *
   * The sorted replicas are named using the given sortName, and can be accessed using
   * {@link Broker#trackedSortedReplicas(String)}. If the sorted replicas are no longer needed,
   * call {@link #untrackSortedReplicas(String)} or {@link #clearSortedReplicas()}to release memory.
   *
   * @param sortName the name of the tracked sorted replicas.
   * @param selectionFuncs A set of selection functions to decide which replica to include in the sort. If it is {@code null}
   *                      or empty, all the replicas are to be included.
   * @param priorityFuncs A list of priority functions to sort the replicas. Priority functions are applied one by one until
   *                      two replicas are of different priority regards to the current priority function.
   * @param scoreFunc the score function to sort the replicas with the same priority, replicas are sorted in ascending
   *                  order of score.
   * @see SortedReplicas
   */
  void trackSortedReplicas(String sortName,
                           Set<Function<Replica, Boolean>> selectionFuncs,
                           List<Function<Replica, Integer>> priorityFuncs,
                           Function<Replica, Double> scoreFunc) {
    _brokers.forEach(b -> b.trackSortedReplicas(sortName, selectionFuncs, priorityFuncs, scoreFunc));
  }

  /**
   * Untrack the sorted replicas with the given name to release memory.
   *
   * @param sortName the name of the sorted replicas.
   */
  public void untrackSortedReplicas(String sortName) {
    _brokers.forEach(b -> b.untrackSortedReplicas(sortName));
  }

  /**
   * Clear all cached sorted replicas to release memory.
   */
  public void clearSortedReplicas() {
    _brokers.forEach(Broker::clearSortedReplicas);
  }

  /**
   * Clear the content and structure of the cluster.
   */
  public void clear() {
    _racksById.clear();
    _partitionsByTopicPartition.clear();
    _load.clearLoad();
    _maxReplicationFactor = 1;
    _replicationFactorByTopic.clear();
    _numReplicasByTopic.clear();
    _capacityEstimationInfoByBrokerId.clear();
  }

  /**
   * @return Alive racks in the cluster.
   */
  public Set<Rack> aliveRacks() {
    return _racksById.values().stream().filter(Rack::isRackAlive).collect(Collectors.toUnmodifiableSet());
  }

  /**
   * @param optimizationOptions Options to use in checking the number of racks that are alive and allowed replica moves.
   * @return The number of alive racks in the cluster that are allowed replica moves.
   */
  public Set<Rack> aliveRacksAllowedReplicaMoves(OptimizationOptions optimizationOptions) {
    return _racksById.values().stream()
                     .filter(r -> r.isAliveAndAllowedReplicaMoves(optimizationOptions))
                     .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * @param optimizationOptions Options to use in checking the number of racks that are alive and allowed replica moves.
   * @return The number of alive racks in the cluster that are allowed replica moves.
   */
  public int numAliveRacksAllowedReplicaMoves(OptimizationOptions optimizationOptions) {
    int numAliveRacksAllowedReplicaMoves = 0;
    for (Rack rack : _racksById.values()) {
      if (rack.isAliveAndAllowedReplicaMoves(optimizationOptions)) {
        numAliveRacksAllowedReplicaMoves++;
      }
    }
    return numAliveRacksAllowedReplicaMoves;
  }

  /**
   * Get the number of replicas with the given topic name in cluster.
   *
   * @param topic Name of the topic for which the number of replicas in cluster will be counted.
   * @return Number of replicas with the given topic name in cluster.
   */
  public int numTopicReplicas(String topic) {
    return _numReplicasByTopic.getOrDefault(topic, 0);
  }

  /**
   * Get the number of leader replicas in cluster.
   *
   * @return Number of leader replicas in cluster.
   */
  public int numLeaderReplicas() {
    return _partitionsByTopicPartition.size();
  }

  /**
   * Get the number of replicas in cluster.
   *
   * @return Number of replicas in cluster.
   */
  public int numReplicas() {
    return _partitionsByTopicPartition.values().stream().mapToInt(p -> p.replicas().size()).sum();
  }

  /**
   * @return Topics in the cluster.
   */
  public Set<String> topics() {
    Set<String> topics = new HashSet<>();

    for (Rack rack : _racksById.values()) {
      topics.addAll(rack.topics());
    }
    return topics;
  }

  /**
   * Get cluster capacity for the requested resource. Cluster capacity represents the total capacity of the live
   * brokers in the cluster for the requested resource.
   *
   * @param resource Resource for which the capacity will be provided.
   * @return Alive cluster capacity of the resource.
   */
  public double capacityFor(Resource resource) {
    return _clusterCapacity[resource.id()];
  }

  /**
   * Get cluster capacity for the requested resource over brokers that are allowed to receive replicas.
   * Contrary to {@link #capacityFor(Resource)}, drops the capacity of brokers that are excluded from replica moves.
   *
   * @param resource Resource for which the capacity will be provided.
   * @param optimizationOptions Options to take into account while retrieving the capacity.
   * @return Alive cluster capacity of the resource, excluding the capacity of brokers that are excluded from replica moves.
   */
  public double capacityWithAllowedReplicaMovesFor(Resource resource, OptimizationOptions optimizationOptions) {
    Set<Integer> excluded = optimizationOptions.excludedBrokersForReplicaMove();
    double capacityToDrop = _aliveBrokers.stream().filter(b -> excluded.contains(b.id())).mapToDouble(b -> b.capacityFor(resource)).sum();
    return _clusterCapacity[resource.id()] - capacityToDrop;
  }

  /**
   * Set the load for the given replica. This method should be called only once for each replica.
   *
   * @param rackId         Rack id.
   * @param brokerId       Broker Id containing the replica with the given topic partition.
   * @param tp             Topic partition that identifies the replica in this broker.
   * @param metricValues   The load of the replica.
   * @param windows        The windows list of the aggregated metrics.
   */
  public void setReplicaLoad(String rackId,
                             int brokerId,
                             TopicPartition tp,
                             AggregatedMetricValues metricValues,
                             List<Long> windows) {
    // Sanity check for the attempts to push more than allowed number of snapshots having different times.
    if (!broker(brokerId).replica(tp).load().isEmpty()) {
      throw new IllegalStateException(String.format("The load for %s on broker %d, rack %s already has metric values.",
                                                    tp, brokerId, rackId));
    }

    Rack rack = rack(rackId);
    rack.setReplicaLoad(brokerId, tp, metricValues, windows);

    // Update the recent load of cluster.
    _load.addMetricValues(metricValues, windows);
    // If this snapshot belongs to leader, update leadership load.
    Replica leader = partition(tp).leader();
    if (leader != null && leader.broker().id() == brokerId) {
      // load must be updated for each broker containing a replica of the same partition.
      for (Replica replica : partition(tp).replicas()) {
        _potentialLeadershipLoadByBrokerId.get(replica.broker().id()).addMetricValues(metricValues, windows);
      }
    }
  }

  /**
   * If the rack or broker does not exist, create them with UNKNOWN host name. This allows handling
   * of cases where the information of a dead broker is no longer available.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param brokerCapacityInfo The capacity information to use if the broker does not exist.
   */
  public void handleDeadBroker(String rackId, int brokerId, BrokerCapacityInfo brokerCapacityInfo) {
    if (rack(rackId) == null) {
      createRack(rackId);
    }
    if (broker(brokerId) == null) {
      createBroker(rackId, String.format("UNKNOWN_HOST-%d", _unknownHostId++), brokerId, brokerCapacityInfo, false);
    }
  }

  /**
   * Create a replica under given cluster/rack/broker. Add replica to rack and corresponding partition. Get the
   * created replica. Set the replica as offline if it is on a dead broker.
   *
   * The {@link com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor} uses {@link #createReplica(String, int,
   * TopicPartition, int, boolean, boolean, String, boolean)} while setting the replica offline status, and it considers
   * the broken disks as well. Whereas, this method is used only by the unit tests. The relevant unit tests may use
   * {@link Replica#markOriginalOffline()} to mark offline replicas on broken disks.
   *
   * The main reason for this separation is the lack of disk representation in the current broker model. Once the
   * <a href="https://github.com/linkedin/cruise-control/pull/327">patch #327</a> is merged, we can simplify this logic.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param tp             Topic partition information of the replica.
   * @param index          The index of the replica in the replica list.
   * @param isLeader       {@code true} if the replica is a leader, {@code false} otherwise.
   * @return Created replica.
   */
  public Replica createReplica(String rackId, int brokerId, TopicPartition tp, int index, boolean isLeader) {
    return createReplica(rackId, brokerId, tp, index, isLeader, !broker(brokerId).isAlive(), null, false);
  }

  /**
   * Create a replica under given cluster/rack/broker. Add replica to rack and corresponding partition. Get the
   * created replica. Explicitly set the offline status of replica upon creation -- e.g. for replicas on dead disks.
   *
   * @param rackId         Rack id under which the replica will be created.
   * @param brokerId       Broker id under which the replica will be created.
   * @param tp             Topic partition information of the replica.
   * @param index          The index of the replica in the replica list.
   * @param isLeader       {@code true} if the replica is a leader, {@code false} otherwise.
   * @param isOffline      {@code true} if the replica is offline in its original location, {@code false} otherwise.
   * @param logdir         The logdir of replica's hosting disk. If replica placement over disk information is not populated,
   *                       this parameter is null.
   * @param isFuture       {@code true} if the replica does not correspond to any existing replica in the cluster, but a replica
   *                       we are going to add to the cluster. This replica's original broker will not be any existing broker
   *                       so that it will be treated as an immigrant replica for whatever broker it is assigned to and
   *                       grant goals greatest freedom to allocate to an existing broker.
   * @return Created replica.
   */
  public Replica createReplica(String rackId,
                               int brokerId,
                               TopicPartition tp,
                               int index,
                               boolean isLeader,
                               boolean isOffline,
                               String logdir,
                               boolean isFuture) {
    Replica replica;
    Broker broker = broker(brokerId);
    if (!isFuture) {
      Disk disk = null;
      if (logdir != null) {
        disk = broker.disk(logdir);
        if (disk == null) {
          // If dead disk information is not reported by BrokerCapacityConfigResolver, add dead disk information to cluster model..
          if (isOffline) {
            disk = broker.addDeadDisk(logdir);
          } else {
            throw new IllegalStateException("Missing disk information for disk " + logdir + " on broker " + broker);
          }
        }
      }
      replica = new Replica(tp, broker, isLeader, isOffline, disk);
    } else {
      replica = new Replica(tp, GENESIS_BROKER, false);
      replica.setBroker(broker);
    }
    rack(rackId).addReplica(replica);
    // Increment the number of replicas per this topic.
    _numReplicasByTopic.merge(tp.topic(), 1, Integer::sum);

    // Add replica to its partition.
    if (!_partitionsByTopicPartition.containsKey(tp)) {
      // Partition has not been created before.
      _partitionsByTopicPartition.put(tp, new Partition(tp));
      _replicationFactorByTopic.putIfAbsent(tp.topic(), 1);
    }

    Partition partition = _partitionsByTopicPartition.get(tp);
    if (replica.isLeader()) {
      partition.addLeader(replica, index);
      return replica;
    }

    partition.addFollower(replica, index);
    // If leader of this follower was already created and load was pushed to it, add that load to the follower.
    Replica leaderReplica = partition(tp).leader();
    if (leaderReplica != null) {
      _potentialLeadershipLoadByBrokerId.get(brokerId).addLoad(leaderReplica.load());
    }

    // Keep track of the replication factor per topic.
    int replicationFactor = Math.max(_replicationFactorByTopic.get(tp.topic()), partition.followers().size() + 1);
    _replicationFactorByTopic.put(tp.topic(), replicationFactor);

    // Increment the maximum replication factor if the number of replicas of the partition is larger than the
    //  maximum replication factor of previously existing partitions.
    _maxReplicationFactor = Math.max(_maxReplicationFactor, replicationFactor);

    return replica;
  }

  /**
   * Delete a replica from cluster. This method is expected to be called in a batch for all partitions of the topic and in
   * the end the replication factor across partitions are consistent. Also the caller of this method is expected to call
   * {@link #refreshClusterMaxReplicationFactor()} after all replica deletion.
   * @param topicPartition Topic partition of the replica to be removed.
   * @param brokerId Id of the broker hosting the replica.
   */
  public void deleteReplica(TopicPartition topicPartition, int brokerId) {
    int currentReplicaCount = _partitionsByTopicPartition.get(topicPartition).replicas().size();
    if (currentReplicaCount < 2) {
      throw new IllegalStateException(String.format("Unable to delete replica for topic partition %s since it only has %d replicas.",
                                                    topicPartition, currentReplicaCount));
    }
    removeReplica(brokerId, topicPartition);
    // Update partition info.
    Partition partition = _partitionsByTopicPartition.get(topicPartition);
    partition.deleteReplica(brokerId);
    _replicationFactorByTopic.put(topicPartition.topic(), partition.replicas().size());
  }

  /**
   * Refresh the maximum topic replication factor statistic.
   */
  public void refreshClusterMaxReplicationFactor() {
    _maxReplicationFactor = _replicationFactorByTopic.values().stream().max(Integer::compareTo).orElse(0);
  }

  /**
   * Create a broker under this cluster/rack and get the created broker.
   * Add the broker id and info to {@link #_capacityEstimationInfoByBrokerId} if the broker capacity has been estimated.
   *
   * @param rackId Id of the rack that the broker will be created in.
   * @param host The host of this broker
   * @param brokerId Id of the broker to be created.
   * @param brokerCapacityInfo Capacity information of the created broker.
   * @param populateReplicaPlacementInfo Whether populate replica placement over disk information or not.
   * @return Created broker.
   */
  public Broker createBroker(String rackId,
                             String host,
                             int brokerId,
                             BrokerCapacityInfo brokerCapacityInfo,
                             boolean populateReplicaPlacementInfo) {
    _potentialLeadershipLoadByBrokerId.putIfAbsent(brokerId, new Load());
    Rack rack = rack(rackId);
    _brokerIdToRack.put(brokerId, rack);

    if (brokerCapacityInfo.isEstimated()) {
      _capacityEstimationInfoByBrokerId.put(brokerId, brokerCapacityInfo.estimationInfo());
    }
    Broker broker = rack.createBroker(brokerId, host, brokerCapacityInfo, populateReplicaPlacementInfo);
    _aliveBrokers.add(broker);
    _brokers.add(broker);
    refreshCapacity();
    return broker;
  }

  /**
   * Create a rack under this cluster.
   *
   * @param rackId Id of the rack to be created.
   * @return Created rack.
   */
  public Rack createRack(String rackId) {
    Rack rack = new Rack(rackId);
    return _racksById.putIfAbsent(rackId, rack);
  }

  /**
   * For partitions of specified topics, create or delete replicas in given cluster model to change the partition's replication
   * factor to target replication factor. New replicas for partition are added in a rack-aware, round-robin way.
   *
   * @param topicsByReplicationFactor The topics to modify replication factor with target replication factor.
   * @param brokersByRack A map from rack to broker.
   * @param rackByBroker A map from broker to rack.
   * @param cluster Kafka cluster.
   */
  public void createOrDeleteReplicas(Map<Short, Set<String>> topicsByReplicationFactor,
                                     Map<String, List<Integer>> brokersByRack,
                                     Map<Integer, String> rackByBroker,
                                     Cluster cluster) {
    // After replica deletion of some topic partitions, the cluster's maximal replication factor may decrease.
    boolean needToRefreshClusterMaxReplicationFactor = false;

    for (Map.Entry<Short, Set<String>> entry : topicsByReplicationFactor.entrySet()) {
      short replicationFactor = entry.getKey();
      Set<String> topics = entry.getValue();
      for (String topic : topics) {
        List<String> racks = new ArrayList<>(brokersByRack.keySet());
        int[] cursors = new int[racks.size()];
        int rackCursor = 0;
        for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {

          if (partitionInfo.replicas().length == replicationFactor) {
            continue;
          }

          TopicPartition tp = new TopicPartition(topic, partitionInfo.partition());
          Partition partition = partition(tp);
          if (!ModelUtils.hasSameReplicasFor(partition, partitionInfo)) {
            LOG.warn("Detected partition info inconsistent with clusterModel: PartitionInfo: {}, Partition in ClusterModel: {}. "
                     + " Skip creating or deleting replicas for this partition.", partitionInfo, partition);
            continue;
          }

          List<Integer> newAssignedReplica = new ArrayList<>();
          if (partitionInfo.replicas().length < replicationFactor) {
            Set<String> currentOccupiedRack = new HashSet<>();
            // Make sure the current replicas are in new replica list.
            for (Node node : partitionInfo.replicas()) {
              newAssignedReplica.add(node.id());
              currentOccupiedRack.add(rackByBroker.get(node.id()));
            }
            // Add new replica to partition in rack-aware(if possible), round-robin way.
            while (newAssignedReplica.size() < replicationFactor) {
              String rack = racks.get(rackCursor);
              if (!currentOccupiedRack.contains(rack) || currentOccupiedRack.size() == racks.size()) {
                int cursor = cursors[rackCursor];
                Integer brokerId = brokersByRack.get(rack).get(cursor);
                if (!newAssignedReplica.contains(brokerId)) {
                  newAssignedReplica.add(brokersByRack.get(rack).get(cursor));
                  // Create a new replica in the cluster model and populate its load from the leader replica.
                  Load load = partition(tp).leader().getFollowerLoadFromLeader();
                  createReplica(rack, brokerId, tp, partitionInfo.replicas().length, false, false, null, true);
                  setReplicaLoad(rack, brokerId, tp, load.loadByWindows(), load.windows());
                  currentOccupiedRack.add(rack);
                }
                cursors[rackCursor] = (cursor + 1) % brokersByRack.get(rack).size();
              }
              rackCursor = (rackCursor + 1) % racks.size();
            }
          } else {
            // Make sure the leader replica is in new replica list.
            newAssignedReplica.add(partitionInfo.leader().id());
            for (Node node : partitionInfo.replicas()) {
              if (node.id() != newAssignedReplica.get(0)) {
                if (newAssignedReplica.size() < replicationFactor) {
                  newAssignedReplica.add(node.id());
                } else {
                  deleteReplica(new TopicPartition(topic, partitionInfo.partition()), node.id());
                  needToRefreshClusterMaxReplicationFactor = true;
                }
              }
            }
          }
        }
      }
    }
    if (needToRefreshClusterMaxReplicationFactor) {
      refreshClusterMaxReplicationFactor();
    }
  }

  /**
   * Get a list of sorted (in ascending order by resource) alive brokers having utilization under:
   * (given utilization threshold) * (broker and/or host capacity (see {@link Resource#isHostResource} and
   * {@link Resource#isBrokerResource}). Utilization threshold might be any capacity constraint thresholds such as
   * balance or capacity.
   *
   * @param resource             Resource for which brokers will be sorted.
   * @param utilizationThreshold Utilization threshold for the given resource.
   * @return A list of sorted (in ascending order by resource) alive brokers having utilization under:
   * (given utilization threshold) * (broker and/or host capacity).
   */
  public List<Broker> sortedAliveBrokersUnderThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> sortedTargetBrokersUnderCapacityLimit = aliveBrokersUnderThreshold(resource, utilizationThreshold);

    sortedTargetBrokersUnderCapacityLimit.sort((o1, o2) -> {
      double expectedBrokerLoad1 = o1.load().expectedUtilizationFor(resource);
      double expectedBrokerLoad2 = o2.load().expectedUtilizationFor(resource);
      // For host resource we first compare host util then look at the broker util -- even if a resource is a
      // host-resource, but not broker-resource.
      int hostComparison = 0;
      if (resource.isHostResource()) {
        double expectedHostLoad1 = o1.host().load().expectedUtilizationFor(resource);
        double expectedHostLoad2 = o2.host().load().expectedUtilizationFor(resource);
        hostComparison = Double.compare(expectedHostLoad1, expectedHostLoad2);
      }
      return hostComparison == 0 ? Double.compare(expectedBrokerLoad1, expectedBrokerLoad2) : hostComparison;
    });
    return sortedTargetBrokersUnderCapacityLimit;
  }

  /**
   * Get alive broker under threshold for the given resource type.
   *
   * @param resource The resource type.
   * @param utilizationThreshold Utilization threshold for the given resource.
   * @return Alive broker under threshold for the given resource type.
   */
  public List<Broker> aliveBrokersUnderThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> aliveBrokersUnderThreshold = new ArrayList<>();

    for (Broker aliveBroker : aliveBrokers()) {
      if (resource.isBrokerResource()) {
        double brokerCapacityLimit = aliveBroker.capacityFor(resource) * utilizationThreshold;
        double brokerUtilization = aliveBroker.load().expectedUtilizationFor(resource);
        if (brokerUtilization >= brokerCapacityLimit) {
          continue;
        }
      }
      if (resource.isHostResource()) {
        double hostCapacityLimit = aliveBroker.host().capacityFor(resource) * utilizationThreshold;
        double hostUtilization = aliveBroker.host().load().expectedUtilizationFor(resource);
        if (hostUtilization >= hostCapacityLimit) {
          continue;
        }
      }
      aliveBrokersUnderThreshold.add(aliveBroker);
    }
    return aliveBrokersUnderThreshold;
  }

  /**
   * Get alive broker over threshold for the given resource type.
   *
   * @param resource The resource type.
   * @param utilizationThreshold Utilization threshold for the given resource.
   * @return Alive broker over threshold for the given resource type.
   */
  public List<Broker> aliveBrokersOverThreshold(Resource resource, double utilizationThreshold) {
    List<Broker> aliveBrokersOverThreshold = new ArrayList<>();

    for (Broker aliveBroker : aliveBrokers()) {
      if (resource.isBrokerResource()) {
        double brokerCapacityLimit = aliveBroker.capacityFor(resource) * utilizationThreshold;
        double brokerUtilization = aliveBroker.load().expectedUtilizationFor(resource);
        if (brokerUtilization <= brokerCapacityLimit) {
          continue;
        }
      }
      if (resource.isHostResource()) {
        double hostCapacityLimit = aliveBroker.host().capacityFor(resource) * utilizationThreshold;
        double hostUtilization = aliveBroker.host().load().expectedUtilizationFor(resource);
        if (hostUtilization <= hostCapacityLimit) {
          continue;
        }
      }
      aliveBrokersOverThreshold.add(aliveBroker);
    }
    return aliveBrokersOverThreshold;
  }

  /**
   * Sort the partitions in the cluster by the utilization of the given resource.
   * @param resource The resource type.
   * @param wantMaxLoad {@code true} if the requested utilization represents the peak load, {@code false} otherwise.
   * @param wantAvgLoad {@code true} if the requested utilization represents the avg load, {@code false} otherwise.
   * @return A list of partitions sorted by utilization of the given resource.
   */
  public List<Partition> replicasSortedByUtilization(Resource resource, boolean wantMaxLoad, boolean wantAvgLoad) {
    List<Partition> partitionList = new ArrayList<>(_partitionsByTopicPartition.values());
    LOG.debug("Fetching {} partitions from the cluster model", _partitionsByTopicPartition.size());
    partitionList.sort((o1, o2) -> Double.compare(o2.leader().load().expectedUtilizationFor(resource, wantMaxLoad, wantAvgLoad),
                                                  o1.leader().load().expectedUtilizationFor(resource, wantMaxLoad, wantAvgLoad)));
    return partitionList;
  }

  /**
   * (1) Check whether each load in the cluster contains exactly the number of windows defined by the Load.
   * (2) Check whether sum of loads in the cluster / rack / broker / replica are consistent with each other.
   */
  public void sanityCheck() {
    // SANITY CHECK #1: Each load in the cluster must contain exactly the number of windows defined by the Load.
    Map<String, Integer> errorMsgAndNumWindows = new HashMap<>();

    int expectedNumWindows = _load.numWindows();

    // Check leadership loads.
    for (Map.Entry<Integer, Load> entry : _potentialLeadershipLoadByBrokerId.entrySet()) {
      int brokerId = entry.getKey();
      Load load = entry.getValue();
      if (load.numWindows() != expectedNumWindows && broker(brokerId).replicas().size() != 0) {
        errorMsgAndNumWindows.put(String.format("Leadership(%d)", brokerId), load.numWindows());
      }
    }

    // Check rack loads.
    for (Rack rack : _racksById.values()) {
      if (rack.load().numWindows() != expectedNumWindows && rack.replicas().size() != 0) {
        errorMsgAndNumWindows.put(String.format("Rack(%s)", rack.id()), rack.load().numWindows());
      }

      // Check the host load.
      for (Host host : rack.hosts()) {
        if (host.load().numWindows() != expectedNumWindows && host.replicas().size() != 0) {
          errorMsgAndNumWindows.put(String.format("Host(%s)", host.name()), host.load().numWindows());
        }

        // Check broker loads.
        for (Broker broker : rack.brokers()) {
          if (broker.load().numWindows() != expectedNumWindows && broker.replicas().size() != 0) {
            errorMsgAndNumWindows.put(String.format("Broker(%d)", broker.id()), broker.load().numWindows());
          }

          // Check replica loads.
          for (Replica replica : broker.replicas()) {
            if (replica.load().numWindows() != expectedNumWindows) {
              errorMsgAndNumWindows.put(String.format("Replica(%s-%d)", replica.topicPartition(), broker.id()),
                                        replica.load().numWindows());
            }
          }
        }
      }
    }
    StringBuilder exceptionMsg = new StringBuilder();
    for (Map.Entry<String, Integer> entry : errorMsgAndNumWindows.entrySet()) {
      exceptionMsg.append(String.format("[%s: %d]%n", entry.getKey(), entry.getValue()));
    }

    if (exceptionMsg.length() > 0) {
      throw new IllegalArgumentException(String.format("Loads must have all have %d windows. Following loads violate this "
                                                       + "constraint with specified number of windows: %s",
                                                       expectedNumWindows, exceptionMsg));
    }
    // SANITY CHECK #2: Sum of loads in the cluster / rack / broker / replica must be consistent with each other.
    String prologueErrorMsg = "Inconsistent load distribution.";

    // Check equality of sum of the replica load to their broker load for each resource.
    for (Broker broker : brokers()) {
      for (Resource resource : Resource.cachedValues()) {
        double sumOfReplicaUtilization = 0.0;
        for (Replica replica : broker.replicas()) {
          sumOfReplicaUtilization += replica.load().expectedUtilizationFor(resource);
        }
        double brokerUtilization = broker.load().expectedUtilizationFor(resource);
        if (AnalyzerUtils.compare(sumOfReplicaUtilization, brokerUtilization, resource) != 0) {
          throw new IllegalArgumentException(String.format("%s Broker utilization for %s is different from the total replica "
                                                           + "utilization in the broker with id: %d. Sum of the replica utilization: %f, "
                                                           + "broker utilization: %f", prologueErrorMsg, resource, broker.id(),
                                                           sumOfReplicaUtilization, brokerUtilization));
        }
      }
    }

    // Check equality of sum of the broker load to their rack load for each resource.
    Map<Resource, Double> sumOfRackUtilizationByResource = new HashMap<>();
    for (Rack rack : _racksById.values()) {
      Map<Resource, Double> sumOfHostUtilizationByResource = new HashMap<>();
      for (Host host : rack.hosts()) {
        for (Resource resource : Resource.cachedValues()) {
          double sumOfBrokerUtilization = 0.0;
          for (Broker broker : host.brokers()) {
            sumOfBrokerUtilization += broker.load().expectedUtilizationFor(resource);
          }
          double hostUtilization = host.load().expectedUtilizationFor(resource);
          if (AnalyzerUtils.compare(sumOfBrokerUtilization, hostUtilization, resource) != 0) {
            throw new IllegalArgumentException(String.format("%s Host utilization for %s is different from the total broker "
                                                             + "utilization in the host : %s. Sum of the broker utilization: %f, "
                                                             + "host utilization: %f", prologueErrorMsg, resource, host.name(),
                                                             sumOfBrokerUtilization, hostUtilization));
          }
          sumOfHostUtilizationByResource.compute(resource, (k, v) -> (v == null ? 0 : v) + hostUtilization);
        }
      }

      // Check equality of sum of the host load to the rack load for each resource.
      for (Map.Entry<Resource, Double> entry : sumOfHostUtilizationByResource.entrySet()) {
        Resource resource = entry.getKey();
        double sumOfHostsUtil = entry.getValue();
        double rackUtilization = rack.load().expectedUtilizationFor(resource);
        if (AnalyzerUtils.compare(rackUtilization, sumOfHostsUtil, resource) != 0) {
          throw new IllegalArgumentException(String.format("%s Rack utilization for %s is different from the total host "
                                                           + "utilization in rack : %s. Sum of the host utilization: %f, "
                                                           + "rack utilization: %f", prologueErrorMsg, resource, rack.id(),
                                                           sumOfHostsUtil, rackUtilization));
        }
        sumOfRackUtilizationByResource.compute(resource, (k, v) -> (v == null ? 0 : v) + sumOfHostsUtil);
      }
    }

    // Check equality of sum of the rack load to the cluster load for each resource.
    for (Map.Entry<Resource, Double> entry : sumOfRackUtilizationByResource.entrySet()) {
      Resource resource = entry.getKey();
      double sumOfRackUtil = entry.getValue();
      double clusterUtilization = _load.expectedUtilizationFor(resource);
      if (AnalyzerUtils.compare(_load.expectedUtilizationFor(resource), sumOfRackUtil, resource) != 0) {
        throw new IllegalArgumentException(String.format("%s Cluster utilization for %s is different from the total rack "
                                                         + "utilization in the cluster. Sum of the rack utilization: %f, "
                                                         + "cluster utilization: %f", prologueErrorMsg, resource,
                                                         sumOfRackUtil, clusterUtilization));
      }
    }

    // Check equality of the sum of the leadership load to the sum of the load of leader at each broker.
    for (Broker broker : brokers()) {
      double sumOfLeaderOfReplicaUtilization = 0.0;
      for (Replica replica : broker.replicas()) {
        sumOfLeaderOfReplicaUtilization +=
            partition(replica.topicPartition()).leader().load().expectedUtilizationFor(Resource.NW_OUT);
      }
      double potentialLeadershipLoad =
          _potentialLeadershipLoadByBrokerId.get(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
      if (AnalyzerUtils.compare(sumOfLeaderOfReplicaUtilization, potentialLeadershipLoad, Resource.NW_OUT) != 0) {
        throw new IllegalArgumentException(String.format("%s Leadership utilization for %s is different from the total utilization "
                                                         + "leader of replicas in the broker with id: %d. Expected: %f Received: %f",
                                                         prologueErrorMsg, Resource.NW_OUT, broker.id(), sumOfLeaderOfReplicaUtilization,
                                                         potentialLeadershipLoad));
      }

      for (Resource resource : Resource.cachedValues()) {
        if (resource == Resource.CPU) {
          continue;
        }
        double leaderSum = broker.leaderReplicas().stream().mapToDouble(r -> r.load().expectedUtilizationFor(resource)).sum();
        double cachedLoad = broker.leadershipLoadForNwResources().expectedUtilizationFor(resource);
        if (AnalyzerUtils.compare(leaderSum, cachedLoad, resource) != 0) {
          throw new IllegalArgumentException(String.format("%s Leadership load for resource %s is %f but recomputed sum is %f",
                                                           prologueErrorMsg, resource, cachedLoad, leaderSum));
        }
      }
    }
  }

  /**
   * @param config The configurations for Cruise Control.
   * @return Broker level stats.
   */
  public BrokerStats brokerStats(KafkaCruiseControlConfig config) {
    BrokerStats brokerStats = new BrokerStats(config);
    brokers().forEach(broker -> brokerStats.addSingleBrokerStats(broker,
                                                                 potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT),
                                                                 _capacityEstimationInfoByBrokerId.get(broker.id()) != null));
    return brokerStats;
  }

  /**
   * The variance of the derived resources.
   * @return A non-null array where the ith index is the variance of RawAndDerivedResource.ordinal().
   */
  public double[] variance() {
    RawAndDerivedResource[] resources = RawAndDerivedResource.values();
    double[][] utilization = utilizationMatrix();

    double[] variance = new double[resources.length];

    for (int resourceIndex = 0; resourceIndex < resources.length; resourceIndex++) {
      Variance varianceCalculator = new Variance();
      variance[resourceIndex] = varianceCalculator.evaluate(utilization[resourceIndex]);
    }
    return variance;
  }

  /**
   *
   * @return A RawAndDerivedResource x nBroker matrix of derived resource utilization.
   */
  public double[][] utilizationMatrix() {
    RawAndDerivedResource[] resources = RawAndDerivedResource.values();
    double[][] utilization = new double[resources.length][brokers().size()];
    int brokerIndex = 0;
    for (Broker broker : brokers()) {
      double leaderBytesInRate = broker.leadershipLoadForNwResources().expectedUtilizationFor(Resource.NW_IN);
      for (RawAndDerivedResource derivedResource : resources) {
        switch (derivedResource) {
          case DISK:
          case NW_OUT:
          case CPU:
            utilization[derivedResource.ordinal()][brokerIndex] = broker.load().expectedUtilizationFor(derivedResource.derivedFrom());
            break;
          case FOLLOWER_NW_IN:
            utilization[derivedResource.ordinal()][brokerIndex] =
                broker.load().expectedUtilizationFor(derivedResource.derivedFrom()) - leaderBytesInRate;
            break;
          case LEADER_NW_IN:
            utilization[derivedResource.ordinal()][brokerIndex] = leaderBytesInRate;
            break;
          case PWN_NW_OUT:
            utilization[derivedResource.ordinal()][brokerIndex] =
                potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
            break;
          case REPLICAS:
            utilization[derivedResource.ordinal()][brokerIndex] = broker.replicas().size();
            break;
          default:
            throw new IllegalStateException("Unhandled case " + derivedResource + ".");
        }
      }
      brokerIndex++;
    }
    return utilization;
  }

  /**
   * Write to the given output stream.
   *
   * @param out Output stream.
   */
  public void writeTo(OutputStream out) throws IOException {
    String cluster = String.format("<Cluster maxPartitionReplicationFactor=\"%d\">%n", _maxReplicationFactor);
    out.write(cluster.getBytes(StandardCharsets.UTF_8));
    for (Rack rack : _racksById.values()) {
      rack.writeTo(out);
    }
    out.write("</Cluster>".getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    return String.format("ClusterModel[brokerCount=%d,partitionCount=%d,aliveBrokerCount=%d]",
                         _brokers.size(), _partitionsByTopicPartition.size(), _aliveBrokers.size());
  }

  private void refreshCapacity() {
    for (Resource r : Resource.cachedValues()) {
      double capacity = 0;
      for (Rack rack : _racksById.values()) {
        capacity += rack.capacityFor(r);
      }
      _clusterCapacity[r.id()] = capacity;
    }
  }
}
