/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;

/**
 * The execution proposal corresponding to a particular partition.
 */
@JsonResponseClass
public class ExecutionProposal {
  @JsonResponseField
  private static final String TOPIC_PARTITION = "topicPartition";
  @JsonResponseField
  private static final String OLD_LEADER = "oldLeader";
  @JsonResponseField
  private static final String OLD_REPLICAS = "oldReplicas";
  @JsonResponseField
  private static final String NEW_REPLICAS = "newReplicas";

  private final TopicPartition _tp;
  private final long _partitionSize;
  private final ReplicaPlacementInfo _oldLeader;
  private final List<ReplicaPlacementInfo> _oldReplicas;
  private final List<ReplicaPlacementInfo> _newReplicas;
  // Replicas to add are the replicas which are originally not hosted by the broker.
  private final Set<ReplicaPlacementInfo> _replicasToAdd;
  // Replicas to remove are the replicas which are no longer hosted by the broker.
  private final Set<ReplicaPlacementInfo> _replicasToRemove;
  // Replicas to move between disks are the replicas which are to be hosted by a different disk of the same broker.
  private final Map<Integer, ReplicaPlacementInfo> _replicasToMoveBetweenDisksByBroker;
  private final Set<Integer> _oldReplicasSet;
  private final Set<Integer> _newReplicasSet;

  /**
   * Construct an execution proposals.
   * Note that the new leader will be the first replica in the new replica list after the proposal is executed.
   *
   * @param tp the topic partition of this execution proposal
   * @param partitionSize the size of the partition.
   * @param oldLeader the old leader of the partition to determine if leader movement is needed.
   * @param oldReplicas the old replicas for rollback. (Rollback is not supported until KAFKA-6304)
   * @param newReplicas the new replicas of the partition in this order.
   */
  public ExecutionProposal(TopicPartition tp,
                           long partitionSize,
                           ReplicaPlacementInfo oldLeader,
                           List<ReplicaPlacementInfo> oldReplicas,
                           List<ReplicaPlacementInfo> newReplicas) {
    _tp = tp;
    _partitionSize = partitionSize;
    _oldLeader = oldLeader;
    // Allow the old replicas to be empty for partition addition.
    _oldReplicas = oldReplicas == null ? Collections.emptyList() : oldReplicas;
    _newReplicas = newReplicas;
    validate();

    // Populate replicas to add, to remove and to move across disk.
    _newReplicasSet = _newReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toSet());
    _oldReplicasSet = _oldReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toSet());
    _replicasToAdd = _newReplicas.stream().filter(r -> !_oldReplicasSet.contains(r.brokerId())).collect(Collectors.toSet());
    _replicasToRemove = _oldReplicas.stream().filter(r -> !_newReplicasSet.contains(r.brokerId())).collect(Collectors.toSet());
    _replicasToMoveBetweenDisksByBroker = new HashMap<>();
    newReplicas.stream().filter(r -> !_replicasToAdd.contains(r) && !_oldReplicas.contains(r))
               .forEach(r -> _replicasToMoveBetweenDisksByBroker.put(r.brokerId(), r));

    // Verify the proposal will not generate both inter-broker movement and intra-broker replica movement at the same time.
    if (!_replicasToAdd.isEmpty() && !_replicasToMoveBetweenDisksByBroker.isEmpty()) {
      throw new IllegalArgumentException("Change from " + _oldReplicas + " to " + _newReplicas + " will generate both "
                                         + "intra-broker and inter-broker replica movements.");
    }
  }

  private static boolean brokerOrderMatched(Node[] currentOrderedReplicas, List<ReplicaPlacementInfo> replicas) {
    if (replicas.size() != currentOrderedReplicas.length) {
      return false;
    }

    for (int i = 0; i < replicas.size(); i++) {
      if (currentOrderedReplicas[i].id() != replicas.get(i).brokerId()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Package private for unit test.
   * Check whether all replicas of the given partition state are in-sync. This check is resilient against partitions with
   * {@code ISR set > replica set}. Hence, even if the given partition has extra broker ids in the in-sync replica set, as long as all the
   * replicas of the partition are in the in-sync replica set, this function will return {@code true}.
   *
   * @param partitionInfo Current partition state.
   * @return {@code true} if all replicas of the given partition state are in-sync, {@code false} otherwise.
   */
  static boolean areAllReplicasInSync(PartitionInfo partitionInfo) {
    return Arrays.asList(partitionInfo.inSyncReplicas()).containsAll(Arrays.asList(partitionInfo.replicas()));
  }

  /**
   * Check whether the successful completion of inter-broker replica movement from this proposal is reflected in the current
   * ordered replicas in the given cluster and all replicas are in-sync.
   *
   * @param partitionInfo Current partition state.
   * @return {@code true} if successfully completed, {@code false} otherwise.
   */
  public boolean isInterBrokerMovementCompleted(PartitionInfo partitionInfo) {
    return brokerOrderMatched(partitionInfo.replicas(), _newReplicas) && areAllReplicasInSync(partitionInfo);
  }

  /**
   * Check whether the abortion of inter-broker replica movement from this proposal is reflected in the current ordered
   * replicas in the given cluster.
   * There could be a race condition that when we abort a task, it is already completed.
   * In that case, we treat it as aborted as well.
   *
   * @param partitionInfo Current partition state.
   * @return {@code true} if aborted, {@code false} otherwise.
   */
  public boolean isInterBrokerMovementAborted(PartitionInfo partitionInfo) {
    return isInterBrokerMovementCompleted(partitionInfo)
           || brokerOrderMatched(partitionInfo.replicas(), _oldReplicas);
  }

  /**
   * @return The topic for this proposal.
   */
  public String topic() {
    return _tp.topic();
  }

  /**
   * @return The partition id for this proposal.
   */
  public int partitionId() {
    return _tp.partition();
  }

  /**
   * @return The TopicPartition of this proposal.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * @return The old leader of the partition before the executing the proposal.
   */
  public ReplicaPlacementInfo oldLeader() {
    return _oldLeader;
  }

  /**
   * @return The new leader of the partition after executing the proposal.
   */
  public ReplicaPlacementInfo newLeader() {
    return _newReplicas.get(0);
  }

  /**
   * @return The replica list of the partition before executing the proposal.
   */
  public List<ReplicaPlacementInfo> oldReplicas() {
    return Collections.unmodifiableList(_oldReplicas);
  }

  /**
   * @return The broker ID set of the partitions before executing the proposal.
   */
  public Set<Integer> oldReplicasBrokerIdSet() {
    return Collections.unmodifiableSet(_oldReplicasSet);
  }

  /**
   * @return The broker ID set of the partitions after executing the proposal.
   */
  public Set<Integer> newReplicasBrokerIdSet() {
    return Collections.unmodifiableSet(_newReplicasSet);
  }

  /**
   * @return The new replica list fo the partition after executing the proposal.
   */
  public List<ReplicaPlacementInfo> newReplicas() {
    return Collections.unmodifiableList(_newReplicas);
  }

  /**
   * @return The replicas that exist in new replica list but not in old replica list.
   */
  public Set<ReplicaPlacementInfo> replicasToAdd() {
    return Collections.unmodifiableSet(_replicasToAdd);
  }

  /**
   * @return The replicas that exist in old replica list but not in the new replica list.
   */
  public Set<ReplicaPlacementInfo> replicasToRemove() {
    return Collections.unmodifiableSet(_replicasToRemove);
  }

  /**
   * @return The replicas that exist in both old and new replica list but its hosted disk has changed,
   *         which means an intra-broker replica movement for is needed for these replicas.
   */
  public Map<Integer, ReplicaPlacementInfo> replicasToMoveBetweenDisksByBroker() {
    return Collections.unmodifiableMap(_replicasToMoveBetweenDisksByBroker);
  }

  /**
   * @return Whether the proposal involves a replica action. (replica movement, addition, deletion, or swap -- exclude reorder)
   */
  public boolean hasReplicaAction() {
    return !new HashSet<>(_oldReplicas).equals(new HashSet<>(_newReplicas));
  }

  /**
   * @return whether the proposal involves a leader action. i.e. leader movement.
   */
  public boolean hasLeaderAction() {
    return !_oldLeader.equals(_newReplicas.get(0));
  }

  /**
   * @return The total number of bytes to move across brokers involved in this proposal.
   */
  public long interBrokerDataToMoveInMB() {
    return _replicasToAdd.size() * _partitionSize;
  }

  /**
   * @return The total number of bytes to move across disks within the broker involved in this proposal.
   *         Note for intra-broker replica movement on a broker, the amount of data to move across disk is
   *         always the size of the replica since one broker can only host one replica of the topic partition.
   */
  public long intraBrokerDataToMoveInMB() {
    return _partitionSize;
  }

  /**
   * @return The total number of bytes to move involved in this proposal.
   */
  public long dataToMoveInMB() {
    return (_replicasToAdd.size() + _replicasToMoveBetweenDisksByBroker.size()) * _partitionSize;
  }

  private void validate() {
    // Verify old leader exists.
    if (_oldLeader.brokerId() >= 0 && !_oldReplicas.contains(_oldLeader)) {
      throw new IllegalArgumentException(String.format("The old leader %s does not exist in the old replica list %s",
                                                       _oldLeader, _oldReplicas));
    }
    // verify empty new replica list.
    if (_newReplicas == null || _newReplicas.isEmpty()) {
      throw new IllegalArgumentException("The new replica list " + _newReplicas + " cannot be empty.");
    }
    // Verify duplicates
    Set<ReplicaPlacementInfo> checkSet = new HashSet<>(_newReplicas);
    if (checkSet.size() != _newReplicas.size()) {
      throw new IllegalArgumentException("The new replicas list " + _newReplicas + " has duplicate replica.");
    }
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    return Map.of(TOPIC_PARTITION, _tp, OLD_LEADER, _oldLeader.brokerId(),
                  OLD_REPLICAS, _oldReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList()),
                  NEW_REPLICAS, _newReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return String.format("{%s, oldLeader: %d, %s -> %s}", _tp, _oldLeader.brokerId(),
                         _oldReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList()),
                         _newReplicas.stream().mapToInt(ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExecutionProposal)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    ExecutionProposal otherProposal = (ExecutionProposal) other;

    return _tp.equals(otherProposal._tp)
        && _oldLeader == otherProposal._oldLeader
        && _oldReplicas.equals(otherProposal._oldReplicas)
        && _newReplicas.equals(otherProposal._newReplicas);
  }

  @Override
  public int hashCode() {
    int result = _tp.hashCode();
    result = 31 * result + _oldLeader.hashCode();
    for (ReplicaPlacementInfo replica : _oldReplicas) {
      result = 31 * result + replica.hashCode();
    }
    for (ReplicaPlacementInfo replica : _newReplicas) {
      result = 31 * replica.hashCode();
    }
    return result;
  }
}
