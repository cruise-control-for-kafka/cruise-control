/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.*;


/**
 * A partition consists of replicas with the same topic partition. One replica is the leader, the rest of the replicas
 * are followers.
 */
public class Partition implements Serializable {
    private final TopicPartition _tp;
    private final List<Replica> _replicas;
    private Replica _leader;
    // Set of brokers which are unable to host replica of this partition.
    private final Set<Broker> _ineligibleBrokers;

    /**
     * Constructor for Partition class.
     *
     * @param tp Topic partition information for the replica in this partition,
     */
    Partition(TopicPartition tp) {
        _tp = tp;
        _replicas = new ArrayList<>();
        _leader = null;
        _ineligibleBrokers = new HashSet<>();
    }

    /**
     * @return The topic partition of this partition.
     */
    public TopicPartition topicPartition() {
        return _tp;
    }

    /**
     * Add follower to the partition.
     *
     * @param follower Follower replica.
     * @param index    the index the follower should be at.
     */
    void addFollower(Replica follower, int index) {
        if (follower.isLeader()) {
            throw new IllegalArgumentException("Inconsistent leadership information. Trying to add follower replica "
                    + follower + " while it is a leader.");
        }
        if (!follower.topicPartition().equals(_tp)) {
            throw new IllegalArgumentException("Inconsistent topic partition. Trying to add follower replica " + follower
                    + " to partition " + _tp + ".");
        }
        // Add follower to the list of followers.
        _replicas.add(index, follower);
    }

    /**
     * Delete a replica from partition.
     *
     * @param brokerId Id of broker which host the replica to be deleted.
     */
    void deleteReplica(int brokerId) {
        _replicas.removeIf(r -> r.broker().id() == brokerId);
    }

    /**
     * @return The replica list.
     */
    public List<Replica> replicas() {
        return Collections.unmodifiableList(_replicas);
    }

    /**
     * @return All follower replicas.
     */
    public List<Replica> followers() {
        List<Replica> followers = new ArrayList<>();
        _replicas.forEach(r -> {
            if (!r.isLeader()) {
                followers.add(r);
            }
        });
        return followers;
    }

    /**
     * @return Online follower replicas.
     */
    public List<Replica> onlineFollowers() {
        List<Replica> onlineFollowers = new ArrayList<>();
        for (Replica follower : followers()) {
            if (!follower.isCurrentOffline()) {
                onlineFollowers.add(follower);
            }
        }
        return onlineFollowers;
    }

    /**
     * @return The leader replica.
     */
    public Replica leader() {
        return _leader;
    }

    /**
     * Get replica with the given broker id in this partition.
     *
     * @param brokerId Broker id of the requested replica in this partition.
     * @return Replica with the given broker id in this partition.
     */
    Replica replica(long brokerId) {
        for (Replica replica : _replicas) {
            if (replica.broker().id() == brokerId) {
                return replica;
            }
        }

        throw new IllegalArgumentException("Requested replica " + brokerId + " is not a replica of partition " + _tp);
    }

    /**
     * @return The set of brokers that followers reside in.
     */
    public List<Broker> followerBrokers() {
        List<Broker> followerBrokers = new ArrayList<>();
        _replicas.forEach(r -> {
            if (!r.isLeader()) {
                followerBrokers.add(r.broker());
            }
        });
        return followerBrokers;
    }

    /**
     * @return The set of brokers that online followers reside in.
     */
    public List<Broker> onlineFollowerBrokers() {
        List<Broker> onlineFollowerBrokers = new ArrayList<>();
        _replicas.forEach(r -> {
            if (!r.isLeader() && !r.isCurrentOffline()) {
                onlineFollowerBrokers.add(r.broker());
            }
        });
        return onlineFollowerBrokers;
    }

    /**
     * Given two follower indices in the replica list, swap their positions.
     *
     * @param index1 The index of the first follower to be swapped.
     * @param index2 The index of the second follower to be swapped
     */
    public void swapFollowerPositions(int index1, int index2) {
        Replica follower1 = _replicas.get(index1);
        Replica follower2 = _replicas.get(index2);

        if (follower1.isLeader() || follower2.isLeader()) {
            throw new IllegalArgumentException(String.format("%s is not a follower.",
                    follower1.isLeader() ? follower1 : follower2));
        }
        _replicas.set(index2, follower1);
        _replicas.set(index1, follower2);
    }

    /**
     * Given two replica indices in the replica list, swap their positions.
     *
     * @param index1 The index of the first replica to be swapped.
     * @param index2 The index of the second replica to be swapped
     */
    public void swapReplicaPositions(int index1, int index2) {
        Replica replica1 = _replicas.get(index1);
        Replica replica2 = _replicas.get(index2);

        _replicas.set(index2, replica1);
        _replicas.set(index1, replica2);
    }

    /**
     * Move a replica to the end of the replica list.
     *
     * @param replica the replica to move to the end.
     */
    public void moveReplicaToEnd(Replica replica) {
        if (!_replicas.remove(replica)) {
            throw new IllegalStateException(String.format("Did not find replica %s for partition %s.", replica, _tp));
        }
        _replicas.add(replica);
    }

    /**
     * @return The set of brokers that contain replicas of the partition.
     */
    public Set<Broker> partitionBrokers() {
        Set<Broker> partitionBrokers = new HashSet<>();
        _replicas.forEach(r -> partitionBrokers.add(r.broker()));
        return partitionBrokers;
    }

    /**
     * @return The set of racks that contains replicas of the partition.
     */
    public Set<Rack> partitionRacks() {
        Set<Rack> partitionRacks = new HashSet<>();
        _replicas.forEach(r -> partitionRacks.add(r.broker().rack()));
        return partitionRacks;
    }

    /**
     * Set the leader to the value specified by the leader parameter.
     *
     * @param leader Leader replica of partition.
     * @param index  the index the leader replica should be at.
     */
    void addLeader(Replica leader, int index) {
        if (_leader != null) {
            throw new IllegalArgumentException(String.format("Partition %s already has a leader replica %s. Cannot "
                    + "add a new leader replica %s", _tp, _leader, leader));
        }
        if (!leader.isLeader()) {
            throw new IllegalArgumentException("Inconsistent leadership information. Trying to set " + leader.broker()
                    + " as the leader for partition " + _tp + " while the replica is not marked "
                    + "as a leader.");
        }
        _leader = leader;
        _replicas.add(index, leader);
    }

    /**
     * Relocate leadership.
     * Move the leader replica to the head of the replica list.
     *
     * @param prospectiveLeader Prospective leader.
     */
    void relocateLeadership(Replica prospectiveLeader) {
        int leaderPos = _replicas.indexOf(prospectiveLeader);
        swapReplicaPositions(0, leaderPos);
        _leader = prospectiveLeader;
    }

    /**
     * Clear the leader to null and clear followers.
     */
    public void clear() {
        _replicas.clear();
        _leader = null;
    }

    /**
     * Get string representation of Partition in XML format.
     */
    @Override
    public String toString() {
        StringBuilder partition = new StringBuilder().append(String.format("<Partition>%n<Leader>%s</Leader>%n", _leader));

        for (Replica replica : _replicas) {
            if (!replica.isLeader()) {
                partition.append(String.format("<Follower>%s</Follower>%n", replica));
            }
        }
        return partition.append("</Partition>%n").toString();
    }

    /**
     * Record the broker which is unable to host the replica of the partition.
     *
     * @param ineligibleBroker The ineligible broker.
     */
    public void addIneligibleBroker(Broker ineligibleBroker) {
        _ineligibleBrokers.add(ineligibleBroker);
    }

    /**
     * Check if the broker is eligible to host the replica of the partition.
     *
     * @param candidateBroker The candidate broker.
     * @return {@code true} if the broker is eligible to host the replica of the partition, {@code false} otherwise.
     */
    public boolean canAssignReplicaToBroker(Broker candidateBroker) {
        return !_ineligibleBrokers.contains(candidateBroker);
    }
}
