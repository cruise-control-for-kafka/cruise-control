/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A class that holds the information of the replica, including its load, leader, topic partition, and broker. A replica
 * object is created as part of a broker structure.
 */
public class Replica implements Serializable, Comparable<Replica> {
    // Two static final variables for comparison purpose.
    public static final Replica MIN_REPLICA = new Replica(null, null, false);
    public static final Replica MAX_REPLICA = new Replica(null, null, false);
    private final TopicPartition _tp;
    private final Load _load;
    private final Broker _originalBroker;
    private boolean _isOriginalOffline;
    private Broker _broker;
    private boolean _isLeader;
    private final Disk _originalDisk;
    private Disk _disk;

    /**
     * The constructor for an online replica without disk information.
     *
     * @param tp       Topic partition information of the replica.
     * @param broker   The broker of the replica.
     * @param isLeader A flag to represent whether the replica is the isLeader or not.
     */
    Replica(TopicPartition tp, Broker broker, boolean isLeader) {
        this(tp, broker, isLeader, false, null);
    }

    /**
     * A constructor for a replica.
     *
     * @param tp                Topic partition information of the replica.
     * @param broker            The broker of the replica.
     * @param isLeader          A flag to represent whether the replica is the isLeader or not.
     * @param isOriginalOffline {@code true} if the replica is offline in its original location, {@code false} otherwise.
     * @param disk              The disk of the replica. If replica placement over disk information is not populated, this parameter is null.
     */
    Replica(TopicPartition tp, Broker broker, boolean isLeader, boolean isOriginalOffline, Disk disk) {
        _tp = tp;
        _load = new Load();
        _originalBroker = broker;
        _broker = broker;
        _isLeader = isLeader;
        _isOriginalOffline = isOriginalOffline;
        _originalDisk = disk;
        _disk = disk;
    }

    /**
     * Get the original state of the replica before rebalance.
     *
     * @return {@code true} if the replica is offline in its original location (e.g. due to broken disk or broker), {@code false} otherwise.
     */
    public boolean isOriginalOffline() {
        return _isOriginalOffline || !_originalBroker.isAlive();
    }

    /**
     * Check whether the replica is currently offline.
     *
     * @return {@code true} if the replica is currently offline (e.g. due to broken disk or broker), {@code false} otherwise.
     */
    public boolean isCurrentOffline() {
        return (isOriginalOffline() && _broker.id() == _originalBroker.id()) || !_broker.isAlive();
    }

    /**
     * Package private for unit test.
     * Mark the original replica as offline and add it to current offline replicas of the original broker.
     */
    void markOriginalOffline() {
        if (_broker.id() != _originalBroker.id()) {
            throw new IllegalStateException("Cannot mark an immigrant replica as offline.");
        }
        _isOriginalOffline = true;
        _originalBroker.currentOfflineReplicas().add(this);
    }

    /**
     * @return The topic partition.
     */
    public TopicPartition topicPartition() {
        return _tp;
    }

    /**
     * @return The replica load for each resource. Replicas always have an associated load.
     */
    public Load load() {
        return _load;
    }

    /**
     * @return The original broker of this replica before rebalance.
     */
    public Broker originalBroker() {
        return _originalBroker;
    }

    /**
     * @return The broker that the replica resides in.
     */
    public Broker broker() {
        return _broker;
    }

    /**
     * @return {@code true} if the replica is leader, {@code false} otherwise.
     */
    public boolean isLeader() {
        return _isLeader;
    }

    /**
     * @return {@code true} if the replica is an immigrant replica of the broker, {@code false} otherwise.
     */
    public boolean isImmigrant() {
        return _originalBroker != _broker;
    }

    /**
     * Set broker that the replica resides in.
     *
     * @param broker Broker that the replica resides in.
     */
    void setBroker(Broker broker) {
        _broker = broker;
    }

    /**
     * Set disk that the replica resides in.
     *
     * @param disk Disk that the replica resides in.
     */
    void setDisk(Disk disk) {
        _disk = disk;
    }

    /**
     * Get disk that the replica originally resides in. If the replica-over-disk placement information is not populated in
     * {@link ClusterModel}, null will be returned.
     *
     * @return The original disk of the replica.
     */
    public Disk originalDisk() {
        return _originalDisk;
    }

    /**
     * Get disk that the replica resides in. If the replica-over-disk placement information is not populated in
     * {@link ClusterModel}, null will be returned.
     *
     * @return Disk that the replica resides in.
     */
    public Disk disk() {
        return _disk;
    }

    /**
     * Set Leadership status of the broker
     *
     * @param leader {@code true} if leader, {@code false} otherwise.
     */
    void setLeadership(boolean leader) {
        _isLeader = leader;
    }

    /**
     * Pushes the latest snapshot information containing the snapshot time and resource loads for the replica.
     *
     * @param aggregatedMetricValues The metric values for this replica.
     * @param windows                the windows list of the aggregated metric values.
     */
    void setMetricValues(AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
        _load.initializeMetricValues(aggregatedMetricValues, windows);
    }

    /**
     * Clear the content of monitoring data at each replica in the broker.
     */
    void clearLoad() {
        _load.clearLoad();
    }

    /**
     * (1) Remove leadership from the replica.
     * (2) Clear and get the outbound network load associated with leadership from the given replica.
     * (3) Clear and get the CPU leadership load associated with leadership from the given replica.
     *
     * @return Removed leadership load by windows -- i.e. outbound network and fraction of CPU load by windows.
     */
    AggregatedMetricValues makeFollower() {
        // Remove leadership from the replica.
        AggregatedMetricValues leaderLoadDelta = leaderLoadDelta(true);
        setLeadership(false);
        return leaderLoadDelta;
    }

    /**
     * Get the change of the load when this replica becomes a follower replica. When updateLoad is set to true,
     * the change is actually made to the replica. Otherwise, no change is made.
     *
     * @param updateLoad whether the change to the load should actually be made to the replica
     * @return The change of the load when this replica becomes follower replica.
     */
    private AggregatedMetricValues leaderLoadDelta(boolean updateLoad) {
        if (!_isLeader) {
            throw new IllegalArgumentException("This method can only be invoked on a leader replica.");
        }
        // Get the inbound/outbound network and cpu load associated with leadership from the given replica.
        // All the following metric values are in a shared mode to avoid data copy.
        // Just get the first metric id because CPU only has one metric id in the group. Eventually the per replica
        // CPU utilization will be removed to use resource estimation at broker level.
        short cpuMetricId = KafkaMetricDef.resourceToMetricIds(Resource.CPU).get(0);
        AggregatedMetricValues leadershipNwOutLoad = _load.loadFor(Resource.NW_OUT, true);

        // Create a leadership load delta to store the load change.
        AggregatedMetricValues leadershipLoadDelta = new AggregatedMetricValues();

        // Compute the cpu delta, the order matters here, we need to compute cpu load change before the network outbound
        // load is cleared.
        MetricValues cpuLoadChange = computeCpuLoadAsFollower(leadershipNwOutLoad, updateLoad);
        leadershipLoadDelta.add(cpuMetricId, cpuLoadChange);

        // We need to add the NW_OUT values to the delta before clearing the metric.
        leadershipLoadDelta.add(leadershipNwOutLoad);

        // Remove the outbound network leadership load from replica.
        if (updateLoad) {
            _load.clearLoadFor(Resource.NW_OUT);
        }

        // Return removed leadership load.
        return leadershipLoadDelta;
    }

    /**
     * Get the expected load if this replica becomes a follower replica.
     *
     * @return The expected load.
     */
    public Load getFollowerLoadFromLeader() {
        Load load = new Load();
        load.initializeMetricValues(_load.loadByWindows(), _load.windows());
        load.subtractLoad(leaderLoadDelta(false));
        return load;
    }

    /**
     * Update the CPU load as the replica becomes follower. Return the CPU load change.
     *
     * @param leadershipNwOutLoad the leadership network outbound bytes rate.
     * @param updateLoad          whether the change to the load should actually be made to the replica
     * @return The cpu load change.
     */
    private MetricValues computeCpuLoadAsFollower(AggregatedMetricValues leadershipNwOutLoad, boolean updateLoad) {
        // Just get the first metric id because CPU only has one metric id in the group. Eventually the per replica
        // CPU utilization will be removed to use resource estimation at broker level.
        short cpuMetricId = KafkaMetricDef.resourceToMetricIds(Resource.CPU).get(0);
        // Use the shared data structure so we can set the load directly.
        MetricValues cpuLoad = _load.loadFor(Resource.CPU, true).valuesFor(cpuMetricId);
        AggregatedMetricValues leadershipNwInLoad = _load.loadFor(Resource.NW_IN, true);

        MetricValues cpuLoadChange = new MetricValues(_load.numWindows());
        MetricValues totalNetworkOutLoad =
                leadershipNwOutLoad.valuesForGroup(Resource.NW_OUT.name(), KafkaMetricDef.commonMetricDef(), false);
        MetricValues totalNetworkInLoad =
                leadershipNwInLoad.valuesForGroup(Resource.NW_IN.name(), KafkaMetricDef.commonMetricDef(), false);
        for (int i = 0; i < cpuLoad.length(); i++) {
            double newCpuLoad = ModelUtils.getFollowerCpuUtilFromLeaderLoad(totalNetworkInLoad.get(i),
                    totalNetworkOutLoad.get(i),
                    cpuLoad.get(i));
            // The order matters here. We have to first set the cpu load change, then update the cpu load for this replica.
            cpuLoadChange.set(i, cpuLoad.get(i) - newCpuLoad);
            if (updateLoad) {
                cpuLoad.set(i, newCpuLoad);
            }
        }
        return cpuLoadChange;
    }

    /**
     * (1) Add leadership to the replica.
     * (2) Set the outbound network load associated with leadership.
     * (3) Add the CPU load associated with leadership.
     *
     * @param leadershipLoadDelta Resource to leadership load to be added by windows.
     */
    void makeLeader(AggregatedMetricValues leadershipLoadDelta) {
        // Add leadership to the replica.
        setLeadership(true);
        _load.addLoad(leadershipLoadDelta);
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        Map<String, Object> replicaMap = new HashMap<>();
        replicaMap.put(ModelUtils.IS_LEADER, _isLeader);
        replicaMap.put(ModelUtils.BROKER_ID, _broker.id());
        replicaMap.put(ModelUtils.TOPIC, _tp.topic());
        replicaMap.put(ModelUtils.PARTITION, _tp.partition());
        replicaMap.put(ModelUtils.LOAD, _load.getJsonStructure());
        return replicaMap;
    }

    /**
     * Output writing string representation of this class to the stream.
     *
     * @param out the output stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(String.format("<Replica isLeader=\"%s\" id=\"%d\">%n%s", isLeader(), _broker.id(), _tp).getBytes(StandardCharsets.UTF_8));
        _load.writeTo(out);
        out.write("</Replica>%n".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Get string representation of the {@link Replica}.
     */
    @Override
    public String toString() {
        return String.format(
                "Replica[isLeader=%s,rack=%s,broker=%d,TopicPartition=%s,origBroker=%d,isOriginalOffline=%s,isCurrentOffline=%s]",
                _isLeader, _broker.rack().id(), _broker.id(), _tp,
                _originalBroker == null ? -1 : _originalBroker.id(), isOriginalOffline(), isCurrentOffline());
    }

    /**
     * Compare by (1) offline / online status (2) partition id then (2) original broker id then (3) topic name.
     */
    @Override
    public int compareTo(Replica o) {
        // Primary sort: by offline / online status
        boolean isR1Offline = isCurrentOffline();
        boolean isR2Offline = o.isCurrentOffline();

        if (isR1Offline && !isR2Offline) {
            return -1;
        } else if (!isR1Offline && isR2Offline) {
            return 1;
        }

        // Secondary sort: by partition id.
        if (_tp.partition() > o.topicPartition().partition()) {
            return 1;
        } else if (_tp.partition() < o.topicPartition().partition()) {
            return -1;
        }

        // Tertiary sort: by original broker id.
        if (_originalBroker.id() > o.originalBroker().id()) {
            return 1;
        } else if (_originalBroker.id() < o.originalBroker().id()) {
            return -1;
        }

        // Final sort: by topic name.
        return _tp.topic().compareTo(o.topicPartition().topic());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Replica replica = (Replica) o;
        return Objects.equals(_tp, replica._tp) && _originalBroker.id() == replica.originalBroker().id();
    }

    @Override
    public int hashCode() {
        return Objects.hash(_tp, _originalBroker.id());
    }
}
