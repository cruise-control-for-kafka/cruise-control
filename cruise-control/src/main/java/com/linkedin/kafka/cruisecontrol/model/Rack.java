/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;

/**
 * A class that holds the information of the rack, including its topology, liveness and load for brokers, and
 * replicas. A rack object is created as part of a cluster structure.
 */
public class Rack implements Serializable {
    @Serial
    private static final long serialVersionUID = 6866290448556002509L;
    private final String _id;
    private final Map<String, Host> _hosts;
    private final Map<Integer, Broker> _brokers;
    private final Load _load;
    private final double[] _rackCapacity;

    /**
     * Constructor of the rack class using the given id to identify the rack.
     *
     * @param id An identifier for this rack.
     */
    Rack(String id) {
        _id = id;
        _hosts = new HashMap<>();
        _brokers = new HashMap<>();
        // Initially rack does not contain any load -- cannot create a load with a specific window size.
        _load = new Load();
        _rackCapacity = new double[Resource.cachedValues().size()];
    }

    /**
     * @return The rack load information.
     */
    public Load load() {
        return _load;
    }

    /**
     * @return The rack Id.
     */
    public String id() {
        return _id;
    }

    /**
     * @return The collection of brokers in the current rack.
     */
    public Collection<Broker> brokers() {
        return _brokers.values();
    }

    /**
     * @return The hosts in this rack.
     */
    public Collection<Host> hosts() {
        return _hosts.values();
    }

    /**
     * Get the broker with the given broker id.
     *
     * @param brokerId Id of the queried broker.
     * @return The broker with the id if it is found in the rack; null otherwise.
     */
    public Broker broker(int brokerId) {
        return _brokers.get(brokerId);
    }

    /**
     * @return The list of replicas in the rack.
     */
    public List<Replica> replicas() {
        List<Replica> replicas = new ArrayList<>();

        for (Host host : _hosts.values()) {
            replicas.addAll(host.replicas());
        }
        return replicas;
    }

    /**
     * @return A set of topic names in the cluster.
     */
    public Set<String> topics() {
        Set<String> topics = new HashSet<>();

        for (Broker broker : _brokers.values()) {
            topics.addAll(broker.topics());
        }
        return topics;
    }

    /**
     * Get rack capacity for the requested resource. Rack capacity represents the total capacity of the live
     * brokers in the rack for the requested resource.
     *
     * @param resource Resource for which capacity will be provided.
     * @return Alive rack capacity of the resource.
     */
    public double capacityFor(Resource resource) {
        return _rackCapacity[resource.id()];
    }

    /**
     * Checks if rack has at least one alive servers. If none of the servers is alive, rack is considered dead.
     *
     * @return {@code true} if rack is alive, {@code false} otherwise.
     */
    public boolean isRackAlive() {
        for (Host host : _hosts.values()) {
            if (host.isAlive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the rack is alive and allowed replica moves.
     *
     * @param optimizationOptions Options to use in checking if this rack has hosts that are alive and allowed replica moves.
     * @return Whether the rack is alive and allowed replica moves. The rack is alive and allowed replica moves as long as
     * it has at least one host alive which is not excluded for replica moves.
     */
    public boolean isAliveAndAllowedReplicaMoves(OptimizationOptions optimizationOptions) {
        for (Host host : _hosts.values()) {
            if (host.isAliveAndAllowedReplicaMoves(optimizationOptions)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the removed replica from the rack.
     *
     * @param brokerId Id of the broker containing the
     * @param tp       Topic partition of the replica to be removed.
     * @return The requested replica if the id exists in the rack and the partition is found in the broker, and
     * null otherwise.
     */
    Replica removeReplica(int brokerId, TopicPartition tp) {
        Broker broker = _brokers.get(brokerId);
        if (broker != null) {
            // Remove the replica and the associated load from the broker that it resides in.
            Replica removedReplica = broker.host().removeReplica(brokerId, tp);
            // Remove the load of the removed replica from the recent load of the rack.
            _load.subtractLoad(removedReplica.load());
            // Return the removed replica.
            return removedReplica;
        }

        return null;
    }

    /**
     * Add this replica and related load to the destination broker / destination rack.
     *
     * @param replica Replica to be added to the cluster.
     */
    void addReplica(Replica replica) {
        replica.broker().host().addReplica(replica);
        // Add replica load to the recent rack load.
        _load.addLoad(replica.load());
    }

    /**
     * (1) Make the replica with the given topic partition and brokerId a follower.
     * (2) Remove and get the outbound network load associated with leadership from the given replica.
     *
     * @param brokerId Id of the broker containing the replica.
     * @param tp       TopicPartition of the replica for which the outbound network load will be removed.
     * @return Leadership load by snapshot time.
     */
    AggregatedMetricValues makeFollower(int brokerId, TopicPartition tp) {
        Host host = _brokers.get(brokerId).host();
        AggregatedMetricValues leadershipLoadDelta = host.makeFollower(brokerId, tp);
        // Remove leadership load from recent load.
        _load.subtractLoad(leadershipLoadDelta);
        return leadershipLoadDelta;
    }

    /**
     * (1) Make the replica with the given topic partition and brokerId the leader.
     * (2) Add the outbound network load associated with leadership to the given replica.
     * (3) Add the CPU load associated with leadership.
     *
     * @param brokerId            Id of the broker containing the replica.
     * @param tp                  TopicPartition of the replica for which the outbound network load will be added.
     * @param leadershipLoadDelta Resource to leadership load to be added by windows.
     */
    void makeLeader(int brokerId,
                    TopicPartition tp,
                    AggregatedMetricValues leadershipLoadDelta) {
        Host host = _brokers.get(brokerId).host();
        host.makeLeader(brokerId, tp, leadershipLoadDelta);
        // Add leadership load to recent load.
        _load.addLoad(leadershipLoadDelta);
    }

    /**
     * Clear the content of monitoring data at each replica in the rack.
     * Typically, if a change is detected in topology, this method is called to clear the monitoring data collected
     * with the old topology.
     */
    void clearLoad() {
        _hosts.values().forEach(Host::clearLoad);
        _load.clearLoad();
    }

    /**
     * Set the replica load.
     *
     * @param brokerId               Broker Id containing the replica with the given topic partition.
     * @param tp                     Topic partition that identifies the replica in this broker.
     * @param aggregatedMetricValues The metric values for this replica.
     * @param windows                The windows list of the aggregated metric values.
     */
    void setReplicaLoad(int brokerId, TopicPartition tp, AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
        Host host = _brokers.get(brokerId).host();
        host.setReplicaLoad(brokerId, tp, aggregatedMetricValues, windows);
        // Update the recent load of this rack.
        _load.addMetricValues(aggregatedMetricValues, windows);
    }

    /**
     * Create a broker under this rack, and get the created broker.
     *
     * @param brokerId                     Id of the broker to be created.
     * @param hostName                     The hostName of the broker
     * @param brokerCapacityInfo           Capacity information of the created broker.
     * @param populateReplicaPlacementInfo Whether populate replica placement over disk information or not.
     * @return Created broker.
     */
    Broker createBroker(int brokerId,
                        String hostName,
                        BrokerCapacityInfo brokerCapacityInfo,
                        boolean populateReplicaPlacementInfo) {
        Host host = _hosts.computeIfAbsent(hostName, name -> new Host(name, this));
        Broker broker = host.createBroker(brokerId, brokerCapacityInfo, populateReplicaPlacementInfo);
        _brokers.put(brokerId, broker);
        for (Map.Entry<Resource, Double> entry : brokerCapacityInfo.capacity().entrySet()) {
            Resource resource = entry.getKey();
            _rackCapacity[resource.id()] += (resource == Resource.CPU) ? (entry.getValue() * brokerCapacityInfo.numCpuCores())
                    : entry.getValue();
        }
        return broker;
    }

    /**
     * Set the broker state and update the capacity
     *
     * @param brokerId Broker id.
     * @param newState The new state of the broker.
     */
    void setBrokerState(int brokerId, Broker.State newState) {
        // Broker is dead
        Broker broker = broker(brokerId);
        broker.host().setBrokerState(brokerId, newState);
        for (Resource r : Resource.cachedValues()) {
            double capacity = 0;
            for (Host h : _hosts.values()) {
                if (h.isAlive()) {
                    capacity += h.capacityFor(r);
                }
            }
            _rackCapacity[r.id()] = capacity;
        }
    }

    /**
     * Mark specified disk dead and update the capacity.
     *
     * @param brokerId The id of broker which host the disk.
     * @param logdir   Log directory of the disk.
     */
    void markDiskDead(int brokerId, String logdir) {
        Broker broker = broker(brokerId);
        double capacityLost = broker.host().markDiskDead(brokerId, logdir);
        _rackCapacity[DISK.id()] -= capacityLost;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        List<Object> hostList = new ArrayList<>();
        for (Host host : _hosts.values()) {
            hostList.add(host.getJsonStructure());
        }
        Map<String, Object> rackMap = new HashMap<>();
        rackMap.put(ModelUtils.RACK_ID, _id);
        rackMap.put(ModelUtils.HOSTS, hostList);
        return rackMap;
    }

    /**
     * Output writing string representation of this class to the stream.
     *
     * @param out the output stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        String rack = String.format("<Rack id=\"%s\">%n", _id);
        out.write(rack.getBytes(StandardCharsets.UTF_8));
        for (Host host : _hosts.values()) {
            host.writeTo(out);
        }
        out.write("</Rack>%n".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Get string representation of {@link Rack}.
     */
    @Override
    public String toString() {
        return "Rack{" + "_id=\"" + _id + "\", _hosts=" + _hosts.size() + ", _brokers=" + _brokers.size() + ", _load=" + _load + '}';
    }
}
