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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;

public class Host implements Serializable {
    private static final double DEAD_HOST_CAPACITY = -1.0;
    private final Map<Integer, Broker> _brokers;
    private final Set<Replica> _replicas;
    private final Rack _rack;
    private final String _name;
    private final Load _load;
    private final double[] _hostCapacity;
    private int _aliveBrokers;

    Host(String name, Rack rack) {
        _name = name;
        _brokers = new HashMap<>();
        _replicas = new HashSet<>();
        _rack = rack;
        _load = new Load();
        _hostCapacity = new double[Resource.cachedValues().size()];
        _aliveBrokers = 0;
    }

    // Getters
    public Rack rack() {
        return _rack;
    }

    public Broker broker(int brokerId) {
        return _brokers.get(brokerId);
    }

    public Collection<Broker> brokers() {
        return _brokers.values();
    }

    public Set<Replica> replicas() {
        return Collections.unmodifiableSet(_replicas);
    }

    /**
     * @return All the topics that have at least one partition on the host.
     */
    public Set<String> topics() {
        Set<String> topics = new HashSet<>();
        _brokers.values().forEach(broker -> topics.addAll(broker.topics()));
        return topics;
    }

    /**
     * Get host capacity for the requested resource. Rack capacity represents the total capacity of the live
     * brokers in the rack for the requested resource.
     *
     * @param resource Resource for which capacity will be provided.
     * @return Alive host capacity of the resource.
     */
    public double capacityFor(Resource resource) {
        return _aliveBrokers > 0 ? _hostCapacity[resource.id()] : DEAD_HOST_CAPACITY;
    }

    /**
     * @return whether the host is alive. The host is alive as long as it has at least one broker alive.
     */
    public boolean isAlive() {
        for (Broker broker : _brokers.values()) {
            if (broker.isAlive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the host is alive and allowed replica moves.
     *
     * @param optimizationOptions Options to retrieve excluded brokers for replica moves.
     * @return Whether the host is alive and allowed replica moves. The host is alive and allowed replica moves as long as
     * it has at least one broker alive which is not excluded for replica moves.
     */
    public boolean isAliveAndAllowedReplicaMoves(OptimizationOptions optimizationOptions) {
        Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForReplicaMove();
        for (Broker broker : _brokers.values()) {
            if (broker.isAlive() && !excludedBrokers.contains(broker.id())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return The load on the rack.
     */
    public Load load() {
        return _load;
    }

    /**
     * Mark specified disk dead and update the capacity
     *
     * @param brokerId The id of broker which host the disk.
     * @param logdir   Log directory of the disk.
     * @return The disk capacity lost.
     */
    double markDiskDead(int brokerId, String logdir) {
        Broker broker = broker(brokerId);
        double capacityLost = broker.markDiskDead(logdir);
        _hostCapacity[DISK.id()] -= capacityLost;
        return capacityLost;
    }

    /**
     * @return The name of the host
     */
    public String name() {
        return _name;
    }

    /**
     * Create a broker under this host, and get the created broker.
     *
     * @param brokerId                     Id of the broker to be created.
     * @param brokerCapacityInfo           Capacity information of the created broker.
     * @param populateReplicaPlacementInfo Whether populate replica placement over disk information or not.
     * @return Created broker.
     */
    Broker createBroker(Integer brokerId, BrokerCapacityInfo brokerCapacityInfo, boolean populateReplicaPlacementInfo) {
        Broker broker = new Broker(this, brokerId, brokerCapacityInfo, populateReplicaPlacementInfo);
        _brokers.put(brokerId, broker);
        _aliveBrokers++;
        for (Map.Entry<Resource, Double> entry : brokerCapacityInfo.capacity().entrySet()) {
            Resource resource = entry.getKey();
            _hostCapacity[resource.id()] += (resource == Resource.CPU) ? (entry.getValue() * brokerCapacityInfo.numCpuCores())
                    : entry.getValue();
        }
        return broker;
    }

    /**
     * Set broker state and update the capacity
     *
     * @param brokerId Broker id.
     * @param newState The new state of the broker.
     */
    void setBrokerState(int brokerId, Broker.State newState) {
        Broker broker = broker(brokerId);
        if (broker.isAlive() && newState == Broker.State.DEAD) {
            for (Resource r : Resource.cachedValues()) {
                _hostCapacity[r.id()] -= broker.capacityFor(r);
            }
            _aliveBrokers--;
        } else if (!broker.isAlive() && newState != Broker.State.DEAD) {
            for (Resource r : Resource.cachedValues()) {
                _hostCapacity[r.id()] += broker.capacityFor(r);
            }
            _aliveBrokers++;
        }
        broker.setState(newState);
    }

    void addReplica(Replica replica) {
        _replicas.add(replica);
        _brokers.get(replica.broker().id()).addReplica(replica);
        _load.addLoad(replica.load());
    }

    Replica removeReplica(int brokerId, TopicPartition tp) {
        Broker broker = _brokers.get(brokerId);
        if (broker == null) {
            throw new IllegalStateException(String.format("Cannot remove replica for %s from broker %s because "
                    + "it does not exist in host %s", tp, brokerId, _name));
        }
        Replica replica = broker.removeReplica(tp);
        _replicas.remove(replica);
        _load.subtractLoad(replica.load());
        return replica;
    }

    AggregatedMetricValues makeFollower(int brokerId, TopicPartition tp) {
        Broker broker = broker(brokerId);
        if (broker == null) {
            throw new IllegalStateException(String.format("Cannot make replica %s on broker %d as follower because the broker"
                    + " does not exist in host %s", tp, brokerId, _name));
        }
        AggregatedMetricValues leadershipLoadDelta = broker.makeFollower(tp);

        // Remove leadership load from recent load.
        _load.subtractLoad(leadershipLoadDelta);
        return leadershipLoadDelta;
    }

    void makeLeader(int brokerId,
                    TopicPartition tp,
                    AggregatedMetricValues leadershipLoadDelta) {
        Broker broker = _brokers.get(brokerId);
        broker.makeLeader(tp, leadershipLoadDelta);
        // Add leadership load to recent load.
        _load.addLoad(leadershipLoadDelta);
    }

    void setReplicaLoad(int brokerId,
                        TopicPartition tp,
                        AggregatedMetricValues aggregatedMetricValues,
                        List<Long> windows) {
        Broker broker = _brokers.get(brokerId);
        broker.setReplicaLoad(tp, aggregatedMetricValues, windows);
        _load.addMetricValues(aggregatedMetricValues, windows);
    }

    void clearLoad() {
        _brokers.values().forEach(Broker::clearLoad);
        _load.clearLoad();
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        Map<String, Object> hostMap = new HashMap<>();
        List<Map<String, Object>> brokerList = new ArrayList<>();
        for (Broker broker : _brokers.values()) {
            brokerList.add(broker.getJsonStructure());
        }
        hostMap.put(ModelUtils.NAME, _name);
        hostMap.put(ModelUtils.BROKERS, brokerList);
        return hostMap;
    }

    /**
     * Write to the given output stream.
     *
     * @param out Output stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        String host = String.format("<Host name=\"%s\">%n", _name);
        out.write(host.getBytes(StandardCharsets.UTF_8));
        for (Broker broker : _brokers.values()) {
            broker.writeTo(out);
        }
        out.write("</Host>%n".getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        StringBuilder host = new StringBuilder().append(String.format("<Host name=\"%s\">%n", _name));

        for (Broker broker : _brokers.values()) {
            host.append(broker.toString());
        }

        return host.append("</Host>%n").toString();
    }

}
