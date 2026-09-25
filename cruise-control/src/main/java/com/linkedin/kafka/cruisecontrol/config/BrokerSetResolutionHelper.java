/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Cluster;

/**
 * This is a helper class to contain helper functions to get brokerSet mappings and reverse mappings
 */
public class BrokerSetResolutionHelper {
    private Map<String, Set<Integer>> _brokersByBrokerSetId;
    private Map<Integer, String> _brokerSetIdByBrokerId;

    public BrokerSetResolutionHelper(Cluster cluster, BrokerSetResolver brokerSetResolver) throws BrokerSetResolutionException {
        initializeBrokerSetMappings(getRackIdByBrokerIdMapping(cluster), brokerSetResolver);
    }

    public BrokerSetResolutionHelper(ClusterModel clusterModel, BrokerSetResolver brokerSetResolver) throws BrokerSetResolutionException {
        initializeBrokerSetMappings(getRackIdByBrokerIdMapping(clusterModel), brokerSetResolver);
    }

    /**
     * Init method to initialize the broker set to broker mapping and reverse mapping
     *
     * @param rackIdByBrokerId  a map of broker ids to rack ids in the cluster
     * @param brokerSetResolver broker set resolver object
     */
    private void initializeBrokerSetMappings(Map<Integer, String> rackIdByBrokerId, BrokerSetResolver brokerSetResolver)
            throws BrokerSetResolutionException {
        _brokersByBrokerSetId = brokerSetResolver.brokerIdsByBrokerSetId(rackIdByBrokerId);
        _brokerSetIdByBrokerId = new HashMap<>();

        _brokersByBrokerSetId.forEach((brokerSet, brokers) -> {
            brokers.forEach(broker -> _brokerSetIdByBrokerId.put(broker, brokerSet));
        });
    }

    /**
     * Helper function to get Broker by BrokerSet mapping
     *
     * @return broker by broker set id Mapping
     */
    public Map<String, Set<Integer>> brokersByBrokerSetId() {
        return _brokersByBrokerSetId;
    }

    /**
     * Helper function to get BrokerSet by Broker mapping
     *
     * @return broker set id by broker id Mapping
     */
    public Map<Integer, String> brokerSetIdByBrokerId() {
        return _brokerSetIdByBrokerId;
    }

    /**
     * Helper function to get brokerSet for a broker
     *
     * @param brokerId broker object
     * @return broker set id
     */
    public String brokerSetId(Integer brokerId) throws BrokerSetResolutionException {
        String brokerSetId = _brokerSetIdByBrokerId.get(brokerId);

        if (brokerSetId != null) {
            return brokerSetId;
        }

        throw new BrokerSetResolutionException(String.format("Failed to resolve BrokerSet for Broker %s", brokerId));
    }

    /**
     * Get the broker to rack mapping from cluster
     *
     * @param cluster the kafka cluster
     * @return the map of broker id to rack id
     */
    public static Map<Integer, String> getRackIdByBrokerIdMapping(Cluster cluster) {
        Map<Integer, String> rackIdByBrokerId = new HashMap<>();
        cluster.nodes().forEach(node -> rackIdByBrokerId.put(node.id(), node.rack()));
        return rackIdByBrokerId;
    }

    /**
     * Get the broker to rack mapping from cluster model
     *
     * @param clusterModel the cluster model
     * @return the map of broker id to rack id
     */
    public static Map<Integer, String> getRackIdByBrokerIdMapping(ClusterModel clusterModel) {
        Map<Integer, String> rackIdByBrokerId = new HashMap<>();
        clusterModel.brokers().forEach(broker -> rackIdByBrokerId.put(broker.id(), broker.rack().id()));
        return rackIdByBrokerId;
    }
}
