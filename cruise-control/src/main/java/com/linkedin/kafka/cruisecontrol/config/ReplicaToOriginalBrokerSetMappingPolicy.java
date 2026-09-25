/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;


/**
 * This replica to broker set assignment policy assigns the replica to the broker set that is the broker set of its original broker.
 * This means that replica's original broker id already dictates the broker set the replica belongs to.
 */
public class ReplicaToOriginalBrokerSetMappingPolicy implements ReplicaToBrokerSetMappingPolicy {
    /**
     * Maps a replica to its original broker set
     *
     * @param replica                   replica object
     * @param clusterModel              cluster model object
     * @param brokerSetResolutionHelper broker set resolution helper
     * @return A broker set Id
     */
    @Override
    public String brokerSetIdForReplica(final Replica replica, final ClusterModel clusterModel,
                                        final BrokerSetResolutionHelper brokerSetResolutionHelper) throws BrokerSetResolutionException {
        Broker originalBroker = replica.originalBroker();
        return brokerSetResolutionHelper.brokerSetId(originalBroker.id());
    }
}
