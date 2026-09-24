/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.exception.ReplicaToBrokerSetMappingException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * Provides a mapping policy for replicas to a broker set id
 */
@InterfaceStability.Evolving
public interface ReplicaToBrokerSetMappingPolicy {
    /**
     * Maps a replica to a broker set
     *
     * @param replica                   replica object
     * @param clusterModel              cluster model object
     * @param brokerSetResolutionHelper broker set resolution helper
     * @return A broker set Id
     */
    String brokerSetIdForReplica(Replica replica, ClusterModel clusterModel, BrokerSetResolutionHelper brokerSetResolutionHelper)
            throws BrokerSetResolutionException, ReplicaToBrokerSetMappingException;
}
