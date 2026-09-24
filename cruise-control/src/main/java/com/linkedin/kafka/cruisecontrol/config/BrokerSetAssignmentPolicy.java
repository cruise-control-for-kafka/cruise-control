/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * For the Brokers that do not have a BrokerSet assigned, this policy will determine the BrokerSet
 * assignment
 */
@InterfaceStability.Evolving
public interface BrokerSetAssignmentPolicy {
    /**
     * Assigns broker sets to the brokers that do not have broker sets assigned
     *
     * @param clusterModel             cluster model object
     * @param existingBrokerSetMapping existing mapping of broker sets to broker ids
     * @return A map of broker Ids by their broker set Id
     * @deprecated This method will be replaced by {@link #assignBrokerSetsForUnresolvedBrokers(Map, Map)} TODO: this method should be removed soon
     */
    @Deprecated
    Map<String, Set<Integer>> assignBrokerSetsForUnresolvedBrokers(ClusterModel clusterModel, Map<String, Set<Integer>> existingBrokerSetMapping)
            throws BrokerSetResolutionException;

    /**
     * Assigns broker sets to the brokers that do not have broker sets assigned
     *
     * @param rackIdToBrokerId         a map of broker ids to rack ids in the cluster
     * @param existingBrokerSetMapping existing mapping of broker sets to broker ids
     * @return A map of broker Ids by their broker set Id
     */
    Map<String, Set<Integer>> assignBrokerSetsForUnresolvedBrokers(Map<Integer, String> rackIdToBrokerId,
                                                                   Map<String, Set<Integer>> existingBrokerSetMapping)
            throws BrokerSetResolutionException;
}
