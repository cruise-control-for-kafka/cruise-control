/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.annotation.InterfaceStability;
import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;


/**
 * The interface for getting the broker set information. Users should implement this interface so Cruise Control can
 * read broker sets and corresponding broker ids in order to use BrokerSet related Goals.
 */
@InterfaceStability.Evolving
public interface BrokerSetResolver extends CruiseControlConfigurable {
    /**
     * Get all broker sets of a cluster.
     *
     * @param clusterModel cluster model object
     * @return A map of broker Ids by their broker set Id
     * @deprecated This method will be replaced by {@link #brokerIdsByBrokerSetId(Map)}. TODO: this method should be removed soon
     */
    @Deprecated
    Map<String, Set<Integer>> brokerIdsByBrokerSetId(ClusterModel clusterModel) throws BrokerSetResolutionException;

    /**
     * Get all broker sets of a cluster.
     *
     * @param rackIdByBrokerId a map of broker ids to rack ids in the cluster
     * @return A map of broker Ids by their broker set Id
     */
    Map<String, Set<Integer>> brokerIdsByBrokerSetId(Map<Integer, String> rackIdByBrokerId) throws BrokerSetResolutionException;
}
