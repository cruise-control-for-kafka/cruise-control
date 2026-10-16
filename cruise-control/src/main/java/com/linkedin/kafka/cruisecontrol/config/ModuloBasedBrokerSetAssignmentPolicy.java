/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * For any non-mapped Brokers, this policy assigns brokerSet based by doing a simple modulo on the number of brokerSets.
 * Works as a lenient broker set mapping policy where unmapped brokers go to any of the existing brokerSet.
 */
public class ModuloBasedBrokerSetAssignmentPolicy implements BrokerSetAssignmentPolicy {
  /**
   * Assigns a broker set to non-mapped brokers based on modulo.
   *
   * @return A map of broker Ids by their broker set Id
   */
  @Override
  public Map<String, Set<Integer>> assignBrokerSetsForUnresolvedBrokers(final ClusterModel clusterModel,
                                                                        final Map<String, Set<Integer>> existingBrokerSetMapping) {
    Set<Broker> allMappedBrokers = existingBrokerSetMapping.values()
                                                           .stream()
                                                           .flatMap(Collection::stream)
                                                           .map(clusterModel::broker)
                                                           .filter(Objects::nonNull)
                                                           .collect(Collectors.toSet());

    Set<Broker> extraBrokersInClusterModel = new HashSet<>(clusterModel.brokers());
    extraBrokersInClusterModel.removeAll(allMappedBrokers);

    int numberOfBrokerSets = existingBrokerSetMapping.size();
    List<String> brokerSetIds = new ArrayList<>(existingBrokerSetMapping.keySet());
    Collections.sort(brokerSetIds);

    extraBrokersInClusterModel.stream().forEach(broker -> {
      String brokerSet = brokerSetIds.get(broker.id() % numberOfBrokerSets);
      Set<Integer> brokerIdsForBrokerSet = existingBrokerSetMapping.getOrDefault(brokerSet, new HashSet<>());
      brokerIdsForBrokerSet.add(broker.id());
      existingBrokerSetMapping.put(brokerSet, brokerIdsForBrokerSet);
    });

    return existingBrokerSetMapping;
  }

  /**
   * Assigns a broker set to non-mapped brokers based on modulo.
   *
   * @return A map of broker Ids by their broker set Id
   */
  @Override
  public Map<String, Set<Integer>> assignBrokerSetsForUnresolvedBrokers(final Map<Integer, String> rackIdToBrokerId,
                                                                        final Map<String, Set<Integer>> existingBrokerSetMapping) {
    Set<Integer> allMappedBrokers = existingBrokerSetMapping.values()
                                                            .stream()
                                                            .flatMap(Collection::stream)
                                                            .collect(Collectors.toSet());

    Set<Integer> unmappedBrokers = new HashSet<>(rackIdToBrokerId.keySet());
    unmappedBrokers.removeAll(allMappedBrokers);

    int numberOfBrokerSets = existingBrokerSetMapping.size();
    List<String> brokerSetIds = new ArrayList<>(existingBrokerSetMapping.keySet());
    Collections.sort(brokerSetIds);

    unmappedBrokers.forEach(brokerId -> {
      String brokerSet = brokerSetIds.get(brokerId % numberOfBrokerSets);
      Set<Integer> brokerIdsForBrokerSet = existingBrokerSetMapping.getOrDefault(brokerSet, new HashSet<>());
      brokerIdsForBrokerSet.add(brokerId);
      existingBrokerSetMapping.put(brokerSet, brokerIdsForBrokerSet);
    });

    return existingBrokerSetMapping;
  }
}
