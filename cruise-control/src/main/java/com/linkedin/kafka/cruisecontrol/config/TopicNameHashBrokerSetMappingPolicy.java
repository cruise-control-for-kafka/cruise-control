/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.exception.ReplicaToBrokerSetMappingException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.hash.Hashing;


/**
 * This replica to broker set assignment policy assigns the replica to the broker set by the hash of the topic name.
 * Users can utilize this if they want to have the least movements when they add new broker set. since this logic leverages
 * consistent hashing, as long as brokerSet ids are ordered set and newly added broker set gets an id that goes at the end of the order,
 * only few of the topics will get mapped to new broker sets.
 * This will result in lesser movement and no shuffling when a new broker set is added.
 * When removing a broker set, remove from the end of the order.
 * <p>
 * This policy does not take in account the size of the broker set or any capacities of the brokers. The only thing that determines which
 * broker set the topic gets mapped to is by the topic's name and the lexicographical ordering of the broker set ids.
 * <p>
 * Please read the documentation of {@link Hashing#consistentHash(long, int)}
 *
 * <p>See the <a href="http://en.wikipedia.org/wiki/Consistent_hashing">Wikipedia article on
 * consistent hashing</a> for more information.
 */
public class TopicNameHashBrokerSetMappingPolicy implements ReplicaToBrokerSetMappingPolicy {
    private final Map<String, String> _brokerSetIdByTopicCache = new HashMap<>();
    private Set<String> _brokerSetIdCache = new HashSet<>();
    private List<String> _sortedBrokerSetIdsCache = new ArrayList<>();

    /**
     * Gets a replica's topic name and consistent hashing, this class maps the topics to broker sets
     *
     * @param replica                   replica object
     * @param clusterModel              cluster model object
     * @param brokerSetResolutionHelper broker set resolution helper
     * @return A broker set Id
     */
    @Override
    public String brokerSetIdForReplica(final Replica replica, final ClusterModel clusterModel,
                                        final BrokerSetResolutionHelper brokerSetResolutionHelper) throws ReplicaToBrokerSetMappingException {
        String topicName = replica.topicPartition().topic();

        Map<String, Set<Integer>> brokersByBrokerSetId = brokerSetResolutionHelper.brokersByBrokerSetId();
        int numBrokerSets = brokersByBrokerSetId.size();

        if (numBrokerSets == 0) {
            throw new ReplicaToBrokerSetMappingException("Can not map replica to a broker set since there are no broker sets.");
        }

        // If the broker sets haven't changed and topic mapping is cached, then read from cache
        if (brokersByBrokerSetId.keySet().equals(_brokerSetIdCache) && _brokerSetIdByTopicCache.containsKey(topicName)) {
            return _brokerSetIdByTopicCache.get(topicName);
        }

        // If the broker set ids have changed then clear the caches and re-map topics since we use consistent hash ring
        if (!brokersByBrokerSetId.keySet().equals(_brokerSetIdCache)) {
            _brokerSetIdByTopicCache.clear();
            _sortedBrokerSetIdsCache.clear();
            _brokerSetIdCache = new HashSet<>(brokersByBrokerSetId.keySet());
        }

        String brokerSetId = brokerSetIdForTopic(topicName, brokersByBrokerSetId);
        _brokerSetIdByTopicCache.put(topicName, brokerSetId);
        return brokerSetId;
    }

    /**
     * Private method to calculate the mapping of topic to broker set and update the caches
     *
     * @param topicName            topic name string
     * @param brokersByBrokerSetId broker set id to broker ids mapping
     * @return broker set id
     */
    private String brokerSetIdForTopic(String topicName, Map<String, Set<Integer>> brokersByBrokerSetId) {
        // If sorted broker set id list is not cached, then cache or else avoid sorting each time
        if (_sortedBrokerSetIdsCache.isEmpty()) {
            // Sorting to make the ordering work with consistent hashing
            _sortedBrokerSetIdsCache = new ArrayList<>(brokersByBrokerSetId.keySet());
            Collections.sort(_sortedBrokerSetIdsCache);
        }

        int murmurHash = Math.abs(Hashing.murmur3_128().hashString(topicName, StandardCharsets.UTF_8).asInt());
        return _sortedBrokerSetIdsCache.get(Hashing.consistentHash(murmurHash, _sortedBrokerSetIdsCache.size()));
    }
}
