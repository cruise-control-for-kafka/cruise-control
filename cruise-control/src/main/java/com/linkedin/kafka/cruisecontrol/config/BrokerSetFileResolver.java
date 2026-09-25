/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.common.utils.Utils;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerSetResolutionException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * BrokerSet information store implementation based out of File config broker.set.config.file
 * By default the property is set to value of brokerSets.json
 * <p>
 * Example Broker Set Data File :
 * <pre>
 * {
 *   "brokerSets":[
 *     {
 *       "brokerSetId": "Blue",
 *       "brokerIds": [0, 1, 2]
 *     },
 *     {
 *       "brokerSetId": "Green",
 *       "brokerIds": [3, 4, 5]
 *     }
 *   ]
 * }
 * </pre>
 */
public class BrokerSetFileResolver implements BrokerSetResolver {
    public static final String BROKER_SET_ASSIGNMENT_POLICY_OBJECT_CONFIG = "broker.set.assignment.policy.object";
    private String _configFile;
    private BrokerSetAssignmentPolicy _brokerSetAssignmentPolicy;

    @Override
    public void configure(Map<String, ?> configs) {
        _configFile = (String) configs.get(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG);
        _brokerSetAssignmentPolicy = (BrokerSetAssignmentPolicy) Utils.validateNotNull(
                configs.get(BROKER_SET_ASSIGNMENT_POLICY_OBJECT_CONFIG),
                () -> String.format("Missing %s when creating broker set file resolver",
                        BROKER_SET_ASSIGNMENT_POLICY_OBJECT_CONFIG));
    }

    @Override
    public Map<String, Set<Integer>> brokerIdsByBrokerSetId(ClusterModel clusterModel) throws BrokerSetResolutionException {
        return _brokerSetAssignmentPolicy.assignBrokerSetsForUnresolvedBrokers(clusterModel, loadBrokerSetDataOrThrow());
    }

    @Override
    public Map<String, Set<Integer>> brokerIdsByBrokerSetId(Map<Integer, String> rackIdByBrokerId)
            throws BrokerSetResolutionException {
        return _brokerSetAssignmentPolicy.assignBrokerSetsForUnresolvedBrokers(rackIdByBrokerId, loadBrokerSetDataOrThrow());
    }

    private Map<String, Set<Integer>> loadBrokerSetDataOrThrow() throws BrokerSetResolutionException {
        try {
            return loadBrokerSetData();
        } catch (IOException e) {
            throw new BrokerSetResolutionException(e.getMessage());
        }
    }

    private Map<String, Set<Integer>> loadBrokerSetData() throws IOException {
        try (Reader reader = Files.newBufferedReader(Path.of(_configFile), StandardCharsets.UTF_8)) {
            Gson gson = new Gson();
            final BrokerSets brokerSets = gson.fromJson(reader, BrokerSets.class);
            final Set<BrokerSet> brokerSetSet = brokerSets.brokerSets;
            final Map<String, Set<Integer>> brokerIdsByBrokerSetId = new HashMap<>();
            if (brokerSetSet != null) {
                for (BrokerSet brokerSet : brokerSetSet) {
                    brokerIdsByBrokerSetId.put(brokerSet.brokerSetId, brokerSet.brokerIds);
                }
            }
            return brokerIdsByBrokerSetId;
        }
    }

    private record BrokerSets(Set<BrokerSet> brokerSets) {
    }

    private record BrokerSet(String brokerSetId, Set<Integer> brokerIds) {
    }
}
