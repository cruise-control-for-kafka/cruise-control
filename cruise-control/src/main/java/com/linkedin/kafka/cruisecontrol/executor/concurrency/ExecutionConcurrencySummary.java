/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.concurrency;

import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * The summary of the currency execution concurrency. It provides the current min/max/avg value of the broker concurrency of different type.
 */
public class ExecutionConcurrencySummary {
    private final Map<Integer, Integer> _interBrokerPartitionMovementConcurrency;
    private final Map<Integer, Integer> _intraBrokerPartitionMovementConcurrency;
    private final Map<Integer, Integer> _brokerLeadershipMovementConcurrency;
    private final Integer _clusterLeadershipMovementConcurrency;
    private final boolean _initialized;

    public ExecutionConcurrencySummary(boolean initialized,
                                       Map<Integer, Integer> interBrokerPartitionMovementConcurrency,
                                       Map<Integer, Integer> intraBrokerPartitionMovementConcurrency,
                                       Map<Integer, Integer> brokerLeadershipMovementConcurrency,
                                       Integer clusterLeadershipMovementConcurrency) {
        _initialized = initialized;
        _interBrokerPartitionMovementConcurrency = new HashMap<>(interBrokerPartitionMovementConcurrency);
        _intraBrokerPartitionMovementConcurrency = new HashMap<>(intraBrokerPartitionMovementConcurrency);
        _brokerLeadershipMovementConcurrency = new HashMap<>(brokerLeadershipMovementConcurrency);
        _clusterLeadershipMovementConcurrency = clusterLeadershipMovementConcurrency;
    }

    /**
     * Get the min broker execution concurrency of the cluster
     *
     * @param concurrencyType the concurrency type of the min execution concurrency
     * @return the min broker execution concurrency of the cluster. If not initialized, return 0.
     */
    public synchronized int getMinExecutionConcurrency(ConcurrencyType concurrencyType) {
        if (!_initialized) {
            return 0;
        }

        sanityCheckValidity();
        switch (concurrencyType) {
            case INTER_BROKER_REPLICA:
                return Collections.min(_interBrokerPartitionMovementConcurrency.values());
            case INTRA_BROKER_REPLICA:
                return Collections.min(_intraBrokerPartitionMovementConcurrency.values());
            case LEADERSHIP_BROKER:
                return Collections.min(_brokerLeadershipMovementConcurrency.values());
            default:
                throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        }
    }

    /**
     * Get the max broker execution concurrency of the cluster
     *
     * @param concurrencyType the concurrency type of the max execution concurrency
     * @return the max broker execution concurrency of the cluster. If not initialized, return 0.
     */
    public synchronized int getMaxExecutionConcurrency(ConcurrencyType concurrencyType) {
        if (!_initialized) {
            return 0;
        }

        sanityCheckValidity();
        switch (concurrencyType) {
            case INTER_BROKER_REPLICA:
                return Collections.max(_interBrokerPartitionMovementConcurrency.values());
            case INTRA_BROKER_REPLICA:
                return Collections.max(_intraBrokerPartitionMovementConcurrency.values());
            case LEADERSHIP_BROKER:
                return Collections.max(_brokerLeadershipMovementConcurrency.values());
            default:
                throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        }
    }

    /**
     * Get the avg broker execution concurrency of the cluster
     *
     * @param concurrencyType the concurrency type of the avg execution concurrency
     * @return the avg broker execution concurrency of the cluster. If not initialized, return 0.
     */
    public synchronized double getAvgExecutionConcurrency(ConcurrencyType concurrencyType) {
        if (!_initialized) {
            return 0;
        }

        sanityCheckValidity();
        switch (concurrencyType) {
            case INTER_BROKER_REPLICA:
                return _interBrokerPartitionMovementConcurrency.values().stream().mapToDouble(d -> d).average()
                        .orElse(-1);
            case INTRA_BROKER_REPLICA:
                return _intraBrokerPartitionMovementConcurrency.values().stream().mapToDouble(d -> d).average()
                        .orElse(-1);
            case LEADERSHIP_BROKER:
                return _brokerLeadershipMovementConcurrency.values().stream().mapToDouble(d -> d).average()
                        .orElse(-1);
            default:
                throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        }
    }

    /**
     * Get the cluster leadership movement concurrency.
     *
     * @return the cluster leadership movement concurrency. If not initialized or the concurrency is null, return 0.
     */
    public synchronized int getClusterLeadershipMovementConcurrency() {
        if (!_initialized || _clusterLeadershipMovementConcurrency == null) {
            return 0;
        }
        return _clusterLeadershipMovementConcurrency.intValue();
    }

    private void sanityCheckValidity() {
        if (_interBrokerPartitionMovementConcurrency.isEmpty()
                || _intraBrokerPartitionMovementConcurrency.isEmpty()
                || _brokerLeadershipMovementConcurrency.isEmpty()) {
            throw new IllegalArgumentException("Broker concurrency is not populated.");
        }
    }
}

