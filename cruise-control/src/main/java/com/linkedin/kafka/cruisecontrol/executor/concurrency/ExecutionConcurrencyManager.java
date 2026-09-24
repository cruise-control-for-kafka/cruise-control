/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.concurrency;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Manages the allowed concurrency for all execution types: inter-broker/intra-broker/leadership movement.
 * <p>
 * For each execution type, the default allowed concurrency is defined by configuration, and the requested concurrency is an optional
 * parameter announced on triggering the rebalance.
 * </p>
 * <p>
 * At the beginning of the execution, each broker starts with the same requested concurrency (or the default concurrency if
 * requested concurrency not provided). During the execution, each broker adjust its allowed concurrency based on its own metric.
 * </p>
 * <p>
 * The total execution concurrency for all types is capped at the config value of max.num.cluster.movements.
 * </p>
 *
 */
public class ExecutionConcurrencyManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionConcurrencyManager.class);

    private static final int MIN_CONCURRENCY_PER_BROKER = 1;

    // The total allowed movement concurrency for all types in the cluster. This value cannot be overridden at runtime.
    private final int _clusterMovementConcurrency;

    // The allowed inter-broker partition movement concurrency for each broker.
    private final int _defaultInterBrokerPartitionMovementConcurrency;
    private Integer _requestedInterBrokerPartitionMovementConcurrency;

    // The allowed intra-broker partition movement concurrency for each broker.
    private final int _defaultIntraBrokerPartitionMovementConcurrency;
    private Integer _requestedIntraBrokerPartitionMovementConcurrency;

    // The allowed leadership movement concurrency in the cluster.
    private final int _defaultClusterLeadershipMovementConcurrency;
    // For leadership movement, dynamic adjusting of cluster concurrency based on isr and broker metrics is also supported
    // in addition to adjusting per broker concurrency.
    // The leadership movement task is more bottleneck on controller, so in general, the cluster allowed concurrency is
    // smaller than the broker allowed concurrency multiplied by the number of brokers in the cluster.
    // It would be safer and more efficient to have both cluster and broker concurrency adjusters for leadership
    // movements. If with broker concurrency adjustor alone, the execution of leadership movements might be unnecessarily
    // slow due to low cluster concurrency; if with cluster concurrency adjuster alone, we do not have the ability to
    // limit the traffic to slow brokers.
    private Integer _requestedClusterLeadershipMovementConcurrency;
    // The allowed initial leadership movement concurrency for a broker.
    private final int _defaultPerBrokerLeadershipMovementConcurrency;
    private Integer _requestedBrokerLeadershipMovementConcurrency;

    // The allowed inter-broker partition movement concurrency in the cluster.
    private final int _defaultClusterInterBrokerPartitionMovementConcurrency;
    private Integer _requestedClusterInterBrokerPartitionMovementConcurrency;

    // The dynamic allowed-concurrency for each broker. These values are adjusted by the concurrency adjuster at runtime.
    private final Map<Integer, Integer> _interBrokerPartitionMovementConcurrency;
    private final Map<Integer, Integer> _intraBrokerPartitionMovementConcurrency;
    private final Map<Integer, Integer> _brokerLeadershipMovementConcurrency;

    private ExecutionConcurrencySummary _executionConcurrencySummary;
    private boolean _initialized;

    public ExecutionConcurrencyManager(KafkaCruiseControlConfig config) {
        _clusterMovementConcurrency = config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
        _defaultInterBrokerPartitionMovementConcurrency = config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);
        _defaultIntraBrokerPartitionMovementConcurrency = config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG);
        _defaultClusterLeadershipMovementConcurrency = config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
        _defaultClusterInterBrokerPartitionMovementConcurrency = config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
        _defaultPerBrokerLeadershipMovementConcurrency = config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG);
        _interBrokerPartitionMovementConcurrency = new HashMap<>();
        _intraBrokerPartitionMovementConcurrency = new HashMap<>();
        _brokerLeadershipMovementConcurrency = new HashMap<>();
        _requestedClusterInterBrokerPartitionMovementConcurrency = null;
        refreshExecutionConcurrencySummary();
    }

    /**
     * Initialize the execution concurrency manager. This method has to be called before any execution.
     *
     * @param brokers                                          all brokers with replicas
     * @param requestedInterBrokerPartitionMovementConcurrency the requested inter broker partition movement concurrency per broker
     * @param requestedIntraBrokerPartitionMovementConcurrency the requested intra broker partition movement concurrency per broker
     * @param requestedClusterLeadershipMovementConcurrency    the requested leadership movement concurrency in the cluster
     * @param requestedBrokerLeadershipMovementConcurrency     the requested leadership movement concurrency involved in a broker
     */
    public synchronized void initialize(Set<Integer> brokers,
                                        Integer requestedInterBrokerPartitionMovementConcurrency,
                                        Integer requestedIntraBrokerPartitionMovementConcurrency,
                                        Integer requestedClusterLeadershipMovementConcurrency,
                                        Integer requestedBrokerLeadershipMovementConcurrency) {

        LOG.info("Initialize ExecutionConcurrencyManager with requested inter-broker/intra-broker/cluster leadership/"
                        + "broker leadership"
                        + " concurrency: {}/{}/{}/{} on brokers {}.",
                requestedInterBrokerPartitionMovementConcurrency,
                requestedIntraBrokerPartitionMovementConcurrency,
                requestedClusterLeadershipMovementConcurrency,
                requestedBrokerLeadershipMovementConcurrency,
                brokers);
        _interBrokerPartitionMovementConcurrency.clear();
        _intraBrokerPartitionMovementConcurrency.clear();
        _brokerLeadershipMovementConcurrency.clear();
        _requestedInterBrokerPartitionMovementConcurrency = requestedInterBrokerPartitionMovementConcurrency;
        _requestedIntraBrokerPartitionMovementConcurrency = requestedIntraBrokerPartitionMovementConcurrency;
        _requestedClusterLeadershipMovementConcurrency = requestedClusterLeadershipMovementConcurrency;
        _requestedBrokerLeadershipMovementConcurrency = requestedBrokerLeadershipMovementConcurrency;
        for (int brokerId : brokers) {
            _interBrokerPartitionMovementConcurrency.put(brokerId, interBrokerPartitionMovementConcurrency());
            _intraBrokerPartitionMovementConcurrency.put(brokerId, intraBrokerPartitionMovementConcurrency());
            _brokerLeadershipMovementConcurrency.put(brokerId, requestedBrokerLeadershipMovementConcurrency());
        }
        _initialized = true;
        refreshExecutionConcurrencySummary();
    }

    /**
     * Reset the execution concurrency manager. This method has to be called after any execution. It clears up the broker concurrency and
     * requested concurrency.
     */
    public synchronized void reset() {
        _requestedInterBrokerPartitionMovementConcurrency = null;
        _requestedIntraBrokerPartitionMovementConcurrency = null;
        _requestedClusterLeadershipMovementConcurrency = null;
        _requestedBrokerLeadershipMovementConcurrency = null;
        _requestedClusterInterBrokerPartitionMovementConcurrency = null;
        _interBrokerPartitionMovementConcurrency.clear();
        _intraBrokerPartitionMovementConcurrency.clear();
        _brokerLeadershipMovementConcurrency.clear();
        _initialized = false;
        refreshExecutionConcurrencySummary();
    }

    /**
     * Returns whether the concurrency manager is initialized
     *
     * @return true if the concurrency manager is initialzed
     */
    public boolean isInitialized() {
        return _initialized;
    }

    /**
     * Retrieve the movement concurrency of the given broker concurrency type.
     *
     * @param brokerId        The brokerId to get concurrency.
     * @param concurrencyType The type of concurrency for which the allowed movement concurrency is requested.
     * @return The movement concurrency of the given concurrency type.
     */
    public synchronized int getExecutionBrokerConcurrency(int brokerId, ConcurrencyType concurrencyType) {
        return switch (concurrencyType) {
            case INTER_BROKER_REPLICA -> interBrokerPartitionMovementConcurrency(brokerId);
            case INTRA_BROKER_REPLICA -> intraBrokerPartitionMovementConcurrency(brokerId);
            case LEADERSHIP_BROKER -> leadershipMovementConcurrency(brokerId);
            default ->
                    throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        };
    }

    /**
     * Retrieve the cluster leadership concurrency.
     *
     * @return The cluster leadership concurrency.
     */
    public synchronized int getExecutionClusterLeadershipConcurrency() {
        return maxClusterLeadershipMovements();
    }

    /**
     * Retrieve the movement concurrency map of the given concurrency type.
     *
     * @param concurrencyType The type of concurrency for which the allowed movement concurrency is requested.
     * @return The movement concurrency of the given concurrency type.
     */
    public synchronized Map<Integer, Integer> getExecutionConcurrencyPerBroker(ConcurrencyType concurrencyType) {
        return switch (concurrencyType) {
            case INTER_BROKER_REPLICA -> _interBrokerPartitionMovementConcurrency;
            case INTRA_BROKER_REPLICA -> _intraBrokerPartitionMovementConcurrency;
            case LEADERSHIP_BROKER -> _brokerLeadershipMovementConcurrency;
            default ->
                    throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        };
    }

    /**
     * Set the allowed per broker execution concurrency for all brokers or set the cluster concurrency.
     *
     * @param concurrency     the allowed concurrency to set
     * @param concurrencyType the concurrency type of the execution
     */
    public synchronized void setExecutionConcurrencyForAllBrokersOrCluster(Integer concurrency, ConcurrencyType concurrencyType) {
        sanityCheckRequestedConcurrency(concurrency, concurrencyType);
        switch (concurrencyType) {
            case INTER_BROKER_REPLICA:
                _requestedInterBrokerPartitionMovementConcurrency = concurrency;
                _interBrokerPartitionMovementConcurrency.replaceAll((k, v) -> concurrency);
                break;
            case INTRA_BROKER_REPLICA:
                _requestedIntraBrokerPartitionMovementConcurrency = concurrency;
                _intraBrokerPartitionMovementConcurrency.replaceAll((k, v) -> concurrency);
                break;
            case LEADERSHIP_BROKER:
                _requestedBrokerLeadershipMovementConcurrency = concurrency;
                _brokerLeadershipMovementConcurrency.replaceAll((k, v) -> concurrency);
                break;
            case LEADERSHIP_CLUSTER:
                _requestedClusterLeadershipMovementConcurrency = concurrency;
                break;
            default:
                throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        }
        refreshExecutionConcurrencySummary();
    }

    /**
     * Set the allowed execution concurrency of a certain concurrency type for a broker
     *
     * @param brokerId        the id of the broker to set allowed concurrency
     * @param concurrency     the allowed concurrency to set
     * @param concurrencyType the concurrency type of the execution
     */
    public synchronized void setExecutionConcurrencyForBroker(int brokerId, Integer concurrency, ConcurrencyType concurrencyType) {
        sanityCheckRequestedConcurrency(concurrency, concurrencyType);
        switch (concurrencyType) {
            case INTER_BROKER_REPLICA:
                _interBrokerPartitionMovementConcurrency.put(brokerId, concurrency);
                break;
            case INTRA_BROKER_REPLICA:
                _intraBrokerPartitionMovementConcurrency.put(brokerId, concurrency);
                break;
            case LEADERSHIP_BROKER:
                _brokerLeadershipMovementConcurrency.put(brokerId, concurrency);
                break;
            default:
                throw new IllegalArgumentException("Unsupported concurrency type " + concurrencyType + " is provided.");
        }
        refreshExecutionConcurrencySummary();
    }

    /**
     * Dynamically set the max inter-broker partition movements in cluster
     * Ensure that the requested max is not greater than the maximum number of allowed movements in cluster.
     *
     * @param requestedClusterInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements per broker
     *                                                                (if null, use {@link #_defaultInterBrokerPartitionMovementConcurrency}).
     */
    public synchronized void setClusterInterBrokerPartitionMovementConcurrency(
            Integer requestedClusterInterBrokerPartitionMovementConcurrency) {
        if (requestedClusterInterBrokerPartitionMovementConcurrency != null
                && requestedClusterInterBrokerPartitionMovementConcurrency > _clusterMovementConcurrency) {
            throw new IllegalArgumentException("Attempt to set max inter-broker partition movements ["
                    + requestedClusterInterBrokerPartitionMovementConcurrency
                    + "] to greater than the maximum" + " number of allowed movements in cluster ["
                    + _clusterMovementConcurrency + "].");
        }
        _requestedClusterInterBrokerPartitionMovementConcurrency = requestedClusterInterBrokerPartitionMovementConcurrency;
    }

    /**
     * @return Allowed upper bound of inter broker partition movements in cluster
     */
    public synchronized int maxClusterInterBrokerPartitionMovements() {
        return _requestedClusterInterBrokerPartitionMovementConcurrency == null ? _defaultClusterInterBrokerPartitionMovementConcurrency
                : _requestedClusterInterBrokerPartitionMovementConcurrency;
    }

    /**
     * @return Allowed upper bound of leadership movements in cluster
     */
    public synchronized int maxClusterLeadershipMovements() {
        return _requestedClusterLeadershipMovementConcurrency == null ? _defaultClusterLeadershipMovementConcurrency
                : _requestedClusterLeadershipMovementConcurrency;
    }

    /**
     * Get the concurrency of each broker without per-broker throttling. This concurrency is simply the overall allowed concurrency divided
     * by broker count.
     *
     * @param brokersWithReplicaMoves       The set of brokers that involve in replica move
     * @param brokersToSkipConcurrencyCheck The set of brokers that is configured to skip concurrency check.
     * @return concurrency of each broker without per-broker throttling
     */
    public int unthrottledConcurrency(Set<Integer> brokersWithReplicaMoves, Set<Integer> brokersToSkipConcurrencyCheck) {
        int numUnthrottledBrokers = (int) brokersWithReplicaMoves.stream().filter(brokersToSkipConcurrencyCheck::contains).count();
        if (numUnthrottledBrokers == 0) {
            // All brokers are throttled.
            return Integer.MAX_VALUE;
        }
        int unthrottledConcurrency = Math.max(_clusterMovementConcurrency / numUnthrottledBrokers, MIN_CONCURRENCY_PER_BROKER);
        LOG.debug("Unthrottled concurrency is {} for {} brokers.", unthrottledConcurrency, numUnthrottledBrokers);
        return unthrottledConcurrency;
    }

    /**
     * Get execution the concurrency summary that can show the avg/min/max allowed broker concurrency
     *
     * @return the execution concurrency summary
     */
    public synchronized ExecutionConcurrencySummary getExecutionConcurrencySummary() {
        return _executionConcurrencySummary;
    }

    private void sanityCheckRequestedConcurrency(Integer concurrency, ConcurrencyType concurrencyType) {
        if (concurrency != null && concurrency >= _clusterMovementConcurrency) {
            throw new IllegalArgumentException("Attempt to set " + concurrencyType + " concurrency ["
                    + concurrency + "] to greater than or equal to the maximum"
                    + " number of allowed movements in cluster [" + _clusterMovementConcurrency + "].");
        }
    }

    private synchronized void refreshExecutionConcurrencySummary() {
        _executionConcurrencySummary = new ExecutionConcurrencySummary(isInitialized(),
                _interBrokerPartitionMovementConcurrency,
                _intraBrokerPartitionMovementConcurrency,
                _brokerLeadershipMovementConcurrency,
                maxClusterLeadershipMovements());
    }

    private synchronized int interBrokerPartitionMovementConcurrency(int brokerId) {
        return _interBrokerPartitionMovementConcurrency.getOrDefault(brokerId, interBrokerPartitionMovementConcurrency());
    }

    private synchronized int interBrokerPartitionMovementConcurrency() {
        return _requestedInterBrokerPartitionMovementConcurrency != null
                ? _requestedInterBrokerPartitionMovementConcurrency : _defaultInterBrokerPartitionMovementConcurrency;
    }

    private synchronized int intraBrokerPartitionMovementConcurrency(int brokerId) {
        return _intraBrokerPartitionMovementConcurrency.getOrDefault(brokerId, intraBrokerPartitionMovementConcurrency());
    }

    private synchronized int intraBrokerPartitionMovementConcurrency() {
        return _requestedIntraBrokerPartitionMovementConcurrency != null
                ? _requestedIntraBrokerPartitionMovementConcurrency : _defaultIntraBrokerPartitionMovementConcurrency;
    }

    private synchronized int requestedBrokerLeadershipMovementConcurrency() {
        return _requestedBrokerLeadershipMovementConcurrency != null
                ? _requestedBrokerLeadershipMovementConcurrency : _defaultPerBrokerLeadershipMovementConcurrency;
    }

    private synchronized int leadershipMovementConcurrency(int brokerId) {
        return _brokerLeadershipMovementConcurrency.getOrDefault(brokerId, requestedBrokerLeadershipMovementConcurrency());
    }
}
