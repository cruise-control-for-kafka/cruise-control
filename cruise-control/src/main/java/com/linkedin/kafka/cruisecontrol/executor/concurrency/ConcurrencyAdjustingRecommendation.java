/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.concurrency;

import java.util.HashSet;
import java.util.Set;


/**
 * This class encapsulates the concurrency adjusting recommendation. There are 3 types of recommendation
 * 1. Recommend changes on concurrency. It also includes one set of brokers to increase concurrency {@link #_brokersToIncreaseConcurrency}
 * and the other set of brokers to decrease concurrency {@link #_brokersToDecreaseConcurrency}.
 * 2. {@link #STOP_EXECUTION} Recommend to stop the execution.
 * 3. {@link #NO_CHANGE_RECOMMENDED} Recommend no changes on concurrency.
 *
 */
public class ConcurrencyAdjustingRecommendation {

    private enum ConcurrencyAdjustingRecommendationType {
        NO_CHANGE_RECOMMENDED,
        CHANGE_RECOMMENDED,
        STOP_EXECUTION
    }

    private enum ClusterConcurrencyAdjustType {
        NO_CHANGE,
        INCREASE,
        DECREASE
    }

    public static final ConcurrencyAdjustingRecommendation NO_CHANGE_RECOMMENDED
            = new ConcurrencyAdjustingRecommendation(ConcurrencyAdjustingRecommendationType.NO_CHANGE_RECOMMENDED);
    public static final ConcurrencyAdjustingRecommendation STOP_EXECUTION =
            new ConcurrencyAdjustingRecommendation(ConcurrencyAdjustingRecommendationType.STOP_EXECUTION);
    private final Set<Integer> _brokersToIncreaseConcurrency;
    private final Set<Integer> _brokersToDecreaseConcurrency;
    private final ConcurrencyAdjustingRecommendationType _type;
    private ClusterConcurrencyAdjustType _clusterConcurrencyAdjustRecommendation;

    public ConcurrencyAdjustingRecommendation() {
        _brokersToDecreaseConcurrency = new HashSet<>();
        _brokersToIncreaseConcurrency = new HashSet<>();
        _type = ConcurrencyAdjustingRecommendationType.CHANGE_RECOMMENDED;
        _clusterConcurrencyAdjustRecommendation = ClusterConcurrencyAdjustType.NO_CHANGE;
    }

    private ConcurrencyAdjustingRecommendation(ConcurrencyAdjustingRecommendationType type) {
        _brokersToDecreaseConcurrency = new HashSet<>();
        _brokersToIncreaseConcurrency = new HashSet<>();
        _clusterConcurrencyAdjustRecommendation = ClusterConcurrencyAdjustType.NO_CHANGE;
        _type = type;
    }

    public void recommendConcurrencyIncrease(int brokerId) {
        _brokersToIncreaseConcurrency.add(brokerId);
    }

    public void recommendConcurrencyDecrease(int brokerId) {
        _brokersToDecreaseConcurrency.add(brokerId);
    }

    public void recommendDecreaseClusterConcurrency() {
        _clusterConcurrencyAdjustRecommendation = ClusterConcurrencyAdjustType.DECREASE;
    }

    public void recommendIncreaseClusterConcurrency() {
        _clusterConcurrencyAdjustRecommendation = ClusterConcurrencyAdjustType.INCREASE;
    }

    public Set<Integer> getBrokersToIncreaseConcurrency() {
        return _brokersToIncreaseConcurrency;
    }

    public Set<Integer> getBrokersToDecreaseConcurrency() {
        return _brokersToDecreaseConcurrency;
    }

    public boolean shouldStopExecution() {
        return _type.equals(ConcurrencyAdjustingRecommendationType.STOP_EXECUTION);
    }

    public boolean noChangeRecommended() {
        return _type.equals(ConcurrencyAdjustingRecommendationType.NO_CHANGE_RECOMMENDED);
    }

    /**
     * Check whether to increase cluster concurrency
     *
     * @return true if you should increase cluster concurrency, otherwise return false.
     */
    public boolean shouldIncreaseClusterConcurrency() {
        return _type == ConcurrencyAdjustingRecommendationType.CHANGE_RECOMMENDED
                && _clusterConcurrencyAdjustRecommendation == ClusterConcurrencyAdjustType.INCREASE;
    }

    /**
     * Check whether  to decrease cluster concurrency
     *
     * @return true if you should decrease cluster concurrency, otherwise return false.
     */
    public boolean shouldDecreaseClusterConcurrency() {
        return _type == ConcurrencyAdjustingRecommendationType.CHANGE_RECOMMENDED
                && _clusterConcurrencyAdjustRecommendation == ClusterConcurrencyAdjustType.DECREASE;
    }
}
