/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;


/**
 * A helper class to create new {@link SortedReplicas} to track for {@link ClusterModel}/{@link Broker}/{@link Disk}.
 * This class provides fluent API to enable user to conveniently customize {@link SortedReplicas} by adding any
 * selection/priority/score functions before creating {@link SortedReplicas}.
 * <p>
 * One example is as follows. Here we want to create a SortedReplica inside clusterModel which includes only follower replicas,
 * prioritize offline replicas and sort replicas based on disk utilization of replica.
 * <p>
 * <pre>
 * {@code
 * new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectFollowers())
 *                           .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeOfflineReplicas());
 *                           .addScoreFunc(addScoreFunc(ReplicaSortFunctionFactory.sortByMetricGroupValue(DISK.name())))
 *                           .trackSortedReplicasFor(sortName, clusterModel)
 * } </pre>
 */
public class SortedReplicasHelper {

    private final Set<Function<Replica, Boolean>> _selectionFuncs;
    private final Set<Function<Replica, Integer>> _priorityFuncs;
    private Function<Replica, Double> _scoreFunc;

    public SortedReplicasHelper() {
        _selectionFuncs = new LinkedHashSet<>();
        _priorityFuncs = new LinkedHashSet<>();
    }

    /**
     * If condition holds, add a selection function to the {@link SortedReplicas} to be created.
     *
     * @param selectionFunc         The selection function to add.
     * @param addConditionSatisfied Whether condition to add the selection function is satisfied or not.
     * @return The helper object itself.
     */
    public SortedReplicasHelper maybeAddSelectionFunc(Function<Replica, Boolean> selectionFunc, boolean addConditionSatisfied) {
        if (addConditionSatisfied) {
            _selectionFuncs.add(selectionFunc);
        }
        return this;
    }

    /**
     * Add a selection function to the {@link SortedReplicas} to be created.
     *
     * @param selectionFunc The selection function to add.
     * @return The helper object itself.
     */
    public SortedReplicasHelper addSelectionFunc(Function<Replica, Boolean> selectionFunc) {
        return maybeAddSelectionFunc(selectionFunc, true);
    }

    /**
     * If condition holds, add a priority function to the {@link SortedReplicas} to be created.
     * Note the order of adding priority function matters. Each call of this method (if condition holds)
     * will add a new priority function to the end of priority function linked hash set, which means when sorting two replicas
     * by priority, this priority function will be applied only if all previous priority functions are unable to decide
     * the priority between two replicas.
     *
     * @param priorityFunc          The priority function to add.
     * @param addConditionSatisfied Whether condition to add the selection function is satisfied or not.
     * @return The helper object itself.
     */
    public SortedReplicasHelper maybeAddPriorityFunc(Function<Replica, Integer> priorityFunc, boolean addConditionSatisfied) {
        if (addConditionSatisfied) {
            _priorityFuncs.add(priorityFunc);
        }
        return this;
    }

    /**
     * Add a priority function to the {@link SortedReplicas} to be created.
     * Note the order of adding priority function matters. Each call of this method will add a new priority function to the
     * end of priority function linked hash set, which means when sorting two replicas by priority, this priority function
     * will be applied only if all previous priority functions are unable to decide the priority between two replicas.
     *
     * @param priorityFunc The priority function to add.
     * @return The helper object itself.
     */
    public SortedReplicasHelper addPriorityFunc(Function<Replica, Integer> priorityFunc) {
        return maybeAddPriorityFunc(priorityFunc, true);
    }

    /**
     * Set score function in the {@link SortedReplicas} to be created.
     *
     * @param scoreFunc The score function.
     * @return The helper object itself.
     */
    public SortedReplicasHelper setScoreFunc(Function<Replica, Double> scoreFunc) {
        _scoreFunc = scoreFunc;
        return this;
    }

    /**
     * Create {@link SortedReplicas} for given {@link Disk} with added selection/priority/score functions.
     *
     * @param sortName The name of {@link SortedReplicas}.
     * @param disk     The disk to create {@link SortedReplicas} to track.
     */
    public void trackSortedReplicasFor(String sortName, Disk disk) {
        disk.trackSortedReplicas(sortName, new HashSet<>(_selectionFuncs), new ArrayList<>(_priorityFuncs), _scoreFunc);
    }

    /**
     * Create {@link SortedReplicas} for given {@link Broker} with added selection/priority/score functions.
     *
     * @param sortName The name of {@link SortedReplicas}.
     * @param broker   The broker to create {@link SortedReplicas} to track.
     */
    public void trackSortedReplicasFor(String sortName, Broker broker) {
        broker.trackSortedReplicas(sortName, new HashSet<>(_selectionFuncs), new ArrayList<>(_priorityFuncs), _scoreFunc);
    }

    /**
     * Create {@link SortedReplicas} for each brokers in the given {@link ClusterModel} with added selection/priority/score functions.
     *
     * @param sortName     The name of {@link SortedReplicas}.
     * @param clusterModel The cluster model to create {@link SortedReplicas}  to track.
     */
    public void trackSortedReplicasFor(String sortName, ClusterModel clusterModel) {
        clusterModel.trackSortedReplicas(sortName, new HashSet<>(_selectionFuncs), new ArrayList<>(_priorityFuncs), _scoreFunc);
    }
}
