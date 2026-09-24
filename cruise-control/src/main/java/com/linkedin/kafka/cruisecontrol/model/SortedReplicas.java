/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.*;
import java.util.function.Function;

/**
 * <p>
 * A class used by the brokers/disks to host the replicas sorted in a certain order.
 * </p>
 * <p>
 * The SortedReplicas uses three type of functions to sort the replicas in the broker/disk.
 *  <ul>
 *     <li>
 *      {@code ScoreFunction}(optional): the score function generates a score for each replica to sort. The replicas are
 *      sorted based on their score in ascending order.
 *    </li>
 *    <li>
 *      {@code SelectionFunction}(optional): the selection function decides which replicas to include in the sorted
 *      replica list. For example, in some cases, the users may only want to have sorted leader replicas. Note there can
 *      be multiple selection functions, only replica which satisfies requirement of all selection functions will be included.
 *    </li>
 *    <li>
 *      {@code PriorityFunction}(optional): the priority function allows users to prioritize certain replicas in the
 *      sorted replicas. The replicas will be sorted by their priority first. There can be multiple priority functions,
 *      which will be applied one by one based on the order in {@link #_priorityFuncs} to resolve priority between two replicas.
 *      In the end, the replicas with the same priority are sorted with their score from the {@code scoreFunction}.
 *      Note that if a priority function is provided, the {@code SortedSet} returned by the
 *      {@link #sortedReplicas(boolean)} is no longer binary searchable based on the score.
 *    </li>
 *  </ul>
 *
 * <p>
 *   The SortedReplicas are initialized lazily, i.e. until one of {@link #sortedReplicas(boolean)} is invoked, the sorted replicas
 *   will not be populated.
 * </p>
 */
public class SortedReplicas {
    private final Broker _broker;
    private final Disk _disk;
    private final SortedSet<Replica> _sortedReplicas;
    private final Set<Function<Replica, Boolean>> _selectionFuncs;
    private final List<Function<Replica, Integer>> _priorityFuncs;
    private final Function<Replica, Double> _scoreFunc;
    private final Comparator<Replica> _replicaComparator;
    private boolean _initialized;

    SortedReplicas(Broker broker,
                   Set<Function<Replica, Boolean>> selectionFuncs,
                   List<Function<Replica, Integer>> priorityFuncs,
                   Function<Replica, Double> scoreFunction) {
        this(broker, null, selectionFuncs, priorityFuncs, scoreFunction, true);
    }

    SortedReplicas(Broker broker,
                   Disk disk,
                   Set<Function<Replica, Boolean>> selectionFuncs,
                   List<Function<Replica, Integer>> priorityFuncs,
                   Function<Replica, Double> scoreFunc,
                   boolean initialize) {
        _broker = broker;
        _disk = disk;
        _selectionFuncs = selectionFuncs;
        _scoreFunc = scoreFunc;
        _priorityFuncs = priorityFuncs;
        _replicaComparator = (Replica r1, Replica r2) -> {
            // First apply priority functions.
            int result = comparePriority(r1, r2);
            if (result != 0) {
                return result;
            }
            // Then apply score function.
            if (_scoreFunc != null) {
                result = Double.compare(_scoreFunc.apply(r1), _scoreFunc.apply(r2));
                if (result != 0) {
                    return result;
                }
            }
            // Fall back to replica's own comparing method.
            return r1.compareTo(r2);
        };
        _sortedReplicas = new TreeSet<>(_replicaComparator);
        // If the sorted replicas need to be initialized, we set the initialized to false and initialize the replicas
        // lazily. If the sorted replicas do not need to be initialized, we simply set the initialized to true, so that
        // all the methods will function normally.
        _initialized = !initialize;
    }

    /**
     * Get the sorted replicas in the ascending order of their priority and score.
     * This method initialize the sorted replicas if it hasn't been initialized.
     *
     * @param clone whether return a clone of the replica set or the set itself. In general, the clone should be avoided
     *              whenever possible, it is only needed where the sorted replica will be updated in the middle of being iterated.
     * @return The sorted replicas in the ascending order of their priority and score.
     */
    public SortedSet<Replica> sortedReplicas(boolean clone) {
        ensureInitialize();
        if (clone) {
            SortedSet<Replica> result = new TreeSet<>(_replicaComparator);
            result.addAll(_sortedReplicas);
            return result;
        }
        return Collections.unmodifiableSortedSet(_sortedReplicas);
    }

    /**
     * @return The selection functions of this {@link SortedReplicas}
     */
    public Set<Function<Replica, Boolean>> selectionFunctions() {
        return _selectionFuncs;
    }

    /**
     * @return The priority functions of this {@link SortedReplicas}
     */
    public List<Function<Replica, Integer>> priorityFunctions() {
        return _priorityFuncs;
    }

    /**
     * @return The score function of this {@link SortedReplicas}
     */
    public Function<Replica, Double> scoreFunction() {
        return _scoreFunc;
    }

    /**
     * Add a new replica to the sorted replicas. The replica will be included if it satisfies the requirement of all
     * selection functions. It has no impact if this {@link SortedReplicas} has not been initialized.
     *
     * @param replica the replica to add.
     */
    public void add(Replica replica) {
        if (_initialized) {
            if (_selectionFuncs == null || _selectionFuncs.stream().allMatch(func -> func.apply(replica))) {
                _sortedReplicas.add(replica);
            }
        }
    }

    /**
     * Remove a new replicas to the sorted replicas. It has no impact if this {@link SortedReplicas} has not been
     * initialized.
     *
     * @param replica the replica to remove.
     */
    void remove(Replica replica) {
        if (_initialized) {
            _sortedReplicas.remove(replica);
        }
    }

    // Unit test only function.
    int numReplicas() {
        return _sortedReplicas.size();
    }

    private void ensureInitialize() {
        if (!_initialized) {
            _initialized = true;
            if (_disk != null) {
                _disk.replicas().forEach(this::add);
            } else {
                _broker.replicas().forEach(this::add);
            }
        }
    }

    private int comparePriority(Replica replica1, Replica replica2) {
        if (_priorityFuncs != null) {
            // Apply priority functions one by one until the priority is resolved.
            for (Function<Replica, Integer> priorityFunction : _priorityFuncs) {
                int p1 = priorityFunction.apply(replica1);
                int p2 = priorityFunction.apply(replica2);
                int result = Integer.compare(p1, p2);
                if (result != 0) {
                    return result;
                }
            }
        }
        return 0;
    }
}
