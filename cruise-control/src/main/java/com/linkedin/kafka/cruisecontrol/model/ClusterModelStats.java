/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.averageDiskUtilizationPercentage;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.diskUtilizationPercentage;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;


@JsonResponseClass
public class ClusterModelStats {
    @JsonResponseField
    private static final String METADATA = "metadata";
    @JsonResponseField
    private static final String STATISTICS = "statistics";
    private final Map<Statistic, Map<Resource, Double>> _resourceUtilizationStats;
    private final Map<Statistic, Double> _potentialNwOutUtilizationStats;
    private final Map<Statistic, Number> _replicaStats;
    private final Map<Statistic, Number> _leaderReplicaStats;
    private final Map<Statistic, Number> _topicReplicaStats;
    private int _numBrokers;
    private int _numReplicasInCluster;
    private int _numPartitionsWithOfflineReplicas;
    private int _numTopics;
    private final Map<Resource, Integer> _numBalancedBrokersByResource;
    private int _numBrokersUnderPotentialNwOut;
    private BalancingConstraint _balancingConstraint;
    private double[][] _utilizationMatrix;
    private int _numSnapshotWindows;
    private double _monitoredPartitionsRatio;
    // Number of unbalanced disks in the cluster.
    private int _numUnbalancedDisks;
    // Aggregated standard deviation of disk utilization for the cluster.
    private double _diskUtilizationStDev;
    private Set<Integer> _brokersAllowedReplicaMove;

    /**
     * Constructor for analysis stats.
     */
    ClusterModelStats() {
        _resourceUtilizationStats = new HashMap<>();
        _potentialNwOutUtilizationStats = new HashMap<>();
        _replicaStats = new HashMap<>();
        _leaderReplicaStats = new HashMap<>();
        _topicReplicaStats = new HashMap<>();
        _numBrokers = 0;
        _numReplicasInCluster = 0;
        _numPartitionsWithOfflineReplicas = 0;
        _numTopics = 0;
        _numBrokersUnderPotentialNwOut = 0;
        _numBalancedBrokersByResource = new HashMap<>();
        _numUnbalancedDisks = 0;
        _diskUtilizationStDev = 0;
        _brokersAllowedReplicaMove = Collections.emptySet();
    }

    /**
     * Populate the analysis stats with this cluster and given balancing constraint.
     *
     * @param clusterModel        The state of the cluster.
     * @param balancingConstraint Balancing constraint.
     * @param optimizationOptions Options to take into account while populating stats.
     * @return Analysis stats with this cluster and given balancing constraint.
     */
    ClusterModelStats populate(ClusterModel clusterModel, BalancingConstraint balancingConstraint, OptimizationOptions optimizationOptions) {
        final SortedSet<Broker> brokers = clusterModel.brokers();
        final Set<Broker> aliveBrokers = clusterModel.aliveBrokers();
        Set<String> topics = clusterModel.topics();
        _numBrokers = brokers.size();
        _numTopics = topics.size();
        _balancingConstraint = balancingConstraint;
        _brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
        utilizationForResources(clusterModel, optimizationOptions, aliveBrokers);
        utilizationForPotentialNwOut(clusterModel, optimizationOptions, aliveBrokers);
        numForReplicas(clusterModel, brokers, aliveBrokers);
        numForLeaderReplicas(brokers, aliveBrokers);
        numForAvgTopicReplicas(clusterModel, brokers, topics);
        _utilizationMatrix = clusterModel.utilizationMatrix();
        _numSnapshotWindows = clusterModel.load().numWindows();
        _monitoredPartitionsRatio = clusterModel.monitoredPartitionsRatio();
        populateStatsForDisks(balancingConstraint, aliveBrokers);
        return this;
    }

    /**
     * @return The resource utilization stats for the cluster instance that the object was populated with.
     */
    public Map<Statistic, Map<Resource, Double>> resourceUtilizationStats() {
        return Collections.unmodifiableMap(_resourceUtilizationStats);
    }

    /**
     * @return The potential outbound network utilization stats for the cluster instance that the object was populated with.
     */
    public Map<Statistic, Double> potentialNwOutUtilizationStats() {
        return Collections.unmodifiableMap(_potentialNwOutUtilizationStats);
    }

    /**
     * @return Replica stats for the cluster instance that the object was populated with.
     */
    public Map<Statistic, Number> replicaStats() {
        return Collections.unmodifiableMap(_replicaStats);
    }

    /**
     * @return The leader replica stats for the cluster instance that the object was populated with.
     */
    public Map<Statistic, Number> leaderReplicaStats() {
        return Collections.unmodifiableMap(_leaderReplicaStats);
    }

    /**
     * @return Topic replica stats for the cluster instance that the object was populated with.
     */
    public Map<Statistic, Number> topicReplicaStats() {
        return Collections.unmodifiableMap(_topicReplicaStats);
    }

    /**
     * @return The number of brokers for the cluster instance that the object was populated with.
     */
    public int numBrokers() {
        return _numBrokers;
    }

    /**
     * @return The number of replicas for the cluster instance that the object was populated with.
     */
    public int numReplicasInCluster() {
        return _numReplicasInCluster;
    }

    /**
     * @return The number of number of partitions with offline replicas in the cluster.
     */
    public int numPartitionsWithOfflineReplicas() {
        return _numPartitionsWithOfflineReplicas;
    }

    /**
     * @return The number of topics for the cluster instance that the object was populated with.
     */
    public int numTopics() {
        return _numTopics;
    }

    /**
     * @return The number of balanced brokers that are not excluded for replica moves by resource for the cluster instance
     * that the object was populated with.
     */
    public Map<Resource, Integer> numBalancedBrokersByResource() {
        return Collections.unmodifiableMap(_numBalancedBrokersByResource);
    }

    /**
     * @return The number of brokers that are not excluded for replica moves under potential nw out for the cluster instance
     * that the object was populated with.
     */
    public int numBrokersUnderPotentialNwOut() {
        return _numBrokersUnderPotentialNwOut;
    }

    /**
     * @return Balancing constraint used while populating the stats.
     */
    public BalancingConstraint balancingConstraint() {
        return _balancingConstraint;
    }

    /**
     * This is the utilization matrix generated from {@link ClusterModel#utilizationMatrix()}.
     *
     * @return Non-null if populate has been called else this may return null.
     */
    public double[][] utilizationMatrix() {
        return _utilizationMatrix;
    }

    /**
     * @return The monitored partition percentage of this cluster model.
     */
    public double monitoredPartitionsPercentage() {
        return _monitoredPartitionsRatio * UNIT_INTERVAL_TO_PERCENTAGE;
    }

    /**
     * @return The number of windows used by this cluster model.
     */
    public int numWindows() {
        return _numSnapshotWindows;
    }

    /**
     * Get the number of unbalanced disk in this cluster model;
     * A disk is taken as unbalanced if its utilization percentage is out of the range centered at its broker utilization
     * percentage with boundary determined by
     * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#DISK_BALANCE_THRESHOLD_CONFIG}.
     *
     * @return The number of unbalanced disk in this cluster model.
     */
    public int numUnbalancedDisks() {
        return _numUnbalancedDisks;
    }

    /**
     * @return The standard deviation of disk utilization of this cluster model.
     */
    public double diskUtilizationStandardDeviation() {
        return _diskUtilizationStDev;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        return Map.of(METADATA, new ClusterModelStatsMetaData(numBrokers(), numReplicasInCluster(), numTopics()).getJsonStructure(),
                STATISTICS, new ClusterModelStatsValue(_resourceUtilizationStats,
                        _potentialNwOutUtilizationStats,
                        _replicaStats,
                        _leaderReplicaStats,
                        _topicReplicaStats).getJsonStructure());
    }

    /**
     * @return A string representation of the cluster counts including brokers, replicas, and topics.
     */
    public String toStringCounts() {
        return String.format("%d brokers %d replicas %d topics.", numBrokers(), numReplicasInCluster(), numTopics());
    }

    @Override
    public String toString() {
        return new ClusterModelStatsValue(_resourceUtilizationStats,
                _potentialNwOutUtilizationStats,
                _replicaStats,
                _leaderReplicaStats,
                _topicReplicaStats).toString();
    }

    /**
     * Generate statistics of utilization for resources among alive brokers in the given cluster.
     * Average and standard deviation calculations are based on brokers not excluded for replica moves.
     *
     * @param clusterModel        The state of the cluster.
     * @param optimizationOptions Options to take into account while retrieving cluster capacity.
     * @param aliveBrokers        Alive brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void utilizationForResources(ClusterModel clusterModel, OptimizationOptions optimizationOptions, Set<Broker> aliveBrokers) {
        // Average, maximum, and standard deviation of utilization by resource.
        Map<Resource, Double> avgUtilizationByResource = new HashMap<>();
        Map<Resource, Double> maxUtilizationByResource = new HashMap<>();
        Map<Resource, Double> minUtilizationByResource = new HashMap<>();
        Map<Resource, Double> stDevUtilizationByResource = new HashMap<>();
        for (Resource resource : Resource.cachedValues()) {
            double resourceUtilization = clusterModel.load().expectedUtilizationFor(resource);
            double avgUtilizationPercentage = resourceUtilization / clusterModel.capacityWithAllowedReplicaMovesFor(resource, optimizationOptions);

            double balanceUpperThreshold = GoalUtils.computeResourceUtilizationBalanceThreshold(avgUtilizationPercentage,
                    resource,
                    _balancingConstraint,
                    optimizationOptions.isTriggeredByGoalViolation(),
                    ResourceDistributionGoal.BALANCE_MARGIN,
                    false);

            double balanceLowerThreshold = GoalUtils.computeResourceUtilizationBalanceThreshold(avgUtilizationPercentage,
                    resource,
                    _balancingConstraint,
                    optimizationOptions.isTriggeredByGoalViolation(),
                    ResourceDistributionGoal.BALANCE_MARGIN,
                    true);

            // Maximum, minimum, and standard deviation utilization for the resource.
            double hottestBrokerUtilization = 0.0;
            double coldestBrokerUtilization = Double.MAX_VALUE;
            double varianceSum = 0.0;
            int numBalancedBrokersInBrokersAllowedReplicaMove = 0;
            for (Broker broker : aliveBrokers) {
                double utilization = resource.isHostResource() ? broker.host().load().expectedUtilizationFor(resource)
                        : broker.load().expectedUtilizationFor(resource);
                hottestBrokerUtilization = Math.max(hottestBrokerUtilization, utilization);
                coldestBrokerUtilization = Math.min(coldestBrokerUtilization, utilization);

                if (_brokersAllowedReplicaMove.contains(broker.id())) {
                    double capacity = resource.isHostResource() ? broker.host().capacityFor(resource)
                            : broker.capacityFor(resource);
                    double utilizationPercentage = utilization / capacity;
                    if (utilizationPercentage >= balanceLowerThreshold && utilizationPercentage <= balanceUpperThreshold) {
                        numBalancedBrokersInBrokersAllowedReplicaMove++;
                    }
                    varianceSum += Math.pow(utilization - avgUtilizationPercentage * capacity, 2);
                }
            }
            _numBalancedBrokersByResource.put(resource, numBalancedBrokersInBrokersAllowedReplicaMove);
            avgUtilizationByResource.put(resource, resourceUtilization / _brokersAllowedReplicaMove.size());
            maxUtilizationByResource.put(resource, hottestBrokerUtilization);
            minUtilizationByResource.put(resource, coldestBrokerUtilization);
            stDevUtilizationByResource.put(resource, Math.sqrt(varianceSum / _brokersAllowedReplicaMove.size()));
        }
        _resourceUtilizationStats.put(Statistic.AVG, avgUtilizationByResource);
        _resourceUtilizationStats.put(Statistic.MAX, maxUtilizationByResource);
        _resourceUtilizationStats.put(Statistic.MIN, minUtilizationByResource);
        _resourceUtilizationStats.put(Statistic.ST_DEV, stDevUtilizationByResource);
    }

    /**
     * Generate statistics of potential network outbound utilization among alive brokers in the given cluster.
     * Average and standard deviation calculations are based on brokers not excluded for replica moves.
     *
     * @param clusterModel        The state of the cluster.
     * @param optimizationOptions Options to take into account while retrieving cluster capacity.
     * @param aliveBrokers        Alive brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void utilizationForPotentialNwOut(ClusterModel clusterModel, OptimizationOptions optimizationOptions, Set<Broker> aliveBrokers) {
        // Average, minimum, and maximum: network outbound utilization and replicas in brokers.
        double maxPotentialNwOut = 0.0;
        double minPotentialNwOut = Double.MAX_VALUE;
        double varianceSum = 0.0;
        double potentialNwOutInCluster = aliveBrokers.stream()
                .filter(b -> _brokersAllowedReplicaMove.contains(b.id()))
                .mapToDouble(b -> clusterModel.potentialLeadershipLoadFor(b.id())
                        .expectedUtilizationFor(Resource.NW_OUT))
                .sum();
        double capacity = clusterModel.capacityWithAllowedReplicaMovesFor(Resource.NW_OUT, optimizationOptions);
        double avgPotentialNwOutUtilizationPct = potentialNwOutInCluster / capacity;
        double capacityThreshold = _balancingConstraint.capacityThreshold(Resource.NW_OUT);
        for (Broker broker : aliveBrokers) {
            double brokerUtilization = clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
            double brokerCapacity = broker.capacityFor(Resource.NW_OUT);
            maxPotentialNwOut = Math.max(maxPotentialNwOut, brokerUtilization);
            minPotentialNwOut = Math.min(minPotentialNwOut, brokerUtilization);

            if (_brokersAllowedReplicaMove.contains(broker.id())) {
                if (brokerUtilization / brokerCapacity <= capacityThreshold) {
                    _numBrokersUnderPotentialNwOut++;
                }
                varianceSum += (Math.pow(brokerUtilization - avgPotentialNwOutUtilizationPct * brokerCapacity, 2));
            }
        }
        _potentialNwOutUtilizationStats.put(Statistic.AVG, potentialNwOutInCluster / _brokersAllowedReplicaMove.size());
        _potentialNwOutUtilizationStats.put(Statistic.MAX, maxPotentialNwOut);
        _potentialNwOutUtilizationStats.put(Statistic.MIN, minPotentialNwOut);
        _potentialNwOutUtilizationStats.put(Statistic.ST_DEV, Math.sqrt(varianceSum / _brokersAllowedReplicaMove.size()));
    }

    /**
     * Generate statistics for replicas in the given cluster.
     *
     * @param clusterModel The state of the cluster.
     * @param brokers      Brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     * @param aliveBrokers Alive brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void numForReplicas(ClusterModel clusterModel, SortedSet<Broker> brokers, Set<Broker> aliveBrokers) {
        populateReplicaStats(broker -> broker.replicas().size(),
                _replicaStats,
                brokers,
                aliveBrokers);
        _numReplicasInCluster = clusterModel.numReplicas();
        // Set the number of partitions with offline replicas.
        Set<TopicPartition> partitionsWithOfflineReplicas = new HashSet<>();
        for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
            partitionsWithOfflineReplicas.add(replica.topicPartition());
        }
        _numPartitionsWithOfflineReplicas = partitionsWithOfflineReplicas.size();
    }

    /**
     * Generate statistics for leader replicas in the given cluster.
     *
     * @param brokers      Brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     * @param aliveBrokers Alive brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void numForLeaderReplicas(SortedSet<Broker> brokers, Set<Broker> aliveBrokers) {
        populateReplicaStats(broker -> broker.leaderReplicas().size(),
                _leaderReplicaStats,
                brokers,
                aliveBrokers);
    }

    /**
     * Generate statistics for replicas of interest in the given cluster.
     * Average and standard deviation calculations are based on brokers not excluded for replica moves.
     *
     * @param numInterestedReplicasFunc function to calculate number of replicas of interest on a broker.
     * @param interestedReplicaStats    statistics for replicas of interest.
     * @param brokers                   Brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     * @param aliveBrokers              Alive brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void populateReplicaStats(Function<Broker, Integer> numInterestedReplicasFunc,
                                      Map<Statistic, Number> interestedReplicaStats,
                                      SortedSet<Broker> brokers,
                                      Set<Broker> aliveBrokers) {
        // Average, minimum, and maximum number of replicas of interest in brokers.
        int maxInterestedReplicasInBroker = 0;
        int minInterestedReplicasInBroker = Integer.MAX_VALUE;
        int numInterestedReplicasInCluster = 0;
        for (Broker broker : brokers) {
            int numInterestedReplicasInBroker = numInterestedReplicasFunc.apply(broker);
            numInterestedReplicasInCluster += numInterestedReplicasInBroker;
            maxInterestedReplicasInBroker = Math.max(maxInterestedReplicasInBroker, numInterestedReplicasInBroker);
            minInterestedReplicasInBroker = Math.min(minInterestedReplicasInBroker, numInterestedReplicasInBroker);
        }
        double avgInterestedReplicas = ((double) numInterestedReplicasInCluster) / _brokersAllowedReplicaMove.size();

        // Standard deviation of replicas of interest in alive brokers.
        double variance = 0.0;
        for (Broker broker : aliveBrokers) {
            if (_brokersAllowedReplicaMove.contains(broker.id())) {
                variance += (Math.pow((double) numInterestedReplicasFunc.apply(broker) - avgInterestedReplicas, 2)
                        / _brokersAllowedReplicaMove.size());
            }
        }

        interestedReplicaStats.put(Statistic.AVG, avgInterestedReplicas);
        interestedReplicaStats.put(Statistic.MAX, maxInterestedReplicasInBroker);
        interestedReplicaStats.put(Statistic.MIN, minInterestedReplicasInBroker);
        interestedReplicaStats.put(Statistic.ST_DEV, Math.sqrt(variance));
    }

    /**
     * Generate statistics for topic replicas in the given cluster.
     * Average and standard deviation calculations are based on brokers not excluded for replica moves.
     *
     * @param clusterModel The state of the cluster.
     * @param brokers      Brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     * @param topics       Topics in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void numForAvgTopicReplicas(ClusterModel clusterModel, SortedSet<Broker> brokers, Set<String> topics) {
        _topicReplicaStats.put(Statistic.AVG, 0.0);
        _topicReplicaStats.put(Statistic.MAX, 0);
        _topicReplicaStats.put(Statistic.MIN, Integer.MAX_VALUE);
        _topicReplicaStats.put(Statistic.ST_DEV, 0.0);
        for (String topic : topics) {
            int maxTopicReplicasInBroker = 0;
            int minTopicReplicasInBroker = Integer.MAX_VALUE;
            double avgTopicReplicas = ((double) clusterModel.numTopicReplicas(topic)) / _brokersAllowedReplicaMove.size();
            double variance = 0.0;
            for (Broker broker : brokers) {
                int numTopicReplicasInBroker = broker.numReplicasOfTopicInBroker(topic);
                maxTopicReplicasInBroker = Math.max(maxTopicReplicasInBroker, numTopicReplicasInBroker);
                minTopicReplicasInBroker = Math.min(minTopicReplicasInBroker, numTopicReplicasInBroker);
                if (broker.isAlive() && _brokersAllowedReplicaMove.contains(broker.id())) {
                    // Standard deviation of replicas in brokers allowed replica move.
                    variance += (Math.pow(numTopicReplicasInBroker - avgTopicReplicas, 2) / _brokersAllowedReplicaMove.size());
                }
            }

            _topicReplicaStats.put(Statistic.AVG, _topicReplicaStats.get(Statistic.AVG).doubleValue() + avgTopicReplicas);
            _topicReplicaStats.put(Statistic.MAX,
                    Math.max(_topicReplicaStats.get(Statistic.MAX).intValue(), maxTopicReplicasInBroker));
            _topicReplicaStats.put(Statistic.MIN,
                    Math.min(_topicReplicaStats.get(Statistic.MIN).intValue(), minTopicReplicasInBroker));
            _topicReplicaStats.put(Statistic.ST_DEV, (Double) _topicReplicaStats.get(Statistic.ST_DEV) + Math.sqrt(variance));
        }

        _topicReplicaStats.put(Statistic.AVG, _topicReplicaStats.get(Statistic.AVG).doubleValue() / _numTopics);
        _topicReplicaStats.put(Statistic.ST_DEV, _topicReplicaStats.get(Statistic.ST_DEV).doubleValue() / _numTopics);
    }

    /**
     * Generate statistics for disks in the given cluster.
     * For each alive disk on disk broker in the cluster, check whether its utilization percentage is within the range centered
     * at its broker utilization percentage with boundary determined by
     * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#DISK_BALANCE_THRESHOLD_CONFIG}.
     * If the disk utilization percentage is out of the boundary, the disk is counted as unbalanced.
     * Also sum up the variance of utilization for each alive disk and get an aggregated standard deviation.
     *
     * @param balancingConstraint Balancing constraint.
     * @param aliveBrokers        Alive brokers in the cluster -- passed to this function to avoid recomputing them using cluster model.
     */
    private void populateStatsForDisks(BalancingConstraint balancingConstraint, Set<Broker> aliveBrokers) {
        double totalDiskUtilizationVariance = 0;
        int numAliveDisks = 0;
        for (Broker broker : aliveBrokers) {
            double brokerDiskUtilization = averageDiskUtilizationPercentage(broker);
            double upperLimit = brokerDiskUtilization * balancingConstraint.resourceBalancePercentage(Resource.DISK);
            double lowerLimit = brokerDiskUtilization * Math.max(0, (2 - balancingConstraint.resourceBalancePercentage(Resource.DISK)));
            for (Disk disk : broker.disks()) {
                if (!disk.isAlive()) {
                    continue;
                }
                double diskUtilizationPercentage = diskUtilizationPercentage(disk);
                if (diskUtilizationPercentage > upperLimit || diskUtilizationPercentage < lowerLimit) {
                    _numUnbalancedDisks++;
                }
                totalDiskUtilizationVariance += Math.pow(diskUtilizationPercentage - brokerDiskUtilization, 2);
                numAliveDisks++;
            }
        }
        if (numAliveDisks > 0) {
            _diskUtilizationStDev = Math.sqrt(totalDiskUtilizationVariance / numAliveDisks);
        }
    }
}
