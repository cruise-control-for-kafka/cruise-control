/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetAssignmentPolicy;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetFileResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.ReplicaToBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;


/**
 * A class that holds the information of balancing constraint of resources, balance and capacity thresholds, and self
 * healing distribution threshold multiplier.
 */
public class BalancingConstraint {
  private final Map<Resource, Double> _resourceBalancePercentage;
  private final double _replicaBalancePercentage;
  private final double _leaderReplicaBalancePercentage;
  private final double _topicLeaderReplicaBalancePercentage;
  private final int _topicLeaderReplicaBalanceMinGap;
  private final int _topicLeaderReplicaBalanceMaxGap;
  private final double _topicReplicaBalancePercentage;
  private final int _topicReplicaBalanceMinGap;
  private final int _topicReplicaBalanceMaxGap;
  private final double _goalViolationDistributionThresholdMultiplier;
  private final Map<Resource, Double> _capacityThreshold;
  private final Map<Resource, Double> _lowUtilizationThreshold;
  private final long _maxReplicasPerBroker;
  private final long _overprovisionedMaxReplicasPerBroker;
  private final int _overprovisionedMinBrokers;
  private final int _overprovisionedMinExtraRacks;
  private final Pattern _topicsWithMinLeadersPerBrokerPattern;
  private final int _minTopicLeadersPerBroker;
  private final long _fastModePerBrokerMoveTimeoutMs;
  private final BrokerSetResolver _brokerSetResolver;
  private final ReplicaToBrokerSetMappingPolicy _replicaToBrokerSetMappingPolicy;

  /**
   * Constructor for Balancing Constraint.
   * (1) Sets resources in descending order of balancing priority.
   * (2) Initializes balance percentages, capacity thresholds, and the maximum number of replicas per broker with the
   * corresponding default values.
   */
  public BalancingConstraint(KafkaCruiseControlConfig config) {
    _resourceBalancePercentage = new HashMap<>();
    _capacityThreshold = new HashMap<>();
    _lowUtilizationThreshold = new HashMap<>();

    // Set default values for balance percentages.
    _resourceBalancePercentage.put(Resource.DISK, config.getDouble(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG));
    _resourceBalancePercentage.put(Resource.CPU, config.getDouble(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG));
    _resourceBalancePercentage.put(Resource.NW_IN, config.getDouble(AnalyzerConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG));
    _resourceBalancePercentage.put(Resource.NW_OUT, config.getDouble(AnalyzerConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG));
    // Set default values for alive resource capacity threshold.
    _capacityThreshold.put(Resource.DISK, config.getDouble(AnalyzerConfig.DISK_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.CPU, config.getDouble(AnalyzerConfig.CPU_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.NW_IN, config.getDouble(AnalyzerConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG));
    _capacityThreshold.put(Resource.NW_OUT, config.getDouble(AnalyzerConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG));
    // Set low utilization threshold
    _lowUtilizationThreshold.put(Resource.DISK, config.getDouble(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.CPU, config.getDouble(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.NW_IN, config.getDouble(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG));
    _lowUtilizationThreshold.put(Resource.NW_OUT, config.getDouble(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG));
    // Set default value for the maximum number of replicas per broker.
    _maxReplicasPerBroker = config.getLong(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG);
    _overprovisionedMaxReplicasPerBroker = config.getLong(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG);
    _overprovisionedMinBrokers = config.getInt(AnalyzerConfig.OVERPROVISIONED_MIN_BROKERS_CONFIG);
    _overprovisionedMinExtraRacks = config.getInt(AnalyzerConfig.OVERPROVISIONED_MIN_EXTRA_RACKS_CONFIG);
    // Set default value for the balance percentage of (1) replica, (2) leader replica and (3) topic replica distribution.
    _replicaBalancePercentage = config.getDouble(AnalyzerConfig.REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _leaderReplicaBalancePercentage = config.getDouble(AnalyzerConfig.LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _topicLeaderReplicaBalancePercentage = config.getDouble(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _topicLeaderReplicaBalanceMinGap = config.getInt(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG);
    _topicLeaderReplicaBalanceMaxGap = config.getInt(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG);
    _topicReplicaBalancePercentage = config.getDouble(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG);
    _topicReplicaBalanceMinGap = config.getInt(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG);
    _topicReplicaBalanceMaxGap = config.getInt(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG);
    _goalViolationDistributionThresholdMultiplier = config.getDouble(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG);
    // Set default value for the topics that must have a minimum number of leader replicas on brokers that are not
    // excluded for replica move.
    _topicsWithMinLeadersPerBrokerPattern = Pattern.compile(config.getString(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG));
    _minTopicLeadersPerBroker = config.getInt(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG);
    // Set default value for the per broker move timeout in fast mode in milliseconds
    _fastModePerBrokerMoveTimeoutMs = config.getLong(AnalyzerConfig.FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_CONFIG);
    // BrokerSet Data resolver class
    Map<String, Object> parameterConfigOverridesForBrokerSetResolver = new HashMap<>();
    parameterConfigOverridesForBrokerSetResolver.put(BrokerSetFileResolver.BROKER_SET_ASSIGNMENT_POLICY_OBJECT_CONFIG,
                                                     config.getConfiguredInstance(AnalyzerConfig.BROKER_SET_ASSIGNMENT_POLICY_CLASS_CONFIG,
                                                                                  BrokerSetAssignmentPolicy.class));
    _brokerSetResolver = config.getConfiguredInstance(AnalyzerConfig.BROKER_SET_RESOLVER_CLASS_CONFIG, BrokerSetResolver.class,
                                                      parameterConfigOverridesForBrokerSetResolver);
    // Replica to Broker Set mapping policy class
    _replicaToBrokerSetMappingPolicy = config.getConfiguredInstance(AnalyzerConfig.REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG,
                                                                    ReplicaToBrokerSetMappingPolicy.class);
  }

  Properties setProps(Properties props) {
    props.put(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.DISK).toString());
    props.put(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.CPU).toString());
    props.put(AnalyzerConfig.NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.NW_IN).toString());
    props.put(AnalyzerConfig.NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG, _resourceBalancePercentage.get(Resource.NW_OUT).toString());

    props.put(AnalyzerConfig.DISK_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.DISK).toString());
    props.put(AnalyzerConfig.CPU_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.CPU).toString());
    props.put(AnalyzerConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.NW_IN).toString());
    props.put(AnalyzerConfig.NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG, _capacityThreshold.get(Resource.NW_OUT).toString());

    props.put(AnalyzerConfig.DISK_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.DISK).toString());
    props.put(AnalyzerConfig.CPU_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.CPU).toString());
    props.put(AnalyzerConfig.NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.NW_IN).toString());
    props.put(AnalyzerConfig.NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG, _lowUtilizationThreshold.get(Resource.NW_OUT).toString());

    props.put(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(_maxReplicasPerBroker));
    props.put(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(_overprovisionedMaxReplicasPerBroker));
    props.put(AnalyzerConfig.OVERPROVISIONED_MIN_BROKERS_CONFIG, Integer.toString(_overprovisionedMinBrokers));
    props.put(AnalyzerConfig.OVERPROVISIONED_MIN_EXTRA_RACKS_CONFIG, Integer.toString(_overprovisionedMinExtraRacks));
    props.put(AnalyzerConfig.REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, Double.toString(_replicaBalancePercentage));
    props.put(AnalyzerConfig.LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, Double.toString(_leaderReplicaBalancePercentage));
    props.put(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, Double.toString(_topicReplicaBalancePercentage));
    props.put(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, Integer.toString(_topicReplicaBalanceMinGap));
    props.put(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG, Integer.toString(_topicReplicaBalanceMaxGap));
    props.put(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG, Double.toString(_topicLeaderReplicaBalancePercentage));
    props.put(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, Integer.toString(_topicLeaderReplicaBalanceMinGap));
    props.put(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG, Integer.toString(_topicLeaderReplicaBalanceMaxGap));
    props.put(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, Double.toString(_goalViolationDistributionThresholdMultiplier));
    props.put(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, _topicsWithMinLeadersPerBrokerPattern.pattern());
    props.put(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG, Integer.toString(_minTopicLeadersPerBroker));
    props.put(AnalyzerConfig.FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_CONFIG, Long.toString(_fastModePerBrokerMoveTimeoutMs));
    props.put(AnalyzerConfig.BROKER_SET_RESOLVER_CLASS_CONFIG, _brokerSetResolver.getClass().getName());
    props.put(AnalyzerConfig.REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG, _replicaToBrokerSetMappingPolicy.getClass().getName());
    return props;
  }

  /**
   * @return The maximum number of replicas per broker.
   */
  public long maxReplicasPerBroker() {
    return _maxReplicasPerBroker;
  }

  /**
   * @return The maximum number of replicas that should reside on each broker to consider a cluster as overprovisioned after balancing its
   * replica distribution.
   */
  public long overprovisionedMaxReplicasPerBroker() {
    return _overprovisionedMaxReplicasPerBroker;
  }

  /**
   * @return The minimum number of alive brokers for the cluster to be eligible in overprovisioned consideration.
   */
  public int overprovisionedMinBrokers() {
    return _overprovisionedMinBrokers;
  }

  /**
   * @return The minimum number of extra racks to consider a cluster as overprovisioned such that the cluster has at least the configured
   * number of extra alive racks in addition to the number of racks needed to place replica of each partition to a separate rack -- e.g. a
   * cluster with 6 racks is overprovisioned in terms of its number of racks if the maximum replication factor is 4 and this config is 2.
   */
  public int overprovisionedMinExtraRacks() {
    return _overprovisionedMinExtraRacks;
  }

  /**
   * @return The replica balance percentage for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal}.
   */
  public double replicaBalancePercentage() {
    return _replicaBalancePercentage;
  }

  /**
   * @return The leader replica balance percentage for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal}.
   */
  public double leaderReplicaBalancePercentage() {
    return _leaderReplicaBalancePercentage;
  }

  /**
   * @return Topic replica balance percentage for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal}.
   */
  public double topicReplicaBalancePercentage() {
    return _topicReplicaBalancePercentage;
  }

  /**
   * @return Topic replica balance minimum gap for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal}.
   */
  public int topicReplicaBalanceMinGap() {
    return _topicReplicaBalanceMinGap;
  }

  /**
   * @return Topic replica balance maximum gap for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal}.
   */
  public int topicReplicaBalanceMaxGap() {
    return _topicReplicaBalanceMaxGap;
  }

  /**
   * @return Topic replica balance percentage for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeaderReplicaDistributionGoal}.
   */
  public double topicLeaderReplicaBalancePercentage() {
    return _topicLeaderReplicaBalancePercentage;
  }

  /**
   * @return Topic replica balance minimum gap for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeaderReplicaDistributionGoal}.
   */
  public int topicLeaderReplicaBalanceMinGap() {
    return _topicLeaderReplicaBalanceMinGap;
  }

  /**
   * @return Topic replica balance maximum gap for {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeaderReplicaDistributionGoal}.
   */
  public int topicLeaderReplicaBalanceMaxGap() {
    return _topicLeaderReplicaBalanceMaxGap;
  }

  /**
   * @return Goal violation distribution threshold multiplier to be used in detection and fixing goal violations.
   */
  public double goalViolationDistributionThresholdMultiplier() {
    return _goalViolationDistributionThresholdMultiplier;
  }

  /**
   * Get the balance percentage for the requested resource. We give a balance margin to avoid the case
   * that right after a rebalance we need to issue another rebalance.
   *
   * @param resource Resource for which the balance percentage will be provided.
   * @return Resource balance percentage.
   */
  public double resourceBalancePercentage(Resource resource) {
    return _resourceBalancePercentage.get(resource);
  }

  /**
   * Get the capacity threshold for the requested resource.
   *
   * @param resource Resource for which the capacity threshold will be provided.
   * @return Capacity threshold for the requested resource.
   */
  public double capacityThreshold(Resource resource) {
    return _capacityThreshold.get(resource);
  }

  /**
   * Get the low utilization threshold for a resource.
   * @param resource Resource for which the low utilization threshold will be provided.
   * @return The low utilization threshold.
   */
  public double lowUtilizationThreshold(Resource resource) {
    return _lowUtilizationThreshold.get(resource);
  }

  /**
   * Get the regex pattern of topics with a minimum number of leaders on brokers that are not excluded for replica move.
   *
   * @return Regex pattern of topic names
   */
  public Pattern topicsWithMinLeadersPerBrokerPattern() {
    return _topicsWithMinLeadersPerBrokerPattern;
  }

  /**
   * Get the minimum required number of leader replica per broker for topics that must have leader replica per broker
   * that is not excluded for replica move.
   *
   * @return Topic count
   */
  public int minTopicLeadersPerBroker() {
    return _minTopicLeadersPerBroker;
  }

  /**
   * @return The per broker move timeout in fast mode in milliseconds.
   */
  public long fastModePerBrokerMoveTimeoutMs() {
    return _fastModePerBrokerMoveTimeoutMs;
  }

  /**
   * @return The Broker Set Resolver.
   */
  public BrokerSetResolver brokerSetResolver() {
    return _brokerSetResolver;
  }

  /**
   * @return The Replica to Broker Set mapping policy.
   */
  public ReplicaToBrokerSetMappingPolicy replicaToBrokerSetMappingPolicy() {
    return _replicaToBrokerSetMappingPolicy;
  }

  /**
   * Set resource balance percentage for the given resource.
   *
   * @param resource Resource for which the balance percentage will be set.
   * @param balancePercentage Balance percentage for the given resource.
   */
  private void setBalancePercentageFor(Resource resource, double balancePercentage) {
    if (balancePercentage < 1) {
      throw new IllegalArgumentException("Balance Percentage cannot be less than 1.0");
    }
    _resourceBalancePercentage.put(resource, balancePercentage);
  }

  /**
   * Set a common resource balance percentage for all resources.
   *
   * @param resourceBalancePercentage Common balance percentage for all resources.
   */
  void setResourceBalancePercentage(double resourceBalancePercentage) {
    for (Resource resource : Resource.cachedValues()) {
      setBalancePercentageFor(resource, resourceBalancePercentage);
    }
  }

  /**
   * Set alive resource capacity threshold for the given resource.
   *
   * @param resource Resource for which the capacity threshold will be set.
   * @param capacityThreshold Capacity threshold for the given resource.
   */
  private void setCapacityThresholdFor(Resource resource, double capacityThreshold) {
    if (capacityThreshold <= 0 || capacityThreshold > 1) {
      throw new IllegalArgumentException("Capacity Threshold must be in (0, 1].");
    }
    _capacityThreshold.put(resource, capacityThreshold);
  }

  /**
   * Set alive resource capacity threshold for all resources.
   *
   * @param capacityThreshold Common capacity threshold for all resources in alive brokers.
   */
  void setCapacityThreshold(double capacityThreshold) {
    for (Resource resource : Resource.cachedValues()) {
      setCapacityThresholdFor(resource, capacityThreshold);
    }
  }

  /**
   * Get string representation of {@link BalancingConstraint}.
   */
  @Override
  public String toString() {
    return String.format("BalancingConstraint[cpuBalancePercentage=%.4f,diskBalancePercentage=%.4f,"
                         + "inboundNwBalancePercentage=%.4f,outboundNwBalancePercentage=%.4f,cpuCapacityThreshold=%.4f,"
                         + "diskCapacityThreshold=%.4f,inboundNwCapacityThreshold=%.4f,outboundNwCapacityThreshold=%.4f,"
                         + "maxReplicasPerBroker=%d,replicaBalancePercentage=%.4f,leaderReplicaBalancePercentage=%.4f,"
                         + "topicReplicaBalancePercentage=%.4f,topicReplicaBalanceGap=[%d,%d],"
                         + "topicLeaderReplicaBalancePercentage=%.4f,topicLeaderReplicaBalanceGap=[%d,%d],"
                         + "goalViolationDistributionThresholdMultiplier=%.4f,"
                         + "topicsWithMinLeadersPerBrokerPattern=%s,"
                         + "minTopicLeadersPerBroker=%d,fastModePerBrokerMoveTimeoutMs=%d,"
                         + "brokerSetDataStore=%s,"
                         + "replicaToBrokerSetMappingPolicy=%s]",
                         _resourceBalancePercentage.get(Resource.CPU), _resourceBalancePercentage.get(Resource.DISK),
                         _resourceBalancePercentage.get(Resource.NW_IN), _resourceBalancePercentage.get(Resource.NW_OUT),
                         _capacityThreshold.get(Resource.CPU), _capacityThreshold.get(Resource.DISK),
                         _capacityThreshold.get(Resource.NW_IN), _capacityThreshold.get(Resource.NW_OUT),
                         _maxReplicasPerBroker, _replicaBalancePercentage, _leaderReplicaBalancePercentage,
                         _topicReplicaBalancePercentage, _topicReplicaBalanceMinGap, _topicReplicaBalanceMaxGap,
                         _topicLeaderReplicaBalancePercentage, _topicLeaderReplicaBalanceMinGap, _topicLeaderReplicaBalanceMaxGap,
                         _goalViolationDistributionThresholdMultiplier, _topicsWithMinLeadersPerBrokerPattern.pattern(),
                         _minTopicLeadersPerBroker, _fastModePerBrokerMoveTimeoutMs, _brokerSetResolver.getClass().getName(),
                         _replicaToBrokerSetMappingPolicy.getClass().getName());
  }
}
