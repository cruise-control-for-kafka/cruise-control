/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.analyzer.DefaultOptimizationOptionsGenerator;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptionsGenerator;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.BrokerSetAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.rackaware.NoOpRackAwareGoalRackIdMapper;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.rackaware.RackAwareGoalRackIdMapper;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetAssignmentPolicy;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetFileResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.NoOpBrokerSetAssignmentPolicy;
import com.linkedin.kafka.cruisecontrol.config.ReplicaToBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.TopicNameHashBrokerSetMappingPolicy;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;


/**
 * A class to keep Cruise Control Analyzer Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class AnalyzerConfig {

  /**
   * <code>cpu.balance.threshold</code>
   */
  public static final String CPU_BALANCE_THRESHOLD_CONFIG = "cpu.balance.threshold";
  public static final double DEFAULT_CPU_BALANCE_THRESHOLD = 1.10;
  public static final String CPU_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for CPU utilization. "
      + "For example, 1.10 means the highest CPU usage of a broker should not be above 1.10x of average CPU utilization"
      + " of all the brokers.";

  /**
   * <code>disk.balance.threshold</code>
   */
  public static final String DISK_BALANCE_THRESHOLD_CONFIG = "disk.balance.threshold";
  public static final double DEFAULT_DISK_BALANCE_THRESHOLD = 1.10;
  public static final String DISK_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for disk utilization. "
      + "For example, 1.10 means the highest disk usage of a broker should not be above 1.10x of average disk utilization"
      + " of all the brokers.";

  /**
   * <code>network.inbound.balance.threshold</code>
   */
  public static final String NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG = "network.inbound.balance.threshold";
  public static final double DEFAULT_NETWORK_INBOUND_BALANCE_THRESHOLD = 1.10;
  public static final String NETWORK_INBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for network "
      + "inbound usage. For example, 1.10 means the highest network inbound usage of a broker should not be above "
      + "1.10x of average network inbound usage of all the brokers.";

  /**
   * <code>network.outbound.balance.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG = "network.outbound.balance.threshold";
  public static final double DEFAULT_NETWORK_OUTBOUND_BALANCE_THRESHOLD = 1.10;
  public static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for network "
      + "outbound usage. For example, 1.10 means the highest network outbound usage of a broker should not be above "
      + "1.10x of average network outbound usage of all the brokers.";

  /**
   * <code>replica.count.balance.threshold</code>
   */
  public static final String REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "replica.count.balance.threshold";
  public static final double DEFAULT_REPLICA_COUNT_BALANCE_THRESHOLD = 1.10;
  public static final String REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for replica "
      + "distribution. For example, 1.10 means the highest replica count of a broker should not be above 1.10x of "
      + "average replica count of all brokers.";

  /**
   * <code>leader.replica.count.balance.threshold</code>
   */
  public static final String LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "leader.replica.count.balance.threshold";
  public static final double DEFAULT_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD = 1.10;
  public static final String LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "leader replica distribution. For example, 1.10 means the highest leader replica count of a broker should not be "
      + "above 1.10x of average leader replica count of all alive brokers.";

  /**
   * <code>topic.replica.count.balance.threshold</code>
   */
  public static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "topic.replica.count.balance.threshold";
  public static final double DEFAULT_TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD = 3.00;
  public static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "replica distribution from each topic. For example, 1.80 means the highest topic replica count of a broker "
      + "should not be above 1.80x of average replica count of all brokers for the same topic.";

  /**
   * <code>topic.replica.count.balance.min.gap</code>
   */
  public static final String TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG = "topic.replica.count.balance.min.gap";
  public static final int DEFAULT_TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP = 2;
  public static final String TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_DOC = "The minimum allowed gap between a balance limit and"
      + " the average replica count for each topic. A balance limit is set via topic.replica.count.balance.threshold config."
      + " If the difference between the computed limit and the average replica count for the relevant topic is smaller than"
      + " the value specified by this config, the limit is adjusted accordingly.";

  /**
   * <code>topic.replica.count.balance.max.gap</code>
   */
  public static final String TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG = "topic.replica.count.balance.max.gap";
  public static final int DEFAULT_TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP = 40;
  public static final String TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_DOC = "The maximum allowed gap between a balance limit and"
      + " the average replica count for each topic. A balance limit is set via topic.replica.count.balance.threshold config."
      + " If the difference between the computed limit and the average replica count for the relevant topic is greater than"
      + " the value specified by this config, the limit is adjusted accordingly.";

  /**
   * <code>cpu.capacity.threshold</code>
   */
  public static final String CPU_CAPACITY_THRESHOLD_CONFIG = "cpu.capacity.threshold";
  public static final double DEFAULT_CPU_CAPACITY_THRESHOLD = 0.7;
  public static final String CPU_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.cpu.capacity that is "
      + "allowed to be used on a broker. The analyzer will enforce a hard goal that the cpu utilization "
      + "of a broker cannot be higher than (broker.cpu.capacity * cpu.capacity.threshold).";

  /**
   * <code>disk.capacity.threshold</code>
   */
  public static final String DISK_CAPACITY_THRESHOLD_CONFIG = "disk.capacity.threshold";
  public static final double DEFAULT_DISK_CAPACITY_THRESHOLD = 0.8;
  public static final String DISK_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.disk.capacity that is "
      + "allowed to be used on a broker. The analyzer will enforce a hard goal that the disk usage "
      + "of a broker cannot be higher than (broker.disk.capacity * disk.capacity.threshold).";

  /**
   * <code>network.inbound.capacity.threshold</code>
   */
  public static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG = "network.inbound.capacity.threshold";
  public static final double DEFAULT_NETWORK_INBOUND_CAPACITY_THRESHOLD = 0.8;
  public static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total "
      + "broker.network.inbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal "
      + "that the disk usage of a broker cannot be higher than (broker.network.inbound.capacity * "
      + "network.inbound.capacity.threshold).";

  /**
   * <code>network.outbound.capacity.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG = "network.outbound.capacity.threshold";
  public static final double DEFAULT_NETWORK_OUTBOUND_CAPACITY_THRESHOLD = 0.8;
  public static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total "
      + "broker.network.outbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal "
      + "that the disk usage of a broker cannot be higher than (broker.network.outbound.capacity * "
      + "network.outbound.capacity.threshold).";

  /**
   * <code>cpu.low.utilization.threshold</code>
   */
  public static final String CPU_LOW_UTILIZATION_THRESHOLD_CONFIG = "cpu.low.utilization.threshold";
  public static final double DEFAULT_CPU_LOW_UTILIZATION_THRESHOLD = 0.0;
  public static final String CPU_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold to define the utilization of CPU is low enough that"
      + " rebalance is not worthwhile. The cluster will only be in a low utilization state when all the brokers are below the low "
      + "utilization threshold. Such a cluster is overprovisioned in terms of its CPU utilization. The threshold is in percentage.";

  /**
   * <code>disk.low.utilization.threshold</code>
   */
  public static final String DISK_LOW_UTILIZATION_THRESHOLD_CONFIG = "disk.low.utilization.threshold";
  public static final double DEFAULT_DISK_LOW_UTILIZATION_THRESHOLD = 0.0;
  public static final String DISK_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold to define the utilization of disk is low enough that"
      + " rebalance is not worthwhile. The cluster will only be in a low utilization state when all the brokers are below the low "
      + "utilization threshold. Such a cluster is overprovisioned in terms of its disk utilization. The threshold is in percentage.";

  /**
   * <code>network.inbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.inbound.low.utilization.threshold";
  public static final double DEFAULT_NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD = 0.0;
  public static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold to define the utilization of network inbound rate"
      + " is low enough that rebalance is not worthwhile. The cluster will only be in a low utilization state when all the brokers are below "
      + "the low utilization threshold. Such a cluster is overprovisioned in terms of its network inbound rate. The threshold is in percentage.";

  /**
   * <code>network.outbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.outbound.low.utilization.threshold";
  public static final double DEFAULT_NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD = 0.0;
  public static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold to define the utilization of network outbound rate"
      + " is low enough that rebalance is not worthwhile. The cluster will only be in a low utilization state when all the brokers are below "
      + "the low utilization threshold. Such a cluster is overprovisioned in terms of its network outbound rate. The threshold is in percentage.";

  /**
   * <code>proposal.expiration.ms</code>
   */
  public static final String PROPOSAL_EXPIRATION_MS_CONFIG = "proposal.expiration.ms";
  public static final long DEFAULT_PROPOSAL_EXPIRATION_MS = TimeUnit.MINUTES.toMillis(15);
  public static final String PROPOSAL_EXPIRATION_MS_DOC = "Kafka Cruise Control will cache one of the best proposal among all "
      + "the optimization proposal candidates it recently computed. This configuration defines when will the"
      + "cached proposal be invalidated and needs a recomputation. If proposal.expiration.ms is set to 0, Cruise Control"
      + "will continuously compute the proposal candidates.";

  /**
   * <code>max.replicas.per.broker</code>
   */
  public static final String MAX_REPLICAS_PER_BROKER_CONFIG = "max.replicas.per.broker";
  public static final long DEFAULT_MAX_REPLICAS_PER_BROKER = 10000L;
  public static final String MAX_REPLICAS_PER_BROKER_DOC = "The maximum number of replicas allowed to reside on a "
      + "broker. The analyzer will enforce a hard goal that the number of replica on a broker cannot be higher than "
      + "this config.";

  /**
   * <code>num.proposal.precompute.threads</code>
   */
  public static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG = "num.proposal.precompute.threads";
  public static final int DEFAULT_NUM_PROPOSAL_PRECOMPUTE_THREADS = 1;
  public static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC = "The number of thread used to precompute the "
      + "optimization proposal candidates. The more threads are used, the more memory and CPU resource will be used.";

  /**
   * <code>optimization.options.generator.class</code>
   */
  public static final String OPTIMIZATION_OPTIONS_GENERATOR_CLASS_CONFIG = "optimization.options.generator.class";
  public static final String DEFAULT_OPTIMIZATION_OPTIONS_GENERATOR_CLASS = DefaultOptimizationOptionsGenerator.class.getName();
  public static final String OPTIMIZATION_OPTIONS_GENERATOR_CLASS_DOC = String.format("The class implementing %s interface and "
      + "is used to generate optimization options for proposal calculations.", OptimizationOptionsGenerator.class.getName());

  /**
   * <code>rack.aware.goal.rack.id.mapper.class</code>
   */
  public static final String RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS_CONFIG = "rack.aware.goal.rack.id.mapper.class";
  public static final String DEFAULT_RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS = NoOpRackAwareGoalRackIdMapper.class.getName();
  public static final String RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS_DOC =
      String.format("The class implementing %s interface to use to transform rack IDs for RackAwareGoals."
                    + " Rack-aware replica placement will be performed upon the transformed rack ID.",
                    RackAwareGoalRackIdMapper.class.getName());

  /**
   * <code>goals</code>
   */
  public static final String GOALS_CONFIG = "goals";
  public static final String DEFAULT_GOALS = new StringJoiner(",").add(BrokerSetAwareGoal.class.getName())
                                                                  .add(RackAwareGoal.class.getName())
                                                                  .add(RackAwareDistributionGoal.class.getName())
                                                                  .add(MinTopicLeadersPerBrokerGoal.class.getName())
                                                                  .add(ReplicaCapacityGoal.class.getName())
                                                                  .add(DiskCapacityGoal.class.getName())
                                                                  .add(NetworkInboundCapacityGoal.class.getName())
                                                                  .add(NetworkOutboundCapacityGoal.class.getName())
                                                                  .add(CpuCapacityGoal.class.getName())
                                                                  .add(ReplicaDistributionGoal.class.getName())
                                                                  .add(PotentialNwOutGoal.class.getName())
                                                                  .add(DiskUsageDistributionGoal.class.getName())
                                                                  .add(NetworkInboundUsageDistributionGoal.class.getName())
                                                                  .add(NetworkOutboundUsageDistributionGoal.class.getName())
                                                                  .add(CpuUsageDistributionGoal.class.getName())
                                                                  .add(LeaderReplicaDistributionGoal.class.getName())
                                                                  .add(LeaderBytesInDistributionGoal.class.getName())
                                                                  .add(TopicReplicaDistributionGoal.class.getName())
                                                                  .add(KafkaAssignerDiskUsageDistributionGoal.class.getName())
                                                                  .add(KafkaAssignerEvenRackAwareGoal.class.getName())
                                                                  .add(PreferredLeaderElectionGoal.class.getName()).toString();
  public static final String GOALS_DOC = "A set of case insensitive inter-broker goals. Inter-broker goals facilitate in "
      + " distributing the load across brokers.";

  /**
   * <code>intra.broker.goals</code>
   */
  public static final String INTRA_BROKER_GOALS_CONFIG = "intra.broker.goals";
  public static final String DEFAULT_INTRA_BROKER_GOALS = new StringJoiner(",")
          .add(IntraBrokerDiskCapacityGoal.class.getName())
          .add(IntraBrokerDiskUsageDistributionGoal.class.getName())
          .toString();
  public static final String INTRA_BROKER_GOALS_DOC = "A list of case insensitive intra-broker goals in the order of priority. "
      + "The high priority goals will be executed first. The intra-broker goals are only relevant if intra-broker operation is "
      + "supported (i.e. in  Cruise Control versions above 2.*), otherwise this list should be empty.";

  /**
   * <code>hard.goals</code>
   */
  public static final String HARD_GOALS_CONFIG = "hard.goals";
  public static final String DEFAULT_HARD_GOALS = new StringJoiner(",").add(RackAwareGoal.class.getName())
                                                                       .add(MinTopicLeadersPerBrokerGoal.class.getName())
                                                                       .add(ReplicaCapacityGoal.class.getName())
                                                                       .add(DiskCapacityGoal.class.getName())
                                                                       .add(NetworkInboundCapacityGoal.class.getName())
                                                                       .add(NetworkOutboundCapacityGoal.class.getName())
                                                                       .add(CpuCapacityGoal.class.getName()).toString();
  public static final String HARD_GOALS_DOC = "A list of case insensitive, inter-broker hard goals. Hard goals will be enforced "
      + "to execute if Cruise Control runs in non-kafka-assigner mode and skip_hard_goal_check parameter is not set in request.";

  /**
   * <code>default.goals</code>
   */
  public static final String DEFAULT_GOALS_CONFIG = "default.goals";
  public static final String DEFAULT_DEFAULT_GOALS = new StringJoiner(",").add(RackAwareGoal.class.getName())
                                                                          .add(MinTopicLeadersPerBrokerGoal.class.getName())
                                                                          .add(ReplicaCapacityGoal.class.getName())
                                                                          .add(DiskCapacityGoal.class.getName())
                                                                          .add(NetworkInboundCapacityGoal.class.getName())
                                                                          .add(NetworkOutboundCapacityGoal.class.getName())
                                                                          .add(CpuCapacityGoal.class.getName())
                                                                          .add(ReplicaDistributionGoal.class.getName())
                                                                          .add(PotentialNwOutGoal.class.getName())
                                                                          .add(DiskUsageDistributionGoal.class.getName())
                                                                          .add(NetworkInboundUsageDistributionGoal.class.getName())
                                                                          .add(NetworkOutboundUsageDistributionGoal.class.getName())
                                                                          .add(CpuUsageDistributionGoal.class.getName())
                                                                          .add(TopicReplicaDistributionGoal.class.getName())
                                                                          .add(LeaderReplicaDistributionGoal.class.getName())
                                                                          .add(LeaderBytesInDistributionGoal.class.getName()).toString();
  public static final String DEFAULT_GOALS_DOC = "The list of inter-broker goals that will be used by default if no goal list "
      + "is provided. This list of goals will also be used for proposal pre-computation.";

  /**
   * <code>goal.balancedness.priority.weight</code>
   */
  public static final String GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG = "goal.balancedness.priority.weight";
  public static final double DEFAULT_GOAL_BALANCEDNESS_PRIORITY_WEIGHT = 1.1;
  public static final String GOAL_BALANCEDNESS_PRIORITY_WEIGHT_DOC = "The impact of having one level higher goal priority"
      + " on the relative balancedness score. For example, 1.1 means that a goal with higher priority will have the 1.1x"
      + " balancedness weight of the lower priority goal (assuming the same goal.balancedness.strictness.weight values for"
      + " both goals).";

  /**
   * <code>goal.balancedness.strictness.weight</code>
   */
  public static final String GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG = "goal.balancedness.strictness.weight";
  public static final double DEFAULT_GOAL_BALANCEDNESS_STRICTNESS_WEIGHT = 1.5;
  public static final String GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_DOC = "The impact of strictness (i.e. hard or soft goal)"
      + " on the relative balancedness score. For example, 1.5 means that a hard goal will have the 1.5x balancedness "
      + "weight of a soft goal (assuming goal.balancedness.priority.weight is 1).";

  /**
   * <code>allow.capacity.estimation.on.proposal.precompute</code>
   */
  public static final String ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_CONFIG = "allow.capacity.estimation.on.proposal.precompute";
  public static final boolean DEFAULT_ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE = true;
  public static final String ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_DOC = "The flag to indicate whether to "
      + "allow capacity estimation on proposal precomputation.";

  /**
   * <code>topics.with.min.leaders.per.broker</code>
   */
  public static final String TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG = "topics.with.min.leaders.per.broker";
  public static final String DEFAULT_TOPICS_WITH_MIN_LEADERS_PER_BROKER = "";
  public static final String DEFAULT_TOPICS_WITH_MIN_LEADERS_PER_BROKER_DOC = "The topics that should have a minimum "
      + "number of leaders on brokers that are not excluded for replica move. It is a regex.";

  /**
   * <code>min.topic.leaders.per.broker</code>
   */
  public static final String MIN_TOPIC_LEADERS_PER_BROKER_CONFIG = "min.topic.leaders.per.broker";
  public static final int DEFAULT_MIN_TOPIC_LEADERS_PER_BROKER = 1;
  public static final String MIN_TOPIC_LEADERS_PER_BROKER_DOC = "The minimum required number of leaders per broker"
      + " for topics that must have leader on brokers that are not excluded for replica move."
      + " When set to 0, the minimum number of leaders per broker is computed"
      + " dynamically for each topic as no-of-topic-leaders / no-of-brokers";

  /**
   * <code>topics.excluded.from.partition.movement</code>
   */
  public static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG = "topics.excluded.from.partition.movement";
  public static final String DEFAULT_TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT = "";
  public static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC = "The topics that should be excluded from the "
      + "partition movement. It is a regex. Notice that this regex will be ignored when decommission a broker is invoked.";

  /**
   * <code>goal.violation.distribution.threshold.multiplier</code>
   */
  public static final String GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG = "goal.violation.distribution.threshold.multiplier";
  public static final double DEFAULT_GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER = 1.0;
  public static final String GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_DOC = "The multiplier applied to the threshold"
      + " of distribution goals used for detecting and fixing anomalies. For example, 2.50 means the threshold for each "
      + "distribution goal (i.e. Replica Distribution, Leader Replica Distribution, Resource Distribution, and Topic Replica "
      + "Distribution Goals) will be 2.50x of the value used in manual goal optimization requests (e.g. rebalance).";

  /**
   * <code>overprovisioned.min.extra.racks</code>
   */
  public static final String OVERPROVISIONED_MIN_EXTRA_RACKS_CONFIG = "overprovisioned.min.extra.racks";
  public static final int DEFAULT_OVERPROVISIONED_MIN_EXTRA_RACKS = 2;
  public static final String OVERPROVISIONED_MIN_EXTRA_RACKS_DOC = "The minimum number of extra racks to consider a cluster as "
      + "overprovisioned such that the cluster has at least the configured number of extra alive racks in addition to the number of racks "
      + "needed to place replica of each partition to a separate rack -- e.g. a cluster with 6 racks is overprovisioned in terms "
      + "of its number of racks if the maximum replication factor is 4 and this config is 2.";

  /**
   * <code>overprovisioned.min.brokers</code>
   */
  public static final String OVERPROVISIONED_MIN_BROKERS_CONFIG = "overprovisioned.min.brokers";
  public static final int DEFAULT_OVERPROVISIONED_MIN_BROKERS = 3;
  public static final String OVERPROVISIONED_MIN_BROKERS_DOC = "The minimum number of alive brokers for the cluster to be "
      + "eligible in overprovisioned consideration -- i.e. the cluster has at least the configured number of alive brokers.";

  /**
   * <code>overprovisioned.max.replicas.per.broker</code>
   */
  public static final String OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG = "overprovisioned.max.replicas.per.broker";
  public static final long DEFAULT_OVERPROVISIONED_MAX_REPLICAS_PER_BROKER = 1500L;
  public static final String OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_DOC = "The maximum number of replicas that should reside on each "
      + "broker to consider a cluster as overprovisioned after balancing its replica distribution.";

  /**
   * <code>fast.mode.per.broker.move.timeout.ms</code>
   */
  public static final String FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_CONFIG = "fast.mode.per.broker.move.timeout.ms";
  public static final long DEFAULT_FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS = 500L;
  public static final String FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_DOC = "The per broker move timeout in fast mode in milliseconds. "
      + "Users can run goal optimizations in fast mode by setting the fast_mode parameter to true in relevant endpoints. "
      + "This mode intends to provide a more predictable runtime for goal optimizations.";

  /**
   * <code>broker.set.resolver.class</code>
   */
  public static final String BROKER_SET_RESOLVER_CLASS_CONFIG = "broker.set.resolver.class";
  public static final String DEFAULT_BROKER_SET_RESOLVER_CLASS = BrokerSetFileResolver.class.getName();
  public static final String BROKER_SET_RESOLVER_CLASS_DOC = String.format(
      "The class implements %s interface and is used to generate broker set information to be used by BrokerSet related goals.",
      BrokerSetResolver.class.getName());

  /**
   * <code>broker.set.config.file</code>
   */
  public static final String BROKER_SET_CONFIG_FILE_CONFIG = "broker.set.config.file";
  public static final String DEFAULT_BROKER_SET_CONFIG_FILE = "config/brokerSets.json";
  public static final String BROKER_SET_CONFIG_FILE_DOC = "The file path that holds broker sets ids to broker ids mapping information.";

  /**
   * <code>broker.set.assignment.policy.class</code>
   */
  public static final String BROKER_SET_ASSIGNMENT_POLICY_CLASS_CONFIG = "broker.set.assignment.policy.class";
  public static final String DEFAULT_BROKER_SET_ASSIGNMENT_POLICY_CLASS = NoOpBrokerSetAssignmentPolicy.class.getName();
  public static final String BROKER_SET_ASSIGNMENT_POLICY_CLASS_DOC = String.format(
      "The class implements %s interface and is used to generate broker sets to broker ids mapping for broker ids that are not provided by %s.",
      BrokerSetAssignmentPolicy.class.getName(), BROKER_SET_RESOLVER_CLASS_CONFIG);

  /**
   * <code>replica.to.broker.set.mapping.policy.class</code>
   */
  public static final String REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG = "replica.to.broker.set.mapping.policy.class";
  public static final String DEFAULT_REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS = TopicNameHashBrokerSetMappingPolicy.class.getName();
  public static final String REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_DOC =
      String.format("The class implements %s interface and is used to generate replica to broker set mapping.",
                    ReplicaToBrokerSetMappingPolicy.class.getName());

  private AnalyzerConfig() {
  }

  /**
   * Define configs for Analyzer.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Analyzer.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(CPU_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CPU_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            CPU_BALANCE_THRESHOLD_DOC)
                    .define(DISK_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_DISK_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            DISK_BALANCE_THRESHOLD_DOC)
                    .define(NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_NETWORK_INBOUND_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            NETWORK_INBOUND_BALANCE_THRESHOLD_DOC)
                    .define(NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_NETWORK_OUTBOUND_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC)
                    .define(REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_REPLICA_COUNT_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
                    .define(LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
                    .define(TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD,
                            atLeast(1),
                            ConfigDef.Importance.HIGH,
                            TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
                    .define(TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_DOC)
                    .define(TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_DOC)
                    .define(CPU_CAPACITY_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CPU_CAPACITY_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.HIGH,
                            CPU_CAPACITY_THRESHOLD_DOC)
                    .define(DISK_CAPACITY_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_DISK_CAPACITY_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.HIGH,
                            DISK_CAPACITY_THRESHOLD_DOC)
                    .define(NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_NETWORK_INBOUND_CAPACITY_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.HIGH,
                            NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC)
                    .define(NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_NETWORK_OUTBOUND_CAPACITY_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.HIGH,
                            NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC)
                    .define(CPU_LOW_UTILIZATION_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_CPU_LOW_UTILIZATION_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.MEDIUM,
                            CPU_LOW_UTILIZATION_THRESHOLD_DOC)
                    .define(DISK_LOW_UTILIZATION_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_DISK_LOW_UTILIZATION_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.MEDIUM,
                            DISK_LOW_UTILIZATION_THRESHOLD_DOC)
                    .define(NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.MEDIUM,
                            NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC)
                    .define(NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD,
                            between(0, 1),
                            ConfigDef.Importance.MEDIUM,
                            NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC)
                    .define(PROPOSAL_EXPIRATION_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_PROPOSAL_EXPIRATION_MS,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            PROPOSAL_EXPIRATION_MS_DOC)
                    .define(MAX_REPLICAS_PER_BROKER_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_MAX_REPLICAS_PER_BROKER,
                            atLeast(0),
                            ConfigDef.Importance.MEDIUM,
                            MAX_REPLICAS_PER_BROKER_DOC)
                    .define(NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_NUM_PROPOSAL_PRECOMPUTE_THREADS,
                            between(0, 1),
                            ConfigDef.Importance.LOW,
                            NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC)
                    .define(GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_GOALS,
                            ConfigDef.Importance.HIGH,
                            GOALS_DOC)
                    .define(INTRA_BROKER_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_INTRA_BROKER_GOALS,
                            ConfigDef.Importance.HIGH,
                            INTRA_BROKER_GOALS_DOC)
                    .define(HARD_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_HARD_GOALS,
                            ConfigDef.Importance.HIGH,
                            HARD_GOALS_DOC)
                    .define(DEFAULT_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_DEFAULT_GOALS,
                            ConfigDef.Importance.HIGH,
                            DEFAULT_GOALS_DOC)
                    .define(GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_GOAL_BALANCEDNESS_PRIORITY_WEIGHT,
                            between(1, 2),
                            ConfigDef.Importance.LOW,
                            GOAL_BALANCEDNESS_PRIORITY_WEIGHT_DOC)
                    .define(GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_GOAL_BALANCEDNESS_STRICTNESS_WEIGHT,
                            between(1, 2),
                            ConfigDef.Importance.LOW,
                            GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_DOC)
                    .define(ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE,
                            ConfigDef.Importance.LOW,
                            ALLOW_CAPACITY_ESTIMATION_ON_PROPOSAL_PRECOMPUTE_DOC)
                    .define(TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT,
                            ConfigDef.Importance.LOW,
                            TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC)
                    .define(TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_TOPICS_WITH_MIN_LEADERS_PER_BROKER,
                            ConfigDef.Importance.LOW,
                            DEFAULT_TOPICS_WITH_MIN_LEADERS_PER_BROKER_DOC)
                    .define(MIN_TOPIC_LEADERS_PER_BROKER_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_MIN_TOPIC_LEADERS_PER_BROKER,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            MIN_TOPIC_LEADERS_PER_BROKER_DOC)
                    .define(GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER,
                            atLeast(1),
                            ConfigDef.Importance.MEDIUM,
                            GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_DOC)
                    .define(OPTIMIZATION_OPTIONS_GENERATOR_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_OPTIMIZATION_OPTIONS_GENERATOR_CLASS,
                            ConfigDef.Importance.LOW,
                            OPTIMIZATION_OPTIONS_GENERATOR_CLASS_DOC)
                    .define(RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS,
                            ConfigDef.Importance.LOW,
                            RACK_AWARE_GOAL_RACK_ID_MAPPER_CLASS_DOC)
                    .define(OVERPROVISIONED_MIN_EXTRA_RACKS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_OVERPROVISIONED_MIN_EXTRA_RACKS,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            OVERPROVISIONED_MIN_EXTRA_RACKS_DOC)
                    .define(OVERPROVISIONED_MIN_BROKERS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_OVERPROVISIONED_MIN_BROKERS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            OVERPROVISIONED_MIN_BROKERS_DOC)
                    .define(OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_OVERPROVISIONED_MAX_REPLICAS_PER_BROKER,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_DOC)
                    .define(FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            FAST_MODE_PER_BROKER_MOVE_TIMEOUT_MS_DOC)
                    .define(BROKER_SET_RESOLVER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_BROKER_SET_RESOLVER_CLASS,
                            ConfigDef.Importance.LOW,
                            BROKER_SET_RESOLVER_CLASS_DOC)
                    .define(BROKER_SET_CONFIG_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_BROKER_SET_CONFIG_FILE,
                            ConfigDef.Importance.LOW,
                            BROKER_SET_CONFIG_FILE_DOC)
                    .define(BROKER_SET_ASSIGNMENT_POLICY_CLASS_CONFIG,
                            ConfigDef.Type.CLASS, DEFAULT_BROKER_SET_ASSIGNMENT_POLICY_CLASS,
                            ConfigDef.Importance.LOW,
                            BROKER_SET_ASSIGNMENT_POLICY_CLASS_DOC)
                    .define(REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG,
                            ConfigDef.Type.CLASS, DEFAULT_REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS,
                            ConfigDef.Importance.LOW,
                            REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_DOC);
  }
}
