/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.BrokerSetAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.ReplicaToOriginalBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.TopicPatternBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.BROKEN_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.NEW_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.REGRESSION;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with various balancing percentages and capacity thresholds using various deterministic clusters.
 */
@RunWith(Parameterized.class)
public class DeterministicClusterTest {
  private final BalancingConstraint _balancingConstraint;
  private final ClusterModel _cluster;
  private final List<String> _goalNameByPriority;
  private final List<OptimizationVerifier.Verification> _verifications;

  @Rule
  public ExpectedException _expected = ExpectedException.none();

  /**
   * Constructor for Deterministic Cluster Test.
   *
   * @param balancingConstraint Balancing constraint.
   * @param cluster             The state of the cluster.
   * @param goalNameByPriority  Name of goals by the order of execution priority.
   */
  public DeterministicClusterTest(BalancingConstraint balancingConstraint,
                                  ClusterModel cluster,
                                  List<String> goalNameByPriority,
                                  List<OptimizationVerifier.Verification> verifications,
                                  Class<? extends Throwable> expectedException) {
    _balancingConstraint = balancingConstraint;
    _cluster = cluster;
    _goalNameByPriority = goalNameByPriority;
    _verifications = verifications;
    if (expectedException != null) {
      _expected.expect(expectedException);
    }
  }

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> p = new ArrayList<>();

    List<String> goalNameByPriority = Arrays.asList(RackAwareGoal.class.getName(),
                                                    RackAwareDistributionGoal.class.getName(),
                                                    MinTopicLeadersPerBrokerGoal.class.getName(),
                                                    ReplicaCapacityGoal.class.getName(),
                                                    DiskCapacityGoal.class.getName(),
                                                    NetworkInboundCapacityGoal.class.getName(),
                                                    NetworkOutboundCapacityGoal.class.getName(),
                                                    CpuCapacityGoal.class.getName(),
                                                    ReplicaDistributionGoal.class.getName(),
                                                    PotentialNwOutGoal.class.getName(),
                                                    DiskUsageDistributionGoal.class.getName(),
                                                    NetworkInboundUsageDistributionGoal.class.getName(),
                                                    NetworkOutboundUsageDistributionGoal.class.getName(),
                                                    CpuUsageDistributionGoal.class.getName(),
                                                    LeaderReplicaDistributionGoal.class.getName(),
                                                    LeaderBytesInDistributionGoal.class.getName(),
                                                    TopicReplicaDistributionGoal.class.getName(),
                                                    PreferredLeaderElectionGoal.class.getName());

    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, BROKEN_BROKERS, REGRESSION);

    // ----------##TEST: REPLICA SWAP OPERATIONS.
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(getDefaultCruiseControlProperties()));
    balancingConstraint.setResourceBalancePercentage(TestConstants.ZERO_BALANCE_PERCENTAGE);
    p.add(params(balancingConstraint, DeterministicCluster.unbalanced4(), Collections.singletonList(DiskUsageDistributionGoal.class.getName()),
          verifications, null));
    p.add(params(balancingConstraint, DeterministicCluster.unbalanced4(),
                 Collections.singletonList(IntraBrokerDiskUsageDistributionGoal.class.getName()),
                 verifications, null));

    // ----------##TEST: BALANCE PERCENTAGES.
    List<Double> balancePercentages = new ArrayList<>();
    balancePercentages.add(TestConstants.HIGH_BALANCE_PERCENTAGE);
    balancePercentages.add(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
    balancePercentages.add(TestConstants.LOW_BALANCE_PERCENTAGE);

    // -- TEST DECK #1: SMALL CLUSTER.
    for (Double balancePercentage : balancePercentages) {
      Properties properties = getDefaultCruiseControlProperties();
      properties.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, DeterministicCluster.T2);
      balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(properties));
      balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
      balancingConstraint.setResourceBalancePercentage(balancePercentage);
      p.add(params(balancingConstraint, DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
    }
    // -- TEST DECK #2: MEDIUM CLUSTER.
    for (Double balancePercentage : balancePercentages) {
      Properties properties = getDefaultCruiseControlProperties();
      properties.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, DeterministicCluster.TOPIC_A);
      balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(properties));
      balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
      balancingConstraint.setResourceBalancePercentage(balancePercentage);
      p.add(params(balancingConstraint, DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
    }

    // ----------##TEST: CAPACITY THRESHOLD.
    List<Double> capacityThresholds = new ArrayList<>();
    capacityThresholds.add(TestConstants.HIGH_CAPACITY_THRESHOLD);
    capacityThresholds.add(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    capacityThresholds.add(TestConstants.LOW_CAPACITY_THRESHOLD);

    // -- TEST DECK #3: SMALL CLUSTER.
    for (Double capacityThreshold : capacityThresholds) {
      balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(getDefaultCruiseControlProperties()));
      balancingConstraint.setResourceBalancePercentage(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
      balancingConstraint.setCapacityThreshold(capacityThreshold);
      p.add(params(balancingConstraint, DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
    }
    // -- TEST DECK #4: MEDIUM CLUSTER.
    for (Double capacityThreshold : capacityThresholds) {
      balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(getDefaultCruiseControlProperties()));
      balancingConstraint.setResourceBalancePercentage(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
      balancingConstraint.setCapacityThreshold(capacityThreshold);
      p.add(params(balancingConstraint, DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
    }

    // ----------##TEST: BROKER CAPACITY.
    List<Double> brokerCapacities = new ArrayList<>();
    brokerCapacities.add(TestConstants.LARGE_BROKER_CAPACITY);
    brokerCapacities.add(TestConstants.MEDIUM_BROKER_CAPACITY);
    brokerCapacities.add(TestConstants.SMALL_BROKER_CAPACITY);

    // -- TEST DECK #5: SMALL AND MEDIUM CLUSTERS.
    for (Double capacity : brokerCapacities) {
      Map<Resource, Double> testBrokerCapacity = new HashMap<>();
      testBrokerCapacity.put(Resource.CPU, capacity);
      testBrokerCapacity.put(Resource.DISK, capacity);
      testBrokerCapacity.put(Resource.NW_IN, capacity);
      testBrokerCapacity.put(Resource.NW_OUT, capacity);

      p.add(params(balancingConstraint, DeterministicCluster.smallClusterModel(testBrokerCapacity),
                   goalNameByPriority, verifications, null));
      p.add(params(balancingConstraint, DeterministicCluster.mediumClusterModel(testBrokerCapacity),
                   goalNameByPriority, verifications, null));
    }

    List<String> kafkaAssignerGoals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getName(),
                                                    KafkaAssignerDiskUsageDistributionGoal.class.getName());
    List<OptimizationVerifier.Verification> kafkaAssignerVerifications = Arrays.asList(BROKEN_BROKERS, REGRESSION);
    // Small cluster.
    p.add(params(balancingConstraint, DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                 kafkaAssignerGoals, kafkaAssignerVerifications, null));
    // Medium cluster.
    p.add(params(balancingConstraint, DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
                 kafkaAssignerGoals, kafkaAssignerVerifications, null));
    // Rack-aware satisfiable.
    p.add(params(balancingConstraint, DeterministicCluster.rackAwareSatisfiable(),
                 kafkaAssignerGoals, kafkaAssignerVerifications, null));
    // Rack-aware unsatisfiable.
    p.add(params(balancingConstraint, DeterministicCluster.rackAwareUnsatisfiable(),
                 kafkaAssignerGoals, kafkaAssignerVerifications, OptimizationFailureException.class));

    Properties properties = getDefaultCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, TestConstants.TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS);
    balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(properties));

    // Min-topic-leader-replica-per-broker satisfiable.
    p.add(params(balancingConstraint, DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable(),
                 Collections.singletonList(MinTopicLeadersPerBrokerGoal.class.getName()), verifications, null));

    // Min-topic-leader-replica-per-broker satisfiable2.
    p.add(params(balancingConstraint, DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable2(),
                 Collections.singletonList(MinTopicLeadersPerBrokerGoal.class.getName()), verifications, null));

    // Min-topic-leader-replica-per-broker unsatisfiable.
    p.add(params(balancingConstraint, DeterministicCluster.leaderReplicaPerBrokerUnsatisfiable(),
                 Collections.singletonList(MinTopicLeadersPerBrokerGoal.class.getName()),
                 verifications, OptimizationFailureException.class));

    // Min-topic-leader-replica-per-broker satisfiable3.
    properties = getDefaultCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG, "4");
    p.add(params(new BalancingConstraint(new KafkaCruiseControlConfig(properties)), DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable3(),
                 Collections.singletonList(MinTopicLeadersPerBrokerGoal.class.getName()), verifications, null));

    // Min-topic-leader-replica-per-broker satisfiable4 to test the case where two topics need to have at least one leader per broker.
    properties = getDefaultCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG, "1");
    // Try to match "topic0" and "topic1"
    properties.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, "topic\\d");
    p.add(params(new BalancingConstraint(new KafkaCruiseControlConfig(properties)), DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable4(),
                 Collections.singletonList(MinTopicLeadersPerBrokerGoal.class.getName()), verifications, null));
    // Min-topic-leader-replica-per-broker satisfiable5 to test the case where two topics need to have the leaders evenly distributed to 3 brokers
    properties = getDefaultCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG, "0");
    // Try to match "topic0" and "topic1"
    properties.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, "topic\\d");
    p.add(params(new BalancingConstraint(new KafkaCruiseControlConfig(properties)), DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable5(),
                 Collections.singletonList(MinTopicLeadersPerBrokerGoal.class.getName()), verifications, null));

    // Broker Set Awareness
    goalNameByPriority = Arrays.asList(BrokerSetAwareGoal.class.getName(),
                                       RackAwareGoal.class.getName(),
                                       RackAwareDistributionGoal.class.getName(),
                                       ReplicaCapacityGoal.class.getName(),
                                       DiskCapacityGoal.class.getName(),
                                       NetworkInboundCapacityGoal.class.getName(),
                                       NetworkOutboundCapacityGoal.class.getName(),
                                       CpuCapacityGoal.class.getName());

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable1(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable1(),
                 goalNameByPriority, verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable2(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable2(),
                 goalNameByPriority, verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable3(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable3(),
                 goalNameByPriority, verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable4(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable4(),
                 goalNameByPriority, verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable5(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));
    // This test is to validate that an unbalanced brokerset aware cluster can move replicas around, if another goal like DiskCapacityGoal is violated
    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable5(),
                 Arrays.asList(BrokerSetAwareGoal.class.getName(),
                               DiskCapacityGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable5(),
                 Arrays.asList(DiskCapacityGoal.class.getName(),
                               BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable6(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable6(),
                 goalNameByPriority, verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable8(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetSatisfiable8(),
                 goalNameByPriority, verifications, null));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable1(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, OptimizationFailureException.class));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable1(),
                 goalNameByPriority, verifications, OptimizationFailureException.class));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable1(),
                 Arrays.asList(DiskCapacityGoal.class.getName(),
                               BrokerSetAwareGoal.class.getName()), verifications, OptimizationFailureException.class));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable3(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, OptimizationFailureException.class));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable3(),
                 goalNameByPriority, verifications, OptimizationFailureException.class));

    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable4(),
                 Arrays.asList(DiskUsageDistributionGoal.class.getName(),
                               BrokerSetAwareGoal.class.getName()), verifications, OptimizationFailureException.class));
    // Since DiskUsageDistribution is Soft Goal, the exception won't be thrown if the optimization suggested does not abide by BrokerSetAwareGoal
    p.add(params(balancingConstraint, DeterministicCluster.brokerSetUnSatisfiable4(),
                 Arrays.asList(BrokerSetAwareGoal.class.getName(),
                               DiskUsageDistributionGoal.class.getName()), verifications, null));

    // Broker Set Awareness with topic patterns: topic "hot" is pinned to broker set "fast", while topic "cold" is unconstrained.
    properties = getDefaultCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG,
                           TopicPatternBrokerSetMappingPolicy.class.getName());
    properties.setProperty(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG,
                           Objects.requireNonNull(DeterministicClusterTest.class.getClassLoader()
                                                                            .getResource("testBrokerSetsWithTopicPatterns.json")).getFile());
    BalancingConstraint topicPatternBalancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(properties));
    topicPatternBalancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    topicPatternBalancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    p.add(params(topicPatternBalancingConstraint, DeterministicCluster.brokerSetTopicPatternSatisfiable(),
                 Collections.singletonList(BrokerSetAwareGoal.class.getName()), verifications, null));

    p.add(params(topicPatternBalancingConstraint, DeterministicCluster.brokerSetTopicPatternSatisfiable(),
                 goalNameByPriority, verifications, null));
    return p;
  }

  private static Properties getDefaultCruiseControlProperties() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(6L));
    properties.setProperty(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(6L));
    properties.setProperty(AnalyzerConfig.REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG,
                           ReplicaToOriginalBrokerSetMappingPolicy.class.getName());
    return properties;
  }

  private static Object[] params(BalancingConstraint balancingConstraint,
                                 ClusterModel cluster,
                                 List<String> goalNameByPriority,
                                 List<OptimizationVerifier.Verification> verifications,
                                 Class<? extends Throwable> expectedException) {
    return new Object[]{balancingConstraint, cluster, goalNameByPriority, verifications, expectedException};
  }

  @Test
  public void test() throws Exception {
    try {
      assertTrue("Deterministic Cluster Test failed to improve the existing state.",
          OptimizationVerifier.executeGoalsFor(_balancingConstraint, _cluster, _goalNameByPriority, _verifications));
    } catch (OptimizationFailureException optimizationFailureException) {
      // This exception is thrown if rebalance fails due to alive brokers having insufficient capacity.
      if (!optimizationFailureException.getMessage().contains("Insufficient capacity for ")) {
        throw optimizationFailureException;
      }
    }
  }
}
