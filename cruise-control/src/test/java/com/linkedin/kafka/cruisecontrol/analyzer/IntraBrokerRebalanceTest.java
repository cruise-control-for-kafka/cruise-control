/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskIORateDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.RandomCluster;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.*;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.*;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with intra-broker rebalance goals using various random clusters.
 */
@RunWith(Parameterized.class)
public class IntraBrokerRebalanceTest {

  private final int _testId;
  private final Map<ClusterProperty, Number> _modifiedProperties;
  private final List<String> _goalNameByPriority;
  private final BalancingConstraint _balancingConstraint;
  private final Set<String> _excludedTopics;
  private final List<OptimizationVerifier.Verification> _verifications;

  /**
   * Constructor of Self Healing Test.
   *
   * @param testId Test id.
   * @param modifiedProperties Modified cluster properties over the {@link TestConstants#BASE_PROPERTIES}.
   * @param goalNameByPriority Goal name by priority.
   * @param balancingConstraint Balancing constraint.
   * @param excludedTopics Excluded topics.
   * @param verifications the verifications to make.
   */
  public IntraBrokerRebalanceTest(int testId,
                                  Map<ClusterProperty, Number> modifiedProperties,
                                  List<String> goalNameByPriority,
                                  BalancingConstraint balancingConstraint,
                                  Collection<String> excludedTopics,
                                  List<OptimizationVerifier.Verification> verifications) {
    _testId = testId;
    _modifiedProperties = modifiedProperties;
    _goalNameByPriority = goalNameByPriority;
    _balancingConstraint = balancingConstraint;
    _excludedTopics = new HashSet<>(excludedTopics);
    _verifications = verifications;
  }

  @Test
  public void test() throws Exception {
    // Create cluster properties by applying modified properties to base properties.
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(_modifiedProperties);

    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.UNIFORM, true,
                          true, _excludedTopics);

    assertTrue("Goals failed to optimize cluster.",
               OptimizationVerifier.executeGoalsFor(_balancingConstraint, clusterModel, _goalNameByPriority,
                                                    _excludedTopics, _verifications));

  }

  private static Object[] params(int testId,
                                 Map<ClusterProperty, Number> modifiedProperties,
                                 List<String> goalNameByPriority,
                                 BalancingConstraint balancingConstraint,
                                 Collection<String> excludedTopics,
                                 List<OptimizationVerifier.Verification> verifications) {
    return new Object[]{testId, modifiedProperties, goalNameByPriority, balancingConstraint, excludedTopics, verifications};
  }

  /**
   * Populate parameters for the {@link OptimizationVerifier}.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    Collection<Object[]> p = new ArrayList<>();
    List<String> goalNameByPriority = Arrays.asList(IntraBrokerDiskCapacityGoal.class.getName(),
                                                    IntraBrokerDiskUsageDistributionGoal.class.getName(),
                                                    IntraBrokerDiskIORateDistributionGoal.class.getName());
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(2000L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    List<OptimizationVerifier.Verification> verifications = Arrays.asList(GOAL_VIOLATION, REGRESSION);
    // Multiple soft goals may trade off against each other (e.g. disk usage vs. disk I/O rate), hence only verify that
    // no goal regresses -- similar to the verifications of multiple soft goals in RandomClusterTest.
    List<String> diskUsageGoalNameByPriority = goalNameByPriority.subList(0, 2);
    List<OptimizationVerifier.Verification> multiSoftGoalVerifications = Collections.singletonList(REGRESSION);

    int testId = 0;
    // -- TEST DECK #1: HEALTHY CLUSTER.
    List<String> testGoal;
    Map<ClusterProperty, Number> jbodCluster = new HashMap<>();
    jbodCluster.put(ClusterProperty.POPULATE_REPLICA_PLACEMENT_INFO, 1);
    // Test: Single Goal.
    for (int i = 0; i < goalNameByPriority.size(); i++) {
      testGoal = goalNameByPriority.subList(i, i + 1);
      p.add(params(testId++, jbodCluster, testGoal, balancingConstraint, Collections.emptySet(), verifications));
    }
    // Test: Multiple Goals.
    p.add(params(testId++, jbodCluster, diskUsageGoalNameByPriority, balancingConstraint, Collections.emptySet(), verifications));
    p.add(params(testId++, jbodCluster, goalNameByPriority, balancingConstraint, Collections.emptySet(), multiSoftGoalVerifications));

    // -- TEST DECK #2: CLUSTER WITH DEAD&BROKEN BROKER.
    Set<String> excludedTopics = Set.of(T1, T2);
    // Test: Single Goal.
    for (int i = 0; i < goalNameByPriority.size(); i++) {
      testGoal = goalNameByPriority.subList(i, i + 1);
      p.add(params(testId++, jbodCluster, testGoal, balancingConstraint, excludedTopics, verifications));
    }
    // Test: Multiple Goals.
    p.add(params(testId++, jbodCluster, diskUsageGoalNameByPriority, balancingConstraint, excludedTopics, verifications));
    p.add(params(testId++, jbodCluster, goalNameByPriority, balancingConstraint, excludedTopics, multiSoftGoalVerifications));

    // -- TEST DECK #3: CLUSTER WITH EXCLUDED TOPIC.
    Map<ClusterProperty, Number> unhealthyCluster = new HashMap<>();
    unhealthyCluster.put(ClusterProperty.POPULATE_REPLICA_PLACEMENT_INFO, 1);
    unhealthyCluster.put(ClusterProperty.NUM_BROKERS_WITH_BAD_DISK, 5);
    unhealthyCluster.put(ClusterProperty.NUM_DEAD_BROKERS, 5);
    // Test: Single Goal.
    for (int i = 0; i < goalNameByPriority.size(); i++) {
      testGoal = goalNameByPriority.subList(i, i + 1);
      p.add(params(testId++, unhealthyCluster, testGoal, balancingConstraint, Collections.emptySet(), verifications));
    }
    // Test: Multiple Goal.
    p.add(params(testId++, unhealthyCluster, diskUsageGoalNameByPriority, balancingConstraint, Collections.emptySet(), verifications));
    p.add(params(testId++, unhealthyCluster, goalNameByPriority, balancingConstraint, Collections.emptySet(),
                 multiSoftGoalVerifications));

    return p;
  }
}
