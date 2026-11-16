/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.RandomCluster;
import com.linkedin.kafka.cruisecontrol.model.Replica;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.*;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with intra-broker rebalance goals using various random clusters, and for the intra-broker disk
 * balance threshold {@link AnalyzerConfig#INTRA_BROKER_DISK_BALANCE_THRESHOLD_CONFIG}.
 */
@RunWith(Parameterized.class)
public class IntraBrokerRebalanceTest {
  private static final double STRICT_BALANCE_THRESHOLD = 1.02;
  private static final double LOOSE_BALANCE_THRESHOLD = 1.5;
  private static final OptimizationOptions OPTIMIZATION_OPTIONS =
      new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

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

  @Test
  public void testIntraBrokerDiskBalanceThresholdFallsBackToDiskBalanceThreshold() {
    KafkaCruiseControlConfig config = config(LOOSE_BALANCE_THRESHOLD, null);
    assertEquals(LOOSE_BALANCE_THRESHOLD, new BalancingConstraint(config).intraBrokerDiskBalancePercentage(), 0.0);
    // Goals are configured with the merged config values, which must keep the fallback.
    BalancingConstraint goalConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(config.mergedConfigValues()));
    assertEquals(LOOSE_BALANCE_THRESHOLD, goalConstraint.intraBrokerDiskBalancePercentage(), 0.0);
  }

  @Test
  public void testIntraBrokerDiskBalanceThresholdIsIndependentOfDiskBalanceThreshold() {
    BalancingConstraint constraint = new BalancingConstraint(config(LOOSE_BALANCE_THRESHOLD, STRICT_BALANCE_THRESHOLD));
    assertEquals(STRICT_BALANCE_THRESHOLD, constraint.intraBrokerDiskBalancePercentage(), 0.0);
    assertEquals(LOOSE_BALANCE_THRESHOLD, constraint.resourceBalancePercentage(Resource.DISK), 0.0);
  }

  @Test
  public void testInvalidIntraBrokerDiskBalanceThreshold() {
    assertThrows(ConfigException.class, () -> config(LOOSE_BALANCE_THRESHOLD, 0.9));
  }

  @Test
  public void testGoalUsesLooseIntraBrokerDiskBalanceThreshold() throws Exception {
    // Disk balance threshold across brokers is strict, but intra-broker disk balance threshold is loose.
    KafkaCruiseControlConfig config = config(STRICT_BALANCE_THRESHOLD, LOOSE_BALANCE_THRESHOLD);
    ClusterModel clusterModel = unbalanced4();
    assertEquals(0, clusterModel.getClusterStats(new BalancingConstraint(config), OPTIMIZATION_OPTIONS).numUnbalancedDisks());

    Map<TopicPartition, String> logDirsBefore = logDirByPartition(clusterModel);
    assertTrue(intraBrokerDiskUsageDistributionGoal(config).optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    // Disks are balanced wrt the intra-broker disk balance threshold, hence no replica should be moved.
    assertEquals(logDirsBefore, logDirByPartition(clusterModel));
  }

  @Test
  public void testGoalUsesStrictIntraBrokerDiskBalanceThreshold() throws Exception {
    // Disk balance threshold across brokers is loose, but intra-broker disk balance threshold is strict.
    KafkaCruiseControlConfig config = config(LOOSE_BALANCE_THRESHOLD, STRICT_BALANCE_THRESHOLD);
    BalancingConstraint constraint = new BalancingConstraint(config);
    ClusterModel clusterModel = unbalanced4();
    assertTrue(clusterModel.getClusterStats(constraint, OPTIMIZATION_OPTIONS).numUnbalancedDisks() > 0);

    Map<TopicPartition, String> logDirsBefore = logDirByPartition(clusterModel);
    assertTrue(intraBrokerDiskUsageDistributionGoal(config).optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    // Disks are unbalanced wrt the intra-broker disk balance threshold, hence replicas should be moved between disks.
    assertNotEquals(logDirsBefore, logDirByPartition(clusterModel));
    assertEquals(0, clusterModel.getClusterStats(constraint, OPTIMIZATION_OPTIONS).numUnbalancedDisks());
  }

  private static KafkaCruiseControlConfig config(double diskBalanceThreshold, Double intraBrokerDiskBalanceThreshold) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, Double.toString(diskBalanceThreshold));
    if (intraBrokerDiskBalanceThreshold != null) {
      props.setProperty(AnalyzerConfig.INTRA_BROKER_DISK_BALANCE_THRESHOLD_CONFIG, Double.toString(intraBrokerDiskBalanceThreshold));
    }
    return new KafkaCruiseControlConfig(props);
  }

  private static IntraBrokerDiskUsageDistributionGoal intraBrokerDiskUsageDistributionGoal(KafkaCruiseControlConfig config) {
    // Configure the goal the same way Cruise Control does, i.e. with the merged config values.
    IntraBrokerDiskUsageDistributionGoal goal = new IntraBrokerDiskUsageDistributionGoal();
    goal.configure(config.mergedConfigValues());
    return goal;
  }

  private static Map<TopicPartition, String> logDirByPartition(ClusterModel clusterModel) {
    Map<TopicPartition, String> logDirByPartition = new HashMap<>();
    for (Replica replica : clusterModel.leaderReplicas()) {
      logDirByPartition.put(replica.topicPartition(), replica.disk().logDir());
    }
    return logDirByPartition;
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
                                                    IntraBrokerDiskUsageDistributionGoal.class.getName());
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(2000L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    List<OptimizationVerifier.Verification> verifications = Arrays.asList(GOAL_VIOLATION, REGRESSION);

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
    p.add(params(testId++, jbodCluster, goalNameByPriority, balancingConstraint, Collections.emptySet(), verifications));

    // -- TEST DECK #2: CLUSTER WITH DEAD&BROKEN BROKER.
    Set<String> excludedTopics = Set.of(T1, T2);
    // Test: Single Goal.
    for (int i = 0; i < goalNameByPriority.size(); i++) {
      testGoal = goalNameByPriority.subList(i, i + 1);
      p.add(params(testId++, jbodCluster, testGoal, balancingConstraint, excludedTopics, verifications));
    }
    // Test: Multiple Goals.
    p.add(params(testId++, jbodCluster, goalNameByPriority, balancingConstraint, excludedTopics, verifications));

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
    p.add(params(testId++, unhealthyCluster, goalNameByPriority, balancingConstraint, Collections.emptySet(), verifications));

    return p;
  }
}
