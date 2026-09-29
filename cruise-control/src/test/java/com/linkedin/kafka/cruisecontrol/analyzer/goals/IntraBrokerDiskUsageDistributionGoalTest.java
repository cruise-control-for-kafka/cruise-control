/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link IntraBrokerDiskUsageDistributionGoal} and its balance threshold
 * {@link AnalyzerConfig#INTRA_BROKER_DISK_BALANCE_THRESHOLD_CONFIG}.
 */
public class IntraBrokerDiskUsageDistributionGoalTest {
  private static final double STRICT_BALANCE_THRESHOLD = 1.02;
  private static final double LOOSE_BALANCE_THRESHOLD = 1.5;
  private static final OptimizationOptions OPTIMIZATION_OPTIONS =
      new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

  @Test
  public void testIntraBrokerDiskBalanceThresholdFallsBackToDiskBalanceThreshold() {
    BalancingConstraint constraint = balancingConstraint(LOOSE_BALANCE_THRESHOLD, null);
    assertEquals(LOOSE_BALANCE_THRESHOLD, constraint.intraBrokerDiskBalancePercentage(), 0.0);
  }

  @Test
  public void testIntraBrokerDiskBalanceThresholdIsIndependentOfDiskBalanceThreshold() {
    BalancingConstraint constraint = balancingConstraint(LOOSE_BALANCE_THRESHOLD, STRICT_BALANCE_THRESHOLD);
    assertEquals(STRICT_BALANCE_THRESHOLD, constraint.intraBrokerDiskBalancePercentage(), 0.0);
    assertEquals(LOOSE_BALANCE_THRESHOLD, constraint.resourceBalancePercentage(Resource.DISK), 0.0);
  }

  @Test
  public void testInvalidIntraBrokerDiskBalanceThreshold() {
    assertThrows(ConfigException.class, () -> balancingConstraint(LOOSE_BALANCE_THRESHOLD, 0.9));
  }

  @Test
  public void testGoalUsesLooseIntraBrokerDiskBalanceThreshold() throws Exception {
    // Disk balance threshold across brokers is strict, but intra-broker disk balance threshold is loose.
    BalancingConstraint constraint = balancingConstraint(STRICT_BALANCE_THRESHOLD, LOOSE_BALANCE_THRESHOLD);
    ClusterModel clusterModel = DeterministicCluster.unbalanced4();
    assertEquals(0, clusterModel.getClusterStats(constraint, OPTIMIZATION_OPTIONS).numUnbalancedDisks());

    Map<TopicPartition, String> logDirsBefore = logDirByPartition(clusterModel);
    IntraBrokerDiskUsageDistributionGoal goal = new IntraBrokerDiskUsageDistributionGoal(constraint);
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    // Disks are balanced wrt the intra-broker disk balance threshold, hence no replica should be moved.
    assertEquals(logDirsBefore, logDirByPartition(clusterModel));
  }

  @Test
  public void testGoalUsesStrictIntraBrokerDiskBalanceThreshold() throws Exception {
    // Disk balance threshold across brokers is loose, but intra-broker disk balance threshold is strict.
    BalancingConstraint constraint = balancingConstraint(LOOSE_BALANCE_THRESHOLD, STRICT_BALANCE_THRESHOLD);
    ClusterModel clusterModel = DeterministicCluster.unbalanced4();
    assertTrue(clusterModel.getClusterStats(constraint, OPTIMIZATION_OPTIONS).numUnbalancedDisks() > 0);

    Map<TopicPartition, String> logDirsBefore = logDirByPartition(clusterModel);
    IntraBrokerDiskUsageDistributionGoal goal = new IntraBrokerDiskUsageDistributionGoal(constraint);
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), OPTIMIZATION_OPTIONS));
    // Disks are unbalanced wrt the intra-broker disk balance threshold, hence replicas should be moved between disks.
    assertNotEquals(logDirsBefore, logDirByPartition(clusterModel));
    assertEquals(0, clusterModel.getClusterStats(constraint, OPTIMIZATION_OPTIONS).numUnbalancedDisks());
  }

  private static BalancingConstraint balancingConstraint(double diskBalanceThreshold, Double intraBrokerDiskBalanceThreshold) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.DISK_BALANCE_THRESHOLD_CONFIG, Double.toString(diskBalanceThreshold));
    if (intraBrokerDiskBalanceThreshold != null) {
      props.setProperty(AnalyzerConfig.INTRA_BROKER_DISK_BALANCE_THRESHOLD_CONFIG, Double.toString(intraBrokerDiskBalanceThreshold));
    }
    return new BalancingConstraint(new KafkaCruiseControlConfig(props));
  }

  private static Map<TopicPartition, String> logDirByPartition(ClusterModel clusterModel) {
    Map<TopicPartition, String> logDirByPartition = new HashMap<>();
    for (Replica replica : clusterModel.leaderReplicas()) {
      logDirByPartition.put(replica.topicPartition(), replica.disk().logDir());
    }
    return logDirByPartition;
  }
}
