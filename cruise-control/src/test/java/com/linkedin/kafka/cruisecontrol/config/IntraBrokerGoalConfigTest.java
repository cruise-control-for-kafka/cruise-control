/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getIntraBrokerDetectionGoals;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingIntraBrokerGoalNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for the configs of intra-broker goals used for anomaly detection and self-healing.
 */
public class IntraBrokerGoalConfigTest {
  private static final String INTRA_BROKER_DISK_CAPACITY_GOAL = IntraBrokerDiskCapacityGoal.class.getName();
  private static final String INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL = IntraBrokerDiskUsageDistributionGoal.class.getName();

  private static KafkaCruiseControlConfig config(Map<String, String> configOverrides) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.putAll(configOverrides);
    return new KafkaCruiseControlConfig(props);
  }

  private static List<String> intraBrokerDetectionGoalNames(KafkaCruiseControlConfig config) {
    return getIntraBrokerDetectionGoals(config).stream().map(Goal::name).collect(Collectors.toList());
  }

  @Test
  public void testIntraBrokerGoalsByDefault() {
    List<String> intraBrokerGoalNames = List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName(),
                                                IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());
    KafkaCruiseControlConfig config = config(Collections.emptyMap());
    assertEquals(intraBrokerGoalNames, intraBrokerDetectionGoalNames(config));
    assertEquals(intraBrokerGoalNames, getSelfHealingIntraBrokerGoalNames(config));

    // Unless set, intra-broker goals for anomaly detection and self-healing follow customized intra-broker goals.
    config = config(Map.of(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL));
    List<String> customizedIntraBrokerGoalNames = List.of(IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());
    assertEquals(customizedIntraBrokerGoalNames, intraBrokerDetectionGoalNames(config));
    assertEquals(customizedIntraBrokerGoalNames, getSelfHealingIntraBrokerGoalNames(config));
  }

  @Test
  public void testExplicitIntraBrokerGoals() {
    KafkaCruiseControlConfig config = config(Map.of(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG,
                                                    INTRA_BROKER_DISK_CAPACITY_GOAL,
                                                    AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG,
                                                    INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL));
    assertEquals(List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName()), intraBrokerDetectionGoalNames(config));
    assertEquals(List.of(IntraBrokerDiskUsageDistributionGoal.class.getSimpleName()), getSelfHealingIntraBrokerGoalNames(config));
  }

  @Test
  public void testEmptyIntraBrokerGoals() {
    KafkaCruiseControlConfig config = config(Map.of(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG, "",
                                                    AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG, ""));
    // Empty intra-broker goals for anomaly detection disable the detection of intra-broker goal violations.
    assertTrue(getIntraBrokerDetectionGoals(config).isEmpty());
    // Empty intra-broker goals for self-healing fall back to intra-broker goals, consistent with self.healing.goals.
    assertEquals(List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName(), IntraBrokerDiskUsageDistributionGoal.class.getSimpleName()),
                 getSelfHealingIntraBrokerGoalNames(config));
  }

  @Test
  public void testUnsupportedIntraBrokerGoals() {
    for (String intraBrokerGoalsConfig : List.of(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG,
                                                 AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG)) {
      // A goal that is not an intra-broker goal.
      assertThrows(ConfigException.class, () -> config(Map.of(intraBrokerGoalsConfig, RackAwareGoal.class.getName())));
      // An intra-broker goal that is excluded from intra-broker goals.
      assertThrows(ConfigException.class, () -> config(Map.of(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, INTRA_BROKER_DISK_USAGE_DISTRIBUTION_GOAL,
                                                              intraBrokerGoalsConfig, INTRA_BROKER_DISK_CAPACITY_GOAL)));
    }
  }
}
