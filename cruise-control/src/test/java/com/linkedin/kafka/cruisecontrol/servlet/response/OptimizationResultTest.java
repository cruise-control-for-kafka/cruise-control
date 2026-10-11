/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.DiskBalanceStats;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RebalanceParameters;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class OptimizationResultTest {
  private static final String INTER_BROKER_GOAL = DiskUsageDistributionGoal.class.getSimpleName();
  private static final double DELTA = 1e-6;
  private static final String INTRA_BROKER_GOAL = IntraBrokerDiskUsageDistributionGoal.class.getSimpleName();

  /**
   * Optimize {@link DeterministicCluster#unbalanced4()}, which has disk information, with the given goal.
   *
   * @param config The Kafka Cruise Control config.
   * @param goalName Name of the goal to optimize.
   * @return The optimizer result.
   */
  private static OptimizerResult optimizeDiskAwareCluster(KafkaCruiseControlConfig config, String goalName) throws Exception {
    return optimize(config, DeterministicCluster.unbalanced4(), goalName);
  }

  private static OptimizerResult optimize(KafkaCruiseControlConfig config, ClusterModel clusterModel, String goalName) throws Exception {
    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(), EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));
    return goalOptimizer.optimizations(clusterModel, goalsByPriority(List.of(goalName), config), new OperationProgress());
  }

  /**
   * @return A cluster model with a broker having two disks, where four replicas, each using 20% of a disk, reside on
   * {@link TestConstants#LOGDIR0}.
   */
  private static ClusterModel clusterModelWithUnbalancedDisks() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    clusterModel.createRack("r0");
    clusterModel.createBroker("r0", "0", 0, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY), true);
    for (int partition = 0; partition < 4; partition++) {
      TopicPartition tp = new TopicPartition("topic", partition);
      clusterModel.createReplica("r0", 0, tp, 0, true, false, LOGDIR0, false);
      clusterModel.setReplicaLoad("r0", 0, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, 0.2 * TestConstants.LARGE_BROKER_CAPACITY / 2),
                                  Collections.singletonList(1L));
    }
    return clusterModel;
  }

  private static String response(OptimizerResult optimizerResult, KafkaCruiseControlConfig config, boolean json) {
    RebalanceParameters parameters = EasyMock.niceMock(RebalanceParameters.class);
    EasyMock.expect(parameters.json()).andReturn(json).anyTimes();
    EasyMock.expect(parameters.endPoint()).andReturn(CruiseControlEndPoint.REBALANCE).anyTimes();
    EasyMock.replay(parameters);

    OptimizationResult response = new OptimizationResult(optimizerResult, config);
    response.discardIrrelevantResponse(parameters);
    return response.cachedResponse();
  }

  /**
   * @param response Plaintext response.
   * @param goalName Goal name.
   * @return The stats printed for the given goal.
   */
  private static String goalStats(String response, String goalName) {
    int start = response.indexOf("Stats for " + goalName + "(");
    assertTrue(response, start >= 0);
    int end = response.indexOf("Stats for ", start + 1);
    if (end < 0) {
      end = response.indexOf("Cluster load after rebalance", start);
    }
    return response.substring(start, end);
  }

  @Test
  public void testPlaintextGoalStatsOfDiskAwareCluster() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    // An inter-broker goal can change both broker-level and disk-level stats.
    OptimizerResult optimizerResult = optimizeDiskAwareCluster(config, INTER_BROKER_GOAL);
    assertFalse(optimizerResult.isIntraBrokerGoalName(INTER_BROKER_GOAL));
    String interBrokerGoalStats = goalStats(response(optimizerResult, config, false), INTER_BROKER_GOAL);
    assertTrue(interBrokerGoalStats, interBrokerGoalStats.contains("AVG:{"));
    assertTrue(interBrokerGoalStats, interBrokerGoalStats.contains("DISKS:{"));

    // An intra-broker goal cannot change broker-level stats.
    optimizerResult = optimizeDiskAwareCluster(config, INTRA_BROKER_GOAL);
    assertTrue(optimizerResult.isIntraBrokerGoalName(INTRA_BROKER_GOAL));
    String intraBrokerGoalStats = goalStats(response(optimizerResult, config, false), INTRA_BROKER_GOAL);
    assertFalse(intraBrokerGoalStats, intraBrokerGoalStats.contains("AVG:{"));
    assertTrue(intraBrokerGoalStats, intraBrokerGoalStats.contains("DISKS:{alive: 4 "));
  }

  @Test
  public void testJsonGoalStatsOfDiskAwareCluster() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    for (String goalName : List.of(INTER_BROKER_GOAL, INTRA_BROKER_GOAL)) {
      Map<String, Object> response = new Gson().fromJson(response(optimizeDiskAwareCluster(config, goalName), config, true),
                                                         new TypeToken<Map<String, Object>>() { }.getType());
      List<Map<String, Object>> goalSummary = cast(response.get("goalSummary"));
      assertEquals(1, goalSummary.size());
      // Broker-level stats are kept for all goals, so that JSON clients do not break.
      Map<String, Object> clusterModelStats = cast(goalSummary.get(0).get("clusterModelStats"));
      assertTrue(clusterModelStats.containsKey("statistics"));
      Map<String, Object> diskStatistics = cast(clusterModelStats.get("diskStatistics"));
      assertEquals(4, ((Number) diskStatistics.get("numAliveDisks")).intValue());
    }
  }

  @Test
  public void testDiskBalanceBeforeAndAfterOptimization() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    OptimizerResult optimizerResult = optimize(config, clusterModelWithUnbalancedDisks(), INTRA_BROKER_GOAL);
    DiskBalanceStats before = optimizerResult.clusterModelStatsBeforeOptimization().diskBalanceStats();
    DiskBalanceStats after = optimizerResult.clusterModelStats().diskBalanceStats();
    // Before optimization, the disks are at 80% and 0%; after it, at 40% each.
    assertEquals(2, before.numUnbalancedDisks());
    assertEquals(80.0, before.maxDiskUtilizationPct(), DELTA);
    assertEquals(80.0, before.maxDiskUtilizationSpreadPct(), DELTA);
    assertEquals(0, after.numUnbalancedDisks());
    assertEquals(40.0, after.maxDiskUtilizationPct(), DELTA);
    assertEquals(0.0, after.maxDiskUtilizationSpreadPct(), DELTA);

    String summary = optimizerResult.getProposalSummary();
    assertTrue(summary, summary.contains(String.format("%nDisk balance (before -> after): unbalanced disks 2 -> 0, max disk utilization "
                                                       + "80.00%% -> 40.00%%, max spread within a broker 80.00 -> 0.00pp.")));
    Map<String, Object> jsonSummary = optimizerResult.getProposalSummaryForJson();
    assertEquals(before.numUnbalancedDisks(), jsonSummary.get("numUnbalancedDisksBefore"));
    assertEquals(after.numUnbalancedDisks(), jsonSummary.get("numUnbalancedDisksAfter"));
    assertEquals(before.maxDiskUtilizationPct(), jsonSummary.get("maxDiskUtilizationPctBefore"));
    assertEquals(after.maxDiskUtilizationPct(), jsonSummary.get("maxDiskUtilizationPctAfter"));
    assertEquals(before.maxDiskUtilizationSpreadPct(), jsonSummary.get("maxDiskUtilizationSpreadPctBefore"));
    assertEquals(after.maxDiskUtilizationSpreadPct(), jsonSummary.get("maxDiskUtilizationSpreadPctAfter"));
  }

  @Test
  public void testNoDiskBalanceWithoutDiskInfo() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(), EasyMock.mock(Executor.class),
                                                    EasyMock.mock(AdminClient.class));
    OptimizerResult optimizerResult = goalOptimizer.optimizations(DeterministicCluster.unbalanced(),
                                                                  goalsByPriority(List.of(INTER_BROKER_GOAL), config),
                                                                  new OperationProgress());

    assertFalse(optimizerResult.getProposalSummary().contains("Disk balance"));
    assertFalse(optimizerResult.getProposalSummaryForJson().containsKey("numUnbalancedDisksBefore"));
    String response = response(optimizerResult, config, false);
    assertFalse(response, response.contains("DISKS"));
    assertTrue(response, goalStats(response, INTER_BROKER_GOAL).contains("AVG:{"));
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object object) {
    return (T) object;
  }
}
