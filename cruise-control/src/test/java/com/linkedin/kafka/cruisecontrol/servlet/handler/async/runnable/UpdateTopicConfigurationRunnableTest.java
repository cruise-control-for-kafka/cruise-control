/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.generateClusterFromClusterModel;
import static org.junit.Assert.assertEquals;


/**
 * Unit tests for {@link UpdateTopicConfigurationRunnable}.
 */
public class UpdateTopicConfigurationRunnableTest {
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  // In the small cluster model, T2 has 3 partitions with 2 replicas each, and there are 10 replicas in total.
  private static final int NUM_REPLICAS = 10;
  private static final int NUM_T2_PARTITIONS = 3;
  private static final short T2_REPLICATION_FACTOR = 2;

  @Test
  public void testLoadBeforeOptimizationOnReplicationFactorDecrease() throws Exception {
    verifyLoadBeforeAndAfterOptimization((short) (T2_REPLICATION_FACTOR - 1));
  }

  @Test
  public void testLoadBeforeOptimizationOnReplicationFactorIncrease() throws Exception {
    verifyLoadBeforeAndAfterOptimization((short) (T2_REPLICATION_FACTOR + 1));
  }

  /**
   * The load before optimization must be the load of the cluster before the replication factor change, and the load after
   * optimization must reflect the replicas that the change adds or removes.
   *
   * @param replicationFactor Target replication factor of T2.
   */
  private static void verifyLoadBeforeAndAfterOptimization(short replicationFactor) throws Exception {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Map<String, Object> loadBeforeChange = clusterModel.brokerStats(null).getJsonStructure();
    KafkaCruiseControl kafkaCruiseControl = mockKafkaCruiseControl(clusterModel);
    EasyMock.replay(kafkaCruiseControl);

    UpdateTopicConfigurationRunnable runnable =
        new UpdateTopicConfigurationRunnable(kafkaCruiseControl, Collections.singletonMap(replicationFactor, Pattern.compile(T2)),
                                             Collections.emptyList(), true, false, false, "anomalyId", () -> "reason", false, true);
    OptimizerResult result = runnable.computeResult();

    assertEquals(loadBeforeChange, result.brokerStatsBeforeOptimization().getJsonStructure());
    assertEquals(NUM_REPLICAS, numReplicas(result.brokerStatsBeforeOptimization()));
    assertEquals(NUM_REPLICAS + NUM_T2_PARTITIONS * (replicationFactor - T2_REPLICATION_FACTOR),
                 numReplicas(result.brokerStatsAfterOptimization()));
    EasyMock.verify(kafkaCruiseControl);
  }

  @SuppressWarnings("unchecked")
  private static KafkaCruiseControl mockKafkaCruiseControl(ClusterModel clusterModel) throws Exception {
    KafkaCruiseControl kafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    EasyMock.expect(kafkaCruiseControl.config()).andReturn(CONFIG).anyTimes();
    EasyMock.expect(kafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject()))
            .andReturn(new ModelCompletenessRequirements(1, 0.0, false));
    EasyMock.expect(kafkaCruiseControl.getLoadMonitorTaskRunnerState())
            .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    EasyMock.expect(kafkaCruiseControl.kafkaCluster()).andReturn(generateClusterFromClusterModel(clusterModel));
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(kafkaCruiseControl.executorState())
            .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet()));
    EasyMock.expect(kafkaCruiseControl.excludedTopics(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Collections.emptySet());

    // Optimize with a real goal optimizer. The goal only needs to succeed for both a smaller and a larger replication factor.
    GoalOptimizer goalOptimizer = new GoalOptimizer(CONFIG, null, Time.SYSTEM, new MetricRegistry(),
                                                    EasyMock.mock(Executor.class), EasyMock.mock(Admin.class));
    List<Goal> goals = Collections.singletonList(AnalyzerUnitTestUtils.goal(ReplicaDistributionGoal.class));
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.eq(clusterModel), EasyMock.anyObject(), EasyMock.anyObject(),
                                                     EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andAnswer(() -> {
              Object[] args = EasyMock.getCurrentArguments();
              return goalOptimizer.optimizations((ClusterModel) args[0], goals, (OperationProgress) args[2],
                                                 (Map<TopicPartition, List<ReplicaPlacementInfo>>) args[3],
                                                 (OptimizationOptions) args[4], (BrokerStats) args[5]);
            });
    return kafkaCruiseControl;
  }

  @SuppressWarnings("unchecked")
  private static int numReplicas(BrokerStats brokerStats) {
    return ((List<Map<String, Object>>) brokerStats.getJsonStructure().get("brokers"))
        .stream().mapToInt(broker -> (Integer) broker.get("Replicas")).sum();
  }
}
