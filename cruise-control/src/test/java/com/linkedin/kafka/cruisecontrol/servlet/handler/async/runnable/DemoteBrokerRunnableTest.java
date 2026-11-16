/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.DemoteBrokerParameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.RACK_BY_BROKER;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;


public class DemoteBrokerRunnableTest {
  private static final int DEMOTED_BROKER_ID = 0;
  private static final int FOLLOWER_BROKER_ID = 1;
  private static final TopicPartition T1P0 = new TopicPartition(T1, 0);
  private static final TopicPartition T2P0 = new TopicPartition(T2, 0);

  @Test
  public void testDemoteWithExcludedTopics() throws Exception {
    Pattern excludedTopics = Pattern.compile(T2);
    ClusterModel clusterModel = demote(excludedTopics, Collections.singleton(T2));

    // The leadership of the partition of the explicitly excluded topic is expected to stay on the demoted broker.
    assertEquals(DEMOTED_BROKER_ID, clusterModel.partition(T2P0).leader().broker().id());
    assertEquals(FOLLOWER_BROKER_ID, clusterModel.partition(T1P0).leader().broker().id());
  }

  @Test
  public void testDemoteWithoutExcludedTopics() throws Exception {
    // Without excluded_topics in the request, the topics excluded from partition movement by config (simulated by T2)
    // are expected to be demoted.
    ClusterModel clusterModel = demote(null, Collections.singleton(T2));

    assertEquals(FOLLOWER_BROKER_ID, clusterModel.partition(T2P0).leader().broker().id());
    assertEquals(FOLLOWER_BROKER_ID, clusterModel.partition(T1P0).leader().broker().id());
  }

  @Test
  public void testDemoteKeepsLeadershipWithoutInSyncFollower() throws Exception {
    // The follower of T1-0 is not in sync, so Kafka cannot elect it: the leadership of T1-0 is expected to stay on the demoted
    // broker even though skip_urp_demotion is false. T2-0 is in sync and is expected to be demoted.
    ClusterModel clusterModel = demote(null, Collections.emptySet(), Collections.singleton(T1P0));

    assertEquals(DEMOTED_BROKER_ID, clusterModel.partition(T1P0).leader().broker().id());
    assertEquals(FOLLOWER_BROKER_ID, clusterModel.partition(T2P0).leader().broker().id());
  }

  private static ClusterModel demote(Pattern requestedExcludedTopics, Set<String> resolvedExcludedTopics) throws Exception {
    return demote(requestedExcludedTopics, resolvedExcludedTopics, Collections.emptySet());
  }

  /**
   * Run a dry-run demotion of {@link #DEMOTED_BROKER_ID} with skip_urp_demotion=false for a user request with the given
   * excluded topics.
   *
   * @param requestedExcludedTopics The excluded_topics pattern in the request, or {@code null} if the request does not specify it.
   * @param resolvedExcludedTopics The excluded topics that Cruise Control resolves for the given pattern.
   * @param partitionsWithLaggingFollower Partitions whose follower on {@link #FOLLOWER_BROKER_ID} is not in sync.
   * @return The cluster model after the demotion.
   */
  private static ClusterModel demote(Pattern requestedExcludedTopics,
                                     Set<String> resolvedExcludedTopics,
                                     Set<TopicPartition> partitionsWithLaggingFollower) throws Exception {
    ClusterModel clusterModel = clusterModel();

    DemoteBrokerParameters parameters = EasyMock.niceMock(DemoteBrokerParameters.class);
    EasyMock.expect(parameters.dryRun()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.brokerIds()).andReturn(Collections.singleton(DEMOTED_BROKER_ID)).anyTimes();
    EasyMock.expect(parameters.brokerIdAndLogdirs()).andReturn(Collections.emptyMap()).anyTimes();
    EasyMock.expect(parameters.excludedTopics()).andReturn(requestedExcludedTopics).anyTimes();
    EasyMock.expect(parameters.skipUrpDemotion()).andReturn(false).anyTimes();

    KafkaCruiseControl kafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    kafkaCruiseControl.sanityCheckDryRun(true, false);
    EasyMock.expect(kafkaCruiseControl.kafkaCluster()).andReturn(cluster(partitionsWithLaggingFollower));
    EasyMock.expect(kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    kafkaCruiseControl.sanityCheckBrokerPresence(Collections.singleton(DEMOTED_BROKER_ID));
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.eq(false), EasyMock.anyObject()))
            .andReturn(clusterModel);
    EasyMock.expect(kafkaCruiseControl.executorState())
            .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet()));
    EasyMock.expect(kafkaCruiseControl.excludedTopics(EasyMock.eq(clusterModel), EasyMock.eq(requestedExcludedTopics)))
            .andReturn(resolvedExcludedTopics);
    OptimizerResult optimizerResult = EasyMock.niceMock(OptimizerResult.class);
    // Apply the goals of the demotion to the cluster model.
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.eq(clusterModel), EasyMock.anyObject(), EasyMock.anyObject(),
                                                     EasyMock.isNull(), EasyMock.anyObject()))
            .andAnswer(() -> {
              List<Goal> goals = EasyMock.getCurrentArgument(1);
              OptimizationOptions optimizationOptions = EasyMock.getCurrentArgument(4);
              assertEquals(resolvedExcludedTopics, optimizationOptions.excludedTopics());
              for (Goal goal : goals) {
                goal.optimize(clusterModel, Collections.emptySet(), optimizationOptions);
              }
              return optimizerResult;
            });
    EasyMock.replay(parameters, kafkaCruiseControl, optimizerResult);

    DemoteBrokerRunnable runnable = new DemoteBrokerRunnable(kafkaCruiseControl, new OperationFuture("Demote"), "uuid", parameters);
    assertSame(optimizerResult, runnable.computeResult());

    EasyMock.verify(parameters, kafkaCruiseControl, optimizerResult);
    return clusterModel;
  }

  /**
   * @param partitionsWithLaggingFollower Partitions whose follower on {@link #FOLLOWER_BROKER_ID} is not in sync.
   * @return The Kafka cluster of {@link #clusterModel()}: T1-0 and T2-0 led by {@link #DEMOTED_BROKER_ID}, with a follower
   * on {@link #FOLLOWER_BROKER_ID}.
   */
  private static Cluster cluster(Set<TopicPartition> partitionsWithLaggingFollower) {
    Node leader = new Node(DEMOTED_BROKER_ID, "h" + DEMOTED_BROKER_ID, 100);
    Node follower = new Node(FOLLOWER_BROKER_ID, "h" + FOLLOWER_BROKER_ID, 100);
    List<PartitionInfo> partitions = new ArrayList<>();
    for (TopicPartition tp : List.of(T1P0, T2P0)) {
      Node[] isr = partitionsWithLaggingFollower.contains(tp) ? new Node[]{leader} : new Node[]{leader, follower};
      partitions.add(new PartitionInfo(tp.topic(), tp.partition(), leader, new Node[]{leader, follower}, isr));
    }
    return new Cluster("id", List.of(leader, follower), partitions, Collections.emptySet(), Collections.emptySet());
  }

  /**
   * @return {@link DeterministicCluster#unbalanced()} cluster, in which {@link #DEMOTED_BROKER_ID} leads both T1-0 and T2-0,
   * with an additional follower for both partitions on {@link #FOLLOWER_BROKER_ID}.
   */
  private static ClusterModel clusterModel() {
    ClusterModel clusterModel = DeterministicCluster.unbalanced();
    String rack = RACK_BY_BROKER.get(FOLLOWER_BROKER_ID).toString();
    AggregatedMetricValues followerLoad = getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 8,
                                                                    TestConstants.LARGE_BROKER_CAPACITY / 2,
                                                                    0.0,
                                                                    TestConstants.LARGE_BROKER_CAPACITY / 2);
    for (TopicPartition tp : List.of(T1P0, T2P0)) {
      clusterModel.createReplica(rack, FOLLOWER_BROKER_ID, tp, 1, false);
      clusterModel.setReplicaLoad(rack, FOLLOWER_BROKER_ID, tp, followerLoad, Collections.singletonList(1L));
    }
    return clusterModel;
  }
}
