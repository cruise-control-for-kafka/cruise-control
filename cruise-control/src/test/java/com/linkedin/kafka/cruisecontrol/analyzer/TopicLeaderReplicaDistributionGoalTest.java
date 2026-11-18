/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicLeaderReplicaDistributionGoalTest {

    private ClusterModel makeSimpleClusterModel(int numBrokers, BiFunction<Integer, Integer, Integer> leaderReplicaAssigner) {
        ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
        BrokerCapacityInfo brokerCapacity = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, "");
        Map<String, AtomicInteger> topicPartitionCnt = new HashMap<>();
        Function<Integer, String> rackIdGetter = bidx -> "rack" + (bidx % 3);
        for (int bidx = 0; bidx < numBrokers; bidx++) {
            String rackId = rackIdGetter.apply(bidx);
            clusterModel.createRack(rackId);
            clusterModel.createBroker(rackId, "broker" + bidx, bidx, brokerCapacity, false);
        }
        for (int bidx = 0; bidx < numBrokers; bidx++) {
            Broker broker = clusterModel.broker(bidx);
            for (int tidx = 0; tidx < 2; tidx++) {
                String topic = "T" + tidx;
                final int numPartitions = leaderReplicaAssigner.apply(bidx, tidx);
                for (int pidx = 0; pidx < numPartitions; pidx++) {
                    int offset = topicPartitionCnt.computeIfAbsent(topic, k -> new AtomicInteger()).getAndIncrement();
                    TopicPartition tp = new TopicPartition(topic, offset);
                    clusterModel.createReplica(broker.rack().id(), broker.id(), tp, 0, true);
                    AggregatedMetricValues aggregatedMetricValues = getAggregatedMetricValues(1.0, 10.0, 13.0, 5.0);
                    clusterModel.setReplicaLoad(broker.rack().id(), broker.id(), tp, aggregatedMetricValues, Collections.singletonList(3L));
                    final int nextBrokerId = (broker.id() + 1) % numBrokers;
                    final String followerRackId = rackIdGetter.apply(nextBrokerId);
                    clusterModel.createReplica(followerRackId, nextBrokerId, tp, 1, false);
                    clusterModel.setReplicaLoad(followerRackId, nextBrokerId, tp, aggregatedMetricValues, Collections.singletonList(3L));
                }
            }
        }
        return clusterModel;
    }

    @Test
    public void testGoalNoopOnSatisfiable() throws Exception {
        final ClusterModel clusterModel = makeSimpleClusterModel(6, (bidx, tid) -> 2);
        final OptimizerResult result = getOptimizerResult(clusterModel);
        assertTrue(result.violatedGoalsBeforeOptimization().isEmpty());
        assertTrue(result.violatedGoalsAfterOptimization().isEmpty());
        for (Broker b : clusterModel.brokers()) {
            assertEquals(4, b.leaderReplicas().size());
        }
    }

    @Test
    public void testGoalLinearLeaderGrowth() throws Exception {
        final ClusterModel clusterModel = makeSimpleClusterModel(6, (bidx, tidx) -> 2 * bidx);
        final OptimizerResult result = getOptimizerResult(clusterModel);

        assertFalse(result.violatedGoalsBeforeOptimization().isEmpty());
        assertTrue(result.violatedGoalsAfterOptimization().isEmpty());
        for (Broker b : clusterModel.brokers()) {
            assertEquals(10, b.leaderReplicas().size());
        }
    }

    @Test
    public void testGoalPreferBrokerWithHigherTotalLeaderOnEquality() throws Exception {
        List<int[]> topicLeaderAssignment = new ArrayList<>();
        topicLeaderAssignment.add(new int[]{6, 6, 4});
        topicLeaderAssignment.add(new int[]{4, 5, 5});
        BiFunction<Integer, Integer, Integer> replicaAssigner = (brokerId, topicId) -> {
            if (topicId >= topicLeaderAssignment.size()) {
                return 5;
            } else if (brokerId >= topicLeaderAssignment.get(topicId).length) {
                return 5;
            } else {
                return topicLeaderAssignment.get(topicId)[brokerId];
            }
        };
        final ClusterModel clusterModel = makeSimpleClusterModel(6, replicaAssigner);
        final OptimizerResult result = getOptimizerResult(clusterModel);

        assertFalse(result.violatedGoalsBeforeOptimization().isEmpty());
        assertTrue(result.violatedGoalsAfterOptimization().isEmpty());
        for (Broker b : clusterModel.brokers()) {
            assertEquals(10, b.leaderReplicas().size());
        }
    }

    private static OptimizerResult getOptimizerResult(ClusterModel clusterModel) throws Exception {
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        props.setProperty(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, "0");
        props.setProperty(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG, "0");
        KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
        BalancingConstraint balancingConstraint = new BalancingConstraint(kafkaCruiseControlConfig);
        GoalOptimizer goalOptimizer = new GoalOptimizer(kafkaCruiseControlConfig,
                null,
                new SystemTime(),
                new MetricRegistry(),
                EasyMock.mock(Executor.class),
                EasyMock.mock(AdminClient.class));
        List<Goal> goals = Collections.singletonList(new TopicLeaderReplicaDistributionGoal(balancingConstraint));
        return goalOptimizer.optimizations(clusterModel, goals, new OperationProgress());
    }
}
