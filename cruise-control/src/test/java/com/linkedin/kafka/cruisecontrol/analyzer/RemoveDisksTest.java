/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RemoveDisksTest {
    private static final double DELTA = 1e-6;
    private final Goal _goalToTest;
    private final ClusterModel _clusterModel;
    private final boolean _expectedToOptimize;
    private final KafkaCruiseControlConfig _kafkaCruiseControlConfig;

    public RemoveDisksTest(Goal goal, ClusterModel clusterModel, KafkaCruiseControlConfig config, boolean expectedToOptimize) {
        _goalToTest = goal;
        _clusterModel = clusterModel;
        _expectedToOptimize = expectedToOptimize;
        _kafkaCruiseControlConfig = config;
    }

    /**
     * Populate parameters for the {@link OptimizationVerifier}.
     *
     * @return Parameters for the {@link OptimizationVerifier}.
     */
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> buildParameters() {
        List<Object[]> parameters = new ArrayList<>();

        final Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        final KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
        final BalancingConstraint balancingConstraint = new BalancingConstraint(kafkaCruiseControlConfig);

        // running 3 tests: one for medium utilization, one for 0 utilization, and one for max utilization.
        // max utilization should not be moved by the intra-broker disk capacity goal.
        List<Double> replicaLoadList = Arrays.asList(
                TestConstants.LARGE_BROKER_CAPACITY / 4,
                0.0,
                TestConstants.LARGE_BROKER_CAPACITY / 2 * balancingConstraint.capacityThreshold(Resource.DISK)
        );
        for (Double replicaLoad : replicaLoadList) {
            final Map<Integer, String> brokerToRack = Map.of(0, "A::0",
                    1, "B::0",
                    2, "C::1");
            final ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
            BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY);
            brokerToRack.values().stream().distinct().forEach(cluster::createRack);
            brokerToRack.forEach((broker, rack) -> cluster.createBroker(
                    rack, Integer.toString(broker), broker,
                    commonBrokerCapacityInfo,
                    commonBrokerCapacityInfo.diskCapacityByLogDir() != null
            ));

            // populate the cluster with a set of replicas
            TopicPartition tp = new TopicPartition("topic", 0);
            cluster.createReplica(brokerToRack.get(0), 0, tp, 0, true, false, TestConstants.LOGDIR0, false);

            // create snapshots and push them to the cluster.
            List<Long> windows = Collections.singletonList(1L);
            cluster.setReplicaLoad(brokerToRack.get(0), 0, tp, getAggregatedMetricValues(40.0, 100.0, 130.0, replicaLoad), windows);

            parameters.add(new Object[]{
                    new IntraBrokerDiskCapacityGoal(true),
                    cluster,
                    kafkaCruiseControlConfig,
                    Math.abs(replicaLoad - TestConstants.LARGE_BROKER_CAPACITY / 2 * balancingConstraint.capacityThreshold(Resource.DISK)) > .0000001
            });
        }

        return parameters;
    }

    @Test
    public void testRemoveDisks() throws KafkaCruiseControlException {
        Broker broker = _clusterModel.broker(0);
        double removedDiskCapacity = broker.disk(TestConstants.LOGDIR0).capacity();
        double brokerDiskCapacity = broker.capacityFor(Resource.DISK);
        double hostDiskCapacity = broker.host().capacityFor(Resource.DISK);
        double rackDiskCapacity = broker.rack().capacityFor(Resource.DISK);
        double clusterDiskCapacity = _clusterModel.capacityFor(Resource.DISK);

        // mark disk 0 of broker 0 for removal -- marking it again is a no-op.
        _clusterModel.markDiskForRemoval(0, TestConstants.LOGDIR0);
        _clusterModel.markDiskForRemoval(0, TestConstants.LOGDIR0);

        // The capacity of the disk is dropped from the capacity of its broker, host, rack and cluster.
        assertTrue(broker.disk(TestConstants.LOGDIR0).isMarkedForRemoval());
        assertEquals(0.0, broker.disk(TestConstants.LOGDIR0).capacity(), DELTA);
        assertEquals(brokerDiskCapacity - removedDiskCapacity, broker.capacityFor(Resource.DISK), DELTA);
        assertEquals(hostDiskCapacity - removedDiskCapacity, broker.host().capacityFor(Resource.DISK), DELTA);
        assertEquals(rackDiskCapacity - removedDiskCapacity, broker.rack().capacityFor(Resource.DISK), DELTA);
        assertEquals(clusterDiskCapacity - removedDiskCapacity, _clusterModel.capacityFor(Resource.DISK), DELTA);

        List<Goal> goalsByPriority = Collections.singletonList(_goalToTest);
        _goalToTest.configure(_kafkaCruiseControlConfig.mergedConfigValues());
        GoalOptimizer goalOptimizer = new GoalOptimizer(_kafkaCruiseControlConfig,
                null,
                Time.SYSTEM,
                new MetricRegistry(),
                EasyMock.mock(Executor.class),
                EasyMock.mock(Admin.class));

        if (_expectedToOptimize) {
            final Set<ExecutionProposal> proposals =
                    goalOptimizer.optimizations(_clusterModel, goalsByPriority, new OperationProgress()).goalProposals();
            assertEquals(1, proposals.size());
            assertEquals(1, proposals.iterator().next().replicasToMoveBetweenDisksByBroker().size());
            assertEquals(0, proposals.iterator().next().replicasToMoveBetweenDisksByBroker().get(0).brokerId().intValue());
            assertEquals(TestConstants.LOGDIR1, proposals.iterator().next().replicasToMoveBetweenDisksByBroker().get(0).logdir());
        } else {
            assertThrows(OptimizationFailureException.class,
                    () -> goalOptimizer.optimizations(_clusterModel, goalsByPriority, new OperationProgress()));
        }
    }
}
