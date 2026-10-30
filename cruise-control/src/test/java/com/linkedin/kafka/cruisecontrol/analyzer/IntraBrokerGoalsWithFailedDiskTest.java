/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the intra-broker goals on a broker with a failed disk, with the cluster model built in the order of
 * {@link com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor}: the failed disk is marked dead before the replicas are
 * created, so its offline replicas are on no disk, but still count in the load of the broker.
 * With two 1000 MB disks, 900 MB of offline replicas on the failed disk and 500 MB on the other disk, the broker load
 * (1400 MB) is above its alive disk capacity (1000 MB) times the capacity threshold (0.8), while the load of its alive disk
 * (500 MB) is not.
 */
public class IntraBrokerGoalsWithFailedDiskTest {
  private static final String RACK = "r0";
  private static final int BROKER_WITH_FAILED_DISK = 0;
  private static final int OTHER_BROKER = 1;
  private static final String LOGDIR0 = "/d0";
  private static final String LOGDIR1 = "/d1";
  private static final String LOGDIR2 = "/d2";
  private static final double DISK_CAPACITY = 1000.0;
  private static final double OFFLINE_REPLICA_DISK_LOAD = 900.0;
  private static final double ONLINE_REPLICA_DISK_LOAD = 500.0;
  private static final TopicPartition OFFLINE_TP = new TopicPartition("offline", 0);
  private static final TopicPartition ONLINE_TP = new TopicPartition("online", 0);
  private static final List<Long> WINDOWS = Collections.singletonList(1L);
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  @Test
  public void testRebalanceDiskWithFailedDisk() throws KafkaCruiseControlException {
    ClusterModel clusterModel = clusterModel(Arrays.asList(LOGDIR0, LOGDIR1), LOGDIR1, LOGDIR0);
    assertEquals(DISK_CAPACITY, clusterModel.broker(BROKER_WITH_FAILED_DISK).capacityFor(Resource.DISK), 0.0);

    // No disk is above its capacity limit, and the failed disk cannot take replicas.
    assertTrue(optimize(clusterModel, rebalanceDiskGoals()).isEmpty());
  }

  @Test
  public void testRebalanceDiskWithFailedDiskMissingFromCapacityConfig() throws KafkaCruiseControlException {
    // The failed logdir has no capacity in the capacity config, so it is added as a dead disk.
    ClusterModel clusterModel = clusterModel(Collections.singletonList(LOGDIR0), LOGDIR1, LOGDIR0);
    assertEquals(Disk.State.DEAD, clusterModel.broker(BROKER_WITH_FAILED_DISK).disk(LOGDIR1).state());
    assertEquals(DISK_CAPACITY, clusterModel.broker(BROKER_WITH_FAILED_DISK).capacityFor(Resource.DISK), 0.0);

    assertTrue(optimize(clusterModel, rebalanceDiskGoals()).isEmpty());
  }

  @Test
  public void testRemoveDisksWithFailedDisk() throws KafkaCruiseControlException {
    ClusterModel clusterModel = clusterModel(Arrays.asList(LOGDIR0, LOGDIR1, LOGDIR2), LOGDIR2, LOGDIR0);
    clusterModel.broker(BROKER_WITH_FAILED_DISK).disk(LOGDIR0).markDiskForRemoval();

    Goal intraBrokerDiskCapacityGoal = new IntraBrokerDiskCapacityGoal(true);
    intraBrokerDiskCapacityGoal.configure(CONFIG.mergedConfigValues());
    Set<ExecutionProposal> proposals = optimize(clusterModel, Collections.singletonList(intraBrokerDiskCapacityGoal));

    // The replica on the removed disk moves to the other alive disk.
    assertEquals(1, proposals.size());
    ExecutionProposal proposal = proposals.iterator().next();
    assertEquals(ONLINE_TP, proposal.topicPartition());
    assertEquals(1, proposal.replicasToMoveBetweenDisksByBroker().size());
    assertEquals(LOGDIR1, proposal.replicasToMoveBetweenDisksByBroker().get(BROKER_WITH_FAILED_DISK).logdir());
  }

  /**
   * Build a cluster model in the order of {@link com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor}:
   * <ol>
   *   <li>Create the brokers with the disks of the capacity config.</li>
   *   <li>Mark the failed disk dead, or add it as a dead disk if it is not in the capacity config.</li>
   *   <li>Create the replicas: an offline replica on no disk for the failed disk, and an online replica on the given disk.</li>
   *   <li>Set the replica loads, and the state of the broker with a failed disk.</li>
   * </ol>
   * The other replica of each partition is on the single disk of the other broker, which has enough capacity for them.
   *
   * @param logdirs Logdirs of the capacity config of the broker with a failed disk.
   * @param failedLogdir Failed logdir of the broker.
   * @param onlineReplicaLogdir Logdir of the online replica of the broker.
   * @return The cluster model.
   */
  private static ClusterModel clusterModel(List<String> logdirs, String failedLogdir, String onlineReplicaLogdir) {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    clusterModel.createRack(RACK);
    createBroker(clusterModel, BROKER_WITH_FAILED_DISK, logdirs, DISK_CAPACITY);
    createBroker(clusterModel, OTHER_BROKER, Collections.singletonList(LOGDIR0), 4 * DISK_CAPACITY);

    Broker broker = clusterModel.broker(BROKER_WITH_FAILED_DISK);
    if (broker.disk(failedLogdir) == null) {
      broker.addDeadDisk(failedLogdir);
    } else {
      clusterModel.markDiskDead(BROKER_WITH_FAILED_DISK, failedLogdir);
    }

    createReplica(clusterModel, OTHER_BROKER, OFFLINE_TP, 0, true, false, LOGDIR0, OFFLINE_REPLICA_DISK_LOAD);
    createReplica(clusterModel, BROKER_WITH_FAILED_DISK, OFFLINE_TP, 1, false, true, null, OFFLINE_REPLICA_DISK_LOAD);
    createReplica(clusterModel, BROKER_WITH_FAILED_DISK, ONLINE_TP, 0, true, false, onlineReplicaLogdir, ONLINE_REPLICA_DISK_LOAD);
    createReplica(clusterModel, OTHER_BROKER, ONLINE_TP, 1, false, false, LOGDIR0, ONLINE_REPLICA_DISK_LOAD);

    clusterModel.setBrokerState(BROKER_WITH_FAILED_DISK, Broker.State.BAD_DISKS);
    return clusterModel;
  }

  private static void createBroker(ClusterModel clusterModel, int brokerId, List<String> logdirs, double diskCapacity) {
    Map<String, Double> diskCapacityByLogdir = new HashMap<>();
    logdirs.forEach(logdir -> diskCapacityByLogdir.put(logdir, diskCapacity));
    Map<Resource, Double> capacity = Map.of(Resource.CPU, 100.0, Resource.DISK, diskCapacity * logdirs.size(),
                                            Resource.NW_IN, 10000.0, Resource.NW_OUT, 10000.0);
    clusterModel.createBroker(RACK, "h" + brokerId, brokerId, new BrokerCapacityInfo(capacity, diskCapacityByLogdir), true);
  }

  private static void createReplica(ClusterModel clusterModel,
                                    int brokerId,
                                    TopicPartition tp,
                                    int index,
                                    boolean isLeader,
                                    boolean isOffline,
                                    String logdir,
                                    double diskLoad) {
    clusterModel.createReplica(RACK, brokerId, tp, index, isLeader, isOffline, logdir, false);
    clusterModel.setReplicaLoad(RACK, brokerId, tp, getAggregatedMetricValues(1.0, 10.0, isLeader ? 10.0 : 0.0, diskLoad), WINDOWS);
  }

  private static List<Goal> rebalanceDiskGoals() {
    List<Goal> goals = Arrays.asList(new IntraBrokerDiskCapacityGoal(), new IntraBrokerDiskUsageDistributionGoal());
    goals.forEach(goal -> goal.configure(CONFIG.mergedConfigValues()));
    return goals;
  }

  private static Set<ExecutionProposal> optimize(ClusterModel clusterModel, List<Goal> goals) throws KafkaCruiseControlException {
    GoalOptimizer goalOptimizer = new GoalOptimizer(CONFIG, null, Time.SYSTEM, new MetricRegistry(),
                                                    EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    return goalOptimizer.optimizations(clusterModel, goals, new OperationProgress()).goalProposals();
  }
}
