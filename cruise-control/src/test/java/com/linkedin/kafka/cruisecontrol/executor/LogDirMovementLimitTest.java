/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link LogDirMovementLimit}. Brokers 0, 1 and 2 have logdirs {@link #D1} and {@link #D2}. Broker 0 leads
 * {@link #A0} and {@link #A1} in {@link #D1}, and {@link #B0} in {@link #D2}.
 */
public class LogDirMovementLimitTest {
  private static final String D1 = "/d1";
  private static final String D2 = "/d2";
  private static final long GB = 1024L * 1024L * 1024L;
  private static final TopicPartition A0 = new TopicPartition("A", 0);
  private static final TopicPartition A1 = new TopicPartition("A", 1);
  private static final TopicPartition B0 = new TopicPartition("B", 0);
  private static final TopicPartition C0 = new TopicPartition("C", 0);
  private static final long ALERTING_THRESHOLD_MS = 1000L;
  private long _executionId = 0;

  @Test
  public void testSourceLogDirIsLimited() {
    LogDirMovementLimit limit = limit(1, defaultLogDirs());
    ExecutionTask a0To1 = interBrokerTask(A0, 0, 1);
    ExecutionTask a1To2 = interBrokerTask(A1, 0, 2);
    ExecutionTask b0To2 = interBrokerTask(B0, 0, 2);
    limit.refresh(null, List.of(), Set.of(0, 1, 2));

    assertTrue(limit.tryReserve(a0To1));
    // A1 is read from the same logdir of broker 0 as A0.
    assertFalse(limit.tryReserve(a1To2));
    // B0 is read from the other logdir of broker 0.
    assertTrue(limit.tryReserve(b0To2));

    // With a limit of 2, both movements out of D1 of broker 0 can start.
    LogDirMovementLimit limitOf2 = limit(2, defaultLogDirs());
    limitOf2.refresh(null, List.of(), Set.of(0, 1, 2));
    assertTrue(limitOf2.tryReserve(interBrokerTask(A0, 0, 1)));
    assertTrue(limitOf2.tryReserve(interBrokerTask(A1, 0, 2)));
  }

  @Test
  public void testDestinationLogDirIsPickedUnderTheLimit() {
    Map<Integer, Map<String, LogDirDescription>> logDirs = defaultLogDirs();
    // C0 is being created in D1 of broker 1 by a movement in progress.
    logDirs.get(1).put(D1, new LogDirDescription(null, Map.of(C0, new ReplicaInfo(10, 0, false)), 2 * GB, GB));
    LogDirMovementLimit limit = limit(1, logDirs);
    ExecutionTask c0To1 = interBrokerTask(C0, 2, 1);
    limit.refresh(null, List.of(c0To1), Set.of(0, 1, 2));

    ExecutionTask a0To1 = interBrokerTask(A0, 0, 1);
    assertTrue(limit.tryReserve(a0To1));
    assertEquals(Map.of(new TopicPartitionReplica("A", 0, 1), D2), limit.pickedLogDirs(List.of(a0To1)));
    // Both logdirs of broker 1 are now in use.
    assertFalse(limit.tryReserve(interBrokerTask(B0, 0, 1)));

    // The next check still counts the movement in progress to the logdir picked for it, until the replica exists.
    limit.refresh(null, List.of(c0To1, a0To1), Set.of(0, 1, 2));
    assertFalse(limit.tryReserve(interBrokerTask(B0, 0, 1)));
    assertEquals(Map.of(0, Map.of(D1, 1), 1, Map.of(D1, 1, D2, 1), 2, Map.of(D1, 1)), limit.numMovementsByLogDirByBroker());
  }

  @Test
  public void testDestinationLogDirWithRoomIsPreferred() {
    Map<Integer, Map<String, LogDirDescription>> logDirs = defaultLogDirs();
    // D1 of broker 2 has fewer replicas and the replica does not fit; D2 has more replicas and the replica fits.
    logDirs.get(2).put(D1, new LogDirDescription(null, Map.of(), 2 * GB, 100L));
    logDirs.get(2).put(D2, new LogDirDescription(null, Map.of(C0, new ReplicaInfo(10, 0, false)), 2 * GB, GB));
    LogDirMovementLimit limit = limit(1, logDirs);
    limit.refresh(null, List.of(), Set.of(0, 1, 2));
    ExecutionTask a0To2 = interBrokerTask(A0, 0, 2);
    assertTrue(limit.tryReserve(a0To2));
    assertEquals(Map.of(new TopicPartitionReplica("A", 0, 2), D2), limit.pickedLogDirs(List.of(a0To2)));
  }

  @Test
  public void testSourceIsTheCurrentLeader() {
    // Leadership of A0 moved to broker 1, whose D1 is busy with a movement in progress.
    Map<Integer, Map<String, LogDirDescription>> logDirs = defaultLogDirs();
    logDirs.get(1).put(D1, new LogDirDescription(null, Map.of(A0, new ReplicaInfo(1000, 0, false),
                                                              C0, new ReplicaInfo(10, 0, false)), 2 * GB, GB));
    LogDirMovementLimit limit = limit(1, logDirs);
    Node[] nodes = {new Node(0, "h0", 9092), new Node(1, "h1", 9092), new Node(2, "h2", 9092)};
    Cluster cluster = new Cluster("c", List.of(nodes), List.of(new PartitionInfo("A", 0, nodes[1], new Node[]{nodes[0], nodes[1]},
                                                                                 new Node[]{nodes[0], nodes[1]})), Set.of(), Set.of());
    limit.refresh(cluster, List.of(interBrokerTask(C0, 2, 1)), Set.of(0, 1, 2));
    assertFalse(limit.tryReserve(interBrokerTask(A0, 0, 2)));
  }

  @Test
  public void testIntraBrokerMovementsAreLimited() {
    LogDirMovementLimit limit = limit(1, defaultLogDirs());
    ExecutionTask a0ToD2 = intraBrokerTask(A0, 0, D1, D2);
    limit.refresh(null, List.of(a0ToD2), Set.of(0));
    // B0 moves from D2, which A0 moves to.
    assertFalse(limit.tryReserve(intraBrokerTask(B0, 0, D2, D1)));
    // Without the movement in progress, B0 can start, and then A1 cannot.
    limit.refresh(null, List.of(), Set.of(0));
    assertTrue(limit.tryReserve(intraBrokerTask(B0, 0, D2, D1)));
    assertFalse(limit.tryReserve(intraBrokerTask(A1, 0, D1, D2)));
  }

  @Test
  public void testBrokerWithUnknownLogDirsIsNotLimited() {
    LogDirMovementLimit limit = limit(1, defaultLogDirs());
    // Broker 1 is not described: its logdirs are not limited, and Kafka picks the logdir of the replica to add.
    limit.refresh(null, List.of(), Set.of(0));
    ExecutionTask a0To1 = interBrokerTask(A0, 0, 1);
    assertTrue(limit.tryReserve(a0To1));
    assertTrue(limit.pickedLogDirs(List.of(a0To1)).isEmpty());
  }

  private static Map<Integer, Map<String, LogDirDescription>> defaultLogDirs() {
    Map<Integer, Map<String, LogDirDescription>> logDirs = new HashMap<>();
    logDirs.put(0, new HashMap<>(Map.of(D1, new LogDirDescription(null, Map.of(A0, new ReplicaInfo(1000, 0, false),
                                                                               A1, new ReplicaInfo(1000, 0, false)), 2 * GB, GB),
                                        D2, new LogDirDescription(null, Map.of(B0, new ReplicaInfo(1000, 0, false)), 2 * GB, GB))));
    for (int broker : List.of(1, 2)) {
      logDirs.put(broker, new HashMap<>(Map.of(D1, new LogDirDescription(null, Map.of(), 2 * GB, GB),
                                               D2, new LogDirDescription(null, Map.of(), 2 * GB, GB))));
    }
    logDirs.get(2).put(D1, new LogDirDescription(null, Map.of(C0, new ReplicaInfo(10, 0, false)), 2 * GB, GB));
    return logDirs;
  }

  private static LogDirMovementLimit limit(int maxMovementsPerLogDir, Map<Integer, Map<String, LogDirDescription>> logDirs) {
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeLogDirs(EasyMock.anyObject())).andAnswer(() -> {
      @SuppressWarnings("unchecked")
      Set<Integer> brokers = (Set<Integer>) EasyMock.getCurrentArguments()[0];
      Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = new HashMap<>();
      brokers.forEach(b -> descriptions.put(b, KafkaFuture.completedFuture(logDirs.get(b))));
      DescribeLogDirsResult result = EasyMock.mock(DescribeLogDirsResult.class);
      EasyMock.expect(result.descriptions()).andReturn(descriptions);
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.replay(mockAdminClient);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    return new LogDirMovementLimit(mockAdminClient, config, maxMovementsPerLogDir);
  }

  private ExecutionTask interBrokerTask(TopicPartition tp, int oldLeader, int destination) {
    ExecutionProposal proposal = new ExecutionProposal(tp, 1, new ReplicaPlacementInfo(oldLeader), List.of(new ReplicaPlacementInfo(oldLeader)),
                                                       List.of(new ReplicaPlacementInfo(destination)));
    return new ExecutionTask(_executionId++, proposal, INTER_BROKER_REPLICA_ACTION, ALERTING_THRESHOLD_MS);
  }

  private ExecutionTask intraBrokerTask(TopicPartition tp, int broker, String from, String to) {
    ExecutionProposal proposal = new ExecutionProposal(tp, 1, new ReplicaPlacementInfo(broker, from),
                                                       List.of(new ReplicaPlacementInfo(broker, from)),
                                                       List.of(new ReplicaPlacementInfo(broker, to)));
    return new ExecutionTask(_executionId++, proposal, broker, INTRA_BROKER_REPLICA_ACTION, ALERTING_THRESHOLD_MS);
  }
}
