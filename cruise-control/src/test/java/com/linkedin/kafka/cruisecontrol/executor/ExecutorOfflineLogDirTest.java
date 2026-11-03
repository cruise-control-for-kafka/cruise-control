/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataAdminClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.MockTime;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.RANDOM_UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for refusing replica movements between the disks of a broker that has an offline logdir.
 */
public class ExecutorOfflineLogDirTest {
  private static final String LOGDIR_1 = "/tmp/kafka-logs-1";
  private static final String LOGDIR_2 = "/tmp/kafka-logs-2";

  @Test
  public void testRefuseIntraBrokerMovementsOnBrokerWithOfflineLogDir() throws Exception {
    // Brokers 0 and 1 have replica movements between their disks; broker 0 has an offline logdir. Broker 2 only has an
    // inter-broker replica movement, so its logdirs are not checked.
    List<ExecutionProposal> proposals = List.of(
        new ExecutionProposal(new TopicPartition("t0", 0), 0, new ReplicaPlacementInfo(0, LOGDIR_1),
                              List.of(new ReplicaPlacementInfo(0, LOGDIR_1)), List.of(new ReplicaPlacementInfo(0, LOGDIR_2))),
        new ExecutionProposal(new TopicPartition("t0", 1), 0, new ReplicaPlacementInfo(1, LOGDIR_2),
                              List.of(new ReplicaPlacementInfo(1, LOGDIR_2)), List.of(new ReplicaPlacementInfo(1, LOGDIR_1))),
        new ExecutionProposal(new TopicPartition("t1", 0), 0, new ReplicaPlacementInfo(2),
                              List.of(new ReplicaPlacementInfo(2)), List.of(new ReplicaPlacementInfo(3))));

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    DescribeLogDirsResult mockDescribeLogDirsResult = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(mockAdminClient.describeLogDirs(Set.of(0, 1))).andReturn(mockDescribeLogDirsResult);
    EasyMock.expect(mockDescribeLogDirsResult.descriptions()).andReturn(Map.of(
        0, KafkaFuture.completedFuture(Map.of(LOGDIR_1, new LogDirDescription(null, Map.of()),
                                              LOGDIR_2, new LogDirDescription(new KafkaStorageException("disk failed"), Map.of()))),
        1, KafkaFuture.completedFuture(Map.of(LOGDIR_1, new LogDirDescription(null, Map.of()),
                                              LOGDIR_2, new LogDirDescription(null, Map.of())))));
    mockAdminClient.close();
    EasyMock.replay(mockAdminClient, mockDescribeLogDirsResult);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    Executor executor = new Executor(config, new MockTime(), new MetricRegistry(), mockAdminClient,
                                     EasyMock.mock(MetadataAdminClient.class), null, EasyMock.mock(AnomalyDetectorManager.class));
    executor.setGeneratingProposalsForExecution(RANDOM_UUID, ExecutorOfflineLogDirTest.class::getSimpleName, true);
    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> execute(executor, proposals));
    assertTrue(exception.getMessage(), exception.getMessage().contains(String.format("brokers [0]: they have offline logdirs {0=[%s]}",
                                                                                     LOGDIR_2)));
    assertTrue(exception.getMessage(), exception.getMessage().contains("destination_broker_ids"));

    // The executor is released, so a new execution can be requested.
    assertFalse(executor.hasOngoingExecution());
    assertEquals(ExecutorState.State.NO_TASK_IN_PROGRESS, executor.state().state());
    executor.setGeneratingProposalsForExecution(RANDOM_UUID, ExecutorOfflineLogDirTest.class::getSimpleName, true);
    executor.failGeneratingProposalsForExecution(RANDOM_UUID);
    executor.shutdown();
    EasyMock.verify(mockAdminClient, mockDescribeLogDirsResult);
  }

  private static void execute(Executor executor, Collection<ExecutionProposal> proposals) throws Exception {
    executor.executeProposals(proposals, Set.of(), null, EasyMock.mock(LoadMonitor.class), null, null, null, null, null, null, null,
                              null, true, RANDOM_UUID, false, false, false);
  }
}
