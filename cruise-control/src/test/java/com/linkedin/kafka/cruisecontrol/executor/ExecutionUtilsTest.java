/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.PreferredLeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ExecutionUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionUtilsTest.class);
  private static final String TOPIC_NAME = "topic-name";
  private static final TopicPartition P0 = new TopicPartition(TOPIC_NAME, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC_NAME, 1);
  private static final TopicPartition P2 = new TopicPartition(TOPIC_NAME, 2);
  // Both partitions are successful
  private static final Map<TopicPartition, Optional<Throwable>> SUCCESSFUL_PARTITIONS = Map.of(P0, Optional.empty(), P1, Optional.empty());
  // One partition successful, the other has UnknownTopicOrPartitionException
  private static final Map<TopicPartition, Optional<Throwable>> ONE_WITH_UTOP = Map.of(P0, Optional.empty(), P1,
                                                                                       Optional.of(new UnknownTopicOrPartitionException()));
  // One partition successful, the other has InvalidTopicException
  private static final Map<TopicPartition, Optional<Throwable>> ONE_WITH_IT = Map.of(P0, Optional.empty(), P1,
                                                                                     Optional.of(new InvalidTopicException()));
  // One partition successful, the other has ElectionNotNeededException
  private static final Map<TopicPartition, Optional<Throwable>> ONE_WITH_ENN = Map.of(P0, Optional.empty(), P1,
                                                                                      Optional.of(new ElectionNotNeededException("")));
  // One partition successful, the other has PreferredLeaderNotAvailableException
  private static final Map<TopicPartition, Optional<Throwable>> ONE_WITH_PLNA = Map.of(P0, Optional.empty(), P1,
                                                                                       Optional.of(new PreferredLeaderNotAvailableException("")));
  // One partition successful, the other has NotControllerException
  private static final Map<TopicPartition, Optional<Throwable>> ONE_WITH_NC = Map.of(P0, Optional.empty(), P1,
                                                                                     Optional.of(new NotControllerException("")));

  @Test
  public void testProcessAlterPartitionReassignmentResult() throws Exception {
    // Case 1: Handle null result input. Expect no side effect.
    ExecutionUtils.processAlterPartitionReassignmentsResult(null, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

    // Case 2: Handle successful execution results
    AlterPartitionReassignmentsResult result = EasyMock.mock(AlterPartitionReassignmentsResult.class);
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(null))
            .once();
    EasyMock.replay(result);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 3: Handle invalid replica assignment exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(new ExecutionException(Errors.INVALID_REPLICA_ASSIGNMENT.exception())))
            .once();
    EasyMock.replay(result);
    Set<TopicPartition> deadTopicPartitions = new HashSet<>();
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), deadTopicPartitions, Collections.emptySet());
    assertEquals(Collections.singleton(new TopicPartition(TOPIC_NAME, P0.partition())), deadTopicPartitions);
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 4: Handle unknown topic or partition exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(new ExecutionException(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception())))
            .once();
    EasyMock.replay(result);
    Set<TopicPartition> deletedTopicPartitions = new HashSet<>();
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, deletedTopicPartitions, Collections.emptySet(), Collections.emptySet());
    assertEquals(Collections.singleton(new TopicPartition(TOPIC_NAME, P0.partition())), deletedTopicPartitions);
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 5: Handle no reassign in progress exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(new ExecutionException(Errors.NO_REASSIGNMENT_IN_PROGRESS.exception())))
            .once();
    EasyMock.replay(result);
    Set<TopicPartition> noReassignmentToCancelTopicPartitions = new HashSet<>();
    ExecutionUtils.processAlterPartitionReassignmentsResult(result,
                                                            Collections.emptySet(),
                                                            Collections.emptySet(),
                                                            noReassignmentToCancelTopicPartitions);
    assertEquals(Collections.singleton(new TopicPartition(TOPIC_NAME, P0.partition())), noReassignmentToCancelTopicPartitions);
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 6: Handle execution timeout exception. Expect no side effect.
    org.apache.kafka.common.errors.TimeoutException kafkaTimeoutException = new org.apache.kafka.common.errors.TimeoutException();
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(new ExecutionException(kafkaTimeoutException)))
            .times(2);
    EasyMock.replay(result);
    Exception thrownException = assertThrows(IllegalStateException.class, () ->
      ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet())
    );
    Assert.assertEquals(kafkaTimeoutException, thrownException.getCause());
    EasyMock.verify(result);
    EasyMock.reset(result);

    // Case 7: Handle future wait interrupted exception
    EasyMock.expect(result.values())
            .andReturn(getKafkaFutureByTopicPartition(new InterruptedException()))
            .times(2);
    EasyMock.replay(result);
    ExecutionUtils.processAlterPartitionReassignmentsResult(result, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    EasyMock.verify(result);
    EasyMock.reset(result);
  }

  @Test
  public void testGetInterBrokerReplicaTasksToReexecute() {
    Set<TopicPartition> partitionsInMovement = Set.of(P0, P1);
    ReplicaPlacementInfo replicaPlacementInfoPlaceHolder = new ReplicaPlacementInfo(BROKER_ID_PLACEHOLDER);

    ExecutionProposal proposalForPartition0 =
        new ExecutionProposal(P0, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
                              Collections.singletonList(replicaPlacementInfoPlaceHolder),
                              Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal proposalForPartition1 =
        new ExecutionProposal(P1, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
                              Collections.singletonList(replicaPlacementInfoPlaceHolder),
                              Collections.singletonList(replicaPlacementInfoPlaceHolder));
    ExecutionProposal proposalForPartition2 =
        new ExecutionProposal(P2, PRODUCE_SIZE_IN_BYTES, replicaPlacementInfoPlaceHolder,
                              Collections.singletonList(replicaPlacementInfoPlaceHolder),
                              Collections.singletonList(replicaPlacementInfoPlaceHolder));

    ExecutionTask executionTaskForPartition0 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, proposalForPartition0,
                                                                 ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask executionTaskForPartition1 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, proposalForPartition1,
                                                                 ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    ExecutionTask executionTaskForPartition2 = new ExecutionTask(EXECUTION_ID_PLACEHOLDER, proposalForPartition2,
                                                                 ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);

    // Case 1: all partitions in candidate tasks are already in movement. Expect nothing to re-execute
    List<ExecutionTask> tasks1 = List.of(executionTaskForPartition0, executionTaskForPartition1);
    List<ExecutionTask> tasksToReexecute1 = ExecutionUtils.getInterBrokerReplicaTasksToReexecute(partitionsInMovement, tasks1);
    Assert.assertTrue(tasksToReexecute1.isEmpty());

    // Case 2: some of partitions in candidate tasks are in movement and some are not. Expect some tasks to re-execute
    List<ExecutionTask> tasks2 = List.of(executionTaskForPartition0, executionTaskForPartition1, executionTaskForPartition2);
    List<ExecutionTask> tasksToReexecute2 = ExecutionUtils.getInterBrokerReplicaTasksToReexecute(partitionsInMovement, tasks2);
    Assert.assertEquals(1, tasksToReexecute2.size());
    Assert.assertEquals(P2, tasksToReexecute2.get(0).proposal().topicPartition());

    // Case 3: Partitions of candidate tasks is subset of partitions in movement. Expect nothing to re-execute
    List<ExecutionTask> tasks3 = List.of(executionTaskForPartition0);
    List<ExecutionTask> tasksToReexecute3 = ExecutionUtils.getInterBrokerReplicaTasksToReexecute(partitionsInMovement, tasks3);
    Assert.assertTrue(tasksToReexecute3.isEmpty());
  }

  @Test
  public void testProcessElectLeadersResult() throws Exception {
    // Case 1: Handle null result input. Expect no side effect.
    ExecutionUtils.processElectLeadersResult(null, Collections.emptySet());

    KafkaFutureImpl<Map<TopicPartition, Optional<Throwable>>> partitions = EasyMock.mock(KafkaFutureImpl.class);
    Constructor<ElectLeadersResult> constructor = null;
    try {
      constructor = ElectLeadersResult.class.getDeclaredConstructor(KafkaFuture.class);
    } catch (NoSuchMethodException e) {
      LOG.debug("Unable to find Kafka 3.0+ constructor for ElectLeaderResult class", e);
    }
    if (constructor == null) {
      try {
        constructor = ElectLeadersResult.class.getDeclaredConstructor(KafkaFutureImpl.class);
      } catch (NoSuchMethodException e) {
        LOG.debug("Unable to find Kafka 3.0- constructor for ElectLeaderResult class", e);
      }
    }
    if (constructor == null) {
      throw new NoSuchElementException("Unable to find viable constructor for the ElectionLeadersResult class");
    }
    constructor.setAccessible(true);
    ElectLeadersResult result;

    // Case 2: Handle both partitions are successful
    // Case 3: Handle one partition successful, the other has ElectionNotNeededException
    // Case 4: Handle one partition successful, the other has PreferredLeaderNotAvailableException
    // Case 5: Handle one partition successful, the other has NotControllerException
    for (Map<TopicPartition, Optional<Throwable>> entry : Set.of(SUCCESSFUL_PARTITIONS, ONE_WITH_ENN, ONE_WITH_PLNA, ONE_WITH_NC)) {
      result = constructor.newInstance(partitions);

      EasyMock.expect(partitions.get()).andReturn(entry).once();
      EasyMock.replay(partitions);

      ExecutionUtils.processElectLeadersResult(result, Collections.emptySet());
      EasyMock.verify(partitions);
      EasyMock.reset(partitions);
    }

    // Case 6: Handle one partition successful, the other has UnknownTopicOrPartitionException
    // Case 7: Handle one partition successful, the other has InvalidTopicException
    for (Map<TopicPartition, Optional<Throwable>> entry : Set.of(ONE_WITH_UTOP, ONE_WITH_IT)) {
      result = constructor.newInstance(partitions);

      EasyMock.expect(partitions.get()).andReturn(entry).once();
      EasyMock.replay(partitions);

      Set<TopicPartition> deletedTopicPartitions = new HashSet<>();
      ExecutionUtils.processElectLeadersResult(result, deletedTopicPartitions);
      Assert.assertEquals(1, deletedTopicPartitions.size());
      Assert.assertEquals(P1, deletedTopicPartitions.iterator().next());
      EasyMock.verify(partitions);
      EasyMock.reset(partitions);
    }

    // Case 8: Handle execution timeout exception. Expect no side effect.
    // Case 9: Handle execution ClusterAuthorization exception. Expect no side effect.
    // Case 10: Handle unexpected execution exception (i.e. ControllerMovedException). Expect no side effect.
    for (Throwable entry : Set.of(new org.apache.kafka.common.errors.TimeoutException(),
                                  new org.apache.kafka.common.errors.ClusterAuthorizationException(""),
                                  new org.apache.kafka.common.errors.ControllerMovedException(""))) {
      result = constructor.newInstance(partitions);

      EasyMock.expect(partitions.get()).andThrow(new ExecutionException(entry)).once();
      EasyMock.replay(partitions);

      ElectLeadersResult exceptionResult = result;
      Exception thrownException = assertThrows(IllegalStateException.class,
                                               () -> ExecutionUtils.processElectLeadersResult(exceptionResult, Collections.emptySet()));
      Assert.assertEquals(entry, thrownException.getCause());
      EasyMock.verify(partitions);
      EasyMock.reset(partitions);
    }

    // Case 11: Handle future wait interrupted exception
    result = constructor.newInstance(partitions);

    EasyMock.expect(partitions.get()).andThrow(new InterruptedException()).once();
    EasyMock.replay(partitions);

    ExecutionUtils.processElectLeadersResult(result, Collections.emptySet());
    EasyMock.verify(partitions);
    EasyMock.reset(partitions);
  }

  private static Map<TopicPartition, KafkaFuture<Void>> getKafkaFutureByTopicPartition(@Nullable Exception futureException) throws Exception {
    Map<TopicPartition, KafkaFuture<Void>> futureByTopicPartition = new HashMap<>();
    KafkaFuture<Void> kafkaFuture = EasyMock.mock(KafkaFuture.class);
    if (futureException == null) {
      EasyMock.expect(kafkaFuture.get()).andReturn(null).once();
    } else {
      EasyMock.expect(kafkaFuture.get()).andThrow(futureException).once();
    }
    EasyMock.replay(kafkaFuture);
    futureByTopicPartition.put(new TopicPartition(TOPIC_NAME, P0.partition()), kafkaFuture);
    return futureByTopicPartition;
  }
}
