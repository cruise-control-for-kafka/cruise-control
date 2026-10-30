/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class PersistedExecutionStateSerdeTest {
  static final TopicPartition TP0 = new TopicPartition("topic", 0);
  static final TopicPartition TP1 = new TopicPartition("topic", 1);
  static final TopicPartition TP2 = new TopicPartition("other-topic", 0);

  /**
   * @return An execution state with an inter-broker replica move with a leader move (TP0), an intra-broker replica move (TP1),
   * and an inter-broker replica move (TP2) in different task states.
   */
  static PersistedExecutionState sampleState() {
    ExecutionProposal interBroker = new ExecutionProposal(TP0, 100, new ReplicaPlacementInfo(0),
                                                          Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                          Arrays.asList(new ReplicaPlacementInfo(2), new ReplicaPlacementInfo(1)));
    ExecutionProposal intraBroker = new ExecutionProposal(TP1, 200, new ReplicaPlacementInfo(1, "/d1"),
                                                          Arrays.asList(new ReplicaPlacementInfo(1, "/d1"), new ReplicaPlacementInfo(2, "/d1")),
                                                          Arrays.asList(new ReplicaPlacementInfo(1, "/d2"), new ReplicaPlacementInfo(2, "/d1")));
    ExecutionProposal otherInterBroker = new ExecutionProposal(TP2, 0, new ReplicaPlacementInfo(1),
                                                               Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(2)),
                                                               Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(0)));
    return new PersistedExecutionState.Builder("uuid-1", PersistedExecutionState.Operation.EXECUTE_PROPOSALS)
        .reason("rebalance")
        .triggeredByUserRequest(true)
        .startTimeMs(1000L)
        .lastUpdateTimeMs(2000L)
        .resumeCount(1)
        .executorState(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS)
        .removedBrokers(Set.of(3))
        .brokersToSkipConcurrencyCheck(Set.of(3, 4))
        .interBrokerPartitionMovementConcurrency(5)
        .maxInterBrokerPartitionMovements(50)
        .clusterLeadershipMovementConcurrency(100)
        .executionProgressCheckIntervalMs(10000L)
        .replicaMovementStrategy("PostponeUrpReplicaMovementStrategy,BaseReplicaMovementStrategy")
        .replicationThrottle(1000000L)
        .skipInterBrokerReplicaConcurrencyAdjustment(true)
        .cleanupOnly(true)
        .outsideExecutionFingerprint(new ReplicaPlacementFingerprint(-42L, 7, Map.of(0, 5, 1, 9)))
        .logDirFingerprintByBroker(Map.of(1, 123L, 2, -456L))
        .proposals(Arrays.asList(interBroker, intraBroker, otherInterBroker))
        .tasks(Arrays.asList(
            new PersistedTask(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, TP0, PersistedTask.NO_BROKER_ID,
                              ExecutionTaskState.IN_PROGRESS, 1500L, -1L),
            new PersistedTask(ExecutionTask.TaskType.LEADER_ACTION, TP0, PersistedTask.NO_BROKER_ID, ExecutionTaskState.PENDING, -1L, -1L),
            new PersistedTask(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, TP1, 1, ExecutionTaskState.COMPLETED, 1200L, 1300L),
            new PersistedTask(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, TP2, PersistedTask.NO_BROKER_ID,
                              ExecutionTaskState.DEAD, 1100L, 1400L)))
        .build();
  }

  static void assertProposalEquals(ExecutionProposal expected, ExecutionProposal actual) {
    // ExecutionProposal#equals compares the old leader by reference, hence compare the fields.
    assertEquals(expected.topicPartition(), actual.topicPartition());
    assertEquals(expected.partitionSize(), actual.partitionSize());
    assertEquals(expected.oldLeader(), actual.oldLeader());
    assertEquals(expected.oldReplicas(), actual.oldReplicas());
    assertEquals(expected.newReplicas(), actual.newReplicas());
    assertEquals(expected.replicasToMoveBetweenDisksByBroker(), actual.replicasToMoveBetweenDisksByBroker());
  }

  static void assertStateEquals(PersistedExecutionState expected, PersistedExecutionState actual) {
    assertEquals(expected.version(), actual.version());
    assertEquals(expected.uuid(), actual.uuid());
    assertEquals(expected.reason(), actual.reason());
    assertEquals(expected.operation(), actual.operation());
    assertEquals(expected.triggeredByUserRequest(), actual.triggeredByUserRequest());
    assertEquals(expected.startTimeMs(), actual.startTimeMs());
    assertEquals(expected.lastUpdateTimeMs(), actual.lastUpdateTimeMs());
    assertEquals(expected.resumeCount(), actual.resumeCount());
    assertEquals(expected.executorState(), actual.executorState());
    assertEquals(expected.removedBrokers(), actual.removedBrokers());
    assertEquals(expected.demotedBrokers(), actual.demotedBrokers());
    assertEquals(expected.brokersToSkipConcurrencyCheck(), actual.brokersToSkipConcurrencyCheck());
    assertEquals(expected.interBrokerPartitionMovementConcurrency(), actual.interBrokerPartitionMovementConcurrency());
    assertEquals(expected.maxInterBrokerPartitionMovements(), actual.maxInterBrokerPartitionMovements());
    assertEquals(expected.intraBrokerPartitionMovementConcurrency(), actual.intraBrokerPartitionMovementConcurrency());
    assertEquals(expected.clusterLeadershipMovementConcurrency(), actual.clusterLeadershipMovementConcurrency());
    assertEquals(expected.brokerLeadershipMovementConcurrency(), actual.brokerLeadershipMovementConcurrency());
    assertEquals(expected.executionProgressCheckIntervalMs(), actual.executionProgressCheckIntervalMs());
    assertEquals(expected.replicaMovementStrategy(), actual.replicaMovementStrategy());
    assertEquals(expected.replicationThrottle(), actual.replicationThrottle());
    assertEquals(expected.kafkaAssignerMode(), actual.kafkaAssignerMode());
    assertEquals(expected.skipInterBrokerReplicaConcurrencyAdjustment(), actual.skipInterBrokerReplicaConcurrencyAdjustment());
    assertEquals(expected.cleanupOnly(), actual.cleanupOnly());
    assertEquals(expected.outsideExecutionFingerprint(), actual.outsideExecutionFingerprint());
    assertEquals(expected.logDirFingerprintByBroker(), actual.logDirFingerprintByBroker());
    assertEquals(expected.proposals().keySet(), actual.proposals().keySet());
    expected.proposals().forEach((tp, proposal) -> assertProposalEquals(proposal, actual.proposals().get(tp)));
    assertEquals(expected.tasks(), actual.tasks());
  }

  @Test
  public void testRoundTrip() {
    PersistedExecutionState state = sampleState();
    assertStateEquals(state, PersistedExecutionStateSerde.fromJson(PersistedExecutionStateSerde.toJson(state)));
  }

  @Test
  public void testRoundTripWithDefaults() {
    PersistedExecutionState state = new PersistedExecutionState.Builder("uuid-2", PersistedExecutionState.Operation.DEMOTE_BROKERS)
        .demotedBrokers(List.of(1, 2))
        .build();
    PersistedExecutionState deserialized = PersistedExecutionStateSerde.fromJson(PersistedExecutionStateSerde.toJson(state));
    assertStateEquals(state, deserialized);
    assertNull(deserialized.executorState());
    assertNull(deserialized.removedBrokers());
    assertNull(deserialized.replicationThrottle());
    assertNull(deserialized.replicaMovementStrategy());
    assertEquals("", deserialized.reason());
    assertTrue(deserialized.tasks().isEmpty());
    assertTrue(deserialized.proposals().isEmpty());
    assertFalse(deserialized.cleanupOnly());
    assertNull(deserialized.outsideExecutionFingerprint());
    assertNull(deserialized.logDirFingerprintByBroker());
  }

  @Test
  public void testVersion1StateIsReadable() {
    // A version 1 state has none of the fields added in version 2.
    JsonObject json = JsonParser.parseString(PersistedExecutionStateSerde.toJson(sampleState())).getAsJsonObject();
    json.addProperty(PersistedExecutionStateSerde.VERSION, 1);
    json.remove(PersistedExecutionStateSerde.CLEANUP_ONLY);
    json.remove(PersistedExecutionStateSerde.OUTSIDE_EXECUTION_FINGERPRINT);
    json.remove(PersistedExecutionStateSerde.LOGDIR_FINGERPRINT_BY_BROKER);
    PersistedExecutionState state = PersistedExecutionStateSerde.fromJson(json.toString());
    assertEquals(1, state.version());
    assertFalse(state.cleanupOnly());
    assertNull(state.outsideExecutionFingerprint());
    assertNull(state.logDirFingerprintByBroker());
    assertEquals(sampleState().tasks(), state.tasks());
  }

  @Test
  public void testStreamedJsonEqualsString() throws IOException {
    StringWriter writer = new StringWriter();
    PersistedExecutionStateSerde.writeJson(sampleState(), writer);
    assertEquals(PersistedExecutionStateSerde.toJson(sampleState()), writer.toString());
    // Absent optional values are omitted rather than written as null.
    assertFalse(writer.toString().contains("null"));
  }

  @Test
  public void testUnsupportedVersion() {
    JsonObject json = JsonParser.parseString(PersistedExecutionStateSerde.toJson(sampleState())).getAsJsonObject();
    json.addProperty(PersistedExecutionStateSerde.VERSION, PersistedExecutionState.CURRENT_VERSION + 1);
    assertThrows(IllegalArgumentException.class, () -> PersistedExecutionStateSerde.fromJson(json.toString()));
  }

  @Test
  public void testMissingRequiredField() {
    JsonObject json = JsonParser.parseString(PersistedExecutionStateSerde.toJson(sampleState())).getAsJsonObject();
    json.remove(PersistedExecutionStateSerde.TASKS);
    assertThrows(IllegalArgumentException.class, () -> PersistedExecutionStateSerde.fromJson(json.toString()));
  }

  @Test
  public void testMalformedJson() {
    assertThrows(IllegalArgumentException.class, () -> PersistedExecutionStateSerde.fromJson("{\"uuid\": "));
    assertThrows(IllegalArgumentException.class, () -> PersistedExecutionStateSerde.fromJson("[]"));
  }

  @Test
  public void testTaskWithoutProposalIsRejected() {
    PersistedExecutionState.Builder builder = sampleState().toBuilder();
    builder.tasks(List.of(new PersistedTask(ExecutionTask.TaskType.LEADER_ACTION, new TopicPartition("unknown", 0),
                                            PersistedTask.NO_BROKER_ID, ExecutionTaskState.PENDING, -1L, -1L)));
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void testToBuilderRetainsContent() {
    PersistedExecutionState state = sampleState();
    assertStateEquals(state, state.toBuilder().build());
    PersistedExecutionState resumed = state.toBuilder().resumeCount(state.resumeCount() + 1).build();
    assertEquals(state.resumeCount() + 1, resumed.resumeCount());
    assertFalse(resumed.tasks().isEmpty());
  }
}
