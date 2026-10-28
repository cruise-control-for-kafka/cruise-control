/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;


/**
 * Serializes / deserializes a {@link PersistedExecutionState} to / from JSON.
 *
 * The JSON document is built explicitly (rather than via reflection) to keep the persisted format stable and independent
 * of the internal representation of the executor classes.
 */
public final class PersistedExecutionStateSerde {
  static final String VERSION = "version";
  static final String UUID = "uuid";
  static final String REASON = "reason";
  static final String OPERATION = "operation";
  static final String TRIGGERED_BY_USER_REQUEST = "triggeredByUserRequest";
  static final String START_TIME_MS = "startTimeMs";
  static final String LAST_UPDATE_TIME_MS = "lastUpdateTimeMs";
  static final String RESUME_COUNT = "resumeCount";
  static final String EXECUTOR_STATE = "executorState";
  static final String PARAMETERS = "parameters";
  static final String REMOVED_BROKERS = "removedBrokers";
  static final String DEMOTED_BROKERS = "demotedBrokers";
  static final String BROKERS_TO_SKIP_CONCURRENCY_CHECK = "brokersToSkipConcurrencyCheck";
  static final String INTER_BROKER_PARTITION_MOVEMENT_CONCURRENCY = "interBrokerPartitionMovementConcurrency";
  static final String MAX_INTER_BROKER_PARTITION_MOVEMENTS = "maxInterBrokerPartitionMovements";
  static final String INTRA_BROKER_PARTITION_MOVEMENT_CONCURRENCY = "intraBrokerPartitionMovementConcurrency";
  static final String CLUSTER_LEADERSHIP_MOVEMENT_CONCURRENCY = "clusterLeadershipMovementConcurrency";
  static final String BROKER_LEADERSHIP_MOVEMENT_CONCURRENCY = "brokerLeadershipMovementConcurrency";
  static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS = "executionProgressCheckIntervalMs";
  static final String REPLICA_MOVEMENT_STRATEGY = "replicaMovementStrategy";
  static final String REPLICATION_THROTTLE = "replicationThrottle";
  static final String KAFKA_ASSIGNER_MODE = "kafkaAssignerMode";
  static final String SKIP_INTER_BROKER_REPLICA_CONCURRENCY_ADJUSTMENT = "skipInterBrokerReplicaConcurrencyAdjustment";
  static final String PROPOSALS = "proposals";
  static final String TOPIC = "topic";
  static final String PARTITION = "partition";
  static final String PARTITION_SIZE = "partitionSize";
  static final String OLD_LEADER = "oldLeader";
  static final String OLD_REPLICAS = "oldReplicas";
  static final String NEW_REPLICAS = "newReplicas";
  static final String BROKER_ID = "brokerId";
  static final String LOGDIR = "logdir";
  static final String TASKS = "tasks";
  static final String TYPE = "type";
  static final String STATE = "state";
  static final String END_TIME_MS = "endTimeMs";
  static final String CLEANUP_ONLY = "cleanupOnly";
  static final String OUTSIDE_EXECUTION_FINGERPRINT = "outsideExecutionFingerprint";
  static final String HASH = "hash";
  static final String NUM_PARTITIONS = "numPartitions";
  static final String NUM_REPLICAS_BY_BROKER = "numReplicasByBroker";
  static final String LOGDIR_FINGERPRINT_BY_BROKER = "logDirFingerprintByBroker";

  private PersistedExecutionStateSerde() {
  }

  /**
   * @param state Execution state to serialize.
   * @return JSON representation of the given execution state.
   */
  public static String toJson(PersistedExecutionState state) {
    StringWriter writer = new StringWriter();
    try {
      writeJson(state, writer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return writer.toString();
  }

  /**
   * Write the JSON representation of the given execution state to the given writer. The document is streamed rather than built
   * in memory first, because an execution may have hundreds of thousands of tasks and it is persisted whenever it progresses.
   *
   * @param state Execution state to serialize.
   * @param out Writer to write the JSON representation of the given execution state to.
   */
  public static void writeJson(PersistedExecutionState state, Writer out) throws IOException {
    JsonWriter json = new JsonWriter(out);
    // Absent optional values are omitted rather than written as null.
    json.setSerializeNulls(false);
    json.beginObject();
    json.name(VERSION).value(state.version());
    json.name(UUID).value(state.uuid());
    json.name(REASON).value(state.reason());
    json.name(OPERATION).value(state.operation().name());
    json.name(TRIGGERED_BY_USER_REQUEST).value(state.triggeredByUserRequest());
    json.name(START_TIME_MS).value(state.startTimeMs());
    json.name(LAST_UPDATE_TIME_MS).value(state.lastUpdateTimeMs());
    json.name(RESUME_COUNT).value(state.resumeCount());
    json.name(EXECUTOR_STATE).value(state.executorState() == null ? null : state.executorState().name());
    json.name(CLEANUP_ONLY).value(state.cleanupOnly());

    json.name(PARAMETERS).beginObject();
    writeIntegers(json, REMOVED_BROKERS, state.removedBrokers());
    writeIntegers(json, DEMOTED_BROKERS, state.demotedBrokers());
    writeIntegers(json, BROKERS_TO_SKIP_CONCURRENCY_CHECK, state.brokersToSkipConcurrencyCheck());
    json.name(INTER_BROKER_PARTITION_MOVEMENT_CONCURRENCY).value(state.interBrokerPartitionMovementConcurrency());
    json.name(MAX_INTER_BROKER_PARTITION_MOVEMENTS).value(state.maxInterBrokerPartitionMovements());
    json.name(INTRA_BROKER_PARTITION_MOVEMENT_CONCURRENCY).value(state.intraBrokerPartitionMovementConcurrency());
    json.name(CLUSTER_LEADERSHIP_MOVEMENT_CONCURRENCY).value(state.clusterLeadershipMovementConcurrency());
    json.name(BROKER_LEADERSHIP_MOVEMENT_CONCURRENCY).value(state.brokerLeadershipMovementConcurrency());
    json.name(EXECUTION_PROGRESS_CHECK_INTERVAL_MS).value(state.executionProgressCheckIntervalMs());
    json.name(REPLICA_MOVEMENT_STRATEGY).value(state.replicaMovementStrategy());
    json.name(REPLICATION_THROTTLE).value(state.replicationThrottle());
    json.name(KAFKA_ASSIGNER_MODE).value(state.kafkaAssignerMode());
    json.name(SKIP_INTER_BROKER_REPLICA_CONCURRENCY_ADJUSTMENT).value(state.skipInterBrokerReplicaConcurrencyAdjustment());
    json.endObject();

    ReplicaPlacementFingerprint fingerprint = state.outsideExecutionFingerprint();
    if (fingerprint != null) {
      json.name(OUTSIDE_EXECUTION_FINGERPRINT).beginObject();
      json.name(HASH).value(fingerprint.hash());
      json.name(NUM_PARTITIONS).value(fingerprint.numPartitions());
      json.name(NUM_REPLICAS_BY_BROKER).beginObject();
      for (Map.Entry<Integer, Integer> entry : fingerprint.numReplicasByBroker().entrySet()) {
        json.name(String.valueOf(entry.getKey())).value(entry.getValue());
      }
      json.endObject();
      json.endObject();
    }
    if (state.logDirFingerprintByBroker() != null) {
      json.name(LOGDIR_FINGERPRINT_BY_BROKER).beginObject();
      for (Map.Entry<Integer, Long> entry : state.logDirFingerprintByBroker().entrySet()) {
        json.name(String.valueOf(entry.getKey())).value(entry.getValue());
      }
      json.endObject();
    }

    json.name(PROPOSALS).beginArray();
    for (ExecutionProposal proposal : state.proposals().values()) {
      json.beginObject();
      json.name(TOPIC).value(proposal.topic());
      json.name(PARTITION).value(proposal.partitionId());
      json.name(PARTITION_SIZE).value(proposal.partitionSize());
      json.name(OLD_LEADER);
      writeReplica(json, proposal.oldLeader());
      json.name(OLD_REPLICAS);
      writeReplicas(json, proposal.oldReplicas());
      json.name(NEW_REPLICAS);
      writeReplicas(json, proposal.newReplicas());
      json.endObject();
    }
    json.endArray();

    json.name(TASKS).beginArray();
    for (PersistedTask task : state.tasks()) {
      json.beginObject();
      json.name(TYPE).value(task.type().name());
      json.name(TOPIC).value(task.topicPartition().topic());
      json.name(PARTITION).value(task.topicPartition().partition());
      if (task.brokerId() != PersistedTask.NO_BROKER_ID) {
        json.name(BROKER_ID).value(task.brokerId());
      }
      json.name(STATE).value(task.state().name());
      json.name(START_TIME_MS).value(task.startTimeMs());
      json.name(END_TIME_MS).value(task.endTimeMs());
      json.endObject();
    }
    json.endArray();
    json.endObject();
    json.flush();
  }

  /**
   * @param json JSON representation of an execution state.
   * @return The deserialized execution state.
   * @throws IllegalArgumentException If the given JSON is not a valid execution state, or has an unsupported version.
   */
  public static PersistedExecutionState fromJson(String json) {
    try {
      JsonObject root = JsonParser.parseString(json).getAsJsonObject();
      int version = required(root, VERSION).getAsInt();
      if (version > PersistedExecutionState.CURRENT_VERSION) {
        throw new IllegalArgumentException(String.format("Unsupported execution state version %d (latest supported: %d).",
                                                         version, PersistedExecutionState.CURRENT_VERSION));
      }
      PersistedExecutionState.Builder builder =
          new PersistedExecutionState.Builder(required(root, UUID).getAsString(),
                                              PersistedExecutionState.Operation.valueOf(required(root, OPERATION).getAsString()))
              .version(version)
              .reason(optionalString(root, REASON))
              .triggeredByUserRequest(required(root, TRIGGERED_BY_USER_REQUEST).getAsBoolean())
              .startTimeMs(required(root, START_TIME_MS).getAsLong())
              .lastUpdateTimeMs(required(root, LAST_UPDATE_TIME_MS).getAsLong())
              .resumeCount(required(root, RESUME_COUNT).getAsInt());
      String executorState = optionalString(root, EXECUTOR_STATE);
      builder.executorState(executorState == null ? null : ExecutorState.State.valueOf(executorState));

      JsonObject parameters = required(root, PARAMETERS).getAsJsonObject();
      builder.removedBrokers(optionalIntegers(parameters, REMOVED_BROKERS))
             .demotedBrokers(optionalIntegers(parameters, DEMOTED_BROKERS))
             .brokersToSkipConcurrencyCheck(optionalIntegers(parameters, BROKERS_TO_SKIP_CONCURRENCY_CHECK))
             .interBrokerPartitionMovementConcurrency(optionalInteger(parameters, INTER_BROKER_PARTITION_MOVEMENT_CONCURRENCY))
             .maxInterBrokerPartitionMovements(optionalInteger(parameters, MAX_INTER_BROKER_PARTITION_MOVEMENTS))
             .intraBrokerPartitionMovementConcurrency(optionalInteger(parameters, INTRA_BROKER_PARTITION_MOVEMENT_CONCURRENCY))
             .clusterLeadershipMovementConcurrency(optionalInteger(parameters, CLUSTER_LEADERSHIP_MOVEMENT_CONCURRENCY))
             .brokerLeadershipMovementConcurrency(optionalInteger(parameters, BROKER_LEADERSHIP_MOVEMENT_CONCURRENCY))
             .executionProgressCheckIntervalMs(optionalLong(parameters, EXECUTION_PROGRESS_CHECK_INTERVAL_MS))
             .replicaMovementStrategy(optionalString(parameters, REPLICA_MOVEMENT_STRATEGY))
             .replicationThrottle(optionalLong(parameters, REPLICATION_THROTTLE))
             .kafkaAssignerMode(required(parameters, KAFKA_ASSIGNER_MODE).getAsBoolean())
             .skipInterBrokerReplicaConcurrencyAdjustment(required(parameters, SKIP_INTER_BROKER_REPLICA_CONCURRENCY_ADJUSTMENT).getAsBoolean());
      // Fields added in version 2 are optional, so that a version 1 state can still be recovered (but not resumed).
      JsonElement cleanupOnly = optional(root, CLEANUP_ONLY);
      builder.cleanupOnly(cleanupOnly != null && cleanupOnly.getAsBoolean());
      JsonElement fingerprint = optional(root, OUTSIDE_EXECUTION_FINGERPRINT);
      if (fingerprint != null) {
        JsonObject f = fingerprint.getAsJsonObject();
        Map<Integer, Integer> numReplicasByBroker = new TreeMap<>();
        required(f, NUM_REPLICAS_BY_BROKER).getAsJsonObject().entrySet()
                                           .forEach(e -> numReplicasByBroker.put(Integer.parseInt(e.getKey()), e.getValue().getAsInt()));
        builder.outsideExecutionFingerprint(new ReplicaPlacementFingerprint(required(f, HASH).getAsLong(),
                                                                            required(f, NUM_PARTITIONS).getAsInt(),
                                                                            numReplicasByBroker));
      }
      JsonElement logDirFingerprints = optional(root, LOGDIR_FINGERPRINT_BY_BROKER);
      if (logDirFingerprints != null) {
        Map<Integer, Long> logDirFingerprintByBroker = new TreeMap<>();
        logDirFingerprints.getAsJsonObject().entrySet()
                          .forEach(e -> logDirFingerprintByBroker.put(Integer.parseInt(e.getKey()), e.getValue().getAsLong()));
        builder.logDirFingerprintByBroker(logDirFingerprintByBroker);
      }

      List<ExecutionProposal> proposals = new ArrayList<>();
      for (JsonElement element : required(root, PROPOSALS).getAsJsonArray()) {
        JsonObject p = element.getAsJsonObject();
        proposals.add(new ExecutionProposal(new TopicPartition(required(p, TOPIC).getAsString(), required(p, PARTITION).getAsInt()),
                                            required(p, PARTITION_SIZE).getAsLong(),
                                            replicaFromJson(required(p, OLD_LEADER).getAsJsonObject()),
                                            replicasFromJson(required(p, OLD_REPLICAS).getAsJsonArray()),
                                            replicasFromJson(required(p, NEW_REPLICAS).getAsJsonArray())));
      }
      builder.proposals(proposals);

      List<PersistedTask> tasks = new ArrayList<>();
      for (JsonElement element : required(root, TASKS).getAsJsonArray()) {
        JsonObject t = element.getAsJsonObject();
        Integer brokerId = optionalInteger(t, BROKER_ID);
        tasks.add(new PersistedTask(ExecutionTask.TaskType.valueOf(required(t, TYPE).getAsString()),
                                    new TopicPartition(required(t, TOPIC).getAsString(), required(t, PARTITION).getAsInt()),
                                    brokerId == null ? PersistedTask.NO_BROKER_ID : brokerId,
                                    ExecutionTaskState.valueOf(required(t, STATE).getAsString()),
                                    required(t, START_TIME_MS).getAsLong(),
                                    required(t, END_TIME_MS).getAsLong()));
      }
      builder.tasks(tasks);
      return builder.build();
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Failed to parse the persisted execution state.", e);
    }
  }

  private static void writeIntegers(JsonWriter json, String name, Collection<Integer> values) throws IOException {
    if (values == null) {
      return;
    }
    json.name(name).beginArray();
    for (int value : values) {
      json.value(value);
    }
    json.endArray();
  }

  private static void writeReplica(JsonWriter json, ReplicaPlacementInfo replica) throws IOException {
    json.beginObject();
    json.name(BROKER_ID).value(replica.brokerId());
    if (replica.logdir() != null) {
      json.name(LOGDIR).value(replica.logdir());
    }
    json.endObject();
  }

  private static void writeReplicas(JsonWriter json, List<ReplicaPlacementInfo> replicas) throws IOException {
    json.beginArray();
    for (ReplicaPlacementInfo replica : replicas) {
      writeReplica(json, replica);
    }
    json.endArray();
  }

  private static ReplicaPlacementInfo replicaFromJson(JsonObject json) {
    int brokerId = required(json, BROKER_ID).getAsInt();
    String logdir = optionalString(json, LOGDIR);
    return logdir == null ? new ReplicaPlacementInfo(brokerId) : new ReplicaPlacementInfo(brokerId, logdir);
  }

  private static List<ReplicaPlacementInfo> replicasFromJson(JsonArray array) {
    List<ReplicaPlacementInfo> replicas = new ArrayList<>(array.size());
    array.forEach(e -> replicas.add(replicaFromJson(e.getAsJsonObject())));
    return replicas;
  }

  private static JsonElement required(JsonObject json, String name) {
    JsonElement element = json.get(name);
    if (element == null || element.isJsonNull()) {
      throw new IllegalArgumentException(String.format("Missing required field '%s' in the persisted execution state.", name));
    }
    return element;
  }

  private static JsonElement optional(JsonObject json, String name) {
    JsonElement element = json.get(name);
    return element == null || element.isJsonNull() ? null : element;
  }

  private static String optionalString(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    return element == null ? null : element.getAsString();
  }

  private static Integer optionalInteger(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    return element == null ? null : element.getAsInt();
  }

  private static Long optionalLong(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    return element == null ? null : element.getAsLong();
  }

  private static List<Integer> optionalIntegers(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    if (element == null) {
      return null;
    }
    List<Integer> values = new ArrayList<>();
    element.getAsJsonArray().forEach(e -> values.add(e.getAsInt()));
    return values;
  }
}
