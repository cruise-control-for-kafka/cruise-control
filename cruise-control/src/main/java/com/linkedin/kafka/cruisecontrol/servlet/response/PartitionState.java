/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;


@JsonResponseClass
public class PartitionState {
  @JsonResponseField
  @Schema(type = "string")
  protected static final String TOPIC = "topic";
  @JsonResponseField
  @Schema(type = "integer", format = "int32")
  protected static final String PARTITION = "partition";
  @JsonResponseField
  @Schema(type = "integer", format = "int32")
  protected static final String LEADER = "leader";
  @JsonResponseField
  @ArraySchema(schema = @Schema(type = "integer", format = "int32"))
  protected static final String REPLICAS = "replicas";
  @JsonResponseField
  @ArraySchema(schema = @Schema(type = "integer", format = "int32"))
  protected static final String IN_SYNC = "in-sync";
  @JsonResponseField
  @ArraySchema(schema = @Schema(type = "integer", format = "int32"))
  protected static final String OUT_OF_SYNC = "out-of-sync";
  @JsonResponseField
  @ArraySchema(schema = @Schema(type = "integer", format = "int32"))
  protected static final String OFFLINE = "offline";
  @JsonResponseField
  @Schema(type = "integer", format = "int32")
  protected static final String MIN_ISR = "min-isr";
  @JsonResponseField(required = false)
  @Schema(type = "boolean", format = "boolean")
  protected static final String REMOTE_STORAGE_ENABLED = "remote-storage-enabled";
  public static final String PARTITION_STATE_FORMAT_WITH_REMOTE_SUFFIX = "s%10s%10s%30s%30s%25s%25s%10s%25s%n";
  public static final String PARTITION_STATE_FORMAT_WITHOUT_REMOTE_SUFFIX = "s%10s%10s%30s%30s%25s%25s%10s%n";
  protected final String _topic;
  protected final int _partition;
  protected final int _leader;
  protected final List<Integer> _replicas;
  protected final List<Integer> _inSyncReplicas;
  protected final Set<Integer> _outOfSyncReplicas;
  protected final Set<Integer> _offlineReplicas;
  protected final int _minIsr;
  protected final boolean _remoteStorageEnabled;
  protected final boolean _displayRemoteStorageEnabled;

  public PartitionState(PartitionInfo partitionInfo, int minIsr, boolean remoteStorageEnabled, boolean displayRemoteStorageEnabled) {
    _topic = partitionInfo.topic();
    _partition = partitionInfo.partition();
    _leader = partitionInfo.leader() == null ? -1 : partitionInfo.leader().id();
    _replicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
    _inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toList());
    _outOfSyncReplicas = new HashSet<>(_replicas);
    _outOfSyncReplicas.removeAll(_inSyncReplicas);
    _offlineReplicas = Arrays.stream(partitionInfo.offlineReplicas()).map(Node::id).collect(Collectors.toSet());
    _minIsr = minIsr;
    _remoteStorageEnabled = remoteStorageEnabled;
    _displayRemoteStorageEnabled = displayRemoteStorageEnabled;
  }

  protected Map<String, Object> getJsonStructure() {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(TOPIC, _topic);
    jsonMap.put(PARTITION, _partition);
    jsonMap.put(LEADER, _leader);
    jsonMap.put(REPLICAS, _replicas);
    jsonMap.put(IN_SYNC, _inSyncReplicas);
    jsonMap.put(OUT_OF_SYNC, _outOfSyncReplicas);
    jsonMap.put(OFFLINE, _offlineReplicas);
    jsonMap.put(MIN_ISR, _minIsr);

    if (_displayRemoteStorageEnabled) {
      jsonMap.put(REMOTE_STORAGE_ENABLED, _remoteStorageEnabled);
    }
    return jsonMap;
  }

  protected String writeKafkaPartitionState(int topicNameLength) {
    if (_displayRemoteStorageEnabled) {
      return String.format("%" + topicNameLength + PARTITION_STATE_FORMAT_WITH_REMOTE_SUFFIX,
          _topic, _partition,
          _leader, _replicas,
          _inSyncReplicas,
          _outOfSyncReplicas,
          _offlineReplicas,
          _minIsr, _remoteStorageEnabled);
    } else {
      return String.format("%" + topicNameLength + PARTITION_STATE_FORMAT_WITHOUT_REMOTE_SUFFIX,
          _topic, _partition,
          _leader, _replicas,
          _inSyncReplicas,
          _outOfSyncReplicas,
          _offlineReplicas,
          _minIsr);
    }
  }
}
