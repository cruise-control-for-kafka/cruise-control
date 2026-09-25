/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class PartitionLoadState extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String RECORDS = "records";
  protected final List<Partition> _sortedPartitions;
  protected final boolean _wantMaxLoad;
  protected final boolean _wantAvgLoad;
  protected final int _entries;
  protected final int _partitionUpperBoundary;
  protected final int _partitionLowerBoundary;
  protected final int _topicNameLength;
  protected Pattern _topic;

  public PartitionLoadState(List<Partition> sortedPartitions,
                            boolean wantMaxLoad,
                            boolean wantAvgLoad,
                            int entries,
                            int partitionUpperBoundary,
                            int partitionLowerBoundary,
                            Pattern topic,
                            int topicNameLength,
                            KafkaCruiseControlConfig config) {
    super(config);
    _sortedPartitions = sortedPartitions;
    _wantMaxLoad = wantMaxLoad;
    _wantAvgLoad = wantAvgLoad;
    _entries = entries;
    _partitionUpperBoundary = partitionUpperBoundary;
    _partitionLowerBoundary = partitionLowerBoundary;
    _topic = topic;
    _topicNameLength = topicNameLength;
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%" + _topicNameLength + "s%10s%30s%20s%20s%20s%20s%20s%n", "PARTITION", "LEADER", "FOLLOWERS",
                            "CPU (%_CORES)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)", "MSG_IN (#/s)"));
    int numEntries = 0;
    for (Partition p : _sortedPartitions) {
      if (shouldSkipPartition(p)) {
        continue;
      }
      if (++numEntries > _entries) {
        break;
      }
      List<Integer> followers = p.followers().stream().map(replica -> replica.broker().id()).collect(Collectors.toList());
      sb.append(String.format("%" + _topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%19.3f%n",
                              p.leader().topicPartition(),
                              p.leader().broker().id(),
                              followers,
                              p.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad, _wantAvgLoad)));
    }
    return sb.toString();
  }

  /**
   * Skips the partition if it does not match the requested topic pattern, or is out of the requested partition scope.
   *
   * @param partition Partition to check whether be included in the response.
   * @return {@code true} to skip partition, {@code false} otherwise.
   */
  private boolean shouldSkipPartition(Partition partition) {
    return (_topic != null && !_topic.matcher(partition.topicPartition().topic()).matches())
           || partition.topicPartition().partition() < _partitionLowerBoundary
           || partition.topicPartition().partition() > _partitionUpperBoundary;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
    // Discard irrelevant response.
    _sortedPartitions.clear();
    _topic = null;
  }

  protected String getJsonString() {
    Map<String, Object> partitionMap = new HashMap<>();
    List<Object> partitionList = new ArrayList<>();
    partitionMap.put(VERSION, JSON_VERSION);
    int numEntries = 0;
    for (Partition p : _sortedPartitions) {
      if (shouldSkipPartition(p)) {
        continue;
      }
      if (++numEntries > _entries) {
        break;
      }
      partitionList.add(new PartitionLoadRecord(p).getJsonStructure());
    }
    partitionMap.put(RECORDS, partitionList);
    Gson gson = new Gson();
    return gson.toJson(partitionMap);
  }

  @JsonResponseClass
  @JsonResponseExternalFields(Resource.class)
  protected class PartitionLoadRecord {
    @JsonResponseField
    protected static final String TOPIC = "topic";
    @JsonResponseField
    protected static final String PARTITION = "partition";
    @JsonResponseField
    protected static final String LEADER = "leader";
    @JsonResponseField
    protected static final String FOLLOWERS = "followers";
    @JsonResponseField
    protected static final String MSG_IN = "msg_in";
    protected Partition _partition;

    PartitionLoadRecord(Partition partition) {
      _partition = partition;
    }

    protected Map<String, Object> getJsonStructure() {
      List<Integer> followers = _partition.followers().stream().map(replica -> replica.broker().id()).collect(Collectors.toList());
      return Map.of(TOPIC, _partition.leader().topicPartition().topic(), PARTITION, _partition.leader().topicPartition().partition(),
                    LEADER, _partition.leader().broker().id(), FOLLOWERS, followers,
                    Resource.CPU.resource(), _partition.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad, _wantAvgLoad),
                    Resource.DISK.resource(), _partition.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad, _wantAvgLoad),
                    Resource.NW_IN.resource(), _partition.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad, _wantAvgLoad),
                    Resource.NW_OUT.resource(), _partition.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad, _wantAvgLoad),
                    MSG_IN, _partition.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad, _wantAvgLoad));
    }
  }
}
