/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
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
  private static final String LEADER_LOGDIR_COLUMN = "LEADER_LOGDIR";
  private static final String FOLLOWER_LOGDIRS_COLUMN = "FOLLOWER_LOGDIRS";
  private static final String UNKNOWN_LOGDIR = "-";
  protected final List<Partition> _sortedPartitions;
  protected final boolean _wantMaxLoad;
  protected final boolean _wantAvgLoad;
  protected final int _entries;
  protected final int _partitionUpperBoundary;
  protected final int _partitionLowerBoundary;
  protected final int _topicNameLength;
  protected final boolean _populateDiskInfo;
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
    this(sortedPartitions, wantMaxLoad, wantAvgLoad, entries, partitionUpperBoundary, partitionLowerBoundary, topic, topicNameLength,
         false, config);
  }

  /**
   * @param sortedPartitions Partitions sorted by the utilization of the requested resource.
   * @param wantMaxLoad {@code true} to report the maximum load of the partitions in the windows.
   * @param wantAvgLoad {@code true} to report the average load of the partitions in the windows.
   * @param entries Maximum number of partitions to report.
   * @param partitionUpperBoundary Highest partition number to report.
   * @param partitionLowerBoundary Lowest partition number to report.
   * @param topic Topics to report, or {@code null} for all topics.
   * @param topicNameLength Length of the partition column in plaintext.
   * @param populateDiskInfo {@code true} to report the logdir of the leader and of each follower of the partitions, which requires
   *                         partitions of a cluster model with replica placement information.
   * @param config The Kafka Cruise Control config.
   */
  public PartitionLoadState(List<Partition> sortedPartitions,
                            boolean wantMaxLoad,
                            boolean wantAvgLoad,
                            int entries,
                            int partitionUpperBoundary,
                            int partitionLowerBoundary,
                            Pattern topic,
                            int topicNameLength,
                            boolean populateDiskInfo,
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
    _populateDiskInfo = populateDiskInfo;
  }

  /**
   * @param replica A replica.
   * @param unknownLogdir The value to return if the logdir of the replica is not known.
   * @return The logdir of the replica, or {@code unknownLogdir} if it is not known (i.e. a replica on a dead broker or a failed disk).
   */
  private static String logdir(Replica replica, String unknownLogdir) {
    return replica.disk() == null ? unknownLogdir : replica.disk().logDir();
  }

  /**
   * @param partition A partition.
   * @param unknownLogdir The value to use for a follower whose logdir is not known.
   * @return The logdir of each follower of the partition, in the order of its followers.
   */
  private static List<String> followerLogdirs(Partition partition, String unknownLogdir) {
    return partition.followers().stream().map(replica -> logdir(replica, unknownLogdir)).collect(Collectors.toList());
  }

  /**
   * @return The partitions to report, in the order to report them in.
   */
  private List<Partition> partitionsToReport() {
    return _sortedPartitions.stream().filter(p -> !shouldSkipPartition(p)).limit(_entries).collect(Collectors.toList());
  }

  protected String getPlaintext() {
    List<Partition> partitionsToReport = partitionsToReport();
    StringBuilder sb = new StringBuilder();
    String loadHeader = String.format("%" + _topicNameLength + "s%10s%30s%20s%20s%20s%20s%20s", "PARTITION", "LEADER", "FOLLOWERS",
                                      "CPU (%_CORES)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)", "MSG_IN (#/s)");
    // The logdir columns come last, so that the other columns stay where they are. The load values are narrower than their
    // header, hence each line is padded to the length of the header for the logdir columns to line up.
    String logdirColumnsFormat = null;
    if (_populateDiskInfo) {
      int leaderLogdirLength = partitionsToReport.stream().map(p -> logdir(p.leader(), UNKNOWN_LOGDIR).length())
                                                 .reduce(LEADER_LOGDIR_COLUMN.length(), Math::max);
      logdirColumnsFormat = "%-" + loadHeader.length() + "s  %-" + leaderLogdirLength + "s  %s%n";
      sb.append(String.format(logdirColumnsFormat, loadHeader, LEADER_LOGDIR_COLUMN, FOLLOWER_LOGDIRS_COLUMN));
    } else {
      sb.append(String.format("%s%n", loadHeader));
    }
    for (Partition p : partitionsToReport) {
      List<Integer> followers = p.followers().stream().map(replica -> replica.broker().id()).collect(Collectors.toList());
      String load = String.format("%" + _topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%19.3f",
                                  p.leader().topicPartition(),
                                  p.leader().broker().id(),
                                  followers,
                                  p.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad, _wantAvgLoad),
                                  p.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad, _wantAvgLoad),
                                  p.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad, _wantAvgLoad),
                                  p.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad, _wantAvgLoad),
                                  p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad, _wantAvgLoad));
      if (logdirColumnsFormat != null) {
        sb.append(String.format(logdirColumnsFormat, load, logdir(p.leader(), UNKNOWN_LOGDIR), followerLogdirs(p, UNKNOWN_LOGDIR)));
      } else {
        sb.append(String.format("%s%n", load));
      }
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
    for (Partition p : partitionsToReport()) {
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
    @JsonResponseField(required = false)
    protected static final String LEADER_LOGDIR = "leaderLogdir";
    @JsonResponseField(required = false)
    protected static final String FOLLOWER_LOGDIRS = "followerLogdirs";
    protected Partition _partition;

    PartitionLoadRecord(Partition partition) {
      _partition = partition;
    }

    protected Map<String, Object> getJsonStructure() {
      List<Integer> followers = _partition.followers().stream().map(replica -> replica.broker().id()).collect(Collectors.toList());
      Map<String, Object> record = new HashMap<>();
      record.put(TOPIC, _partition.leader().topicPartition().topic());
      record.put(PARTITION, _partition.leader().topicPartition().partition());
      record.put(LEADER, _partition.leader().broker().id());
      record.put(FOLLOWERS, followers);
      record.put(Resource.CPU.resource(), _partition.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad, _wantAvgLoad));
      record.put(Resource.DISK.resource(), _partition.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad, _wantAvgLoad));
      record.put(Resource.NW_IN.resource(), _partition.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad, _wantAvgLoad));
      record.put(Resource.NW_OUT.resource(), _partition.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad, _wantAvgLoad));
      record.put(MSG_IN, _partition.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad, _wantAvgLoad));
      if (_populateDiskInfo) {
        // A logdir that is not known is null: Gson leaves out the leader logdir then, and keeps null in the follower logdirs.
        record.put(LEADER_LOGDIR, logdir(_partition.leader(), null));
        record.put(FOLLOWER_LOGDIRS, followerLogdirs(_partition, null));
      }
      return record;
    }
  }
}
