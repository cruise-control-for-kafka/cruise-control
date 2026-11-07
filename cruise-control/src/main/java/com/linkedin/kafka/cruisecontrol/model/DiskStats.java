/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import java.util.Map;


/**
 * A helper class to store statistics about the {@link Disk}.
 */
@JsonResponseClass
public class DiskStats {
  private final int _numLeaderReplicas;
  private final int _numReplicas;
  // For dead disk, its utilization will be null.
  private final Double _utilization;
  private final double _capacity;
  private final double _leaderBytesInRate;
  private final double _followerBytesInRate;
  private final double _bytesOutRate;
  private final double _potentialBytesOutRate;
  @JsonResponseField
  private static final String DISK_MB = "DiskMB";
  @JsonResponseField
  private static final String DISK_PCT = "DiskPct";
  @JsonResponseField
  private static final String NUM_LEADER_REPLICAS = "NumLeaderReplicas";
  @JsonResponseField
  private static final String NUM_REPLICAS = "NumReplicas";
  @JsonResponseField(required = false)
  private static final String LEADER_NW_IN_RATE = "LeaderNwInRate";
  @JsonResponseField(required = false)
  private static final String FOLLOWER_NW_IN_RATE = "FollowerNwInRate";
  @JsonResponseField(required = false)
  private static final String NW_OUT_RATE = "NwOutRate";
  @JsonResponseField(required = false)
  private static final String PNW_OUT_RATE = "PnwOutRate";
  private static final String DEAD_STATE = "DEAD";

  /**
   * @param numLeaderReplicas Number of leader replicas on the disk.
   * @param numReplicas Number of replicas on the disk.
   * @param utilization Disk utilization in MB.
   * @param capacity Disk capacity in MB -- non-positive for a dead disk.
   * @param leaderBytesInRate Total NW_IN rate of the leader replicas on the disk (KB/s).
   * @param followerBytesInRate Total NW_IN rate of the follower replicas on the disk (KB/s).
   * @param bytesOutRate Total NW_OUT rate of the replicas on the disk (KB/s).
   * @param potentialBytesOutRate Total NW_OUT rate of the replicas on the disk if all of them were leaders (KB/s).
   */
  DiskStats(int numLeaderReplicas,
            int numReplicas,
            double utilization,
            double capacity,
            double leaderBytesInRate,
            double followerBytesInRate,
            double bytesOutRate,
            double potentialBytesOutRate) {
    _numLeaderReplicas = numLeaderReplicas;
    _numReplicas = numReplicas;
    _utilization = capacity > 0 ? utilization : null;
    _capacity = capacity;
    _leaderBytesInRate = Math.max(leaderBytesInRate, 0.0);
    _followerBytesInRate = Math.max(followerBytesInRate, 0.0);
    _bytesOutRate = Math.max(bytesOutRate, 0.0);
    _potentialBytesOutRate = Math.max(potentialBytesOutRate, 0.0);
  }

  public int numLeaderReplicas() {
    return _numLeaderReplicas;
  }

  public int numReplicas() {
    return _numReplicas;
  }

  public Double utilization() {
    return _utilization;
  }

  public double capacity() {
    return _capacity;
  }

  /**
   * @return Total NW_IN rate of the leader replicas on the disk (KB/s).
   */
  public double leaderBytesInRate() {
    return _leaderBytesInRate;
  }

  /**
   * @return Total NW_IN rate of the follower replicas on the disk (KB/s).
   */
  public double followerBytesInRate() {
    return _followerBytesInRate;
  }

  /**
   * @return Total NW_OUT rate of the replicas on the disk (KB/s).
   */
  public double bytesOutRate() {
    return _bytesOutRate;
  }

  /**
   * @return Total NW_OUT rate of the replicas on the disk if all of them were leaders (KB/s).
   */
  public double potentialBytesOutRate() {
    return _potentialBytesOutRate;
  }

  public Double utilizationPercentage() {
    return _utilization == null ? null : _utilization * 100.0 / _capacity;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    boolean isDead = _utilization == null;
    return Map.of(DISK_MB, isDead ? DEAD_STATE : _utilization, DISK_PCT, isDead ? DEAD_STATE : utilizationPercentage(),
                  NUM_LEADER_REPLICAS, _numLeaderReplicas, NUM_REPLICAS, _numReplicas,
                  LEADER_NW_IN_RATE, isDead ? DEAD_STATE : _leaderBytesInRate,
                  FOLLOWER_NW_IN_RATE, isDead ? DEAD_STATE : _followerBytesInRate,
                  NW_OUT_RATE, isDead ? DEAD_STATE : _bytesOutRate,
                  PNW_OUT_RATE, isDead ? DEAD_STATE : _potentialBytesOutRate);
  }
}
