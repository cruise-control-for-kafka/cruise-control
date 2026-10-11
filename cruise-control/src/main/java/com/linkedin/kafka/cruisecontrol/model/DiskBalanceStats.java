/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;


/**
 * Statistics about the balance of disk utilization between the disks of each broker -- i.e. the balance that intra-broker
 * goals optimize. These stats are relevant only if the cluster model has disk information.
 *
 * Only alive disks with a positive capacity are taken into account: a disk marked for removal (or configured with no
 * capacity) is being emptied, and its utilization percentage is not defined.
 */
@JsonResponseClass
public final class DiskBalanceStats {
  @JsonResponseField
  private static final String NUM_ALIVE_DISKS = "numAliveDisks";
  @JsonResponseField
  private static final String NUM_UNBALANCED_DISKS = "numUnbalancedDisks";
  @JsonResponseField
  private static final String NUM_DISKS_ABOVE_CAPACITY_LIMIT = "numDisksAboveCapacityLimit";
  @JsonResponseField
  private static final String MAX_DISK_UTILIZATION_PCT = "maxDiskUtilizationPct";
  @JsonResponseField
  private static final String MAX_DISK_UTILIZATION_BROKER = "maxDiskUtilizationBroker";
  @JsonResponseField
  private static final String MAX_DISK_UTILIZATION_LOGDIR = "maxDiskUtilizationLogdir";
  @JsonResponseField
  private static final String MAX_DISK_UTILIZATION_SPREAD_PCT = "maxDiskUtilizationSpreadPct";
  @JsonResponseField(required = false)
  private static final String MAX_DISK_UTILIZATION_SPREAD_BROKER = "maxDiskUtilizationSpreadBroker";
  @JsonResponseField
  private static final String DISK_UTILIZATION_STDEV_PCT = "diskUtilizationStDevPct";
  private final int _numAliveDisks;
  private final int _numUnbalancedDisks;
  private final int _numDisksAboveCapacityLimit;
  private final double _maxDiskUtilizationPct;
  private final int _maxDiskUtilizationBrokerId;
  private final String _maxDiskUtilizationLogdir;
  private final double _maxDiskUtilizationSpreadPct;
  // Null if no broker has at least two disks.
  private final Integer _maxDiskUtilizationSpreadBrokerId;
  private final double _diskUtilizationStDevPct;

  private DiskBalanceStats(int numAliveDisks,
                           int numUnbalancedDisks,
                           int numDisksAboveCapacityLimit,
                           double maxDiskUtilizationPct,
                           int maxDiskUtilizationBrokerId,
                           String maxDiskUtilizationLogdir,
                           double maxDiskUtilizationSpreadPct,
                           Integer maxDiskUtilizationSpreadBrokerId,
                           double diskUtilizationStDevPct) {
    _numAliveDisks = numAliveDisks;
    _numUnbalancedDisks = numUnbalancedDisks;
    _numDisksAboveCapacityLimit = numDisksAboveCapacityLimit;
    _maxDiskUtilizationPct = maxDiskUtilizationPct;
    _maxDiskUtilizationBrokerId = maxDiskUtilizationBrokerId;
    _maxDiskUtilizationLogdir = maxDiskUtilizationLogdir;
    _maxDiskUtilizationSpreadPct = maxDiskUtilizationSpreadPct;
    _maxDiskUtilizationSpreadBrokerId = maxDiskUtilizationSpreadBrokerId;
    _diskUtilizationStDevPct = diskUtilizationStDevPct;
  }

  /**
   * Compute the disk balance stats of the given brokers.
   *
   * A disk is unbalanced if its utilization percentage is out of the range centered at the utilization percentage of the
   * disks of its broker, with boundary determined by
   * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#DISK_BALANCE_THRESHOLD_CONFIG}; and above the
   * capacity limit if its utilization percentage exceeds
   * {@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#DISK_CAPACITY_THRESHOLD_CONFIG}.
   *
   * @param brokers Alive brokers, in the order to report ties in.
   * @param balancingConstraint Balancing constraint.
   * @return The disk balance stats, or {@code null} if the brokers have no alive disk with a positive capacity (e.g. if the
   * cluster model has no disk information).
   */
  static DiskBalanceStats compute(Iterable<Broker> brokers, BalancingConstraint balancingConstraint) {
    double balancePercentage = balancingConstraint.resourceBalancePercentage(Resource.DISK);
    double capacityThreshold = balancingConstraint.capacityThreshold(Resource.DISK);
    int numAliveDisks = 0;
    int numUnbalancedDisks = 0;
    int numDisksAboveCapacityLimit = 0;
    double maxDiskUtilization = -1.0;
    int maxDiskUtilizationBrokerId = -1;
    String maxDiskUtilizationLogdir = null;
    double maxDiskUtilizationSpread = 0.0;
    Integer maxDiskUtilizationSpreadBrokerId = null;
    double totalDiskUtilizationVariance = 0.0;
    for (Broker broker : brokers) {
      List<Disk> disks = new ArrayList<>(broker.disks().size());
      double totalCapacity = 0.0;
      double totalUtilization = 0.0;
      for (Disk disk : broker.disks()) {
        if (disk.isAlive() && disk.capacity() > 0) {
          disks.add(disk);
          totalCapacity += disk.capacity();
          totalUtilization += disk.utilization();
        }
      }
      if (disks.isEmpty()) {
        continue;
      }
      double brokerDiskUtilization = totalUtilization / totalCapacity;
      double upperLimit = brokerDiskUtilization * balancePercentage;
      double lowerLimit = brokerDiskUtilization * Math.max(0, 2 - balancePercentage);
      double minBrokerDiskUtilization = Double.MAX_VALUE;
      double maxBrokerDiskUtilization = -Double.MAX_VALUE;
      for (Disk disk : disks) {
        double diskUtilization = disk.utilization() / disk.capacity();
        numAliveDisks++;
        if (diskUtilization > upperLimit || diskUtilization < lowerLimit) {
          numUnbalancedDisks++;
        }
        if (diskUtilization > capacityThreshold) {
          numDisksAboveCapacityLimit++;
        }
        if (diskUtilization > maxDiskUtilization) {
          maxDiskUtilization = diskUtilization;
          maxDiskUtilizationBrokerId = broker.id();
          maxDiskUtilizationLogdir = disk.logDir();
        }
        minBrokerDiskUtilization = Math.min(minBrokerDiskUtilization, diskUtilization);
        maxBrokerDiskUtilization = Math.max(maxBrokerDiskUtilization, diskUtilization);
        totalDiskUtilizationVariance += Math.pow(diskUtilization - brokerDiskUtilization, 2);
      }
      if (disks.size() > 1 && (maxDiskUtilizationSpreadBrokerId == null
                               || maxBrokerDiskUtilization - minBrokerDiskUtilization > maxDiskUtilizationSpread)) {
        maxDiskUtilizationSpread = maxBrokerDiskUtilization - minBrokerDiskUtilization;
        maxDiskUtilizationSpreadBrokerId = broker.id();
      }
    }
    if (numAliveDisks == 0) {
      return null;
    }
    return new DiskBalanceStats(numAliveDisks,
                                numUnbalancedDisks,
                                numDisksAboveCapacityLimit,
                                maxDiskUtilization * UNIT_INTERVAL_TO_PERCENTAGE,
                                maxDiskUtilizationBrokerId,
                                maxDiskUtilizationLogdir,
                                maxDiskUtilizationSpread * UNIT_INTERVAL_TO_PERCENTAGE,
                                maxDiskUtilizationSpreadBrokerId,
                                Math.sqrt(totalDiskUtilizationVariance / numAliveDisks) * UNIT_INTERVAL_TO_PERCENTAGE);
  }

  /**
   * @return The number of alive disks with a positive capacity.
   */
  public int numAliveDisks() {
    return _numAliveDisks;
  }

  /**
   * @return The number of disks whose utilization is out of the balance range of their broker.
   */
  public int numUnbalancedDisks() {
    return _numUnbalancedDisks;
  }

  /**
   * @return The number of disks whose utilization is above the disk capacity threshold.
   */
  public int numDisksAboveCapacityLimit() {
    return _numDisksAboveCapacityLimit;
  }

  /**
   * @return The highest disk utilization percentage.
   */
  public double maxDiskUtilizationPct() {
    return _maxDiskUtilizationPct;
  }

  /**
   * @return The id of the broker of the disk with the highest utilization percentage.
   */
  public int maxDiskUtilizationBrokerId() {
    return _maxDiskUtilizationBrokerId;
  }

  /**
   * @return The logdir of the disk with the highest utilization percentage.
   */
  public String maxDiskUtilizationLogdir() {
    return _maxDiskUtilizationLogdir;
  }

  /**
   * @return The largest difference between the highest and the lowest disk utilization percentage of a broker, in
   * percentage points.
   */
  public double maxDiskUtilizationSpreadPct() {
    return _maxDiskUtilizationSpreadPct;
  }

  /**
   * @return The id of the broker with the largest disk utilization spread, or {@code null} if no broker has at least two disks.
   */
  public Integer maxDiskUtilizationSpreadBrokerId() {
    return _maxDiskUtilizationSpreadBrokerId;
  }

  /**
   * @return The standard deviation of the utilization percentage of disks from the utilization percentage of their broker,
   * in percentage points.
   */
  public double diskUtilizationStDevPct() {
    return _diskUtilizationStDevPct;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> diskBalanceStats = new HashMap<>();
    diskBalanceStats.put(NUM_ALIVE_DISKS, _numAliveDisks);
    diskBalanceStats.put(NUM_UNBALANCED_DISKS, _numUnbalancedDisks);
    diskBalanceStats.put(NUM_DISKS_ABOVE_CAPACITY_LIMIT, _numDisksAboveCapacityLimit);
    diskBalanceStats.put(MAX_DISK_UTILIZATION_PCT, _maxDiskUtilizationPct);
    diskBalanceStats.put(MAX_DISK_UTILIZATION_BROKER, _maxDiskUtilizationBrokerId);
    diskBalanceStats.put(MAX_DISK_UTILIZATION_LOGDIR, _maxDiskUtilizationLogdir);
    diskBalanceStats.put(MAX_DISK_UTILIZATION_SPREAD_PCT, _maxDiskUtilizationSpreadPct);
    if (_maxDiskUtilizationSpreadBrokerId != null) {
      diskBalanceStats.put(MAX_DISK_UTILIZATION_SPREAD_BROKER, _maxDiskUtilizationSpreadBrokerId);
    }
    diskBalanceStats.put(DISK_UTILIZATION_STDEV_PCT, _diskUtilizationStDevPct);
    return diskBalanceStats;
  }

  @Override
  public String toString() {
    return String.format("DISKS:{alive: %d unbalanced: %d aboveCapacityLimit: %d maxUtilization: %.2f%% (broker %d %s) "
                         + "maxSpreadWithinBroker: %.2fpp%s utilizationStDevFromBrokerAvg: %.2fpp}",
                         _numAliveDisks, _numUnbalancedDisks, _numDisksAboveCapacityLimit, _maxDiskUtilizationPct,
                         _maxDiskUtilizationBrokerId, _maxDiskUtilizationLogdir, _maxDiskUtilizationSpreadPct,
                         _maxDiskUtilizationSpreadBrokerId == null ? "" : String.format(" (broker %d)", _maxDiskUtilizationSpreadBrokerId),
                         _diskUtilizationStDevPct);
  }
}
