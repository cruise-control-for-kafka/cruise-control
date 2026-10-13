/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;


/**
 * Statistics about the balance between the disks of each broker -- i.e. the balance that intra-broker goals optimize --
 * grouped by the metric they are about (currently only the disk usage, see {@link DiskUsageStats}). These stats are relevant
 * only if the cluster model has disk information.
 *
 * Only alive disks with a positive capacity are taken into account: a disk marked for removal (or configured with no
 * capacity) is being emptied, and its utilization percentage is not defined.
 */
@JsonResponseClass
public final class DiskBalanceStats {
  @JsonResponseField
  private static final String NUM_ALIVE_DISKS = "numAliveDisks";
  @JsonResponseField
  private static final String USAGE = "usage";
  private final int _numAliveDisks;
  private final DiskUsageStats _usage;

  private DiskBalanceStats(int numAliveDisks, DiskUsageStats usage) {
    _numAliveDisks = numAliveDisks;
    _usage = usage;
  }

  /**
   * Compute the disk balance stats of the given brokers.
   *
   * @param brokers Alive brokers, in the order to report ties in.
   * @param balancingConstraint Balancing constraint.
   * @return The disk balance stats, or {@code null} if the brokers have no alive disk with a positive capacity (e.g. if the
   * cluster model has no disk information).
   */
  static DiskBalanceStats compute(Iterable<Broker> brokers, BalancingConstraint balancingConstraint) {
    double balancePercentageWithMargin = IntraBrokerDiskUsageDistributionGoal.balancePercentageWithMargin(balancingConstraint);
    double capacityThreshold = balancingConstraint.capacityThreshold(Resource.DISK);
    int numAliveDisks = 0;
    int numUnbalancedDisks = 0;
    int numDisksAboveCapacityLimit = 0;
    double totalDiskUtilization = 0.0;
    Disk maxDisk = null;
    double maxDiskUtilization = -1.0;
    Disk minDisk = null;
    double minDiskUtilization = Double.MAX_VALUE;
    double maxSpreadWithinBroker = 0.0;
    Integer maxSpreadBrokerId = null;
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
      double upperLimit = brokerDiskUtilization * (1 + balancePercentageWithMargin);
      double lowerLimit = brokerDiskUtilization * Math.max(0, 1 - balancePercentageWithMargin);
      double minBrokerDiskUtilization = Double.MAX_VALUE;
      double maxBrokerDiskUtilization = -Double.MAX_VALUE;
      for (Disk disk : disks) {
        double diskUtilization = disk.utilization() / disk.capacity();
        numAliveDisks++;
        totalDiskUtilization += diskUtilization;
        if (diskUtilization > upperLimit || diskUtilization < lowerLimit) {
          numUnbalancedDisks++;
        }
        if (diskUtilization > capacityThreshold) {
          numDisksAboveCapacityLimit++;
        }
        if (diskUtilization > maxDiskUtilization) {
          maxDiskUtilization = diskUtilization;
          maxDisk = disk;
        }
        if (diskUtilization < minDiskUtilization) {
          minDiskUtilization = diskUtilization;
          minDisk = disk;
        }
        minBrokerDiskUtilization = Math.min(minBrokerDiskUtilization, diskUtilization);
        maxBrokerDiskUtilization = Math.max(maxBrokerDiskUtilization, diskUtilization);
        totalDiskUtilizationVariance += Math.pow(diskUtilization - brokerDiskUtilization, 2);
      }
      if (disks.size() > 1 && (maxSpreadBrokerId == null || maxBrokerDiskUtilization - minBrokerDiskUtilization > maxSpreadWithinBroker)) {
        maxSpreadWithinBroker = maxBrokerDiskUtilization - minBrokerDiskUtilization;
        maxSpreadBrokerId = broker.id();
      }
    }
    if (numAliveDisks == 0) {
      return null;
    }
    DiskUsageStats usage = new DiskUsageStats(new DiskUtilizationValue(totalDiskUtilization / numAliveDisks, null, null),
                                              new DiskUtilizationValue(maxDiskUtilization, maxDisk.broker().id(), maxDisk.logDir()),
                                              new DiskUtilizationValue(minDiskUtilization, minDisk.broker().id(), minDisk.logDir()),
                                              new DiskUtilizationValue(Math.sqrt(totalDiskUtilizationVariance / numAliveDisks), null, null),
                                              new DiskUtilizationValue(maxSpreadWithinBroker, maxSpreadBrokerId, null),
                                              numUnbalancedDisks,
                                              numDisksAboveCapacityLimit);
    return new DiskBalanceStats(numAliveDisks, usage);
  }

  /**
   * @return The number of alive disks with a positive capacity.
   */
  public int numAliveDisks() {
    return _numAliveDisks;
  }

  /**
   * @return Stats about the disk usage.
   */
  public DiskUsageStats usage() {
    return _usage;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    return Map.of(NUM_ALIVE_DISKS, _numAliveDisks, USAGE, _usage.getJsonStructure());
  }

  @Override
  public String toString() {
    return String.format("DISKS:{alive: %d}%n%s", _numAliveDisks, _usage);
  }

  /**
   * Stats about the disk usage, i.e. the share of the capacity of each disk that its replicas use, in percent.
   * <ul>
   *   <li>{@code AVG}, {@code MAX}, {@code MIN}: the average, highest and lowest utilization percentage of the disks -- the
   *   latter two with the broker and logdir of the disk.</li>
   *   <li>{@code STD}: the standard deviation of the utilization percentage of each disk from the utilization percentage of its
   *   broker -- i.e. what {@link IntraBrokerDiskUsageDistributionGoal} reduces -- in percentage points.</li>
   *   <li>{@code maxSpreadWithinBroker}: the largest difference between the highest and the lowest utilization percentage of the
   *   disks of a broker, in percentage points, with the broker (if a broker has at least two disks).</li>
   *   <li>The number of unbalanced disks, i.e. the disks that {@link IntraBrokerDiskUsageDistributionGoal} would move replicas
   *   to or from (see {@link IntraBrokerDiskUsageDistributionGoal#balancePercentageWithMargin(BalancingConstraint)}), and the
   *   number of disks above the capacity limit
   *   ({@link com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#DISK_CAPACITY_THRESHOLD_CONFIG}).</li>
   * </ul>
   */
  @JsonResponseClass
  @JsonResponseExternalFields(Statistic.class)
  public static final class DiskUsageStats {
    @JsonResponseField
    private static final String MAX_SPREAD_WITHIN_BROKER = "maxSpreadWithinBroker";
    @JsonResponseField
    private static final String NUM_UNBALANCED_DISKS = "numUnbalancedDisks";
    @JsonResponseField
    private static final String NUM_DISKS_ABOVE_CAPACITY_LIMIT = "numDisksAboveCapacityLimit";
    private final DiskUtilizationValue _avg;
    private final DiskUtilizationValue _max;
    private final DiskUtilizationValue _min;
    private final DiskUtilizationValue _stDev;
    private final DiskUtilizationValue _maxSpreadWithinBroker;
    private final int _numUnbalancedDisks;
    private final int _numDisksAboveCapacityLimit;

    private DiskUsageStats(DiskUtilizationValue avg,
                           DiskUtilizationValue max,
                           DiskUtilizationValue min,
                           DiskUtilizationValue stDev,
                           DiskUtilizationValue maxSpreadWithinBroker,
                           int numUnbalancedDisks,
                           int numDisksAboveCapacityLimit) {
      _avg = avg;
      _max = max;
      _min = min;
      _stDev = stDev;
      _maxSpreadWithinBroker = maxSpreadWithinBroker;
      _numUnbalancedDisks = numUnbalancedDisks;
      _numDisksAboveCapacityLimit = numDisksAboveCapacityLimit;
    }

    /**
     * @return The average utilization percentage of the disks.
     */
    public DiskUtilizationValue avg() {
      return _avg;
    }

    /**
     * @return The highest utilization percentage of a disk, with its broker and logdir.
     */
    public DiskUtilizationValue max() {
      return _max;
    }

    /**
     * @return The lowest utilization percentage of a disk, with its broker and logdir.
     */
    public DiskUtilizationValue min() {
      return _min;
    }

    /**
     * @return The standard deviation of the utilization percentage of each disk from the one of its broker, in percentage points.
     */
    public DiskUtilizationValue stDev() {
      return _stDev;
    }

    /**
     * @return The largest difference between the utilization percentages of the disks of a broker, in percentage points, with the
     * broker -- or without broker if no broker has at least two disks.
     */
    public DiskUtilizationValue maxSpreadWithinBroker() {
      return _maxSpreadWithinBroker;
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
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
      Map<String, Object> diskUsageStats = new HashMap<>();
      diskUsageStats.put(Statistic.AVG.stat(), _avg.getJsonStructure());
      diskUsageStats.put(Statistic.MAX.stat(), _max.getJsonStructure());
      diskUsageStats.put(Statistic.MIN.stat(), _min.getJsonStructure());
      diskUsageStats.put(Statistic.ST_DEV.stat(), _stDev.getJsonStructure());
      diskUsageStats.put(MAX_SPREAD_WITHIN_BROKER, _maxSpreadWithinBroker.getJsonStructure());
      diskUsageStats.put(NUM_UNBALANCED_DISKS, _numUnbalancedDisks);
      diskUsageStats.put(NUM_DISKS_ABOVE_CAPACITY_LIMIT, _numDisksAboveCapacityLimit);
      return diskUsageStats;
    }

    @Override
    public String toString() {
      return String.format("DISK USAGE:{unbalanced: %d aboveCapacityLimit: %d maxSpreadWithinBroker: %.2fpp%s}%n"
                           + "DISK USAGE %s:{utilization: %6.2f%%}%n"
                           + "DISK USAGE %s:{utilization: %6.2f%% (broker %d %s)}%n"
                           + "DISK USAGE %s:{utilization: %6.2f%% (broker %d %s)}%n"
                           + "DISK USAGE %s:{utilization: %6.2fpp}",
                           _numUnbalancedDisks, _numDisksAboveCapacityLimit, _maxSpreadWithinBroker.utilizationPct(),
                           _maxSpreadWithinBroker.brokerId() == null ? "" : String.format(" (broker %d)", _maxSpreadWithinBroker.brokerId()),
                           Statistic.AVG, _avg.utilizationPct(),
                           Statistic.MAX, _max.utilizationPct(), _max.brokerId(), _max.logdir(),
                           Statistic.MIN, _min.utilizationPct(), _min.brokerId(), _min.logdir(),
                           Statistic.ST_DEV, _stDev.utilizationPct());
    }
  }

  /**
   * A disk utilization percentage -- or a difference between such percentages, in percentage points -- with the broker and
   * logdir of the disk it is about, if any.
   */
  @JsonResponseClass
  public static final class DiskUtilizationValue {
    @JsonResponseField
    private static final String UTILIZATION_PCT = "utilizationPct";
    @JsonResponseField(required = false)
    private static final String BROKER = "broker";
    @JsonResponseField(required = false)
    private static final String LOGDIR = "logdir";
    private final double _utilizationPct;
    private final Integer _brokerId;
    private final String _logdir;

    /**
     * @param utilization The utilization as a fraction of the disk capacity (or a difference between such fractions).
     * @param brokerId The id of the broker this value is about, or {@code null} if it is not about a broker.
     * @param logdir The logdir of the disk this value is about, or {@code null} if it is not about a disk.
     */
    private DiskUtilizationValue(double utilization, Integer brokerId, String logdir) {
      _utilizationPct = utilization * UNIT_INTERVAL_TO_PERCENTAGE;
      _brokerId = brokerId;
      _logdir = logdir;
    }

    /**
     * @return The utilization percentage, or a difference between utilization percentages in percentage points.
     */
    public double utilizationPct() {
      return _utilizationPct;
    }

    /**
     * @return The id of the broker this value is about, or {@code null} if it is not about a broker.
     */
    public Integer brokerId() {
      return _brokerId;
    }

    /**
     * @return The logdir of the disk this value is about, or {@code null} if it is not about a disk.
     */
    public String logdir() {
      return _logdir;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
      Map<String, Object> diskUtilizationValue = new HashMap<>();
      diskUtilizationValue.put(UTILIZATION_PCT, _utilizationPct);
      if (_brokerId != null) {
        diskUtilizationValue.put(BROKER, _brokerId);
      }
      if (_logdir != null) {
        diskUtilizationValue.put(LOGDIR, _logdir);
      }
      return diskUtilizationValue;
    }
  }
}
