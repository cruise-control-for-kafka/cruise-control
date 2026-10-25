/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

/**
 * The write rate of partitions (i.e. their leader bytes in rate), based on the metric values of the current window.
 * <ul>
 *   <li>The write rates are refreshed at most once per refresh interval, since new metric samples arrive at most once per
 *   metric sampling interval.</li>
 *   <li>A partition without a valid value in the current window (e.g. right after the window rolled and before its first
 *   sample arrives) keeps its last known write rate. A partition without a last known write rate is omitted.</li>
 * </ul>
 * This class is not thread safe.
 */
class PartitionWriteRateCache {
  private static final short LEADER_BYTES_IN_ID = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN);
  private static final double BYTES_PER_KB = 1024.0;
  private final Time _time;
  private final long _refreshIntervalMs;
  private final Supplier<Map<PartitionEntity, ValuesAndExtrapolations>> _currentMetricValues;
  private final Map<TopicPartition, Double> _lastKnownWriteRateByPartition;
  private Map<TopicPartition, Double> _writeRateByPartition;
  private long _lastRefreshMs;

  /**
   * @param time The time.
   * @param refreshIntervalMs The minimum interval in milliseconds between two refreshes of the write rates.
   * @param currentMetricValues The supplier of the metric values of partitions in the current window.
   */
  PartitionWriteRateCache(Time time, long refreshIntervalMs, Supplier<Map<PartitionEntity, ValuesAndExtrapolations>> currentMetricValues) {
    _time = time;
    _refreshIntervalMs = refreshIntervalMs;
    _currentMetricValues = currentMetricValues;
    _lastKnownWriteRateByPartition = new HashMap<>();
    reset();
  }

  /**
   * @return The write rate of partitions in bytes per second, refreshed if the refresh interval passed since the last refresh.
   */
  Map<TopicPartition, Double> writeRatesInBytesPerSec() {
    long nowMs = _time.milliseconds();
    if (_writeRateByPartition == null || nowMs - _lastRefreshMs >= _refreshIntervalMs) {
      _writeRateByPartition = Collections.unmodifiableMap(updateLastKnown(_currentMetricValues.get(), _lastKnownWriteRateByPartition));
      _lastRefreshMs = nowMs;
    }
    return _writeRateByPartition;
  }

  /**
   * Forget the cached and last known write rates, e.g. at the start of an execution.
   */
  void reset() {
    _lastKnownWriteRateByPartition.clear();
    _writeRateByPartition = null;
    _lastRefreshMs = -1L;
  }

  /**
   * Update the last known write rates with the valid values in the current window. A value is valid unless the current window
   * has no valid extrapolation, i.e. no sample.
   *
   * @param currentMetricValues The metric values of partitions in the current window, whose leader bytes in rate is in KB/s.
   * @param lastKnownWriteRateByPartition The last known write rate of partitions in bytes per second, which is updated.
   * @return A copy of the updated last known write rate of partitions in bytes per second.
   */
  static Map<TopicPartition, Double> updateLastKnown(Map<PartitionEntity, ValuesAndExtrapolations> currentMetricValues,
                                                     Map<TopicPartition, Double> lastKnownWriteRateByPartition) {
    for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : currentMetricValues.entrySet()) {
      ValuesAndExtrapolations valuesAndExtrapolations = entry.getValue();
      Map<Integer, Extrapolation> extrapolations = valuesAndExtrapolations.extrapolations();
      if (extrapolations != null && extrapolations.get(0) == Extrapolation.NO_VALID_EXTRAPOLATION) {
        continue;
      }
      MetricValues leaderBytesIn = valuesAndExtrapolations.metricValues().valuesFor(LEADER_BYTES_IN_ID);
      if (leaderBytesIn != null) {
        lastKnownWriteRateByPartition.put(entry.getKey().tp(), leaderBytesIn.latest() * BYTES_PER_KB);
      }
    }
    return new HashMap<>(lastKnownWriteRateByPartition);
  }
}
