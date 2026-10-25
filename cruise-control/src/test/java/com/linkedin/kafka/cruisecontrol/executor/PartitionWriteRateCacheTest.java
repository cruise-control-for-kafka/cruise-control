/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.Extrapolation;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionWriteRateCacheTest {
  private static final long REFRESH_INTERVAL_MS = 60000L;
  private static final double BYTES_PER_KB = 1024.0;
  private final TopicPartition _t0p0 = new TopicPartition(TOPIC0, 0);
  private final TopicPartition _t0p1 = new TopicPartition(TOPIC0, 1);
  private final TopicPartition _t0p2 = new TopicPartition(TOPIC0, 2);

  @Test
  public void testUpdateLastKnown() {
    Map<TopicPartition, Double> lastKnown = new HashMap<>();
    Map<PartitionEntity, ValuesAndExtrapolations> current = new HashMap<>();
    current.put(new PartitionEntity(_t0p0), valuesAndExtrapolations(10.0, null));
    current.put(new PartitionEntity(_t0p1), valuesAndExtrapolations(0.0, Extrapolation.NO_VALID_EXTRAPOLATION));
    current.put(new PartitionEntity(_t0p2), valuesAndExtrapolations(3.0, Extrapolation.FORCED_INSUFFICIENT));
    // A valid value updates the last known write rate, a partition without a valid value and history is omitted.
    Map<TopicPartition, Double> writeRates = PartitionWriteRateCache.updateLastKnown(current, lastKnown);
    assertEquals(Map.of(_t0p0, 10.0 * BYTES_PER_KB, _t0p2, 3.0 * BYTES_PER_KB), writeRates);
    assertEquals(writeRates, lastKnown);

    // Right after the window rolled, partitions without a sample in the new window keep their last known write rate.
    current.put(new PartitionEntity(_t0p0), valuesAndExtrapolations(0.0, Extrapolation.NO_VALID_EXTRAPOLATION));
    current.put(new PartitionEntity(_t0p1), valuesAndExtrapolations(5.0, Extrapolation.AVG_AVAILABLE));
    current.put(new PartitionEntity(_t0p2), valuesAndExtrapolations(0.0, null));
    writeRates = PartitionWriteRateCache.updateLastKnown(current, lastKnown);
    assertEquals(Map.of(_t0p0, 10.0 * BYTES_PER_KB, _t0p1, 5.0 * BYTES_PER_KB, _t0p2, 0.0), writeRates);
    // The returned write rates are a copy.
    lastKnown.clear();
    assertEquals(3, writeRates.size());
  }

  @Test
  public void testRefreshInterval() {
    MockTime time = new MockTime(0L, 0L, 0L);
    Map<PartitionEntity, ValuesAndExtrapolations> current = new HashMap<>();
    current.put(new PartitionEntity(_t0p0), valuesAndExtrapolations(10.0, null));
    PartitionWriteRateCache cache = new PartitionWriteRateCache(time, REFRESH_INTERVAL_MS, () -> current);
    assertEquals(Map.of(_t0p0, 10.0 * BYTES_PER_KB), cache.writeRatesInBytesPerSec());

    // Not refreshed before the refresh interval passes.
    current.put(new PartitionEntity(_t0p0), valuesAndExtrapolations(20.0, null));
    time.sleep(REFRESH_INTERVAL_MS - 1);
    assertEquals(Map.of(_t0p0, 10.0 * BYTES_PER_KB), cache.writeRatesInBytesPerSec());
    // Refreshed once the refresh interval passed.
    time.sleep(1);
    assertEquals(Map.of(_t0p0, 20.0 * BYTES_PER_KB), cache.writeRatesInBytesPerSec());

    // Reset forgets the cached and last known write rates.
    current.put(new PartitionEntity(_t0p0), valuesAndExtrapolations(0.0, Extrapolation.NO_VALID_EXTRAPOLATION));
    cache.reset();
    assertTrue(cache.writeRatesInBytesPerSec().isEmpty());
  }

  private static ValuesAndExtrapolations valuesAndExtrapolations(double leaderBytesInKBps, Extrapolation extrapolation) {
    MetricValues leaderBytesIn = new MetricValues(1);
    leaderBytesIn.set(0, leaderBytesInKBps);
    Map<Short, MetricValues> valuesByMetricId = new HashMap<>();
    valuesByMetricId.put(KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN), leaderBytesIn);
    Map<Integer, Extrapolation> extrapolations = new HashMap<>();
    if (extrapolation != null) {
      extrapolations.put(0, extrapolation);
    }
    return new ValuesAndExtrapolations(new AggregatedMetricValues(valuesByMetricId), extrapolations);
  }
}
