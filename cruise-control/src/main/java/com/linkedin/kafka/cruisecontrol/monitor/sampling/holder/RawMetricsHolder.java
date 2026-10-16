/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.HashMap;
import java.util.Map;


/**
 * A class that helps store all the {@link CruiseControlMetric} by their {@link RawMetricType}.
 */
class RawMetricsHolder {
  private final Map<RawMetricType, ValueHolder> _rawMetricsByType = new HashMap<>();

  /**
   * Record a cruise control metric value.
   * @param ccm the {@link CruiseControlMetric} to record.
   */
  void recordCruiseControlMetric(CruiseControlMetric ccm) {
    RawMetricType rawMetricType = ccm.rawMetricType();
    ValueHolder
        valueHolder = _rawMetricsByType.computeIfAbsent(rawMetricType, mt -> getValueHolderFor(rawMetricType));
    valueHolder.recordValue(ccm.value(), ccm.time());
  }

  /**
   * Directly set a raw metric value. The existing metric value will be discarded.
   * This method is used when we have to modify the raw metric values to unify the meaning of the metrics across
   * different Kafka versions.
   *
   * @param rawMetricType the raw metric type to set value for.
   * @param value the value to set
   * @param time the time to set
   */
  void setRawMetricValue(RawMetricType rawMetricType, double value, long time) {
    _rawMetricsByType.compute(rawMetricType, (type, vh) -> {
      ValueHolder valueHolder = vh == null ? getValueHolderFor(rawMetricType) : vh;
      valueHolder.reset();
      valueHolder.recordValue(value, time);
      return valueHolder;
    });
  }

  /**
   * Get the value for the given raw metric type.
   * @param rawMetricType the raw metric type to get value for.
   * @return The value of the given raw metric type.
   */
  ValueHolder metricValue(RawMetricType rawMetricType) {
    return _rawMetricsByType.get(rawMetricType);
  }

  private static ValueHolder getValueHolderFor(RawMetricType rawMetricType) {
    KafkaMetricDef kafkaMetricDef = KafkaMetricDef.forRawMetricType(rawMetricType);
    return switch (kafkaMetricDef.valueComputingStrategy()) {
      case AVG -> new ValueAndCount();
      case MAX -> new ValueMax();
      case LATEST -> new ValueAndTime();
    };
  }
}
