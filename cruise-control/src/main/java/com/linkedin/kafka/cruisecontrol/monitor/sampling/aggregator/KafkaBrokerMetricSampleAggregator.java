/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregationOptions;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The metrics sample aggregator for brokers.
 *
 * @see MetricSampleAggregator
 */
public class KafkaBrokerMetricSampleAggregator extends MetricSampleAggregator<String, BrokerEntity> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerMetricSampleAggregator.class);
  private static final double MIN_VALID_BROKER_RATIO = 0.0;
  private static final double MIN_VALID_GROUP_RATIO = 0.0;
  private static final int MIN_VALID_WINDOWS = 1;
  private static final boolean INCLUDE_INVALID_ENTITIES = false;

  private final int _maxAllowedExtrapolationsPerBroker;
  /**
   * Construct the metric sample aggregator.
   *
   * @param config The load monitor configurations.
   */
  public KafkaBrokerMetricSampleAggregator(KafkaCruiseControlConfig config) {
    super(config.getInt(MonitorConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG),
          config.getLong(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG),
          config.getInt(MonitorConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG).byteValue(),
          config.getInt(MonitorConfig.BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG),
          KafkaMetricDef.brokerMetricDef());
    _maxAllowedExtrapolationsPerBroker =
        config.getInt(MonitorConfig.MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG);
    _sampleType = SampleType.BROKER;
  }

  /**
   * Aggregate the metrics for the given brokers.
   * @param brokerEntities the set of brokers to aggregate.
   * @return Metric sample aggregation result for brokers.
   */
  public MetricSampleAggregationResult<String, BrokerEntity> aggregate(Set<BrokerEntity> brokerEntities) {
    AggregationOptions<String, BrokerEntity> aggregationOptions =
        new AggregationOptions<>(MIN_VALID_BROKER_RATIO, MIN_VALID_GROUP_RATIO, MIN_VALID_WINDOWS,
                                 _maxAllowedExtrapolationsPerBroker, brokerEntities,
                                 AggregationOptions.Granularity.ENTITY, INCLUDE_INVALID_ENTITIES);
    if (super.numAvailableWindows() < 1) {
      LOG.trace("No window is available for any broker.");
      return new MetricSampleAggregationResult<>(generation(),
                                                 completeness(-1, System.currentTimeMillis(), aggregationOptions));
    }
    try {
      return aggregate(-1, System.currentTimeMillis(), aggregationOptions);
    } catch (NotEnoughValidWindowsException e) {
      return new MetricSampleAggregationResult<>(generation(),
                                                 completeness(-1, System.currentTimeMillis(), aggregationOptions));
    }
  }
}
