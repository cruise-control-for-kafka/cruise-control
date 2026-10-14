/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Yammer metric processor that process the yammer metrics. Currently all the interested metrics are of type
 * Meter (BytesInRate, BytesOutRate) or Gauge (Partition Size).
 */
public class YammerMetricProcessor implements MetricProcessor<YammerMetricProcessor.Context> {
  private static final Logger LOG = LoggerFactory.getLogger(YammerMetricProcessor.class);

  @Override
  public void processMeter(MetricName metricName, Metered metered, Context context) {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Meter.", metricName);
      double value;
      if (context.reportingInterval().toMillis() <= TimeUnit.MINUTES.toMillis(1)) {
        value = metered.oneMinuteRate();
      } else if (context.reportingInterval().toMillis() <= TimeUnit.MINUTES.toMillis(5)) {
        value = metered.fiveMinuteRate();
      } else {
        value = metered.fifteenMinuteRate();
      }
      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   value);
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  @Override
  public void processCounter(MetricName metricName, Counter counter, Context context) {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.warn("Not processing metric {} of type Counter.", metricName);
    }
  }

  @Override
  public void processHistogram(MetricName metricName, Histogram histogram, Context context) {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Histogram.", metricName);
      // Get max metric value
      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   histogram.max(),
                                                                   MetricsUtils.ATTRIBUTE_MAX);
      context.reporter().sendCruiseControlMetric(ccm);

      // Get mean metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               histogram.mean(),
                                               MetricsUtils.ATTRIBUTE_MEAN);
      context.reporter().sendCruiseControlMetric(ccm);

      Snapshot snapshot = histogram.getSnapshot();
      // Get 50th percentile (i.e. median) metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               snapshot.getMedian(),
                                               MetricsUtils.ATTRIBUTE_50TH_PERCENTILE);
      context.reporter().sendCruiseControlMetric(ccm);

      // Get 999th percentile metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               snapshot.get999thPercentile(),
                                               MetricsUtils.ATTRIBUTE_999TH_PERCENTILE);
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  @Override
  public void processTimer(MetricName metricName, Timer timer, Context context) {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Timer.", metricName);

      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   timer.fiveMinuteRate());
      context.reporter().sendCruiseControlMetric(ccm);
      // Get max metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               timer.max(),
                                               MetricsUtils.ATTRIBUTE_MAX);
      context.reporter().sendCruiseControlMetric(ccm);
      // Get mean metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               timer.mean(),
                                               MetricsUtils.ATTRIBUTE_MEAN);
      context.reporter().sendCruiseControlMetric(ccm);

      Snapshot snapshot = timer.getSnapshot();
      // Get 50th percentile (i.e. median) metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               snapshot.getMedian(),
                                               MetricsUtils.ATTRIBUTE_50TH_PERCENTILE);
      context.reporter().sendCruiseControlMetric(ccm);

      // Get 999th percentile metric value
      ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                               context.brokerId(),
                                               metricName,
                                               snapshot.get999thPercentile(),
                                               MetricsUtils.ATTRIBUTE_999TH_PERCENTILE);
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  @Override
  public void processGauge(MetricName metricName, Gauge<?> gauge, Context context) {
    if (MetricsUtils.isInterested(metricName)) {
      LOG.trace("Processing metric {} of type Gauge.", metricName);
      if (!(gauge.value() instanceof Number value)) {
        throw new IllegalStateException(String.format("The value of yammer metric %s is %s, which is not a number",
                                                      metricName, gauge.value()));
      }
      CruiseControlMetric ccm = MetricsUtils.toCruiseControlMetric(context.time(),
                                                                   context.brokerId(),
                                                                   metricName,
                                                                   value.doubleValue());
      context.reporter().sendCruiseControlMetric(ccm);
    }
  }

  public static final class Context {
    private final CruiseControlMetricsReporter _reporter;
    private final long _time;
    private final int _brokerId;
    private final Duration _reportingInterval;

    public Context(CruiseControlMetricsReporter reporter, long time, int brokerId, long reportingIntervalMs) {
      _reporter = reporter;
      _time = time;
      _brokerId = brokerId;
      _reportingInterval = Duration.ofMillis(reportingIntervalMs);
    }

    private CruiseControlMetricsReporter reporter() {
      return _reporter;
    }

    private long time() {
      return _time;
    }

    private int brokerId() {
      return _brokerId;
    }

    private Duration reportingInterval() {
      return _reportingInterval;
    }
  }
}
