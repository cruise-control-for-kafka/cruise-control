/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.List;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A task for bootstrap with different options.
 */
class BootstrapTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapTask.class);
  public static final MetricSampler.SamplingMode DEFAULT_SAMPLING_MODE = MetricSampler.SamplingMode.ALL;
  private final Time _time;
  private final long _startMs;
  private final long _endMs;
  private final boolean _clearMetrics;
  private final BootstrapMode _mode;
  // Some configurations.
  private final long _samplingIntervalMs;
  private final int _configuredNumSnapshots;
  private final long _configuredSnapshotWindowMs;
  //
  private final MetadataClient _metadataClient;
  private final KafkaPartitionMetricSampleAggregator _metricSampleAggregator;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final MetricFetcherManager _metricFetcherManager;
  private final SampleStore _sampleStore;
  // Track the already bootstrapped range.
  private long _bootstrappedRangeStartMs;
  private long _bootstrappedRangeEndMs;

  // Constructor for RECENT mode.
  BootstrapTask(boolean clearMetrics,
                MetadataClient metadataClient,
                KafkaPartitionMetricSampleAggregator metricSampleAggregator,
                LoadMonitorTaskRunner loadMonitorTaskRunner,
                MetricFetcherManager metricFetcherManager,
                SampleStore sampleStore,
                int configuredNumSnapshots,
                long configuredSnapshotWindowMs,
                long samplingIntervalMs,
                Time time) {
    _mode = BootstrapMode.RECENT;
    _startMs = -1L;
    _endMs = -1L;
    _clearMetrics = clearMetrics;
    _metadataClient = metadataClient;
    _metricSampleAggregator = metricSampleAggregator;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    _metricFetcherManager = metricFetcherManager;
    _sampleStore = sampleStore;
    _configuredNumSnapshots = configuredNumSnapshots;
    _configuredSnapshotWindowMs = configuredSnapshotWindowMs;
    _samplingIntervalMs = samplingIntervalMs;
    _time = time;
    long now = _time.milliseconds();
    _bootstrappedRangeStartMs = now;
    _bootstrappedRangeEndMs = now;
  }

  // Constructor for SINCE mode.
  BootstrapTask(long startMs,
                boolean clearMetrics,
                MetadataClient metadataClient,
                KafkaPartitionMetricSampleAggregator metricSampleAggregator,
                LoadMonitorTaskRunner loadMonitorTaskRunner,
                MetricFetcherManager metricFetcherManager,
                SampleStore sampleStore,
                int configuredNumSnapshots,
                long configuredSnapshotWindowMs,
                long samplingIntervalMs,
                Time time) {
    if (startMs < 0) {
      throw new IllegalArgumentException(String.format("Invalid bootstrap start time %d. The bootstrap since "
                                                       + "time cannot be negative.", startMs));
    }
    _mode = BootstrapMode.SINCE;
    _startMs = startMs;
    _clearMetrics = clearMetrics;
    _endMs = -1L;
    _metadataClient = metadataClient;
    _metricSampleAggregator = metricSampleAggregator;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    _metricFetcherManager = metricFetcherManager;
    _sampleStore = sampleStore;
    _configuredNumSnapshots = configuredNumSnapshots;
    _configuredSnapshotWindowMs = configuredSnapshotWindowMs;
    _samplingIntervalMs = samplingIntervalMs;
    _time = time;
    _bootstrappedRangeStartMs = startMs;
    _bootstrappedRangeEndMs = startMs;
  }

  // Constructor for RANGE mode.
  BootstrapTask(long startMs,
                long endMs,
                boolean clearMetrics,
                MetadataClient metadataClient,
                KafkaPartitionMetricSampleAggregator metricSampleAggregator,
                LoadMonitorTaskRunner loadMonitorTaskRunner,
                MetricFetcherManager metricFetcherManager,
                SampleStore sampleStore,
                int configuredNumSnapshots,
                long configuredSnapshotWindowMs,
                long samplingIntervalMs,
                Time time) {
    if (startMs < 0 || endMs < 0 || endMs <= startMs) {
      throw new IllegalArgumentException(String.format("Invalid bootstrap time range [%d, %d]. The bootstrap end "
                                                       + "time must be non negative and the end time "
                                                       + "must be greater than start time.", startMs, endMs));
    }
    _mode = BootstrapMode.RANGE;
    _startMs = startMs;
    _endMs = endMs;
    _clearMetrics = clearMetrics;
    _metadataClient = metadataClient;
    _metricSampleAggregator = metricSampleAggregator;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    _metricFetcherManager = metricFetcherManager;
    _sampleStore = sampleStore;
    _configuredNumSnapshots = configuredNumSnapshots;
    _configuredSnapshotWindowMs = configuredSnapshotWindowMs;
    _samplingIntervalMs = samplingIntervalMs;
    _time = time;
    _bootstrappedRangeStartMs = startMs;
    _bootstrappedRangeEndMs = startMs;
  }

  private long nextSamplingPeriodStartMs(long now) {
    switch (_mode) {
      case RANGE:
      case SINCE:
        return _bootstrappedRangeEndMs;
      case RECENT:
        if (_bootstrappedRangeEndMs < now - _samplingIntervalMs) {
          // Catching up the latest sampling interval, i.e. sampling forwards.
          return _bootstrappedRangeEndMs;
        } else {
          // There is no newer sampling interval, sampling backwards.
          return _bootstrappedRangeStartMs - _samplingIntervalMs;
        }
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  private long nextSamplingPeriodEndMs(long now) {
    switch (_mode) {
      case RANGE:
        return Math.min(_endMs, _bootstrappedRangeEndMs + _samplingIntervalMs);
      case SINCE:
        return Math.min(now, _bootstrappedRangeEndMs + _samplingIntervalMs);
      case RECENT:
        if (_bootstrappedRangeEndMs < now - _samplingIntervalMs) {
          // Catching up the latest sampling interval, i.e. sampling forwards.
          return _bootstrappedRangeEndMs + _samplingIntervalMs;
        } else {
          // There is no newer sampling interval, sampling backwards.
          return _bootstrappedRangeStartMs;
        }
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  private boolean isDone(long now) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Mode: {}, Sampled range [{}, {}], now = {}, MetricSamplerAggregator snapshot windows: {}, "
                + "sampling interval: {}",
                _mode, _bootstrappedRangeStartMs, _bootstrappedRangeEndMs, now,
                _metricSampleAggregator.allWindows(), _samplingIntervalMs);
    }
    return switch (_mode) {
      case RANGE -> _bootstrappedRangeStartMs == _startMs && _bootstrappedRangeEndMs == _endMs;
      case SINCE -> _bootstrappedRangeStartMs == _startMs
              && _bootstrappedRangeEndMs > now - _samplingIntervalMs;
      case RECENT -> {
        // Because we know that the sampling range end is always up to date. As long as we have enough snapshots
        // in the metric sample aggregator and our sampled range starting time is already no later than the
        // earliest snapshot window starting time, we know the bootstrap has finished.
        List<Long> snapshotWindows = _metricSampleAggregator.allWindows();
        yield snapshotWindows.size() >= _configuredNumSnapshots + 1
              && snapshotWindows.get(0) - _configuredSnapshotWindowMs >= _bootstrappedRangeStartMs
              && _bootstrappedRangeEndMs > now - _samplingIntervalMs;
      }
    };
  }

  private double progress() {
    double progress;
    long sampledRange = (_bootstrappedRangeEndMs - _bootstrappedRangeStartMs);
    if (_mode == BootstrapMode.RANGE) {
      progress = (double) sampledRange / (_endMs - _startMs);
    } else if (_mode == BootstrapMode.SINCE) {
      progress = (double) sampledRange / (_time.milliseconds() - _startMs);
    } else {
      // RECENT mode.
      progress = (double) sampledRange / ((_configuredNumSnapshots + 1) * _configuredSnapshotWindowMs);
    }
    return progress;
  }

  public void run() {
    long bootstrapStartingMs = _time.milliseconds();
    if (_clearMetrics) {
      // Clear the metrics if user asked to do so.
      _metricSampleAggregator.clear();
    }

    if (_mode == BootstrapMode.RANGE) {
      LOG.info("Load monitor is bootstrapping for time range [{}, {}]", _startMs, _endMs);
    } else if (_mode == BootstrapMode.SINCE) {
      LOG.info("Load monitor is bootstrapping since {}", _startMs);
    } else {
      LOG.info("Load monitor is bootstrapping for most recent metric samples.");
    }

    try {
      _loadMonitorTaskRunner.setBootstrapProgress(0.0);

      _metadataClient.refreshMetadata();
      long now = _time.milliseconds();
      do {
        long samplingPeriodStartMs = nextSamplingPeriodStartMs(now);
        long samplingPeriodEndMs = nextSamplingPeriodEndMs(now);
        boolean hasSamplingError =
            _metricFetcherManager.fetchMetricSamples(samplingPeriodStartMs, samplingPeriodEndMs, _samplingIntervalMs,
                                                     _sampleStore, DEFAULT_SAMPLING_MODE);
        if (hasSamplingError) {
          LOG.warn("Bootstrap encountered error when sampling from {} to {}, skipping...", samplingPeriodStartMs,
                   samplingPeriodEndMs);
        }
        _loadMonitorTaskRunner.setBootstrapProgress(progress());
        _bootstrappedRangeStartMs = Math.min(_bootstrappedRangeStartMs, samplingPeriodStartMs);
        _bootstrappedRangeEndMs = Math.max(_bootstrappedRangeEndMs, samplingPeriodEndMs);
        now = _time.milliseconds();
      } while (!isDone(now));
    } catch (Throwable t) {
      LOG.error("Received uncaught exception in bootstrap", t);
      throw t;
    } finally {
      LOG.info("Load monitor finished bootstrapping {} metric samples in {} snapshot windows for time "
               + "range [{}, {}] in {} seconds.", _metricSampleAggregator.numSamples(),
               _metricSampleAggregator.allWindows().size(), _bootstrappedRangeStartMs, _bootstrappedRangeEndMs,
               (_time.milliseconds() - bootstrapStartingMs) / 1000);
      _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING,
                                                LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
      _loadMonitorTaskRunner.setBootstrapProgress(-1.0);
    }
  }

  /**
   * 1. RANGE: Bootstrap a given period
   * 2. SINCE: Bootstrap starting from a past timestamp until catching up with current time.
   * 3. RECENT: Bootstrap the most recent metrics until there are enough snapshots.
   *
   * NOTE: In SINCE and RECENT mode, the existing metric samples will be cleared.
   */
  private enum BootstrapMode {
    RANGE, SINCE, RECENT
  }
}
