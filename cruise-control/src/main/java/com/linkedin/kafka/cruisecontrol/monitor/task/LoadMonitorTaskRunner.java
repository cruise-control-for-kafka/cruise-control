/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.*;


/**
 * A class that is responsible for running all the LoadMonitor tasks.
 */
public class LoadMonitorTaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LoadMonitorTaskRunner.class);
  public static final MetricSampler.SamplingMode DEFAULT_SAMPLING_MODE = MetricSampler.SamplingMode.ALL;
  private final Time _time;
  private final MetricFetcherManager _metricFetcherManager;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  private final MetadataClient _metadataClient;
  private final SampleStore _sampleStore;
  private final SampleStore _sampleStoreForPartitionMetricOnExecution;
  private final ScheduledExecutorService _samplingScheduler;
  private final long _samplingIntervalMs;
  // The following two configuration is actually for MetricSampleAggregator, the MetricFetcherManager uses it to
  // check if a bootstrap is done or not.
  private final int _configuredNumWindows;
  private final long _configuredWindowMs;

  private final AtomicReference<LoadMonitorTaskRunnerState> _state;
  private volatile double _bootstrapProgress;
  private volatile boolean _awaitingPauseSampling;
  // The reason for pausing or resuming metric sampling.
  private volatile String _reasonOfLatestPauseOrResume;
  private volatile MetricSampler.SamplingMode _samplingMode;

  public enum LoadMonitorTaskRunnerState {
    NOT_STARTED, RUNNING, PAUSED, SAMPLING, BOOTSTRAPPING, TRAINING, LOADING
  }

  /**
   * Private constructor to avoid duplicate code.
   *
   * @param config The load monitor configurations.
   * @param partitionMetricSampleAggregator The {@link KafkaPartitionMetricSampleAggregator} to aggregate partition metrics.
   * @param brokerMetricSampleAggregator The {@link KafkaBrokerMetricSampleAggregator} to aggregate broker metrics.
   * @param metadataClient The metadata of the cluster.
   * @param metricDef The metric definitions.
   * @param time The time object.
   * @param dropwizardMetricRegistry The metric registry that holds all the metrics for monitoring Cruise Control.
   * @param brokerCapacityConfigResolver The resolver for retrieving broker capacities.
   */
  public LoadMonitorTaskRunner(KafkaCruiseControlConfig config,
                               KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                               KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                               MetadataClient metadataClient,
                               MetricDef metricDef,
                               Time time,
                               MetricRegistry dropwizardMetricRegistry,
                               BrokerCapacityConfigResolver brokerCapacityConfigResolver) {
    this(config,
        new MetricFetcherManager(config, partitionMetricSampleAggregator, brokerMetricSampleAggregator, metadataClient,
                                 metricDef, time, dropwizardMetricRegistry, brokerCapacityConfigResolver),
        partitionMetricSampleAggregator,
        brokerMetricSampleAggregator,
        metadataClient,
        time);
  }

  /**
   * Package private constructor for unit test duplicate code.
   *
   * @param config The load monitor configurations.
   * @param metricFetcherManager the metric fetcher manager.
   * @param partitionMetricSampleAggregator The {@link KafkaPartitionMetricSampleAggregator} to aggregate partition metrics.
   * @param brokerMetricSampleAggregator The {@link KafkaBrokerMetricSampleAggregator} to aggregate broker metrics.
   * @param metadataClient The metadata of the cluster.
   * @param time The time object.
   */
  LoadMonitorTaskRunner(KafkaCruiseControlConfig config,
                        MetricFetcherManager metricFetcherManager,
                        KafkaPartitionMetricSampleAggregator partitionMetricSampleAggregator,
                        KafkaBrokerMetricSampleAggregator brokerMetricSampleAggregator,
                        MetadataClient metadataClient,
                        Time time) {
    _time = time;
    _metricFetcherManager = metricFetcherManager;
    _partitionMetricSampleAggregator = partitionMetricSampleAggregator;
    _brokerMetricSampleAggregator = brokerMetricSampleAggregator;
    _metadataClient = metadataClient;
    _sampleStore = config.getConfiguredInstance(MonitorConfig.SAMPLE_STORE_CLASS_CONFIG, SampleStore.class);
    _sampleStoreForPartitionMetricOnExecution =
        config.getConfiguredInstance(MonitorConfig.SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS_CONFIG, SampleStore.class);
    long samplingIntervalMs = config.getLong(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG);

    _samplingScheduler =
        Executors.newScheduledThreadPool(2, new KafkaCruiseControlThreadFactory("SamplingScheduler", true, LOG));
    _samplingIntervalMs = samplingIntervalMs;
    _configuredNumWindows = config.getInt(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
    _configuredWindowMs = config.getLong(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);

    _state = new AtomicReference<>(NOT_STARTED);
    _bootstrapProgress = -1.0;
    _awaitingPauseSampling = false;
    _reasonOfLatestPauseOrResume = null;
    _samplingMode = DEFAULT_SAMPLING_MODE;
  }

  /**
   * Bootstrap to load the workload snapshot from the stored MetricSamples from external source.
   * This function does not refresh metadata and does not retry.
   *
   * @param startMs the starting time of the period to bootstrap.
   * @param endMs the end time of the period to bootstrap.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(long startMs, long endMs, boolean clearMetrics) {

    if (_state.compareAndSet(RUNNING, BOOTSTRAPPING)) {
      _samplingScheduler.execute(new BootstrapTask(startMs, endMs, clearMetrics, _metadataClient,
          _partitionMetricSampleAggregator,
                                                  this, _metricFetcherManager, _sampleStore, _configuredNumWindows,
                                                  _configuredWindowMs, _samplingIntervalMs, _time));
    } else {
      throw new IllegalStateException("Cannot bootstrap because the load monitor in " + _state.get() + " state.");
    }
  }

  /**
   * Bootstrap to load the workload snapshot from the stored MetricSamples from external source.
   * This function does not refresh metadata and does not retry.
   *
   * @param startMs the starting time of the period to bootstrap.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(long startMs, boolean clearMetrics) {

    if (_state.compareAndSet(RUNNING, BOOTSTRAPPING)) {
      _samplingScheduler.execute(new BootstrapTask(startMs, clearMetrics, _metadataClient,
          _partitionMetricSampleAggregator,
                                                  this, _metricFetcherManager, _sampleStore, _configuredNumWindows,
                                                  _configuredWindowMs, _samplingIntervalMs, _time));
    } else {
      throw new IllegalStateException("Cannot bootstrap because the load monitor in " + _state.get() + " state.");
    }
  }

  /**
   * Bootstrap to load the workload snapshot from the stored MetricSamples from external source.
   * This function does not refresh metadata and does not retry.
   *
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(boolean clearMetrics) {
    if (_state.compareAndSet(RUNNING, BOOTSTRAPPING)) {
      _samplingScheduler.execute(new BootstrapTask(clearMetrics, _metadataClient, _partitionMetricSampleAggregator,
                                                  this, _metricFetcherManager, _sampleStore, _configuredNumWindows,
                                                  _configuredWindowMs, _samplingIntervalMs, _time));
    } else {
      throw new IllegalStateException("Cannot bootstrap because the load monitor is in " + _state.get() + " state.");
    }
  }

  /**
   * @return The bootstrap progress.
   */
  public double bootStrapProgress() {
    return _bootstrapProgress;
  }

  /**
   * Load the samples from sample store. This task has to be executed before the load monitor actually starts.
   */
  private void loadSamples() {
    if (_state.compareAndSet(RUNNING, LOADING)) {
      _samplingScheduler.execute(new SampleLoadingTask(_sampleStore,
                                                      _partitionMetricSampleAggregator,
                                                      _brokerMetricSampleAggregator,
                                                      this));
    } else {
      throw new IllegalStateException("Cannot load samples because the load monitor is in "
                                          + _state.get() + " state.");
    }
  }

  public double sampleLoadingProgress() {
    return _sampleStore.sampleLoadingProgress();
  }

  /**
   * Train the cluster model
   * @param startMs the starting time of the training period.
   * @param endMs the end time of the training period.
   */
  public void train(long startMs, long endMs) {
    if (_state.compareAndSet(RUNNING, TRAINING)) {
      _samplingScheduler.execute(new TrainingTask(_time, this, _metricFetcherManager, _sampleStore,
                                                 _configuredWindowMs, _samplingIntervalMs, startMs, endMs));
    } else {
      throw new IllegalStateException("Cannot start model training because the load monitor is in "
                                          + _state.get() + " state.");
    }
  }

  /**
   * @return The state of the task runner.
   */
  public LoadMonitorTaskRunnerState state() {
    return _state.get();
  }

  /**
   * Start the metric fetchers and sampling scheduler thread.
   *
   * @param skipLoadingSamples whether skip the sampling loading task or not.
   */
  public void start(boolean skipLoadingSamples) {
    if (!_state.compareAndSet(NOT_STARTED, RUNNING)) {
      throw new IllegalStateException("Cannot start the task runner because the load monitor is in "
                                          + _state.get() + " state.");
    }
    if (!skipLoadingSamples) {
      loadSamples();
    }
    _samplingScheduler.scheduleAtFixedRate(new SamplingTask(_samplingIntervalMs, _metadataClient,
                                                            this, _metricFetcherManager, _sampleStore,
                                                            _sampleStoreForPartitionMetricOnExecution, _time),
                                           0L,
                                           _samplingIntervalMs,
                                           TimeUnit.MILLISECONDS);
  }

  /**
   * Shutdown the task runner.
   */
  public void shutdown() {
    LOG.info("Shutting down load monitor task runner.");
    _samplingScheduler.shutdown();

    try {
      _samplingScheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
      if (!_samplingScheduler.isTerminated()) {
        LOG.warn("The sampling scheduler failed to shutdown in {} ms.", _samplingIntervalMs);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for metric fetcher manager to shutdown.");
    }
    _metricFetcherManager.shutdown();
    try {
      _sampleStore.close();
    } catch (Exception e) {
      LOG.warn("Received exception when closing sample store.", e);
    }
    LOG.info("Load monitor task runner shutdown completed.");
  }

  /**
   * Pause the scheduled sampling tasks.
   * Note if load monitor is still in loading state, the method will be a noop.
   *
   * @param reason The reason for pausing metric sampling.
   * @param forcePauseSampling {@code true} to block metric sampler from starting another {@link LoadMonitorTaskRunnerState#SAMPLING}
   * in case this pause request fails.
   */
  public synchronized void pauseSampling(String reason, boolean forcePauseSampling) {
    if (_state.get() == LOADING) {
      LOG.info("Skip pause sampling since load monitor is in loading state");
      return;
    }
    if (_state.get() != PAUSED && !_state.compareAndSet(RUNNING, PAUSED)) {
      _awaitingPauseSampling = forcePauseSampling;
      throw new IllegalStateException("Cannot pause the load monitor because it is in " + _state.get() + " state.");
    } else {
      _awaitingPauseSampling = false;
      setReasonOfLatestPauseOrResume(reason);
    }
  }

  /**
   * Resume the scheduled sampling tasks.
   * Note if load monitor is still in loading state, the method will be a noop.
   *
   * @param reason The reason for resuming metric sampling.
   */
  public synchronized void resumeSampling(String reason) {
    if (_state.get() == LOADING) {
      LOG.info("Skip resume sampling since load monitor is in loading state");
      return;
    }
    if (_state.get() != RUNNING && !_state.compareAndSet(PAUSED, RUNNING)) {
      throw new IllegalStateException("Cannot resume the load monitor because it is in " + _state.get() + " state");
    }
    setReasonOfLatestPauseOrResume(reason);
  }

  void setReasonOfLatestPauseOrResume(String reason) {
    _reasonOfLatestPauseOrResume = reason;
  }

  /**
   * @return The reason for pausing metric sampling.
   */
  public String reasonOfLatestPauseOrResume() {
    return _reasonOfLatestPauseOrResume;
  }

  /**
   * Allow tasks to know if another thread, e.g. executor, is waiting on sampling to pause.
   * @return {@code true} if another thread is waiting on sampling to pause, {@code false} otherwise.
   */
  public boolean awaitingPauseSampling() {
    return _awaitingPauseSampling;
  }

  /**
   * Set the mode of metric sampling that will take action on the next metric sampling.
   * @param samplingMode Mode of metric sampling.
   */
  public void setSamplingMode(MetricSampler.SamplingMode samplingMode) {
    _samplingMode = samplingMode;
  }

  /**
   * @return The current mode of metric sampling.
   */
  public MetricSampler.SamplingMode samplingMode() {
    return _samplingMode;
  }

  boolean compareAndSetState(LoadMonitorTaskRunnerState expectedState, LoadMonitorTaskRunnerState newState) {
    return _state.compareAndSet(expectedState, newState);
  }

  void setBootstrapProgress(double progress) {
    _bootstrapProgress = progress;
  }
}
