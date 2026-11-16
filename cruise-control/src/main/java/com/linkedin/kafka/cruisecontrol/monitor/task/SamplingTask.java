/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.common.MetadataAdminClient;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.SampleStore;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The task responsible for metric sampling. This task runs periodically.
 */
class SamplingTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingTask.class);
  private final long _samplingIntervalMs;
  private final Time _time;
  private final MetadataAdminClient _metadataClient;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final MetricFetcherManager _metricFetcherManager;
  private final SampleStore _sampleStore;
  private final SampleStore _sampleStoreForPartitionMetricOnExecution;
  private long _lastSamplingPeriodEndTimeMs;

  SamplingTask(long samplingIntervalMs,
               MetadataAdminClient metadataClient,
               LoadMonitorTaskRunner loadMonitorTaskRunner,
               MetricFetcherManager metricFetcherManager,
               SampleStore sampleStore,
               SampleStore sampleStoreForPartitionMetricOnExecution,
               Time time) {
    _samplingIntervalMs = samplingIntervalMs;
    _time = time;
    _metadataClient = metadataClient;
    _loadMonitorTaskRunner = loadMonitorTaskRunner;
    _metricFetcherManager = metricFetcherManager;
    _sampleStore = sampleStore;
    _sampleStoreForPartitionMetricOnExecution = sampleStoreForPartitionMetricOnExecution;
    _lastSamplingPeriodEndTimeMs = _time.milliseconds() - _samplingIntervalMs;
  }

  public void run() {
    long now = _time.milliseconds();
    if (!_loadMonitorTaskRunner.awaitingPauseSampling()
        && _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING,
                                                     LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING)) {
      long samplingPeriodEndMs = now;
      try {
        boolean hasSamplingError;
        long deadline = _time.milliseconds() + _samplingIntervalMs;
        do {
          _metadataClient.cluster();
          samplingPeriodEndMs = _time.milliseconds();

          hasSamplingError = _metricFetcherManager.fetchMetricSamples(_lastSamplingPeriodEndTimeMs,
                                                                      samplingPeriodEndMs,
                                                                      deadline - now,
                                                                      _sampleStore, _sampleStoreForPartitionMetricOnExecution,
                                                                      _loadMonitorTaskRunner.samplingMode());

          if (!hasSamplingError) {
            _lastSamplingPeriodEndTimeMs = samplingPeriodEndMs;
          }
          now = _time.milliseconds();
          if (now > deadline) {
            throw new TimeoutException();
          }
        } while (hasSamplingError);
      } catch (TimeoutException e) {
        LOG.warn("Sampling did not finish in {} ms, skipping this sampling interval.", _samplingIntervalMs);
        // Advance the last sampling period end time.
        _lastSamplingPeriodEndTimeMs = samplingPeriodEndMs;
      } catch (Throwable t) {
        LOG.error("Uncaught exception in sampling", t);
        throw t;
      } finally {
        _loadMonitorTaskRunner.compareAndSetState(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING,
                                                  LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING);
      }
    } else {
      String reason = _loadMonitorTaskRunner.reasonOfLatestPauseOrResume();
      LOG.info("Skip sampling because the load monitor is in {} state{}.", _loadMonitorTaskRunner.state(),
               String.format(reason == null ? "" : " due to %s.", reason));
      // Something else is in progress, we advance the end time to avoid a big metric fetch after bootstrap finishes.
      // Otherwise we may see some memory issue.
      _lastSamplingPeriodEndTimeMs = now - _samplingIntervalMs;
    }
  }
}
