/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.JsonFileTopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.UserTaskManagerConfig;
import com.linkedin.kafka.cruisecontrol.detector.NoopTopicAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.executor.persistence.NoopExecutionStateStore;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.fail;


/**
 * A test util class.
 */
public final class KafkaCruiseControlUnitTestUtils {
  public static final int ANOMALY_DETECTOR_INITIAL_QUEUE_SIZE = 10;
  public static final String OPENAPI_SPEC_PATH = System.getProperty("user.dir") + "/src/main/resources/yaml/base.yaml";
  public static final String CRUISE_CONTROL_PACKAGE = "com.linkedin.kafka.cruisecontrol";
  public static final String JSON_CONTENT_TYPE = "application/json";
  public static final String PLAIN_TEXT_CONTENT_TYPE = "text/plain";

  private KafkaCruiseControlUnitTestUtils() {

  }

  /**
   * @return Kafka Cruise Control properties.
   */
  public static Properties getKafkaCruiseControlProperties() {
    Properties props = new Properties();
    String capacityConfigFile = Objects.requireNonNull(KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource(
            TestConstants.DEFAULT_BROKER_CAPACITY_CONFIG_FILE)).getFile();
    String clusterConfigsFile = Objects.requireNonNull(KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource(
            TestConstants.DEFAULT_CLUSTER_CONFIGS_FILE)).getFile();
    String brokerSetsDataFile = Objects.requireNonNull(KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource(
            TestConstants.DEFAULT_BROKER_SET_RESOLVER_FILE)).getFile();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(JsonFileTopicConfigProvider.CLUSTER_CONFIGS_FILE, clusterConfigsFile);
    props.setProperty(MonitorConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
                      Integer.toString(MonitorConfig.DEFAULT_MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW));
    props.setProperty(MonitorConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG,
                      Integer.toString(MonitorConfig.DEFAULT_MIN_SAMPLES_PER_BROKER_METRICS_WINDOW));
    props.setProperty(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG,
                      Long.toString(ExecutorConfig.DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS));
    props.setProperty(UserTaskManagerConfig.COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(6)));
    props.setProperty(ExecutorConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(24)));
    props.setProperty(ExecutorConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(12)));
    props.setProperty(AnalyzerConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, "2.0");
    props.setProperty(AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                      Boolean.toString(AnomalyDetectorConfig.DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG));
    props.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                      Boolean.toString(AnomalyDetectorConfig.DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                      Boolean.toString(AnomalyDetectorConfig.DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG));
    props.setProperty(AnomalyDetectorConfig.TOPIC_ANOMALY_FINDER_CLASSES_CONFIG, NoopTopicAnomalyFinder.class.getName());
    props.setProperty(AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG, "");
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    props.setProperty(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, brokerSetsDataFile);
    props.setProperty(ExecutorConfig.EXECUTOR_STATE_STORE_CLASS_CONFIG, NoopExecutionStateStore.class.getName());

    return props;
  }

  /**
   * @param cpuUsage CPU usage.
   * @param networkInBoundUsage Network inbound usage.
   * @param networkOutBoundUsage Network outbound usage.
   * @param diskUsage Disk usage.
   * @return The aggregated metric values with the given resource usage.
   */
  public static AggregatedMetricValues getAggregatedMetricValues(double cpuUsage,
                                                                 double networkInBoundUsage,
                                                                 double networkOutBoundUsage,
                                                                 double diskUsage) {
    AggregatedMetricValues aggregateMetricValues = new AggregatedMetricValues();
    setValueForResource(aggregateMetricValues, Resource.CPU, cpuUsage);
    setValueForResource(aggregateMetricValues, Resource.NW_IN, networkInBoundUsage);
    setValueForResource(aggregateMetricValues, Resource.NW_OUT, networkOutBoundUsage);
    setValueForResource(aggregateMetricValues, Resource.DISK, diskUsage);
    return aggregateMetricValues;
  }

  /**
   * Wait until the given condition is {@code true} or throw an exception if the given wait time elapses.
   *
   * @param condition The condition to check
   * @param errorMessage Error message.
   * @param waitTimeMs The maximum time to wait in milliseconds.
   * @param pauseMs The delay between condition checks in milliseconds.
   */
  public static void waitUntilTrue(Supplier<Boolean> condition, String errorMessage, long waitTimeMs, long pauseMs) {
    long startTime = System.currentTimeMillis();
    long pausePeriodMs = Math.min(waitTimeMs, pauseMs);
    while (!condition.get()) {
      if (System.currentTimeMillis() > startTime + waitTimeMs) {
        fail(errorMessage);
      }
      try {
        Thread.sleep(pausePeriodMs);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Set the utilization values of all metrics for a resource in the given AggregatedMetricValues.
   * The first metric has the full resource utilization value, all the rest of the metrics has 0.
   *
   * @param aggregatedMetricValues Aggregated metric values.
   * @param resource Resource for which the metric value will be set.
   * @param value The metric value to be used as the utilization value.
   */
  public static void setValueForResource(AggregatedMetricValues aggregatedMetricValues,
                                         Resource resource,
                                         double value) {
    boolean set = false;
    for (short id : KafkaMetricDef.resourceToMetricIds(resource)) {
      MetricValues metricValues = new MetricValues(1);
      if (!set) {
        metricValues.set(0, value);
        set = true;
      }
      aggregatedMetricValues.add(id, metricValues);
    }
  }
}
