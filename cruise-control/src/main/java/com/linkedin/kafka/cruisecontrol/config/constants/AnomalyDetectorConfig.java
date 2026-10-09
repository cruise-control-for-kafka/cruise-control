/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.detector.BasicProvisioner;
import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.IntraBrokerGoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent;
import com.linkedin.kafka.cruisecontrol.detector.NoopMaintenanceEventReader;
import com.linkedin.kafka.cruisecontrol.detector.NoopMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.NoopTopicAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.NoopNotifier;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;


/**
 * A class to keep Cruise Control Anomaly Detector Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class AnomalyDetectorConfig {
  public static final boolean DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG = true;

  /**
   * <code>metric.anomaly.finder.class</code>
   */
  public static final String METRIC_ANOMALY_FINDER_CLASSES_CONFIG = "metric.anomaly.finder.class";
  public static final String DEFAULT_METRIC_ANOMALY_FINDER_CLASS = NoopMetricAnomalyFinder.class.getName();
  public static final String METRIC_ANOMALY_FINDER_CLASSES_DOC = "A list of metric anomaly finder classes to find the current "
                                                          + "state to identify metric anomalies.";

  /**
   * <code>num.cached.recent.anomaly.states</code>
   */
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG = "num.cached.recent.anomaly.states";
  public static final int DEFAULT_NUM_CACHED_RECENT_ANOMALY_STATES = 10;
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_DOC = "The number of recent anomaly states cached for "
      + "different anomaly types presented via the anomaly substate response of the state endpoint.";

  /**
   * <code>broker.failures.class</code>
   */
  public static final String BROKER_FAILURES_CLASS_CONFIG = "broker.failures.class";
  public static final String DEFAULT_BROKER_FAILURES_CLASS = BrokerFailures.class.getName();
  public static final String BROKER_FAILURES_CLASS_DOC = "The name of class that extends broker failures.";

  /**
   * <code>goal.violations.class</code>
   */
  public static final String GOAL_VIOLATIONS_CLASS_CONFIG = "goal.violations.class";
  public static final String DEFAULT_GOAL_VIOLATIONS_CLASS = GoalViolations.class.getName();
  public static final String GOAL_VIOLATIONS_CLASS_DOC = "The name of class that extends goal violations.";

  /**
   * <code>intra.broker.goal.violations.class</code>
   */
  public static final String INTRA_BROKER_GOAL_VIOLATIONS_CLASS_CONFIG = "intra.broker.goal.violations.class";
  public static final String DEFAULT_INTRA_BROKER_GOAL_VIOLATIONS_CLASS = IntraBrokerGoalViolations.class.getName();
  public static final String INTRA_BROKER_GOAL_VIOLATIONS_CLASS_DOC = "The name of class that extends intra broker goal violations.";

  /**
   * <code>disk.failures.class</code>
   */
  public static final String DISK_FAILURES_CLASS_CONFIG = "disk.failures.class";
  public static final String DEFAULT_DISK_FAILURES_CLASS = DiskFailures.class.getName();
  public static final String DISK_FAILURES_CLASS_DOC = "The name of class that extends disk failures.";

  /**
   * <code>metric.anomaly.class</code>
   */
  public static final String METRIC_ANOMALY_CLASS_CONFIG = "metric.anomaly.class";
  public static final String DEFAULT_METRIC_ANOMALY_CLASS = KafkaMetricAnomaly.class.getName();
  public static final String METRIC_ANOMALY_CLASS_DOC = "The name of class that extends metric anomaly.";

  /**
   * <code>self.healing.goals</code>
   */
  public static final String SELF_HEALING_GOALS_CONFIG = "self.healing.goals";
  public static final List<String> DEFAULT_SELF_HEALING_GOALS = Collections.emptyList();
  public static final String SELF_HEALING_GOALS_DOC = "The list of goals to be used for self-healing relevant anomalies."
      + " If empty, uses the default.goals for self healing.";

  /**
   * <code>self.healing.intra.broker.goals</code>
   */
  public static final String SELF_HEALING_INTRA_BROKER_GOALS_CONFIG = "self.healing.intra.broker.goals";
  public static final List<String> DEFAULT_SELF_HEALING_INTRA_BROKER_GOALS = List.of(
          IntraBrokerDiskCapacityGoal.class.getName(),
          IntraBrokerDiskUsageDistributionGoal.class.getName());
  public static final String SELF_HEALING_INTRA_BROKER_GOALS_DOC = "The list of intra broker goals to be used for self-healing relevant anomalies.";

  /**
   * <code>anomaly.notifier.class</code>
   */
  public static final String ANOMALY_NOTIFIER_CLASS_CONFIG = "anomaly.notifier.class";
  public static final String DEFAULT_ANOMALY_NOTIFIER_CLASS = NoopNotifier.class.getName();
  public static final String ANOMALY_NOTIFIER_CLASS_DOC = "The notifier class to trigger an alert when an anomaly is violated.";

  /**
   * <code>anomaly.detection.goals</code>
   */
  public static final String ANOMALY_DETECTION_GOALS_CONFIG = "anomaly.detection.goals";
  public static final String DEFAULT_ANOMALY_DETECTION_GOALS = new StringJoiner(",")
          .add(RackAwareGoal.class.getName())
          .add(MinTopicLeadersPerBrokerGoal.class.getName())
          .add(ReplicaCapacityGoal.class.getName())
          .add(DiskCapacityGoal.class.getName())
          .toString();
  public static final String ANOMALY_DETECTION_GOALS_DOC = "The goals that anomaly detector should detect if they are violated.";

  /**
   * <code>anomaly.detection.intra.broker.goals</code>
   */
  public static final String ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG = "anomaly.detection.intra.broker.goals";
  public static final String DEFAULT_ANOMALY_DETECTION_INTRA_BROKER_GOALS = new StringJoiner(",")
          .add(IntraBrokerDiskCapacityGoal.class.getName())
          .add(IntraBrokerDiskUsageDistributionGoal.class.getName())
          .toString();
  public static final String ANOMALY_DETECTION_INTRA_BROKER_GOALS_DOC = "The intra broker goals that anomaly detector "
          + "should detect if they are violated.";

  /**
   * <code>self.healing.exclude.recently.demoted.brokers</code>
   */
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG = "self.healing.exclude.recently.demoted.brokers";
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC = "True if recently demoted brokers are excluded "
      + "from optimizations during self healing, false otherwise.";

  /**
   * <code>self.healing.exclude.recently.removed.brokers</code>
   */
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG = "self.healing.exclude.recently.removed.brokers";
  public static final String SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC = "True if recently removed brokers "
      + "are excluded from optimizations during self healing, false otherwise.";

  /**
   * <code>failed.brokers.file.path</code>
   */
  public static final String FAILED_BROKERS_FILE_PATH_CONFIG = "failed.brokers.file.path";
  public static final String DEFAULT_FAILED_BROKERS_FILE_PATH = "fileStore/failedBrokers.txt";
  public static final String FAILED_BROKERS_FILE_PATH_DOC = "The file path to store the failed broker list. This is to "
      + "persist the broker failure time in case Cruise Control failed and restarted when some brokers are down.";

  /**
   * <code>fixable.failed.broker.count.threshold</code>
   */
  public static final String FIXABLE_FAILED_BROKER_COUNT_THRESHOLD_CONFIG = "fixable.failed.broker.count.threshold";
  public static final String FIXABLE_FAILED_BROKER_COUNT_THRESHOLD_DOC = "The upper boundary of concurrently failed broker "
      + "count that are taken as fixable. If too many brokers are failing at the same time, it is often due to something more "
      + "fundamental going wrong and removing replicas off failed brokers cannot alleviate the situation.";
  public static final short DEFAULT_FIXABLE_FAILED_BROKER_COUNT_THRESHOLD = 10;

  /**
   * <code>fixable.failed.broker.percentage.threshold</code>
   */
  public static final String FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD_CONFIG = "fixable.failed.broker.percentage.threshold";
  public static final String FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD_DOC = "The upper boundary of concurrently failed broker "
      + "percentage that are taken as fixable. If large portion of brokers are failing at the same time, it is often due to something "
      + "more fundamental going wrong and removing replicas off failed brokers cannot alleviate the situation.";
  public static final double DEFAULT_FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD = 0.4;

  /**
   * <code>anomaly.detection.interval.ms</code>
   */
  public static final String ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "anomaly.detection.interval.ms";
  public static final long DEFAULT_ANOMALY_DETECTION_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
  public static final String ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that the detectors will "
      + "run to detect the anomalies.";

  /**
   * <code>goal.violation.detection.interval.ms</code>
   */
  public static final String GOAL_VIOLATION_DETECTION_INTERVAL_MS_CONFIG = "goal.violation.detection.interval.ms";
  public static final Long DEFAULT_GOAL_VIOLATION_DETECTION_INTERVAL_MS = null;
  public static final String GOAL_VIOLATION_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that goal violation "
      + "detector will run to detect goal violations. If this interval time is not specified, goal violation detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>metric.anomaly.detection.interval.ms</code>
   */
  public static final String METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "metric.anomaly.detection.interval.ms";
  public static final Long DEFAULT_METRIC_ANOMALY_DETECTION_INTERVAL_MS = null;
  public static final String METRIC_ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that metric anomaly "
      + "detector will run to detect metric anomalies. If this interval time is not specified, metric anomaly detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>disk.failure.detection.interval.ms</code>
   */
  public static final String DISK_FAILURE_DETECTION_INTERVAL_MS_CONFIG = "disk.failure.detection.interval.ms";
  public static final Long DEFAULT_DISK_FAILURE_DETECTION_INTERVAL_MS = null;
  public static final String DISK_FAILURE_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that disk failure "
      + "detector will run to detect disk failures. If this interval time is not specified, disk failure detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>broker.failure.detection.interval.ms</code>
   */
  public static final String BROKER_FAILURE_DETECTION_INTERVAL_MS_CONFIG = "broker.failure.detection.interval.ms";
  public static final Long DEFAULT_BROKER_FAILURE_DETECTION_INTERVAL_MS = null;
  public static final String BROKER_FAILURE_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that broker failure "
      + "detector will run to detect broker failures. If this interval time is not specified, broker failure detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG;

  /**
   * <code>broker.failure.detection.backoff.ms</code>
   */
  public static final String BROKER_FAILURE_DETECTION_BACKOFF_MS_CONFIG = "broker.failure.detection.backoff.ms";
  public static final long DEFAULT_BROKER_FAILURE_DETECTION_BACKOFF_MS = TimeUnit.MINUTES.toMillis(5);
  public static final String BROKER_FAILURE_DETECTION_BACKOFF_MS_DOC = "The backoff time in millisecond before broker failure "
      + "detector triggers another broker failure detection if currently detected broker failure is not ready to fix.";

  /**
   * <code>anomaly.detection.allow.capacity.estimation</code>
   */
  public static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = "anomaly.detection.allow.capacity.estimation";
  public static final boolean DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = true;
  public static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC = "The flag to indicate whether anomaly "
      + "detection threads allow capacity estimation in the generated cluster model they use.";

  /**
   * <code>topic.anomaly.detection.interval.ms</code>
   */
  public static final String TOPIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "topic.anomaly.detection.interval.ms";
  public static final Long DEFAULT_TOPIC_ANOMALY_DETECTION_INTERVAL_MS = null;
  public static final String TOPIC_ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that topic anomaly "
      + "detector will run to detect topic anomalies. If this interval time is not specified, topic anomaly detector "
      + "will run with interval specified in " + ANOMALY_DETECTION_INTERVAL_MS_CONFIG + ".";

  /**
   * <code>topic.anomaly.finder.class</code>
   */
  public static final String TOPIC_ANOMALY_FINDER_CLASSES_CONFIG = "topic.anomaly.finder.class";
  public static final String DEFAULT_TOPIC_ANOMALY_FINDER_CLASS = NoopTopicAnomalyFinder.class.getName();
  public static final String TOPIC_ANOMALY_FINDER_CLASSES_DOC = "A list of topic anomaly finder classes to check the current "
      + "topic state to identify topic anomalies.";

  /**
   * <code>maintenance.event.reader.class</code>
   */
  public static final String MAINTENANCE_EVENT_READER_CLASS_CONFIG = "maintenance.event.reader.class";
  public static final String DEFAULT_MAINTENANCE_EVENT_READER_CLASS = NoopMaintenanceEventReader.class.getName();
  public static final String MAINTENANCE_EVENT_READER_CLASS_DOC = "A maintenance event reader class to retrieve maintenance "
      + "events from the user-defined store.";

  /**
   * <code>maintenance.event.class</code>
   */
  public static final String MAINTENANCE_EVENT_CLASS_CONFIG = "maintenance.event.class";
  public static final String DEFAULT_MAINTENANCE_EVENT_CLASS = MaintenanceEvent.class.getName();
  public static final String MAINTENANCE_EVENT_CLASS_DOC = "The name of class that extends maintenance event.";

  /**
   * <code>maintenance.event.enable.idempotence</code>
   */
  public static final String MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_CONFIG = "maintenance.event.enable.idempotence";
  public static final boolean DEFAULT_MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE = true;
  public static final String MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_DOC = "The flag to indicate whether maintenance event detector will drop "
      + "the duplicate maintenance events detected within the configured retention period.";

  /**
   * <code>maintenance.event.idempotence.retention.ms</code>
   */
  public static final String MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_CONFIG = "maintenance.event.idempotence.retention.ms";
  public static final long DEFAULT_MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS = TimeUnit.MINUTES.toMillis(3);
  public static final String MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_DOC = "The maximum time in ms to store events retrieved from the"
      + " MaintenanceEventReader. Relevant only if idempotency is enabled (see " + MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_CONFIG + ").";

  /**
   * <code>maintenance.event.max.idempotence.cache.size</code>
   */
  public static final String MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_CONFIG = "maintenance.event.max.idempotence.cache.size";
  public static final int DEFAULT_MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE = 25;
  public static final String MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_DOC = "The maximum number of maintenance events cached by the "
      + "MaintenanceEventDetector within the past " + MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_CONFIG + " ms. Relevant only if idempotency"
      + " is enabled (see " + MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_CONFIG + ").";

  /**
   * <code>maintenance.event.stop.ongoing.execution</code>
   */
  public static final String MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION_CONFIG = "maintenance.event.stop.ongoing.execution";
  public static final boolean DEFAULT_MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION = true;
  public static final String MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION_DOC = "The flag to indicate whether a maintenance event will gracefully"
      + " stop the ongoing execution (if any) and wait until the execution stops before starting a fix for the anomaly.";

  /**
   * <code>provisioner.class</code>
   */
  public static final String PROVISIONER_CLASS_CONFIG = "provisioner.class";
  public static final String DEFAULT_PROVISIONER_CLASS = BasicProvisioner.class.getName();
  public static final String PROVISIONER_CLASS_DOC = "A provisioner class for adding / removing resources to / from the cluster. Different"
      + " platforms (e.g. Azure) should implement their own custom provisioners.";

  /**
   * <code>provisioner.enable</code>
   */
  public static final String PROVISIONER_ENABLE_CONFIG = "provisioner.enable";
  public static final boolean DEFAULT_PROVISIONER_ENABLE = true;
  public static final String PROVISIONER_ENABLE_DOC = "The flag to indicate whether use of provisioner is enabled.";

  /**
   * <code>replication.factor.self.healing.skip.rack.awareness.check</code>
   */
  public static final String RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG = "replication.factor.self.healing.skip.rack.awareness.check";
  public static final boolean DEFAULT_RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK = false;
  public static final String RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_DOC = "The flag to indicate whether to skip the rack-awareness "
      + "sanity check while changing the replication factor to self-heal a topic anomaly regarding a replication factor violation.";

  private AnomalyDetectorConfig() {
  }

  /**
   * Define configs for Anomaly Detector.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for Anomaly Detector.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_METRIC_ANOMALY_FINDER_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            METRIC_ANOMALY_FINDER_CLASSES_DOC)
                    .define(NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_NUM_CACHED_RECENT_ANOMALY_STATES,
                            between(1, 100),
                            ConfigDef.Importance.LOW,
                            NUM_CACHED_RECENT_ANOMALY_STATES_DOC)
                    .define(BROKER_FAILURES_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_BROKER_FAILURES_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            BROKER_FAILURES_CLASS_DOC)
                    .define(GOAL_VIOLATIONS_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_GOAL_VIOLATIONS_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            GOAL_VIOLATIONS_CLASS_DOC)
                    .define(INTRA_BROKER_GOAL_VIOLATIONS_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_INTRA_BROKER_GOAL_VIOLATIONS_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            INTRA_BROKER_GOAL_VIOLATIONS_CLASS_DOC)
                    .define(DISK_FAILURES_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_DISK_FAILURES_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            DISK_FAILURES_CLASS_DOC)
                    .define(METRIC_ANOMALY_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_METRIC_ANOMALY_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            METRIC_ANOMALY_CLASS_DOC)
                    .define(SELF_HEALING_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_SELF_HEALING_GOALS,
                            ConfigDef.Importance.HIGH,
                            SELF_HEALING_GOALS_DOC)
                    .define(SELF_HEALING_INTRA_BROKER_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_SELF_HEALING_INTRA_BROKER_GOALS,
                            ConfigDef.Importance.HIGH,
                            SELF_HEALING_INTRA_BROKER_GOALS_DOC)
                    .define(ANOMALY_NOTIFIER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_ANOMALY_NOTIFIER_CLASS,
                            ConfigDef.Importance.LOW,
                            ANOMALY_NOTIFIER_CLASS_DOC)
                    .define(ANOMALY_DETECTION_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ANOMALY_DETECTION_GOALS,
                            ConfigDef.Importance.MEDIUM,
                            ANOMALY_DETECTION_GOALS_DOC)
                    .define(ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ANOMALY_DETECTION_INTRA_BROKER_GOALS,
                            ConfigDef.Importance.MEDIUM,
                            ANOMALY_DETECTION_INTRA_BROKER_GOALS_DOC)
                    .define(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG,
                            ConfigDef.Importance.MEDIUM,
                            SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC)
                    .define(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_SELF_HEALING_EXCLUDE_RECENT_BROKERS_CONFIG,
                            ConfigDef.Importance.MEDIUM,
                            SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC)
                    .define(FAILED_BROKERS_FILE_PATH_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_FAILED_BROKERS_FILE_PATH,
                            ConfigDef.Importance.LOW,
                            FAILED_BROKERS_FILE_PATH_DOC)
                    .define(FIXABLE_FAILED_BROKER_COUNT_THRESHOLD_CONFIG,
                            ConfigDef.Type.SHORT,
                            DEFAULT_FIXABLE_FAILED_BROKER_COUNT_THRESHOLD,
                            ConfigDef.Importance.LOW,
                            FIXABLE_FAILED_BROKER_COUNT_THRESHOLD_DOC)
                    .define(FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD_CONFIG,
                            ConfigDef.Type.DOUBLE,
                            DEFAULT_FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD,
                            ConfigDef.Importance.LOW,
                            FIXABLE_FAILED_BROKER_PERCENTAGE_THRESHOLD_DOC)
                    .define(ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_ANOMALY_DETECTION_INTERVAL_MS,
                            ConfigDef.Importance.LOW,
                            ANOMALY_DETECTION_INTERVAL_MS_DOC)
                    .define(GOAL_VIOLATION_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_GOAL_VIOLATION_DETECTION_INTERVAL_MS,
                            ConfigDef.Importance.LOW,
                            GOAL_VIOLATION_DETECTION_INTERVAL_MS_DOC)
                    .define(METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_METRIC_ANOMALY_DETECTION_INTERVAL_MS,
                            ConfigDef.Importance.LOW,
                            METRIC_ANOMALY_DETECTION_INTERVAL_MS_DOC)
                    .define(DISK_FAILURE_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_DISK_FAILURE_DETECTION_INTERVAL_MS,
                            ConfigDef.Importance.LOW,
                            DISK_FAILURE_DETECTION_INTERVAL_MS_DOC)
                    .define(BROKER_FAILURE_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_BROKER_FAILURE_DETECTION_INTERVAL_MS,
                            ConfigDef.Importance.LOW,
                            BROKER_FAILURE_DETECTION_INTERVAL_MS_DOC)
                    .define(BROKER_FAILURE_DETECTION_BACKOFF_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_BROKER_FAILURE_DETECTION_BACKOFF_MS,
                            ConfigDef.Importance.LOW,
                            BROKER_FAILURE_DETECTION_BACKOFF_MS_DOC)
                    .define(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                            ConfigDef.Importance.LOW,
                            ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC)
                    .define(TOPIC_ANOMALY_FINDER_CLASSES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_TOPIC_ANOMALY_FINDER_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            TOPIC_ANOMALY_FINDER_CLASSES_DOC)
                    .define(TOPIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_TOPIC_ANOMALY_DETECTION_INTERVAL_MS,
                            ConfigDef.Importance.LOW,
                            TOPIC_ANOMALY_DETECTION_INTERVAL_MS_DOC)
                    .define(MAINTENANCE_EVENT_READER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_MAINTENANCE_EVENT_READER_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            MAINTENANCE_EVENT_READER_CLASS_DOC)
                    .define(MAINTENANCE_EVENT_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_MAINTENANCE_EVENT_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            MAINTENANCE_EVENT_CLASS_DOC)
                    .define(MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE,
                            ConfigDef.Importance.LOW,
                            MAINTENANCE_EVENT_ENABLE_IDEMPOTENCE_DOC)
                    .define(MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_DOC)
                    .define(MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_DOC)
                    .define(MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION,
                            ConfigDef.Importance.LOW,
                            MAINTENANCE_EVENT_STOP_ONGOING_EXECUTION_DOC)
                    .define(PROVISIONER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            DEFAULT_PROVISIONER_CLASS,
                            ConfigDef.Importance.MEDIUM,
                            PROVISIONER_CLASS_DOC)
                    .define(PROVISIONER_ENABLE_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_PROVISIONER_ENABLE,
                            ConfigDef.Importance.LOW,
                            PROVISIONER_ENABLE_DOC)
                    .define(RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK,
                            ConfigDef.Importance.LOW,
                            RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_DOC);
  }
}
