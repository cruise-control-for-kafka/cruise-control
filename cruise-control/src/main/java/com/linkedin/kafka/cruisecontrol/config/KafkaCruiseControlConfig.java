/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlParametersConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlRequestConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.UserTaskManagerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.jwt.JwtSecurityProvider;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

/**
 * The configuration class of Kafka Cruise Control.
 *
 * To avoid having a huge monolithic class that mixes unrelated configs, config names, their defaults, and definitions
 * reside in the relevant classes under {@link com.linkedin.kafka.cruisecontrol.config.constants}.
 */
public class KafkaCruiseControlConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  static {
    CONFIG = CruiseControlRequestConfig.define(CruiseControlParametersConfig.define(AnomalyDetectorConfig.define(
        AnalyzerConfig.define(ExecutorConfig.define(MonitorConfig.define(WebServerConfig.define(
            UserTaskManagerConfig.define(new ConfigDef())))))))).withClientSslSupport().withClientSaslSupport();
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals) {
    this(originals, false);
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
    sanityCheckGoalNames();
    sanityCheckSamplingPeriod(originals);
    sanityCheckConcurrency();
    sanityCheckTaskExecutionAlertingThreshold();
    sanityCheckSecurity();
    sanityCheckBalancingConstraints();
    sanityCheckWebServerUrlPrefix();
  }

  /**
   * @return Merged config values.
   */
  public Map<String, Object> mergedConfigValues() {
    Map<String, Object> conf = originals();

    // Use parsed non-null value to overwrite originals.
    // This will keep default values and also keep values that are not defined under ConfigDef.
    values().forEach((k, v) -> {
      if (v != null) {
        conf.put(k, v);
      }
    });
    return conf;
  }

  @Override
  public <T> T getConfiguredInstance(String key, Class<T> t) {
    T o = super.getConfiguredInstance(key, t);
    if (o instanceof CruiseControlConfigurable) {
      ((CruiseControlConfigurable) o).configure(mergedConfigValues());
    }
    return o;
  }

  /**
   * @param key The name of the concrete class of the returned instance
   * @param t The interface of the returned instance
   * @param configOverrides Configuration overrides to use.
   * @param <T> The type of the instance to be returned.
   * @return Configured instance.
   */
  public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
    Class<?> c = getClass(key);
    Map<String, Object> configPairs = mergedConfigValues();
    configPairs.putAll(configOverrides);
    return KafkaCruiseControlConfigUtils.getConfiguredInstance(c, t, configPairs);
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
    List<T> objects = super.getConfiguredInstances(key, t);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(mergedConfigValues());
      }
    }
    return objects;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
    List<T> objects = super.getConfiguredInstances(key, t, configOverrides);
    Map<String, Object> configPairs = mergedConfigValues();
    configPairs.putAll(configOverrides);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(configPairs);
      }
    }
    return objects;
  }

  /**
   * Sanity check for
   * <ul>
   *   <li>{@link AnalyzerConfig#GOALS_CONFIG} and {@link AnalyzerConfig#INTRA_BROKER_GOALS_CONFIG} are non-empty and disjoint.</li>
   *   <li>Case insensitive goal names.</li>
   *   <li>{@link AnalyzerConfig#DEFAULT_GOALS_CONFIG} is non-empty.</li>
   *   <li>{@link AnalyzerConfig#DEFAULT_GOALS_CONFIG} is a subset of {@link AnalyzerConfig#GOALS_CONFIG}.</li>
   *   <li>{@link AnalyzerConfig#HARD_GOALS_CONFIG} is a subset of {@link AnalyzerConfig#DEFAULT_GOALS_CONFIG}.</li>
   *   <li>{@link AnomalyDetectorConfig#SELF_HEALING_GOALS_CONFIG} is a subset of {@link AnalyzerConfig#DEFAULT_GOALS_CONFIG}.</li>
   *   <li>{@link AnomalyDetectorConfig#ANOMALY_DETECTION_GOALS_CONFIG} is a sublist of {@link AnalyzerConfig#DEFAULT_GOALS_CONFIG} otherwise.</li>
   * </ul>
   */
  private void sanityCheckGoalNames() {
    List<String> interBrokerGoalNames = getList(AnalyzerConfig.GOALS_CONFIG);
    // Ensure that inter-broker goals are non-empty.
    if (interBrokerGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure goals configuration with an empty list of goals.");
    }

    List<String> intraBrokerGoalNames = getList(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG);
    // Ensure that intra-broker goals are non-empty.
    if (intraBrokerGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure intra-broker goals configuration with an empty list of goals.");
    }

    // Ensure that inter- and intra-broker goals are disjoint.
    Set<String> commonGoals = new HashSet<>(interBrokerGoalNames);
    commonGoals.retainAll(intraBrokerGoalNames);
    if (!commonGoals.isEmpty()) {
      throw new ConfigException(String.format("Attempt to configure inter-broker (%s) and intra-broker (%s) goals with"
                                              + " common goals (%s).", interBrokerGoalNames, intraBrokerGoalNames, commonGoals));
    }

    Set<String> caseInsensitiveGoalNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String goalName: interBrokerGoalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure goals with case sensitive names.");
      }
    }
    for (String goalName: intraBrokerGoalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure intra-broker goals with case sensitive names.");
      }
    }

    // Ensure that default goals is non-empty.
    List<String> defaultGoalNames = getList(AnalyzerConfig.DEFAULT_GOALS_CONFIG);
    if (defaultGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure default goals configuration with an empty list of goals.");
    }

    // Ensure that default goals are supported inter-broker or intra-broker goals.
    if (defaultGoalNames.stream().anyMatch(g -> !interBrokerGoalNames.contains(g) && !intraBrokerGoalNames.contains(g))) {
      throw new ConfigException(String.format("Attempt to configure default goals with unsupported goals (%s:%s and %s:%s and %s:%s).",
                                              AnalyzerConfig.DEFAULT_GOALS_CONFIG, defaultGoalNames,
                                              AnalyzerConfig.GOALS_CONFIG, interBrokerGoalNames,
                                              AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, intraBrokerGoalNames));
    }

    // Ensure that hard goals are contained in default goals.
    List<String> hardGoalNames = getList(AnalyzerConfig.HARD_GOALS_CONFIG);
    if (hardGoalNames.stream().anyMatch(g -> !defaultGoalNames.contains(g))) {
      throw new ConfigException(String.format("Attempt to configure hard goals with unsupported goals (%s:%s and %s:%s).",
                                              AnalyzerConfig.HARD_GOALS_CONFIG, hardGoalNames,
                                              AnalyzerConfig.DEFAULT_GOALS_CONFIG, defaultGoalNames));
    }

    // Ensure that goals used for self-healing are contained in default goals.
    List<String> selfHealingGoalNames = getList(AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG);
    if (selfHealingGoalNames.stream().anyMatch(g -> !defaultGoalNames.contains(g))) {
      throw new ConfigException(String.format("Attempt to configure self healing goals with unsupported goals (%s:%s and %s:%s).",
                                              AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG, selfHealingGoalNames,
                                              AnalyzerConfig.DEFAULT_GOALS_CONFIG, defaultGoalNames));
    }

    // Ensure that intra-broker goals used for self-healing are contained in intra broker goals.
    List<String> selfHealingIntraBrokerGoalNames = getList(AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG);
    if (selfHealingIntraBrokerGoalNames.stream().anyMatch(g -> !intraBrokerGoalNames.contains(g))) {
      throw new ConfigException(String.format("Attempt to configure self healing goals with unsupported goals (%s:%s and %s:%s).",
                                               AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG, selfHealingIntraBrokerGoalNames,
                                               AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG, intraBrokerGoalNames));
    }
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >=
   *     {@link ExecutorConfig#MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >
   *     {@link ExecutorConfig#NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >
   *     {@link ExecutorConfig#NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG} >=
   *     {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG} >=
   *     {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG} >
   *     {@link ExecutorConfig#NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG} <=
   *     {@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG} <=
   *     {@link ExecutorConfig#NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG} <=
   *     {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG} >
   *     {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG} <=
   *     {@link ExecutorConfig#MAX_NUM_CLUSTER_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG} <=
   *     {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG} >=
   *     {@link ExecutorConfig#NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG}</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG} <=
   *     {@link ExecutorConfig#CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG}</li>
   *   <li>{@link ExecutorConfig#MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG} <=
   *     {@link ExecutorConfig#EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG}</li>
   * </ul>
   */
  void sanityCheckConcurrency() {
    int maxClusterMovementConcurrency = getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);

    int maxPartitionMovementsInCluster = getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    if (maxPartitionMovementsInCluster > maxClusterMovementConcurrency) {
      throw new ConfigException(String.format("Maximum Inter-broker partition movement [%d] cannot be greater than the "
                                              + "maximum number of allowed movements in cluster [%d].",
                                              maxPartitionMovementsInCluster, maxClusterMovementConcurrency));
    }

    int interBrokerPartitionMovementConcurrency = getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);
    if (interBrokerPartitionMovementConcurrency >= maxClusterMovementConcurrency) {
      throw new ConfigException(String.format("Inter-broker partition movement concurrency [%d] must be smaller than the "
                                              + "maximum number of allowed movements in cluster [%d].",
                                              interBrokerPartitionMovementConcurrency, maxClusterMovementConcurrency));
    }

    int intraBrokerPartitionMovementConcurrency = getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG);
    if (intraBrokerPartitionMovementConcurrency >= maxClusterMovementConcurrency) {
      throw new ConfigException(String.format("Intra-broker partition movement concurrency [%d] must be smaller than the "
                                              + "maximum number of allowed movements in cluster [%d].",
                                              intraBrokerPartitionMovementConcurrency, maxClusterMovementConcurrency));
    }

    int leadershipMovementConcurrency = getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
    if (leadershipMovementConcurrency > maxClusterMovementConcurrency) {
      throw new ConfigException(String.format("Leadership movement concurrency [%d] cannot be greater than the maximum number"
                                              + " of allowed movements in cluster [%d].",
                                              leadershipMovementConcurrency, maxClusterMovementConcurrency));
    }

    int leadershipMovementConcurrencyPerBroker = getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG);
    if (leadershipMovementConcurrencyPerBroker > leadershipMovementConcurrency) {
      throw new ConfigException(String.format("Leadership movement concurrency of a broker [%d] cannot be greater than the"
              + " allowed leadership movements in cluster [%d].",
          leadershipMovementConcurrencyPerBroker, leadershipMovementConcurrency));
    }

    int concurrencyAdjusterMaxPartitionMovementsPerBroker = getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);
    if (interBrokerPartitionMovementConcurrency >= concurrencyAdjusterMaxPartitionMovementsPerBroker) {
      throw new ConfigException(String.format("Inter-broker partition movement concurrency [%d] must be smaller than the "
                                              + "concurrency adjuster maximum partition movements per broker [%d].",
                                              interBrokerPartitionMovementConcurrency, concurrencyAdjusterMaxPartitionMovementsPerBroker));
    }

    if (concurrencyAdjusterMaxPartitionMovementsPerBroker > maxClusterMovementConcurrency) {
      throw new ConfigException(String.format("Maximum partition movements per broker of concurrency adjuster [%d] cannot"
                                              + " be greater than the maximum number of allowed movements in cluster [%d].",
                                              concurrencyAdjusterMaxPartitionMovementsPerBroker, maxClusterMovementConcurrency));
    }

    int concurrencyAdjusterMinPartitionMovementsPerBroker = getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG);
    if (interBrokerPartitionMovementConcurrency < concurrencyAdjusterMinPartitionMovementsPerBroker) {
      throw new ConfigException(String.format("Inter-broker partition movement concurrency [%d] cannot be smaller than the"
                                              + " concurrency adjuster minimum partition movements per broker [%d].",
                                              interBrokerPartitionMovementConcurrency, concurrencyAdjusterMinPartitionMovementsPerBroker));
    }

    int concurrencyAdjusterMinLeadershipMovements = getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG);
    if (leadershipMovementConcurrency < concurrencyAdjusterMinLeadershipMovements) {
      throw new ConfigException(String.format("Leadership movement concurrency [%d] cannot be smaller than the concurrency "
                                              + "adjuster minimum leadership movements [%d].", leadershipMovementConcurrency,
                                              concurrencyAdjusterMinLeadershipMovements));
    }

    int concurrencyAdjusterMaxLeadershipMovements = getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG);
    if (leadershipMovementConcurrency > concurrencyAdjusterMaxLeadershipMovements) {
      throw new ConfigException(String.format("Leadership movement concurrency [%d] cannot be greater than the concurrency "
                                              + "adjuster maximum leadership movements [%d].", leadershipMovementConcurrency,
                                              concurrencyAdjusterMaxLeadershipMovements));
    }

    if (concurrencyAdjusterMaxLeadershipMovements > maxClusterMovementConcurrency) {
      throw new ConfigException(String.format("Maximum leadership movements of concurrency adjuster [%d] cannot be greater "
                                              + "than the maximum number of allowed movements in cluster [%d].",
                                              concurrencyAdjusterMaxLeadershipMovements, maxClusterMovementConcurrency));
    }

    int concurrencyAdjusterMinLeadershipMovementsPerBroker = getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG);
    if (leadershipMovementConcurrencyPerBroker < concurrencyAdjusterMinLeadershipMovementsPerBroker) {
      throw new ConfigException(String.format("Leadership movement per broker concurrency [%d] cannot be smaller than the concurrency "
              + "adjuster minimum per broker leadership movements [%d].", leadershipMovementConcurrencyPerBroker,
          concurrencyAdjusterMinLeadershipMovementsPerBroker));
    }

    int concurrencyAdjusterMaxLeadershipMovementsPerBroker = getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG);
    if (leadershipMovementConcurrencyPerBroker > concurrencyAdjusterMaxLeadershipMovementsPerBroker) {
      throw new ConfigException(String.format("Leadership movement concurrency per broker [%d] cannot be greater than the concurrency "
              + "adjuster maximum per broker leadership movements [%d].", leadershipMovementConcurrencyPerBroker,
          concurrencyAdjusterMaxLeadershipMovementsPerBroker));
    }

    if (concurrencyAdjusterMaxLeadershipMovementsPerBroker > concurrencyAdjusterMaxLeadershipMovements) {
      throw new ConfigException(String.format("Maximum per broker leadership movements of concurrency adjuster [%d] cannot be greater "
              + "than the maximum number of allowed movements in cluster [%d].",
          concurrencyAdjusterMaxLeadershipMovementsPerBroker, concurrencyAdjusterMaxLeadershipMovements));
    }

    long minExecutionProgressCheckIntervalMs = getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    long defaultExecutionProgressCheckIntervalMs = getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    if (minExecutionProgressCheckIntervalMs > defaultExecutionProgressCheckIntervalMs) {
      throw new ConfigException(String.format("Minimum execution progress check interval [%d] cannot be greater than the "
                                              + "default execution progress check interval [%d].",
                                              minExecutionProgressCheckIntervalMs, defaultExecutionProgressCheckIntervalMs));
    }
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG} <=
   *   {@link AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG}</li>
   *   <li>{@link AnalyzerConfig#OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG} <= {@link AnalyzerConfig#MAX_REPLICAS_PER_BROKER_CONFIG}</li>
   * </ul>
   */
  private void sanityCheckBalancingConstraints() {
    int topicReplicaBalanceMinGap = getInt(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG);
    int topicReplicaBalanceMaxGap = getInt(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG);
    if (topicReplicaBalanceMinGap > topicReplicaBalanceMaxGap) {
      throw new ConfigException(String.format("Maximum gap of topic replica count balance [%d] cannot be smaller than the "
                                              + "minimum gap of topic replica count balance [%d].",
                                              topicReplicaBalanceMaxGap, topicReplicaBalanceMinGap));
    }

    // Ensure that max allowed replicas per broker is not smaller than the limit under which brokers are considered overprovisioned.
    long maxReplicasPerBroker = getLong(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG);
    long overprovisionedMaxReplicasPerBroker = getLong(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG);
    if (overprovisionedMaxReplicasPerBroker > maxReplicasPerBroker) {
      throw new ConfigException(String.format("Maximum replicas per broker [%d] cannot be smaller than the "
                                              + "overprovisioned maximum replicas per broker [%d].",
                                              maxReplicasPerBroker, overprovisionedMaxReplicasPerBroker));
    }
  }

  /**
   * Sanity check to ensure that {@link ExecutorConfig#LEADER_MOVEMENT_TIMEOUT_MS_CONFIG} >
   * {@link ExecutorConfig#TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG}
   */
  private void sanityCheckTaskExecutionAlertingThreshold() {
    long leaderMovementTimeoutMs = getLong(ExecutorConfig.LEADER_MOVEMENT_TIMEOUT_MS_CONFIG);
    long taskExecutionAlertingThresholdMs = getLong(ExecutorConfig.TASK_EXECUTION_ALERTING_THRESHOLD_MS_CONFIG);
    if (taskExecutionAlertingThresholdMs >= leaderMovementTimeoutMs) {
      throw new ConfigException(String.format("Task execution time alerting threshold [%dms] cannot be greater than leader movement"
                                              + " timeout [%sms].", taskExecutionAlertingThresholdMs, leaderMovementTimeoutMs));
    }
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link MonitorConfig#METADATA_MAX_AGE_MS_CONFIG} is not longer than {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG},</li>
   *   <li>sampling frequency per partition window is within the limits -- i.e. ({@link MonitorConfig#PARTITION_METRICS_WINDOW_MS_CONFIG} /
   *   {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}) <= {@link Byte#MAX_VALUE}, and</li>
   *   <li>sampling frequency per broker window is within the limits -- i.e. ({@link MonitorConfig#BROKER_METRICS_WINDOW_MS_CONFIG} /
   *   {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}) <= {@link Byte#MAX_VALUE}, and</li>
   *   <li>{@link CruiseControlMetricsReporterConfig#CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG} is not longer than
   *   {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}, and</li>
   *   <li>{@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG} is not longer than
   *   {@link AnomalyDetectorConfig#METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG}
   *   (or #ANOMALY_DETECTION_INTERVAL_MS_CONFIG if #METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG is not specified)</li>
   *   <li>{@link ExecutorConfig#CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG} > {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}</li>
   * </ul>
   *
   * Sampling process involves a potential metadata update if the current metadata is stale. The configuration
   * {@link MonitorConfig#METADATA_MAX_AGE_MS_CONFIG} indicates the timeout of such a metadata update. Hence, this subprocess of the sampling
   * process cannot be set with a timeout larger than the total sampling timeout of {@link MonitorConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}.
   *
   * The number of samples at a given window cannot exceed a predefined maximum limit.
   *
   * The metrics reporting interval should not be larger than the metrics sampling interval in order to ensure there is always
   * data to be collected.
   * @param originals Original configs.
   */
  private void sanityCheckSamplingPeriod(Map<?, ?> originals) {
    long samplingIntervalMs = getLong(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG);
    long metadataTimeoutMs = getLong(MonitorConfig.METADATA_MAX_AGE_MS_CONFIG);
    if (metadataTimeoutMs > samplingIntervalMs) {
      throw new ConfigException("Attempt to set metadata refresh timeout [" + metadataTimeoutMs
                                + "] to be longer than sampling period [" + samplingIntervalMs + "].");
    }

    // Ensure that the sampling frequency per partition window is within the limits.
    long partitionSampleWindowMs = getLong(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
    short partitionSamplingFrequency = (short) (partitionSampleWindowMs / samplingIntervalMs);
    if (partitionSamplingFrequency > Byte.MAX_VALUE) {
      throw new ConfigException(String.format("Configured sampling frequency (%d) exceeds the maximum allowed value (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that their"
                                              + " ratio is under this limit.", partitionSamplingFrequency, Byte.MAX_VALUE,
                                              MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure that the sampling frequency per broker window is within the limits.
    long brokerSampleWindowMs = getLong(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG);
    short brokerSamplingFrequency = (short) (brokerSampleWindowMs / samplingIntervalMs);
    if (brokerSamplingFrequency > Byte.MAX_VALUE) {
      throw new ConfigException(String.format("Configured sampling frequency (%d) exceeds the maximum allowed value (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that their"
                                              + " ratio is under this limit.", brokerSamplingFrequency, Byte.MAX_VALUE,
                                              MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure that the metrics reporter reports more often than the sample fetcher samples.
    CruiseControlMetricsReporterConfig reporterConfig = new CruiseControlMetricsReporterConfig(originals, false);
    long reportingIntervalMs = reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG);
    if (reportingIntervalMs > samplingIntervalMs) {
      throw new ConfigException(String.format("Configured metric reporting interval (%d) exceeds metric sampling interval (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that reported "
                                              + "metrics can be properly sampled.",
                                              reportingIntervalMs, samplingIntervalMs,
                                              CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure sampling frequency is higher than metric anomaly detection frequency.
    Long metricAnomalyDetectionIntervalMs = getLong(AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    if (metricAnomalyDetectionIntervalMs == null) {
      metricAnomalyDetectionIntervalMs = getLong(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG);
    }
    if (samplingIntervalMs > metricAnomalyDetectionIntervalMs) {
      throw new ConfigException(String.format("Configured metric sampling interval (%d) exceeds metric anomaly detection interval (%d). "
                                              + "Decrease the value of %s or increase the value of %s (or %s if %s is not specified) to "
                                              + "ensure that metrics anomaly detection does not run too frequently.",
                                              samplingIntervalMs, metricAnomalyDetectionIntervalMs,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                                              AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                                              AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                                              AnomalyDetectorConfig.METRIC_ANOMALY_DETECTION_INTERVAL_MS_CONFIG));
    }

    // Ensure sample fetcher samples more often than the concurrency adjuster evaluates the latest broker metrics.
    long concurrencyAdjusterIntervalMs = getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG);
    if (samplingIntervalMs > concurrencyAdjusterIntervalMs) {
      throw new ConfigException(String.format("Configured metric sampling interval (%d) exceeds concurrency adjuster interval (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that concurrency "
                                              + "adjuster can be properly retrieve and evaluate the latest broker metrics.",
                                              samplingIntervalMs, concurrencyAdjusterIntervalMs,
                                              MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                                              ExecutorConfig.CONCURRENCY_ADJUSTER_INTERVAL_MS_CONFIG));
    }
  }

  /**
   * Package private for unit test.
   * Sanity check to ensure that SSL and authentication is set up correctly. This means the following:
   * <ul>
   *   <li>If <code>webserver.ssl.enable</code> is set, then <code>webserver.ssl.keystore.location</code> and
   *   <code>webserver.ssl.protocol</code> must also be set.</li>
   *   <li><code>If webserver.security.enable</code> is set, then <code>webserver.security.provider</code> must also be
   *   set to be an implementation of the {@link SecurityProvider} interface. Also if it's {@link BasicSecurityProvider}
   *   then <code>basic.auth.credentials.file</code> must point to an existing file.</li>
   * </ul>
   */
  void sanityCheckSecurity() {
    Boolean sslEnabled = getBoolean(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG);
    if (sslEnabled) {
      if (getString(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG) == null) {
        throw new ConfigException("If webserver SSL is enabled, the keystore location must be set.");
      }
      if (getString(WebServerConfig.WEBSERVER_SSL_PROTOCOL_CONFIG) == null) {
        throw new ConfigException("If webserver SSL is enabled, a valid SSL protocol must be set.");
      }
    }
    Boolean securityEnabled = getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG);
    if (securityEnabled) {
      Class<?> securityProvider = getClass(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG);
      if (securityProvider == null || !SecurityProvider.class.isAssignableFrom(securityProvider)) {
        throw new ConfigException(String.format("If webserver security is enabled, a valid security provider must be set "
                                                + "that is an implementation of %s.", SecurityProvider.class.getName()));
      }
      String authCredentialsFile = getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
      if (BasicSecurityProvider.class == securityProvider && !fileExists(authCredentialsFile)) {
        throw new ConfigException(String.format("If %s is used, an existing credentials file must be set.",
                                                BasicSecurityProvider.class.getName()));
      }
      if (JwtSecurityProvider.class == securityProvider) {
        String providerUrl = getString(WebServerConfig.JWT_AUTHENTICATION_PROVIDER_URL_CONFIG);
        if (providerUrl == null || providerUrl.isEmpty()) {
          throw new ConfigException(String.format("When %s is used, %s must be set.",
                                                  JwtSecurityProvider.class.getName(), WebServerConfig.JWT_AUTHENTICATION_PROVIDER_URL_CONFIG));
        }
        String certificateFile = getString(WebServerConfig.JWT_AUTH_CERTIFICATE_LOCATION_CONFIG);
        if (!fileExists(certificateFile)) {
          throw new ConfigException(String.format("If %s is used, an existing certificate file must be set.",
                                                  JwtSecurityProvider.class.getName()));
        }
        String privilegesFile = getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
        if (!fileExists(privilegesFile)) {
          throw new ConfigException(String.format("If %s is used, an existing certificate file must be set.",
                                                  JwtSecurityProvider.class.getName()));
        }
      }
    }
  }

  /**
   * Package private for unit test.
   * Sanity check to ensure that webserver URL prefix is set up correctly. This means the following:
   *  <code>webserver.api.urlprefix</code> and <code>webserver.ui.urlprefix</code> must end with "/*"
   */
  void sanityCheckWebServerUrlPrefix() {
    String expectedSuffix = "/*";
    String webserverApiUrlPrefix = getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
    if (!webserverApiUrlPrefix.endsWith(expectedSuffix)) {
      throw new ConfigException(String.format("Expect value to the config %s ends with %s. Got: %s",
                                              WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG, expectedSuffix, webserverApiUrlPrefix));
    }

    String webserverUiUrlPrefix = getString(WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG);
    if (!webserverUiUrlPrefix.endsWith(expectedSuffix)) {
      throw new ConfigException(String.format("Expect value to the config %s ends with %s. Got: %s",
                                              WebServerConfig.WEBSERVER_UI_URLPREFIX_CONFIG, expectedSuffix, webserverUiUrlPrefix));
    }
  }

  private boolean fileExists(String file) {
    return file != null && Files.exists(Paths.get(file));
  }
}
