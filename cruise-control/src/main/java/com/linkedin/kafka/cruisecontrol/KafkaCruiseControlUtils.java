/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.exception.PartitionNotExistsException;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.config.EnvConfigProvider;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.servlet.ServletException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;


/**
 * Util class for convenience.
 */
public final class KafkaCruiseControlUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlUtils.class);
  // Kafka's org.apache.kafka.common.record.RecordBatch moved to an internal package in 4.3; -1 is the stable
  // sentinel value Kafka uses for "no partition leader epoch".
  private static final int NO_PARTITION_LEADER_EPOCH = -1;
  // Cruise Control sensor types -- i.e. the first element of the metric name.
  public static final String EXECUTOR_SENSOR = "Executor";
  public static final String LOAD_MONITOR_SENSOR = "LoadMonitor";
  public static final String USER_TASK_MANAGER_SENSOR = "UserTaskManager";
  public static final String ANOMALY_DETECTOR_SENSOR = "AnomalyDetector";
  public static final String GOAL_OPTIMIZER_SENSOR = "GoalOptimizer";
  public static final String METRIC_FETCHER_MANAGER_SENSOR = "MetricFetcherManager";
  public static final String KAFKA_CRUISE_CONTROL_SERVLET_SENSOR = "KafkaCruiseControlServlet";
  // Config to pass an Admin client to a pluggable component
  public static final String ADMIN_CLIENT_CONFIG = "admin.client.object";
  public static final double MAX_BALANCEDNESS_SCORE = 100.0;
  public static final long ADMIN_CLIENT_CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
  public static final int SEC_TO_MS = (int) TimeUnit.SECONDS.toMillis(1);
  private static final int MIN_TO_MS = SEC_TO_MS * 60;
  private static final int HOUR_TO_MS = MIN_TO_MS * 60;
  private static final int DAY_TO_MS = HOUR_TO_MS * 24;
  public static final String OPERATION_LOGGER = "operationLogger";
  public static final String REQUESTLOG_LOGGER = "CruiseControlPublicAccessLogger";
  // This will make MetaData.update() trigger a real metadata fetch.
  public static final int REQUEST_VERSION_UPDATE = -1;
  public static final String ENV_CONFIG_PROVIDER_NAME = "env";
  public static final String ENV_CONFIG_PROVIDER_CLASS_CONFIG = ".env.class";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  public static final int TOPIC_AUTO_CREATE_RETRIES = 5;
  public static final String DEFAULT_CLEANUP_POLICY = "delete";
  public static final Random RANDOM = new Random();
  public static final ConfigResource CLUSTER_CONFIG = new ConfigResource(ConfigResource.Type.BROKER, "");

  private KafkaCruiseControlUtils() {

  }

  /**
   * Format the duration from double to human readable string.
   * @param durationMs Duration in milliseconds
   * @return String representation of duration
   */
  public static String toPrettyDuration(double durationMs) {
    if (durationMs < 0) {
      throw new IllegalArgumentException(String.format("Duration cannot be negative value, get %f", durationMs));
    }

    // If the duration is less than one second, represent in milliseconds.
    if (durationMs < SEC_TO_MS) {
      return String.format("%.2f milliseconds", durationMs);
    }

    // If the duration is less than one minute, represent in seconds.
    if (durationMs < MIN_TO_MS) {
      return String.format("%.2f seconds", durationMs / SEC_TO_MS);
    }

    // If the duration is less than one hour, represent in minutes.
    if (durationMs < HOUR_TO_MS) {
      return String.format("%.2f minutes", durationMs / MIN_TO_MS);
    }

    // If the duration is less than one day, represent in hours.
    if (durationMs < DAY_TO_MS) {
      return String.format("%.2f hours", durationMs / HOUR_TO_MS);
    }

    // Represent in days.
    return String.format("%.2f days", durationMs / DAY_TO_MS);
  }

  /**
   * Get a configuration and throw exception if the configuration was not provided.
   * @param configs the config map.
   * @param configName the config to get.
   * @return The configuration string.
   */
  public static String getRequiredConfig(Map<String, ?> configs, String configName) {
    String value = (String) configs.get(configName);
    if (value == null || value.isEmpty()) {
      throw new ConfigException(String.format("Configuration %s must be provided.", configName));
    }
    return value;
  }

  /**
   * Creates the given topic if it does not exist.
   *
   * @param adminClient The adminClient to send createTopics request.
   * @param topicToBeCreated A wrapper around the topic to be created.
   * @return {@code false} if the topic to be created already exists, {@code true} otherwise.
   */
  public static boolean createTopic(AdminClient adminClient, NewTopic topicToBeCreated) {
    try {
      boolean retryResponse = CruiseControlMetricsUtils.retry(() -> {
        try {
          CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(topicToBeCreated));
          createTopicsResult.values().get(topicToBeCreated.name()).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          LOG.info("Topic {} has been created.", topicToBeCreated.name());
          return false;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          if (e.getCause() instanceof TopicExistsException) {
            throw (TopicExistsException) e.getCause();
          }
          LOG.warn("Unable to create topic {}.", topicToBeCreated.name(), e);
          return true;
        }
      }, TOPIC_AUTO_CREATE_RETRIES);
      if (!retryResponse) {
        throw new IllegalStateException(String.format("Failed to create topic %s.", topicToBeCreated.name()));
      }
    } catch (TopicExistsException e) {
      return false;
    }
    return true;
  }

  /**
   * Describe cluster configs.
   *
   * @param adminClient The adminClient to send describeConfigs request.
   * @param timeout Timeout to describe cluster configs.
   * @return Cluster configs, or {@code null} if there is a timeout.
   */
  public static Config describeClusterConfigs(AdminClient adminClient, Duration timeout)
      throws InterruptedException, ExecutionException {
    Set<ConfigResource> resources = Collections.singleton(CLUSTER_CONFIG);
    KafkaFuture<Config> clusterConfigFuture = adminClient.describeConfigs(resources).values().get(CLUSTER_CONFIG);

    Config clusterConfigs = null;
    try {
      clusterConfigs = clusterConfigFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOG.error("Describing cluster configs has timed out.", e);
    }
    return clusterConfigs;
  }

  /**
   * Build a wrapper around the topic with the given desired properties and {@link #DEFAULT_CLEANUP_POLICY}.
   *
   * @param topic The name of the topic.
   * @param partitionCount Desired partition count.
   * @param replicationFactor Desired replication factor.
   * @param retentionMs Desired retention in milliseconds.
   * @return A wrapper around the topic with the given desired properties.
   */
  public static NewTopic wrapTopic(String topic, int partitionCount, short replicationFactor, long retentionMs) {
    if (partitionCount <= 0 || replicationFactor <= 0 || retentionMs <= 0) {
      throw new IllegalArgumentException(String.format("Partition count (%d), replication factor (%d), and retention ms (%d)"
                                                       + " must be positive for the topic (%s).", partitionCount,
                                                       replicationFactor, retentionMs, topic));
    }

    NewTopic newTopic = new NewTopic(topic, partitionCount, replicationFactor);
    Map<String, String> config = new HashMap<>();
    config.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs));
    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, DEFAULT_CLEANUP_POLICY);
    newTopic.configs(config);

    return newTopic;
  }

  /**
   * Add config altering operations to the given configs to alter for configs that differ between current and desired.
   *
   * @param configsToAlter A set of config altering operations to be populated.
   * @param desiredConfig Desired config value by name.
   * @param currentConfig Current config.
   */
  private static void maybeUpdateConfig(Set<AlterConfigOp> configsToAlter, Map<String, String> desiredConfig, Config currentConfig) {
    for (Map.Entry<String, String> entry : desiredConfig.entrySet()) {
      String configName = entry.getKey();
      String targetConfigValue = entry.getValue();
      ConfigEntry currentConfigEntry = currentConfig.get(configName);
      if (currentConfigEntry == null || !currentConfigEntry.value().equals(targetConfigValue)) {
        configsToAlter.add(new AlterConfigOp(new ConfigEntry(configName, targetConfigValue), AlterConfigOp.OpType.SET));
      }
    }
  }

  /**
   * Update topic configurations with the desired configs specified in the given topicToUpdateConfigs.
   *
   * @param adminClient The adminClient to send describeConfigs and incrementalAlterConfigs requests.
   * @param topicToUpdateConfigs Existing topic to update selected configs if needed -- cannot be {@code null}.
   * @return {@code true} if the request is completed successfully, {@code false} if there are any exceptions.
   */
  public static boolean maybeUpdateTopicConfig(AdminClient adminClient, NewTopic topicToUpdateConfigs) {
    String topicName = topicToUpdateConfigs.name();
    // Retrieve topic config to check if it needs an update.
    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(topicResource));
    Config topicConfig;
    try {
      topicConfig = describeConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Config check for topic {} failed due to failure to describe its configs.", topicName, e);
      return false;
    }

    // Update configs if needed.
    Map<String, String> desiredConfig = topicToUpdateConfigs.configs();
    if (desiredConfig != null) {
      Set<AlterConfigOp> alterConfigOps = new HashSet<>();
      maybeUpdateConfig(alterConfigOps, desiredConfig, topicConfig);
      if (!alterConfigOps.isEmpty()) {
        AlterConfigsResult alterConfigsResult
            = adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource, alterConfigOps));
        try {
          alterConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          LOG.warn("Config change for topic {} failed.", topicName, e);
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Increase the partition count of the given existing topic to the desired partition count (if needed).
   *
   * @param adminClient The adminClient to send describeTopics and createPartitions requests.
   * @param topicToAddPartitions Existing topic to add more partitions if needed -- cannot be {@code null}.
   * @return {@link CompletionType#COMPLETED} if the request is completed successfully, {@link CompletionType#COMPLETED_WITH_ERROR} if there
   * are any exceptions, and {@link CompletionType#NO_ACTION} if the partition count is already greater than or equal to the requested count.
   */
  public static CompletionType maybeIncreasePartitionCount(AdminClient adminClient, NewTopic topicToAddPartitions) {
    String topicName = topicToAddPartitions.name();

    // Retrieve partition count of topic to check if it needs a partition count update.
    TopicDescription topicDescription;
    try {
      topicDescription = adminClient.describeTopics(Collections.singletonList(topicName)).topicNameValues()
                                    .get(topicName).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.warn("Partition count increase check for topic {} failed due to failure to describe cluster.", topicName, e);
      return CompletionType.COMPLETED_WITH_ERROR;
    }

    // Update partition count of topic if needed.
    if (topicDescription.partitions().size() < topicToAddPartitions.numPartitions()) {
      CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(
          Collections.singletonMap(topicName, NewPartitions.increaseTo(topicToAddPartitions.numPartitions())));

      try {
        createPartitionsResult.values().get(topicName).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.warn("Partition count increase to {} for topic {} failed{}.", topicToAddPartitions.numPartitions(), topicName,
                 (e.getCause() instanceof ReassignmentInProgressException) ? " due to ongoing reassignment" : "", e);
        return CompletionType.COMPLETED_WITH_ERROR;
      }
    } else {
      return CompletionType.NO_ACTION;
    }

    return CompletionType.COMPLETED;
  }

  /**
   * Sanity check whether there are failures in partition offsets fetched by a consumer. This typically happens due to
   * transient network failures (e.g. Error sending fetch request XXX to node XXX: org.apache.kafka.common.errors.DisconnectException.)
   * that prevents the consumer from getting offset from some brokers for as long as reconnect.backoff.ms.
   *
   * Note the following assumptions for the given parameters:
   * <ul>
   *   <li>if a partition has never been written to, its end offset is expected to be {@code 0}.</li>
   *   <li>if the earliest offset, whose timestamp is greater than or equal to the given timestamp in the corresponding
   *   partition does not exist, {@code null} is expected for the offset for the time of the partition.</li>
   * </ul>
   *
   * @param endOffsets End offsets retrieved by consumer.
   * @param offsetsForTimes Offsets for times retrieved by consumer.
   */
  public static void sanityCheckOffsetFetch(Map<TopicPartition, Long> endOffsets,
                                            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes)
      throws SamplingException {
    Set<TopicPartition> failedToFetchOffsets = new HashSet<>();
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
      if (entry.getValue() == null && endOffsets.get(entry.getKey()) == null) {
        failedToFetchOffsets.add(entry.getKey());
      }
    }

    if (!failedToFetchOffsets.isEmpty()) {
      throw new SamplingException(String.format("Consumer failed to fetch offsets for %s. Consider decreasing "
                                                + "reconnect.backoff.ms to mitigate consumption failures"
                                                + " due to transient network issues.", failedToFetchOffsets));
    }
  }

  /**
   * Check whether the given consumer is done with the consumption of each partition with the given offsets.
   * A consumption is considered as done if either of the following is satisfied:
   * <ul>
   *   <li>The consumer has caught up with the provided offsets</li>
   *   <li>All partitions are paused</li>
   * </ul>
   *
   * @param consumer Consumer
   * @param offsets Offsets for each partition consumption to catch up.
   * @param <K> The type of the consumer key.
   * @param <V> The type of the consumer value.
   * @return {@code true} if the consumption is done, {@code false} otherwise.
   */
  public static <K, V> boolean consumptionDone(Consumer<K, V> consumer, Map<TopicPartition, Long> offsets) {
    Set<TopicPartition> partitionsNotPaused = new HashSet<>(consumer.assignment());
    partitionsNotPaused.removeAll(consumer.paused());
    for (TopicPartition tp : partitionsNotPaused) {
      if (consumer.position(tp) < offsets.get(tp)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sanity check whether
   * <ul>
   *   <li>all hard goals (based on the given config) are included in the provided goal list, and</li>
   *   <li>{@link #sanityCheckNonExistingGoal} is not violated.</li>
   * </ul>
   *
   * There are two scenarios where this check is skipped.
   * <ul>
   *   <li> {@code goals} is {@code null} or empty -- i.e. even if hard goals are excluded from the default goals, this
   *   check will pass</li>
   *   <li> {@code goals} only has {@link PreferredLeaderElectionGoal}, denotes it is a PLE request.</li>
   * </ul>
   *
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to check.
   * @param skipHardGoalCheck {@code true} if hard goal checking is not needed.
   * @param config The configurations for Cruise Control.
   */
  public static void sanityCheckGoals(List<String> goals, boolean skipHardGoalCheck, KafkaCruiseControlConfig config) {
    if (goals != null && !goals.isEmpty() && !skipHardGoalCheck
        && !(goals.size() == 1 && goals.get(0).equals(PreferredLeaderElectionGoal.class.getSimpleName()))) {
      sanityCheckNonExistingGoal(goals, AnalyzerUtils.getCaseInsensitiveGoalsByName(config));
      Set<String> hardGoals = hardGoals(config);
      if (!goals.containsAll(hardGoals)) {
        throw new IllegalArgumentException(String.format("Missing hard goals %s in the provided goals: %s. Add %s=true "
                                                         + "parameter to ignore this sanity check.", hardGoals, goals,
                                                         SKIP_HARD_GOAL_CHECK_PARAM));
      }
    }
  }

  /**
   * Get the set of configured hard goals.
   *
   * @param config The configurations for Cruise Control.
   * @return The set of configured hard goals.
   */
  public static Set<String> hardGoals(KafkaCruiseControlConfig config) {
    return config.getList(AnalyzerConfig.HARD_GOALS_CONFIG).stream()
                 .map(goalName -> goalName.substring(goalName.lastIndexOf(".") + 1)).collect(Collectors.toSet());
  }

  /**
   * Check to ensure that if requested cluster model completeness requires non-zero number of valid windows in cluster model,
   * load monitor should have already finished loading all the samples.
   * Note that even if only one valid window is required, we still need to wait load monitor finish loading all samples. This
   * is because sample loading follow time order, i.e. the first window loaded is the oldest window and the load information
   * in that window is probably stale.
   *
   * @param completenessRequirements Requested cluster model completeness requirement.
   * @param loadMonitorTaskRunnerState Current state of load monitor's task runner.
   */
  public static void sanityCheckLoadMonitorReadiness(ModelCompletenessRequirements completenessRequirements,
                                                     LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState) {
    if (completenessRequirements.minRequiredNumWindows() > 0
        && loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING) {
      throw new IllegalStateException("Unable to generate proposal since load monitor is in "
                                      + LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING + " state.");
    }
  }

  /**
   * Get a goals by priority based on the goal list.
   *
   * @param goals A list of goals.
   * @param config The configurations for Cruise Control.
   * @return A list of goals sorted by highest to lowest priority.
   */
  public static List<Goal> goalsByPriority(List<String> goals, KafkaCruiseControlConfig config) {
    if (goals == null || goals.isEmpty()) {
      return AnalyzerUtils.getDefaultGoalsByPriority(config);
    }
    Map<String, Goal> allGoals = AnalyzerUtils.getCaseInsensitiveGoalsByName(config);
    sanityCheckNonExistingGoal(goals, allGoals);
    return goals.stream().map(allGoals::get).collect(Collectors.toList());
  }

  private static void closeClientWithTimeout(Runnable clientCloseTask, long timeoutMs) {
    Thread t = new Thread(clientCloseTask);
    t.setDaemon(true);
    t.start();
    try {
      t.join(timeoutMs);
    } catch (InterruptedException e) {
      // let it go
    }
    if (t.isAlive()) {
      t.interrupt();
    }
  }

  /**
   * Check if set a contains any element in set b.
   * @param a the first set.
   * @param b the second set.
   * @return {@code true} if a contains at least one of the element in b. {@code false} otherwise;
   */
  public static boolean containsAny(Set<Integer> a, Set<Integer> b) {
    return b.stream().mapToInt(i -> i).anyMatch(a::contains);
  }

  /**
   * Create an instance of AdminClient using the given configurations.
   *
   * @param adminClientConfigs Configurations used for the AdminClient.
   * @return A new instance of AdminClient.
   */
  public static AdminClient createAdminClient(Map<String, Object> adminClientConfigs) {
    return AdminClient.create(adminClientConfigs);
  }

  /**
   * Close the given AdminClient with the default timeout of {@link #ADMIN_CLIENT_CLOSE_TIMEOUT_MS}.
   *
   * @param adminClient AdminClient to be closed.
   */
  public static void closeAdminClientWithTimeout(AdminClient adminClient) {
    closeAdminClientWithTimeout(adminClient, ADMIN_CLIENT_CLOSE_TIMEOUT_MS);
  }

  /**
   * Close the given AdminClient with the given timeout.
   *
   * @param adminClient AdminClient to be closed.
   * @param timeoutMs the timeout.
   */
  public static void closeAdminClientWithTimeout(AdminClient adminClient, long timeoutMs) {
    closeClientWithTimeout(() -> {
      try {
        ((AutoCloseable) adminClient).close();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to close the Admin Client.", e);
      }
    }, timeoutMs);
  }

  /**
   * Parse AdminClient configs based on the given {@link KafkaCruiseControlConfig configs}.
   *
   * @param configs Configs to be used for parsing AdminClient configs.
   * @return AdminClient configs.
   */
  public static Map<String, Object> parseAdminClientConfigs(KafkaCruiseControlConfig configs) {
    Map<String, Object> adminClientConfigs = new HashMap<>();
    // Add bootstrap server.
    List<String> bootstrapServers = configs.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    String bootstrapServersString = bootstrapServers.toString()
                                                    .replace(" ", "")
                                                    .replace("[", "")
                                                    .replace("]", "");
    adminClientConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);
    adminClientConfigs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, configs.getInt(ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG));
    adminClientConfigs.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, configs.getLong(RECONNECT_BACKOFF_MS_CONFIG));

    // Add security protocol (if specified).
    try {
      String securityProtocol = configs.getString(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
      adminClientConfigs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      setStringConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_MECHANISM);
      setClassConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
      setClassConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS);
      setPasswordConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_JAAS_CONFIG);
      setStringConfigIfExists(configs, adminClientConfigs, SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

      // Configure SSL configs (if security protocol is SSL or SASL_SSL)
      if (securityProtocol.equals(SecurityProtocol.SSL.name) || securityProtocol.equals(SecurityProtocol.SASL_SSL.name)) {
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        setStringConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        setPasswordConfigIfExists(configs, adminClientConfigs, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
      }
    } catch (ConfigException ce) {
      // let it go.
    }

    return adminClientConfigs;
  }

  /**
   * Generate a {@link MetadataResponseData} with the given information -- e.g. for creating bootstrap and test response.
   *
   * @param brokers Brokers in the cluster.
   * @param clusterId Cluster Id.
   * @param controllerId Controller Id.
   * @param topicMetadataList Metadata list for the topics in the cluster.
   * @return A {@link MetadataResponseData} with the given information.
   */
  public static MetadataResponse prepareMetadataResponse(List<Node> brokers,
                                                         String clusterId,
                                                         int controllerId,
                                                         List<MetadataResponse.TopicMetadata> topicMetadataList) {
    MetadataResponseData responseData = new MetadataResponseData();
    responseData.setThrottleTimeMs(AbstractResponse.DEFAULT_THROTTLE_TIME);
    brokers.forEach(broker -> responseData.brokers().add(
        new MetadataResponseData.MetadataResponseBroker().setNodeId(broker.id())
                                                         .setHost(broker.host())
                                                         .setPort(broker.port())
                                                         .setRack(broker.rack())));

    responseData.setClusterId(clusterId);
    responseData.setControllerId(controllerId);
    responseData.setClusterAuthorizedOperations(MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
    topicMetadataList.forEach(topicMetadata -> responseData.topics().add(prepareMetadataResponseTopic(topicMetadata)));

    try {
      return createMetadataResponse(responseData);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to create metadata response instance as class could not be found", e);
    }

  }

  /**
   * Creates a {@link MetadataResponse} instance using the appropriate constructor for the version of Kafka on the
   * classpath. It will attempt to use the 2.8+ version first and then fall back to the 2.5+ version. If neither work, a
   * {@link NoSuchElementException} will be thrown.
   *
   * @param responseData The response data to be included in the returned {@link MetadataResponse} instance.
   * @return The {@link MetadataResponse} instance containing the supplied {@link MetadataResponseData}.
   * @throws ClassNotFoundException If a version of {@link MetadataResponse} cannot be found on the classpath.
   */
  private static MetadataResponse createMetadataResponse(MetadataResponseData responseData) throws ClassNotFoundException {

    Class<?> metadataResponseClass = Class.forName(MetadataResponse.class.getName());

    MetadataResponse response = null;

    try {
      // In Kafka 2.8 we need to supply the Metadata response API version (in 2.5 this was version 9 and version 11 in 2.8)
      Constructor<?> kafka28PlusCon = metadataResponseClass.getConstructor(MetadataResponseData.class, short.class);
      response = (MetadataResponse) kafka28PlusCon.newInstance(responseData, (short) 11);
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.debug("Unable to find Kafka 2.8+ constructor for MetadataResponse class", e);
    }

    if (response == null) {
      try {
        Constructor<?> kafka25PlusCon = metadataResponseClass.getConstructor(MetadataResponseData.class);
        response = (MetadataResponse) kafka25PlusCon.newInstance(responseData);
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        LOG.debug("Unable to find Kafka 2.5+ constructor for MetadataResponse class", e);
      }
    }

    if (response != null) {
      return response;
    } else {
      throw new NoSuchElementException("Unable to find viable constructor for MetadataResponse class");
    }
  }

  private static MetadataResponseData.MetadataResponseTopic prepareMetadataResponseTopic(MetadataResponse.TopicMetadata topicMetadata) {
    MetadataResponseData.MetadataResponseTopic metadataResponseTopic = new MetadataResponseData.MetadataResponseTopic();
    metadataResponseTopic.setErrorCode(topicMetadata.error().code())
                         .setName(topicMetadata.topic())
                         .setIsInternal(topicMetadata.isInternal())
                         .setTopicAuthorizedOperations(topicMetadata.authorizedOperations());

    for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
      metadataResponseTopic.partitions().add(
          new MetadataResponseData.MetadataResponsePartition()
              .setErrorCode(partitionMetadata.error.code())
              .setPartitionIndex(partitionMetadata.partition())
              .setLeaderId(partitionMetadata.leaderId.orElse(MetadataResponse.NO_LEADER_ID))
              .setLeaderEpoch(partitionMetadata.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH))
              .setReplicaNodes(partitionMetadata.replicaIds)
              .setIsrNodes(partitionMetadata.inSyncReplicaIds)
              .setOfflineReplicas(partitionMetadata.offlineReplicaIds));
    }

    return metadataResponseTopic;
  }

  private static void setPasswordConfigIfExists(KafkaCruiseControlConfig configs, Map<String, Object> props, String name) {
    try {
      props.put(name, configs.getPassword(name));
    } catch (ConfigException ce) {
      // let it go.
    }
  }

  private static void setStringConfigIfExists(KafkaCruiseControlConfig configs, Map<String, Object> props, String name) {
    try {
      props.put(name, configs.getString(name));
    } catch (ConfigException ce) {
      // let it go.
    }
  }

  private static void setClassConfigIfExists(KafkaCruiseControlConfig configs, Map<String, Object> props, String name) {
    try {
      props.put(name, configs.getClass(name));
    } catch (ConfigException ce) {
      // let it go.
    }
  }

  /**
   * Check if the partition is currently under replicated.
   * @param cluster The current cluster state.
   * @param tp The topic partition to check.
   * @return {@code true} if the partition is currently under replicated.
   */
  public static boolean isPartitionUnderReplicated(Cluster cluster, TopicPartition tp) throws
                                                                                       PartitionNotExistsException {
    PartitionInfo partitionInfo = cluster.partition(tp);
    if (partitionInfo == null) {
      throw new PartitionNotExistsException("Partition " + tp + " does not exist.");
    }
    return partitionInfo.inSyncReplicas().length != partitionInfo.replicas().length;
  }

  /**
   * Compare and ensure two sets are disjoint.
   * @param set1 The first set to compare.
   * @param set2 The second set to compare.
   * @param message The exception's detailed message if two sets are not disjoint.
   * @param <E> The type of elements maintained by the sets.
   */
  public static <E> void ensureDisjoint(Set<E> set1, Set<E> set2, String message) {
    Set<E> interSection = new HashSet<>(set1);
    interSection.retainAll(set2);
    if (!interSection.isEmpty()) {
      throw new IllegalStateException(message);
    }
  }

  /**
   * Sanity check whether the given goals exist in the given supported goals.
   * @param goals A list of goals.
   * @param supportedGoals Supported goals.
   */
  public static void sanityCheckNonExistingGoal(List<String> goals, Map<String, Goal> supportedGoals) {
    Set<String> nonExistingGoals = new HashSet<>();
    goals.stream().filter(goalName -> supportedGoals.get(goalName) == null).forEach(nonExistingGoals::add);

    if (!nonExistingGoals.isEmpty()) {
      throw new IllegalArgumentException("Goals " + nonExistingGoals + " are not supported. Supported: " + supportedGoals.keySet());
    }
  }

  /**
   * Get the balancedness cost of violating goals by their name, where the sum of costs is {@link #MAX_BALANCEDNESS_SCORE}.
   *
   * @param goals The goals to be used for balancing (sorted by priority).
   * @param priorityWeight The impact of having one level higher goal priority on the relative balancedness score.
   * @param strictnessWeight The impact of strictness on the relative balancedness score.
   * @return The balancedness cost of violating goals by their name.
   */
  public static Map<String, Double> balancednessCostByGoal(List<Goal> goals, double priorityWeight, double strictnessWeight) {
    if (goals.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get the balancedness cost.");
    } else if (priorityWeight <= 0 || strictnessWeight <= 0) {
      throw new IllegalArgumentException(String.format("Balancedness weights must be positive (priority:%f, strictness:%f).",
                                                       priorityWeight, strictnessWeight));
    }
    Map<String, Double> balancednessCostByGoal = new HashMap<>();
    // Step-1: Get weights.
    double weightSum = 0.0;
    double previousGoalPriorityWeight = (1 / priorityWeight);
    for (int i = goals.size() - 1; i >= 0; i--) {
      Goal goal = goals.get(i);
      double currentGoalPriorityWeight = priorityWeight * previousGoalPriorityWeight;
      double cost = currentGoalPriorityWeight * (goal.isHardGoal() ? strictnessWeight : 1);
      weightSum += cost;
      balancednessCostByGoal.put(goal.name(), cost);
      previousGoalPriorityWeight = currentGoalPriorityWeight;
    }

    // Step-2: Set costs.
    for (Map.Entry<String, Double> entry : balancednessCostByGoal.entrySet()) {
      entry.setValue(MAX_BALANCEDNESS_SCORE * entry.getValue() / weightSum);
    }

    return balancednessCostByGoal;
  }

  /**
   * Reads the configuration file, parses and validates the configs. Enables the configs to be passed
   * in from environment variables.
   * @param propertiesFile is the file containing the Cruise Control configuration.
   * @return a parsed {@link KafkaCruiseControlConfig}
   * @throws IOException if the configuration file can't be read.
   */
  public static KafkaCruiseControlConfig readConfig(String propertiesFile) throws IOException {
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(propertiesFile)) {
      props.put(AbstractConfig.CONFIG_PROVIDERS_CONFIG, ENV_CONFIG_PROVIDER_NAME);
      props.put(AbstractConfig.CONFIG_PROVIDERS_CONFIG + ENV_CONFIG_PROVIDER_CLASS_CONFIG, EnvConfigProvider.class.getName());
      props.load(propStream);
    }
    return new KafkaCruiseControlConfig(props);
  }

  /**
   * Create a Kafka consumer with the given properties.
   *
   * @param configs The configurations for Cruise Control.
   * @param clientIdPrefix Client id prefix.
   * @param bootstrapServers Bootstrap servers.
   * @param keyDeserializer Key deserializer of the consumer.
   * @param valueDeserializer Value deserializer of the consumer.
   * @param isLatestOffsetReset {@code true} to set the value of {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} to "latest", {@code false}
   * to set it to "earliest".
   * @param <K> The type of the consumer key.
   * @param <KT> The type of the key deserializer.
   * @param <V> The type of the consumer value.
   * @param <VT> The type of the value deserializer.
   * @return A new Kafka consumer.
   */
  public static <K, KT extends Deserializer<K>, V, VT extends Deserializer<V>> Consumer<K, V> createConsumer(Map<String, ?> configs,
                                                                                                             String clientIdPrefix,
                                                                                                             String bootstrapServers,
                                                                                                             Class<KT> keyDeserializer,
                                                                                                             Class<VT> valueDeserializer,
                                                                                                             boolean isLatestOffsetReset) {
    long randomToken = RANDOM.nextLong();
    Properties consumerProps = new Properties();
    consumerProps.putAll(configs);
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-consumer-" + randomToken);
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, isLatestOffsetReset ? "latest" : "earliest");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
    consumerProps.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, configs.get(RECONNECT_BACKOFF_MS_CONFIG).toString());
    consumerProps.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    return new KafkaConsumer<>(consumerProps);
  }

  /**
   *
   * Returns the right KafkaCruiseControl app, depending on the vertx.enabled property.
   *
   * @param config The configurations for Cruise Control.
   * @param port The port for the REST API.
   * @param hostname The hostname for the REST API.
   * @return KafkaCruiseControlApp class depending on the vertx.enabled property.
   * @throws ServletException
   */
  public static KafkaCruiseControlApp getCruiseControlApp(KafkaCruiseControlConfig config, Integer port, String hostname) throws ServletException {
    if (config.getBoolean(WebServerConfig.VERTX_ENABLED_CONFIG)) {
      return new KafkaCruiseControlVertxApp(config, port, hostname);
    }
    return new KafkaCruiseControlServletApp(config, port, hostname);
  }

  public enum CompletionType {
    NO_ACTION, COMPLETED, COMPLETED_WITH_ERROR;

    private static final List<CompletionType> CACHED_VALUES = List.of(values());

    /**
     * Use this instead of values() because values() creates a new array each time.
     * @return enumerated values in the same order as values()
     */
    public static List<CompletionType> cachedValues() {
      return Collections.unmodifiableList(CACHED_VALUES);
    }
  }
}
