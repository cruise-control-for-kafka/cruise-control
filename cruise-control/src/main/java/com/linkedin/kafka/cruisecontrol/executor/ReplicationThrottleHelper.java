/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * See https://kafka.apache.org/documentation/#rep-throttle
 */
class ReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationThrottleHelper.class);
  static final String WILDCARD_ASTERISK = "*";
  static final String LEADER_REPLICATION_THROTTLED_RATE_CONFIG = "leader.replication.throttled.rate";
  static final String FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG = "follower.replication.throttled.rate";
  static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
  static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  static final int RETRIES = 30;
  // The backoff between attempts to read back the applied configs grows exponentially up to a cap. Without the cap,
  // the backoff reaches hours within a dozen attempts, which blocks the execution indefinitely if the configs never
  // converge (e.g. because they are changed outside of Cruise Control). With the cap, RETRIES attempts take ~14 minutes.
  private static final long RETRY_BACKOFF_SCALE_MS = TimeUnit.SECONDS.toMillis(5);
  private static final int RETRY_BACKOFF_BASE = 2;
  private static final int MAX_RETRY_BACKOFF_MS = (int) TimeUnit.SECONDS.toMillis(30);
  // Sources of config values that deleting a dynamic broker or topic config does not remove, as they have lower precedence.
  private static final Set<ConfigEntry.ConfigSource> LOWER_PRECEDENCE_CONFIG_SOURCES = Collections.unmodifiableSet(
      EnumSet.of(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                 ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG,
                 ConfigEntry.ConfigSource.DEFAULT_CONFIG));

  private final Admin _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  private final Set<Integer> _deadBrokers;
  private final AppliedThrottles _appliedThrottles;

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, RETRIES, deadBrokers);
  }

  // for testing
  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, int retries) {
    this(adminClient, throttleRate, retries, new HashSet<>());
  }

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, int retries, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, retries, deadBrokers, new AppliedThrottles());
  }

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, int retries, Set<Integer> deadBrokers,
                            AppliedThrottles appliedThrottles) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._deadBrokers = deadBrokers;
    this._appliedThrottles = appliedThrottles;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      LOG.info("Setting a rebalance throttle of {} bytes/sec", _throttleRate);
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
      // Record the throttles before setting them, so that the throttles are removed even if setting them fails halfway.
      _appliedThrottles._brokers.addAll(participatingBrokers);
      _appliedThrottles._proposals.addAll(replicaMovementProposals);
      for (int broker : participatingBrokers) {
        setThrottledRateIfNecessary(broker);
      }
      for (Map.Entry<String, Set<String>> entry : throttledReplicas.entrySet()) {
        setThrottledReplicas(entry.getKey(), entry.getValue());
      }
    }
  }

  // Determines if a candidate task is ready to have its throttles removed.
  boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return
      // the task should not be in progress
      task.state() != ExecutionTaskState.IN_PROGRESS
      // the task should not be pending
      && task.state() != ExecutionTaskState.PENDING
      // replica throttles only apply to inter-broker replica movement
      && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // determines if a candidate task is in progress and related to inter-broker
  // replica movement.
  boolean taskIsInProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // clear throttles for a specific list of execution tasks
  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks) throws InterruptedException {
    if (throttlingEnabled()) {
      List<ExecutionProposal> completedProposals =
        completedTasks
          .stream()
          // Filter for completed tasks related to inter-broker replica movement
          .filter(this::shouldRemoveThrottleForTask)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which have completed a task with
      // inter-broker replica movement
      Set<Integer> participatingBrokers = getParticipatingBrokers(completedProposals);

      List<ExecutionProposal> inProgressProposals =
        inProgressTasks
          .stream()
          .filter(this::taskIsInProgress)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which currently have in-progress
      // inter-broker replica movement
      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(inProgressProposals);

      // Remove the brokers with in-progress replica moves from the brokers that have
      // completed inter-broker replica moves
      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(participatingBrokers);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing replica movement throttles from brokers in the cluster: {}", brokersToRemoveThrottlesFrom);
      removeThrottles(brokersToRemoveThrottlesFrom, completedProposals);
    }
  }

  /**
   * Removes the throttles that were set, but not removed yet -- e.g. because the execution that set them failed while
   * replicas were still moving, or a broker was unavailable when its throttle was to be removed. The throttles that the
   * given ongoing partition reassignments still need, and the throttled rates of unavailable brokers are retained, so that
   * a later call can remove them.
   *
   * @param partitionsBeingReassigned Partitions that are being reassigned.
   */
  void clearLeftoverThrottles(Set<TopicPartition> partitionsBeingReassigned)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (_appliedThrottles.isEmpty()) {
      return;
    }
    List<ExecutionProposal> finishedProposals = new ArrayList<>();
    List<ExecutionProposal> inProgressProposals = new ArrayList<>();
    for (ExecutionProposal proposal : _appliedThrottles._proposals) {
      if (partitionsBeingReassigned.contains(proposal.topicPartition())) {
        inProgressProposals.add(proposal);
      } else {
        finishedProposals.add(proposal);
      }
    }
    Set<Integer> aliveBrokers = _adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                            .stream().map(Node::id).collect(Collectors.toSet());
    Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(_appliedThrottles._brokers);
    brokersToRemoveThrottlesFrom.retainAll(aliveBrokers);
    brokersToRemoveThrottlesFrom.removeAll(getParticipatingBrokers(inProgressProposals));
    if (!brokersToRemoveThrottlesFrom.isEmpty() || !finishedProposals.isEmpty()) {
      LOG.info("Removing leftover replica movement throttles from brokers {} and from the replicas of {} finished partition movements "
               + "(retaining the throttles of {} ongoing partition movements).", brokersToRemoveThrottlesFrom, finishedProposals.size(),
               inProgressProposals.size());
      removeThrottles(brokersToRemoveThrottlesFrom, finishedProposals);
    }
  }

  /**
   * Removes the throttled rate from the given brokers, and the throttled replicas of the given proposals from their topics.
   * Failing to remove a throttle does not stop removing the others, but fails the call once the others are removed. Throttles
   * that could not be removed are retained in {@link #_appliedThrottles}, so that they can be removed later via
   * {@link #clearLeftoverThrottles(Set)}.
   *
   * @param brokers Brokers to remove the throttled rate from.
   * @param proposals Proposals whose replicas are no longer to be throttled.
   */
  private void removeThrottles(Set<Integer> brokers, List<ExecutionProposal> proposals) throws InterruptedException {
    Set<Integer> failedBrokers = new TreeSet<>();
    Set<String> failedTopics = new TreeSet<>();
    Exception failure = null;
    for (int broker : brokers) {
      try {
        removeThrottledRateFromBroker(broker);
        _appliedThrottles._brokers.remove(broker);
      } catch (ExecutionException | TimeoutException | RuntimeException e) {
        LOG.warn("Failed to remove the replication throttled rate from broker {}.", broker, e);
        failedBrokers.add(broker);
        failure = failure == null ? e : failure;
      }
    }

    Map<String, List<ExecutionProposal>> proposalsByTopic = proposals.stream().collect(Collectors.groupingBy(ExecutionProposal::topic));
    for (Map.Entry<String, List<ExecutionProposal>> entry : proposalsByTopic.entrySet()) {
      String topic = entry.getKey();
      try {
        removeThrottledReplicasFromTopic(topic, getThrottledReplicasByTopic(entry.getValue()).get(topic));
        _appliedThrottles._proposals.removeAll(entry.getValue());
      } catch (ExecutionException | TimeoutException | RuntimeException e) {
        LOG.warn("Failed to remove the throttled replicas of topic {}.", topic, e);
        failedTopics.add(topic);
        failure = failure == null ? e : failure;
      }
    }

    if (failure != null) {
      throw new IllegalStateException(String.format("Failed to remove the replication throttles of brokers %s and topics %s.",
                                                    failedBrokers, failedTopics), failure);
    }
  }

  private boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionProposal> replicaMovementProposals) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      participatingBrokers.addAll(proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
      participatingBrokers.addAll(proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
    }
    participatingBrokers.removeAll(_deadBrokers);
    return participatingBrokers;
  }

  private Map<String, Set<String>> getThrottledReplicasByTopic(List<ExecutionProposal> replicaMovementProposals) {
    Map<String, Set<String>> throttledReplicasByTopic = new HashMap<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      String topic = proposal.topic();
      int partitionId = proposal.partitionId();
      Stream<Integer> brokers = Stream.concat(
        proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId),
        proposal.replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId));
      Set<String> throttledReplicas = throttledReplicasByTopic
        .computeIfAbsent(topic, x -> new TreeSet<>());
      brokers.forEach(brokerId -> throttledReplicas.add(partitionId + ":" + brokerId));
    }
    return throttledReplicasByTopic;
  }

  private void setThrottledRateIfNecessary(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    Config brokerConfigs = getBrokerConfigs(brokerId);
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleRateConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
      ConfigEntry currThrottleRate = brokerConfigs.get(replicaThrottleRateConfigKey);
      if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(_throttleRate))) {
        LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, _throttleRate, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleRateConfigKey, String.valueOf(_throttleRate)), AlterConfigOp.OpType.SET));
      }
    }
    if (!ops.isEmpty()) {
      changeBrokerConfigs(brokerId, ops);
    }
  }

  private Config getTopicConfigs(String topic) throws ExecutionException, InterruptedException, TimeoutException {
    try {
      return getEntityConfigs(new ConfigResource(ConfigResource.Type.TOPIC, topic));
    } catch (Exception e) {
      if (!topicExists(topic)) {
        return new Config(Collections.emptyList());
      }
      throw e;
    }
  }

  private Config getBrokerConfigs(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    return getEntityConfigs(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)));
  }

  private Config getEntityConfigs(ConfigResource cf) throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, Config> configs = _adminClient.describeConfigs(Collections.singletonList(cf)).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return configs.get(cf);
  }

  private void setThrottledReplicas(String topic, Set<String> replicas)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config topicConfigs = getTopicConfigs(topic);
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
      ConfigEntry currThrottledReplicas = topicConfigs.get(replicaThrottleConfigKey);
      if (currThrottledReplicas != null && currThrottledReplicas.value().trim().equals(WILDCARD_ASTERISK)) {
        // The existing setup throttles all replica. So, nothing needs to be changed.
        continue;
      }

      // Merge new throttled replicas with existing configuration values.
      Set<String> newThrottledReplicas = new TreeSet<>(replicas);
      if (currThrottledReplicas != null && !currThrottledReplicas.value().isEmpty()) {
        newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
      }
      ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)), AlterConfigOp.OpType.SET));
    }
    if (!ops.isEmpty()) {
      changeTopicConfigs(topic, ops);
    }
  }

  void changeTopicConfigs(String topic, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    changeTopicConfigs(topic, ops, Collections.emptyMap());
  }

  void changeTopicConfigs(String topic, Collection<AlterConfigOp> ops, Map<String, String> deletedValues)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    try {
      _adminClient.incrementalAlterConfigs(configs).all()
          .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      waitForConfigs(cf, ops, deletedValues);
    } catch (Exception e) {
      if (!topicExists(topic)) {
        LOG.debug("Failed to change configs for topic {} since it does not exist", topic);
        return;
      }
      throw e;
    }
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    changeBrokerConfigs(brokerId, ops, Collections.emptyMap());
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops, Map<String, String> deletedValues)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    waitForConfigs(cf, ops, deletedValues);
  }

  boolean topicExists(String topic) throws InterruptedException, TimeoutException, ExecutionException {
    try {
      return _adminClient.listTopics().names().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).contains(topic);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Unable to check if topic {} exists due to {}", topic, e.getMessage());
      throw e;
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    List<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  /**
   * It gets whether there is any throttled replica specified in the configuration property. If there is and the
   * specified throttled replica does not equal to "*", it modifies the configuration property by removing a
   * given set of replicas from the set of throttled replicas
   *
   * @param topic name of topic which contains <code>replicas</code>
   * @param replicas replicas to remove from the configuration properties
   */
  private void removeThrottledReplicasFromTopic(String topic, Set<String> replicas)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config topicConfigs = getTopicConfigs(topic);
    if (topicConfigs == null) {
      LOG.debug("Skip removing throttled replicas {} from topic {} since no configs can be read", String.join(",", replicas), topic);
      return;
    }
    List<AlterConfigOp> ops = new ArrayList<>();
    Map<String, String> deletedValues = new HashMap<>();

    ConfigEntry currentLeaderThrottledReplicas = topicConfigs.get(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentLeaderThrottledReplicas != null) {
      if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle");
      } else {
        replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
          deletedValues.put(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, currentLeaderThrottledReplicas.value());
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    ConfigEntry currentFollowerThrottledReplicas = topicConfigs.get(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentFollowerThrottledReplicas != null) {
      if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle");
      } else {
        replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
          deletedValues.put(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, currentFollowerThrottledReplicas.value());
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    if (!ops.isEmpty()) {
      changeTopicConfigs(topic, ops, deletedValues);
    }
  }

  private void removeThrottledRateFromBroker(Integer brokerId)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config brokerConfigs = getBrokerConfigs(brokerId);
    ConfigEntry currLeaderThrottle = brokerConfigs.get(LEADER_REPLICATION_THROTTLED_RATE_CONFIG);
    ConfigEntry currFollowerThrottle = brokerConfigs.get(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG);
    List<AlterConfigOp> ops = new ArrayList<>();
    Map<String, String> deletedValues = new HashMap<>();
    if (currLeaderThrottle != null) {
      if (LOWER_PRECEDENCE_CONFIG_SOURCES.contains(currLeaderThrottle.source())) {
        LOG.debug("Skipping removal for leader throttle rate not set on broker {}: {}", brokerId, currLeaderThrottle);
      } else {
        LOG.debug("Removing leader throttle rate: {} on broker {}", currLeaderThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, null), AlterConfigOp.OpType.DELETE));
        deletedValues.put(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, currLeaderThrottle.value());
      }
    }
    if (currFollowerThrottle != null) {
      if (LOWER_PRECEDENCE_CONFIG_SOURCES.contains(currFollowerThrottle.source())) {
        LOG.debug("Skipping removal for follower throttle rate not set on broker {}: {}", brokerId, currFollowerThrottle);
      } else {
        LOG.debug("Removing follower throttle rate: {} on broker {}", currFollowerThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, null), AlterConfigOp.OpType.DELETE));
        deletedValues.put(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, currFollowerThrottle.value());
      }
    }
    if (!ops.isEmpty()) {
      changeBrokerConfigs(brokerId, ops, deletedValues);
    }
  }

  // Retries until we can read the configs changes we just wrote
  void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops) {
    waitForConfigs(cf, ops, Collections.emptyMap());
  }

  /**
   * Wait until the given config changes are applied to the given entity.
   *
   * @param cf The entity whose configs are changed.
   * @param ops The config changes.
   * @param deletedValues The values of the configs that {@code ops} delete, as read before deleting them (if known).
   */
  private void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops, Map<String, String> deletedValues) {
    // Use HashMap::new instead of Collectors.toMap to allow inserting null values
    Map<String, String> expectedConfigs = ops.stream()
            .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
    AtomicInteger attempts = new AtomicInteger();
    boolean retryResponse = CruiseControlMetricsUtils.retry(() -> {
      try {
        Config configs = getEntityConfigs(cf);
        if (configsEqual(configs, expectedConfigs, deletedValues)) {
          return false;
        }
        LOG.warn("Attempt {}/{}: configs {} are not applied to {} yet, current values: {}. If this persists, check whether "
                 + "these configs are being changed outside of Cruise Control.", attempts.incrementAndGet(), _retries,
                 expectedConfigs, cf, currentValues(configs, expectedConfigs.keySet()));
        return true;
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        return false;
      }
    }, RETRY_BACKOFF_SCALE_MS, RETRY_BACKOFF_BASE, _retries, MAX_RETRY_BACKOFF_MS);
    if (!retryResponse) {
      throw new IllegalStateException("The following configs " + ops + " were not applied to " + cf + " within the time limit");
    }
  }

  private static String currentValues(Config configs, Set<String> configNames) {
    return configNames.stream().map(name -> {
      ConfigEntry configEntry = configs.get(name);
      return configEntry == null ? name + "=null" : String.format("%s=%s (%s)", name, configEntry.value(), configEntry.source());
    }).collect(Collectors.joining(", ", "{", "}"));
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    return configsEqual(configs, expectedValues, Collections.emptyMap());
  }

  /**
   * @param configs The configs read from an entity.
   * @param expectedValues The expected values of the configs, {@code null} for a deleted config.
   * @param deletedValues The values of the deleted configs before they were deleted (if known).
   * @return {@code true} if the given configs have the expected values, {@code false} otherwise.
   */
  static boolean configsEqual(Config configs, Map<String, String> expectedValues, Map<String, String> deletedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if (LOWER_PRECEDENCE_CONFIG_SOURCES.contains(configEntry.source()) && entry.getValue() == null) {
        if (configEntry.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG && configEntry.value().equals(deletedValues.get(entry.getKey()))) {
          // A broker may report the Kafka default as the source of a deleted config before it stops reporting the deleted value.
          return false;
        }
        // Deleting a dynamic config does not remove a value from a lower precedence source, e.g. the cluster-wide default.
        LOG.debug("Found {} config: {}, skipping comparison", configEntry.source(), configEntry);
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }

  /**
   * The replication throttles that have been set, but not removed yet: the brokers whose throttled rate has been set, and
   * the inter-broker replica movements whose replicas have been throttled. The helpers of consecutive executions share an
   * instance, so that the throttles an execution could not remove are removed later via {@link #clearLeftoverThrottles(Set)}.
   */
  static final class AppliedThrottles {
    private final Set<Integer> _brokers = ConcurrentHashMap.newKeySet();
    private final Set<ExecutionProposal> _proposals = ConcurrentHashMap.newKeySet();

    /**
     * @return {@code true} if there is no throttle to remove, {@code false} otherwise.
     */
    boolean isEmpty() {
      return _brokers.isEmpty() && _proposals.isEmpty();
    }

    /**
     * @return Brokers whose throttled rate has been set, but not removed yet.
     */
    Set<Integer> brokers() {
      return Collections.unmodifiableSet(_brokers);
    }

    /**
     * @return Inter-broker replica movements whose replicas have been throttled, but not unthrottled yet.
     */
    Set<ExecutionProposal> proposals() {
      return Collections.unmodifiableSet(_proposals);
    }
  }
}
