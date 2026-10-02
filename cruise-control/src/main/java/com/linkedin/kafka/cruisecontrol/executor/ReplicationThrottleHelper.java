/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  // Cap on the number of resources packed into a single AdminClient config request. Requests for
  // TOPIC resources are routed to a single broker, so an unbounded batch could produce an
  // oversized request on clusters with a very large number of topics.
  static final int MAX_RESOURCES_PER_ADMIN_REQUEST = 500;

  private final AdminClient _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  private final Set<Integer> _deadBrokers;

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, RETRIES, deadBrokers);
  }

  // for testing
  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._deadBrokers = new HashSet<Integer>();
  }

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries, Set<Integer> deadBrokers) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._deadBrokers = deadBrokers;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
      LOG.info("Setting rebalance throttle of {} bytes/sec on {} brokers for {} topics ({} replica movements)",
          _throttleRate, participatingBrokers.size(), throttledReplicas.size(), replicaMovementProposals.size());

      // Batch set broker throttle rates. Reads are chunked like every other AdminClient call in
      // this class; each future is resolved eagerly and any failure is rethrown, preserving the
      // fail-fast semantics of the previous .all() call.
      if (!participatingBrokers.isEmpty()) {
        List<ConfigResource> brokerResources = participatingBrokers.stream()
            .map(id -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)))
            .collect(Collectors.toList());
        Map<ConfigResource, KafkaFuture<Config>> brokerFutures = describeConfigsInChunks(brokerResources);

        Map<ConfigResource, Collection<AlterConfigOp>> brokerOps = new HashMap<>();
        for (int brokerId : participatingBrokers) {
          ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
          Config config = brokerFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          List<AlterConfigOp> ops = buildSetThrottleRateOps(config, brokerId);
          if (!ops.isEmpty()) {
            brokerOps.put(cf, ops);
          }
        }

        if (!brokerOps.isEmpty()) {
          LOG.info("Updating throttle rate on {} out of {} brokers", brokerOps.size(), participatingBrokers.size());
          batchAlterBrokerConfigs(brokerOps);
        }
      }

      // Batch set topic throttled replicas.
      // Note: batching widens the read-modify-write window compared to the old per-topic approach.
      // This assumes Cruise Control is the sole writer of throttle configs during an operation.
      if (!throttledReplicas.isEmpty()) {
        Map<String, Config> topicConfigs = batchGetTopicConfigs(throttledReplicas.keySet());

        Map<ConfigResource, Collection<AlterConfigOp>> topicOps = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : throttledReplicas.entrySet()) {
          String topic = entry.getKey();
          Config config = topicConfigs.get(topic);
          if (config == null) {
            LOG.warn("Skip setting throttled replicas for topic {} since no configs can be read", topic);
            continue;
          }
          List<AlterConfigOp> ops = buildSetThrottledReplicasOps(config, entry.getValue());
          if (!ops.isEmpty()) {
            topicOps.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), ops);
          }
        }

        if (!topicOps.isEmpty()) {
          LOG.info("Setting throttled replicas on {} out of {} topics", topicOps.size(), throttledReplicas.size());
          batchAlterTopicConfigs(topicOps);
        }
      }

      LOG.info("Throttle setup complete for {} brokers and {} topics",
          participatingBrokers.size(), throttledReplicas.size());
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
  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks)
  throws ExecutionException, InterruptedException, TimeoutException {
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

      LOG.info("Removing replica movement throttles from {} brokers in the cluster: {}",
          brokersToRemoveThrottlesFrom.size(), brokersToRemoveThrottlesFrom);

      // Batch remove broker throttle rates. Read configs via per-broker futures so that a broker
      // which became unreachable mid-execution only skips throttle removal for itself; throttles
      // on the remaining brokers are still cleared.
      if (!brokersToRemoveThrottlesFrom.isEmpty()) {
        List<ConfigResource> brokerResources = brokersToRemoveThrottlesFrom.stream()
            .map(id -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)))
            .collect(Collectors.toList());
        Map<ConfigResource, KafkaFuture<Config>> brokerFutures = describeConfigsInChunks(brokerResources);

        Map<ConfigResource, Collection<AlterConfigOp>> brokerOps = new HashMap<>();
        for (ConfigResource cf : brokerResources) {
          Config config;
          try {
            config = brokerFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          } catch (ExecutionException | TimeoutException e) {
            LOG.warn("Failed to read configs for broker {} while clearing throttles. Skipping throttle removal for it.",
                cf.name(), e);
            continue;
          }
          List<AlterConfigOp> ops = buildRemoveThrottleRateOps(config, Integer.parseInt(cf.name()));
          if (!ops.isEmpty()) {
            brokerOps.put(cf, ops);
          }
        }

        if (!brokerOps.isEmpty()) {
          batchAlterBrokerConfigs(brokerOps);
        }
      }

      // Batch remove topic throttled replicas
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(completedProposals);
      if (!throttledReplicas.isEmpty()) {
        Map<String, Config> topicConfigs = batchGetTopicConfigs(throttledReplicas.keySet());

        Map<ConfigResource, Collection<AlterConfigOp>> topicOps = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : throttledReplicas.entrySet()) {
          String topic = entry.getKey();
          Config config = topicConfigs.get(topic);
          if (config == null) {
            LOG.debug("Skip removing throttled replicas {} from topic {} since no configs can be read",
                String.join(",", entry.getValue()), topic);
            continue;
          }
          List<AlterConfigOp> ops = buildRemoveThrottledReplicasOps(config, topic, entry.getValue());
          if (!ops.isEmpty()) {
            topicOps.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), ops);
          }
        }

        if (!topicOps.isEmpty()) {
          batchAlterTopicConfigs(topicOps);
        }
      }
    }
  }

  // --- Ops builders extracted from the old single-resource methods ---

  private List<AlterConfigOp> buildSetThrottleRateOps(Config brokerConfigs, int brokerId) {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleRateConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
            FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
      ConfigEntry currThrottleRate = brokerConfigs.get(replicaThrottleRateConfigKey);
      if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(_throttleRate))) {
        LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, _throttleRate, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleRateConfigKey, String.valueOf(_throttleRate)), AlterConfigOp.OpType.SET));
      }
    }
    return ops;
  }

  private List<AlterConfigOp> buildSetThrottledReplicasOps(Config topicConfigs, Set<String> replicas) {
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
      if (currThrottledReplicas != null && !currThrottledReplicas.value().equals("")) {
        newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
      }
      ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)), AlterConfigOp.OpType.SET));
    }
    return ops;
  }

  private List<AlterConfigOp> buildRemoveThrottleRateOps(Config brokerConfigs, int brokerId) {
    List<AlterConfigOp> ops = new ArrayList<>();
    ConfigEntry currLeaderThrottle = brokerConfigs.get(LEADER_REPLICATION_THROTTLED_RATE_CONFIG);
    if (currLeaderThrottle != null) {
      if (currLeaderThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)) {
        LOG.debug("Skipping removal for static leader throttle rate: {} on broker {}", currLeaderThrottle, brokerId);
      } else {
        LOG.debug("Removing leader throttle rate: {} on broker {}", currLeaderThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, null), AlterConfigOp.OpType.DELETE));
      }
    }
    ConfigEntry currFollowerThrottle = brokerConfigs.get(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG);
    if (currFollowerThrottle != null) {
      if (currFollowerThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)) {
        LOG.debug("Skipping removal for static follower throttle rate: {} on broker {}", currFollowerThrottle, brokerId);
      } else {
        LOG.debug("Removing follower throttle rate: {} on broker {}", currFollowerThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, null), AlterConfigOp.OpType.DELETE));
      }
    }
    return ops;
  }

  private List<AlterConfigOp> buildRemoveThrottledReplicasOps(Config topicConfigs, String topic, Set<String> replicas) {
    List<AlterConfigOp> ops = new ArrayList<>();
    ConfigEntry currentLeaderThrottledReplicas = topicConfigs.get(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentLeaderThrottledReplicas != null) {
      if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle for topic {}", topic);
      } else {
        replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    ConfigEntry currentFollowerThrottledReplicas = topicConfigs.get(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentFollowerThrottledReplicas != null) {
      if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle for topic {}", topic);
      } else {
        replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    return ops;
  }

  // --- Batch AdminClient helpers ---

  /**
   * Batch-read topic configs, gracefully handling non-existent topics by returning empty configs.
   * Uses describeConfigs().values() for per-topic error handling.
   *
   * @param topics the set of topic names to read configs for
   * @return a map from topic name to its config, with empty configs for non-existent topics
   */
  private Map<String, Config> batchGetTopicConfigs(Set<String> topics)
  throws ExecutionException, InterruptedException, TimeoutException {
    // Sort topic names for deterministic ordering in logs and diagnostics
    List<ConfigResource> resources = topics.stream()
        .sorted()
        .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
        .collect(Collectors.toList());

    Map<ConfigResource, KafkaFuture<Config>> futures = describeConfigsInChunks(resources);
    Map<String, Config> result = new HashMap<>();
    // Fetched lazily and at most once, to resolve failures without issuing one listTopics per topic.
    Set<String> existingTopics = null;

    for (ConfigResource cf : resources) {
      String topic = cf.name();
      try {
        result.put(topic, futures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      } catch (ExecutionException e) {
        if (existingTopics == null) {
          existingTopics = listTopicNames();
        }
        if (existingTopics.contains(topic)) {
          throw e;
        }
        result.put(topic, new Config(Collections.emptyList()));
      }
    }
    return result;
  }

  /**
   * Batch-write broker configs and wait for verification.
   * Uses .all() (fail-fast) rather than .values() (per-resource) because broker config failures are
   * not expected during normal operation -- unlike topics, brokers are not deleted mid-operation.
   * A broker failure here indicates a serious infrastructure issue that should fail the entire
   * throttle setup rather than partially applying configs.
   *
   * @param ops the map of broker config resources to their alter operations
   */
  private void batchAlterBrokerConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    for (Map<ConfigResource, Collection<AlterConfigOp>> chunkOps : partitionOps(ops)) {
      _adminClient.incrementalAlterConfigs(chunkOps)
          .all().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
    waitForBatchConfigs(ops);
  }

  /**
   * Batch-write topic configs using values() for per-topic error handling, then wait for
   * verification on successfully written topics.
   *
   * @param ops the map of topic config resources to their alter operations
   */
  private void batchAlterTopicConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>(ops.size());
    for (Map<ConfigResource, Collection<AlterConfigOp>> chunkOps : partitionOps(ops)) {
      futures.putAll(_adminClient.incrementalAlterConfigs(chunkOps).values());
    }
    Map<ConfigResource, Collection<AlterConfigOp>> successfulOps = new HashMap<>();
    // Fetched lazily and at most once, to resolve failures without issuing one listTopics per topic.
    Set<String> existingTopics = null;

    for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : futures.entrySet()) {
      String topic = entry.getKey().name();
      try {
        entry.getValue().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        successfulOps.put(entry.getKey(), ops.get(entry.getKey()));
      } catch (ExecutionException e) {
        if (existingTopics == null) {
          existingTopics = listTopicNames();
        }
        if (existingTopics.contains(topic)) {
          throw e;
        }
        LOG.debug("Failed to change configs for topic {} since it does not exist", topic);
      }
    }

    if (!successfulOps.isEmpty()) {
      waitForBatchConfigs(successfulOps);
    }
  }

  /**
   * Batch verify configs by polling describeConfigs until all resources match expected values.
   * Uses per-resource futures via values() so that verification of one resource does not depend on
   * the others. A topic that is confirmed deleted mid-verification is dropped from verification;
   * any other per-resource failure (e.g. a transient network error, or a broker read failure)
   * keeps the resource pending so that it is retried instead of silently passing unverified.
   *
   * @param allOps the map of config resources to their expected alter operations
   */
  void waitForBatchConfigs(Map<ConfigResource, Collection<AlterConfigOp>> allOps) {
    // Resources that still await verification. Verified resources and confirmed-deleted topics are
    // removed as verification progresses.
    Map<ConfigResource, Map<String, String>> pendingByResource = new HashMap<>();
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : allOps.entrySet()) {
      // Use HashMap::new instead of Collectors.toMap to allow inserting null values
      Map<String, String> expected = entry.getValue().stream()
          .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
      pendingByResource.put(entry.getKey(), expected);
    }

    boolean verified = CruiseControlMetricsUtils.retry(() -> {
      try {
        if (pendingByResource.isEmpty()) {
          return false;
        }
        Map<ConfigResource, KafkaFuture<Config>> futures = describeConfigsInChunks(new ArrayList<>(pendingByResource.keySet()));
        // Existing topic names, fetched lazily and at most once per verification attempt, shared
        // by all topics that fail verification in this attempt. Remains null if the listing fails,
        // in which case topics are conservatively treated as still existing and kept pending.
        Set<String> existingTopics = null;
        boolean topicListFetchAttempted = false;
        Iterator<Map.Entry<ConfigResource, Map<String, String>>> pendingIter = pendingByResource.entrySet().iterator();
        while (pendingIter.hasNext()) {
          Map.Entry<ConfigResource, Map<String, String>> entry = pendingIter.next();
          ConfigResource cf = entry.getKey();
          try {
            Config config = futures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (configsEqual(config, entry.getValue())) {
              pendingIter.remove();
            }
          } catch (ExecutionException | TimeoutException e) {
            if (cf.type() == ConfigResource.Type.TOPIC && !topicListFetchAttempted) {
              topicListFetchAttempted = true;
              existingTopics = tryListTopicNames();
            }
            if (cf.type() == ConfigResource.Type.TOPIC && existingTopics != null && !existingTopics.contains(cf.name())) {
              LOG.debug("Skipping config verification for topic {} since it no longer exists", cf.name());
              pendingIter.remove();
            } else {
              // Keep the resource pending: a transient failure must not let an unverified
              // (potentially unthrottled) config pass silently.
              LOG.warn("Failed to verify config for {}; will retry", cf, e);
            }
          }
        }
        return !pendingByResource.isEmpty();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during batch config verification for {} resources, skipping verification",
            pendingByResource.size(), e);
        Thread.currentThread().interrupt();
        return false;
      }
    }, _retries);
    if (!verified) {
      throw new IllegalStateException("The following configs were not applied within the time limit: " + pendingByResource);
    }
  }

  /**
   * Best-effort topic listing used during verification. A failure to list is surfaced as
   * {@code null} so that callers conservatively treat topics as still existing and keep
   * retrying instead of passing unverified.
   *
   * @return the set of existing topic names, or {@code null} if the listing failed
   */
  private Set<String> tryListTopicNames() {
    try {
      return listTopicNames();
    } catch (ExecutionException | TimeoutException e) {
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Issue describeConfigs in chunks of at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources
   * and return the per-resource futures from all chunks.
   *
   * @param resources the config resources to describe
   * @return a map from each resource to its config future
   */
  private Map<ConfigResource, KafkaFuture<Config>> describeConfigsInChunks(List<ConfigResource> resources) {
    Map<ConfigResource, KafkaFuture<Config>> futures = new HashMap<>(resources.size());
    for (int i = 0; i < resources.size(); i += MAX_RESOURCES_PER_ADMIN_REQUEST) {
      List<ConfigResource> chunk = resources.subList(i, Math.min(resources.size(), i + MAX_RESOURCES_PER_ADMIN_REQUEST));
      futures.putAll(_adminClient.describeConfigs(chunk).values());
    }
    return futures;
  }

  /**
   * Partition an alter-config request into chunks of at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources.
   *
   * @param ops the full map of config resources to their alter operations
   * @return the input split into chunks, each holding at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources
   */
  private static List<Map<ConfigResource, Collection<AlterConfigOp>>> partitionOps(Map<ConfigResource, Collection<AlterConfigOp>> ops) {
    List<Map<ConfigResource, Collection<AlterConfigOp>>> chunks = new ArrayList<>();
    Map<ConfigResource, Collection<AlterConfigOp>> currentChunk = new HashMap<>();
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : ops.entrySet()) {
      if (currentChunk.size() == MAX_RESOURCES_PER_ADMIN_REQUEST) {
        chunks.add(currentChunk);
        currentChunk = new HashMap<>();
      }
      currentChunk.put(entry.getKey(), entry.getValue());
    }
    if (!currentChunk.isEmpty()) {
      chunks.add(currentChunk);
    }
    return chunks;
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

  private Set<String> listTopicNames() throws InterruptedException, TimeoutException, ExecutionException {
    try {
      return _adminClient.listTopics().names().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Unable to list topics due to {}", e.getMessage());
      throw e;
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    List<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if (configEntry.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) && entry.getValue() == null) {
        LOG.debug("Found static broker config: {}, skipping comparison", configEntry);
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }
}
