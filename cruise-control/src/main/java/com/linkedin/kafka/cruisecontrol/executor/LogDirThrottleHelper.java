/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ReplicationThrottleHelper.CLIENT_REQUEST_TIMEOUT_MS;

/**
 * Throttles intra-broker replica movements -- i.e. moving replicas between the logdirs of a broker (see KIP-113), by setting
 * the dynamic broker config {@value #LOG_DIR_THROTTLE_CONFIG} on the brokers with intra-broker replica movements, and restores
 * the original per-broker value of the config (if any) once the execution finishes.
 *
 * <p>The throttle only limits copying replicas to their destination logdirs on a broker. Hence, it has no effect on a broker
 * without ongoing intra-broker replica movements, and the throttles are restored once at the end of the execution rather than
 * as the movements of each broker complete. Requests to the cluster are batched across brokers.</p>
 *
 * <p>The brokers this helper has throttled, and the value each of them had before, are recorded in an {@link AppliedThrottles}
 * that the caller owns -- i.e. it outlives this helper. A broker whose original value cannot be restored stays in that record,
 * so that the removal can be retried after the execution (see {@code Executor#removeLeftoverLogDirThrottles()}). Without that,
 * a single failed restore would leave a rate limit on a broker permanently: the config is a dynamic broker config, so it
 * survives broker restarts, and a later execution would read it as the operator's own value and faithfully put it back.</p>
 *
 * <p>This class is not thread safe.</p>
 */
class LogDirThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(LogDirThrottleHelper.class);
  static final String LOG_DIR_THROTTLE_CONFIG = "replica.alter.log.dirs.io.max.bytes.per.second";
  static final int MAX_RESOURCES_PER_ADMIN_REQUEST = 500;
  static final int RETRIES = 30;
  static final long RETRY_BACKOFF_SCALE_MS = 500L;
  static final int RETRY_BACKOFF_BASE = 2;
  static final int MAX_RETRY_SLEEP_MS = (int) TimeUnit.SECONDS.toMillis(10);

  private final Admin _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  // Brokers throttled by this helper, mapped to their original per-broker value of the config (null if they had none).
  // Owned by the caller, so that a throttle this helper fails to restore can be removed later.
  private final Map<Integer, String> _originalThrottleByBroker;

  /**
   * The brokers that have been throttled but not restored yet, mapped to the original per-broker value of the config of each
   * of them (null if a broker had none). Owned by the {@code Executor} rather than by a helper, because it must outlive the
   * execution that applied the throttles: see the class javadoc.
   */
  static class AppliedThrottles {
    private final Map<Integer, String> _originalThrottleByBroker = new HashMap<>();

    Map<Integer, String> originalThrottleByBroker() {
      return _originalThrottleByBroker;
    }

    boolean isEmpty() {
      return _originalThrottleByBroker.isEmpty();
    }
  }

  /**
   * @param adminClient The adminClient to describe and alter broker configs.
   * @param throttleRate The throttle (bytes/second) to apply to intra-broker replica movements (if null, no throttling is applied).
   * @param appliedThrottles The record of throttled brokers to populate, owned by the caller.
   */
  LogDirThrottleHelper(Admin adminClient, Long throttleRate, AppliedThrottles appliedThrottles) {
    this(adminClient, throttleRate, appliedThrottles, RETRIES);
  }

  // for testing
  LogDirThrottleHelper(Admin adminClient, Long throttleRate, AppliedThrottles appliedThrottles, int retries) {
    _adminClient = adminClient;
    _throttleRate = throttleRate;
    _retries = retries;
    _originalThrottleByBroker = appliedThrottles.originalThrottleByBroker();
  }

  /**
   * @return The brokers throttled by this helper, whose throttle has not been restored yet.
   */
  Set<Integer> throttledBrokers() {
    return Collections.unmodifiableSet(new TreeSet<>(_originalThrottleByBroker.keySet()));
  }

  /**
   * Set the throttle on the brokers of the given intra-broker replica movement tasks that have not been throttled yet. The configs
   * of all such brokers are described, altered, and verified with a single batch of requests.
   *
   * @param intraBrokerReplicaMovementTasks Intra-broker replica movement tasks to be executed.
   * @throws IllegalStateException If the throttle cannot be set.
   */
  void setThrottles(Collection<ExecutionTask> intraBrokerReplicaMovementTasks) {
    if (_throttleRate == null) {
      return;
    }
    Set<Integer> brokersToThrottle = new TreeSet<>();
    for (ExecutionTask task : intraBrokerReplicaMovementTasks) {
      if (!_originalThrottleByBroker.containsKey(task.brokerId())) {
        brokersToThrottle.add(task.brokerId());
      }
    }
    if (brokersToThrottle.isEmpty()) {
      return;
    }

    LOG.info("Setting log dir throttle of {} bytes/sec on brokers {}.", _throttleRate, brokersToThrottle);
    String throttleRate = String.valueOf(_throttleRate);
    try {
      Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsInChunks(_adminClient, brokerResources(brokersToThrottle));
      Map<ConfigResource, AlterConfigOp> opByBroker = new HashMap<>();
      for (int brokerId : brokersToThrottle) {
        ConfigResource cf = brokerResource(brokerId);
        ConfigEntry currentThrottle = configFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(LOG_DIR_THROTTLE_CONFIG);
        boolean hasPerBrokerThrottle = isPerBrokerValue(currentThrottle);
        // Track the broker before altering its config so that the throttle is restored even if altering fails midway.
        _originalThrottleByBroker.put(brokerId, hasPerBrokerThrottle ? currentThrottle.value() : null);
        if (!hasPerBrokerThrottle || !throttleRate.equals(currentThrottle.value())) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, throttleRate), AlterConfigOp.OpType.SET));
        }
      }
      List<ConfigResource> failedToAlter = alterAndVerifyConfigs(_adminClient, opByBroker, _retries);
      if (!failedToAlter.isEmpty()) {
        throw new IllegalStateException(String.format("Failed to set %s to %d on %s.", LOG_DIR_THROTTLE_CONFIG, _throttleRate, failedToAlter));
      }
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException(String.format("Failed to set %s on brokers %s.", LOG_DIR_THROTTLE_CONFIG, brokersToThrottle), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(String.format("Interrupted while setting %s on brokers %s.", LOG_DIR_THROTTLE_CONFIG,
                                                    brokersToThrottle), e);
    }
  }

  /**
   * Restore the original per-broker value of the config (or remove the per-broker value if there was none) on all brokers
   * throttled by this helper, using a single batch of requests. If the config of a broker has been changed since this helper set
   * the throttle, the config of that broker is left as is. This method is best-effort and does not throw. A broker whose
   * throttle could not be restored is kept in the applied throttles, so that the removal can be retried afterwards.
   */
  void clearAllThrottles() {
    removeThrottles(_adminClient, _originalThrottleByBroker, String.valueOf(_throttleRate), _retries);
  }

  /**
   * Restore the original per-broker value of {@value #LOG_DIR_THROTTLE_CONFIG} (or remove the per-broker value if there was
   * none) on the brokers of the given applied throttles, and drop the brokers that no longer need it from that record. A broker
   * is dropped when its throttle has been restored, or when its config has been changed since the throttle was applied -- in
   * which case it is left as is, because the new value is somebody else's.
   *
   * <p>Best-effort: it does not throw, and a broker whose config can neither be described nor altered stays in the record so
   * that the next call retries it.</p>
   *
   * @param adminClient The adminClient to describe and alter broker configs.
   * @param originalThrottleByBroker Brokers to restore, mapped to their original per-broker value (null if they had none).
   * @param appliedThrottleRate The value that was applied to these brokers, as a string, or null not to check the current value.
   * @param retries The number of retries while verifying that the cluster reflects the altered configs.
   */
  static void removeThrottles(Admin adminClient, Map<Integer, String> originalThrottleByBroker, String appliedThrottleRate, int retries) {
    if (originalThrottleByBroker.isEmpty()) {
      return;
    }
    Set<Integer> throttledBrokers = new TreeSet<>(originalThrottleByBroker.keySet());
    // Brokers that need no further attempt -- restored, or deliberately left alone.
    Set<Integer> done = new TreeSet<>();
    try {
      Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsInChunks(adminClient, brokerResources(throttledBrokers));
      Map<ConfigResource, AlterConfigOp> opByBroker = new HashMap<>();
      for (int brokerId : throttledBrokers) {
        ConfigResource cf = brokerResource(brokerId);
        ConfigEntry currentThrottle;
        try {
          currentThrottle = configFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(LOG_DIR_THROTTLE_CONFIG);
        } catch (ExecutionException | TimeoutException e) {
          LOG.warn("Failed to describe the configs of broker {} to restore its {}. Will retry.", brokerId, LOG_DIR_THROTTLE_CONFIG, e);
          continue;
        }
        if (appliedThrottleRate != null
            && (!isPerBrokerValue(currentThrottle) || !appliedThrottleRate.equals(currentThrottle.value()))) {
          LOG.info("Skip restoring {} on broker {}, since it has been changed to {} during the execution.", LOG_DIR_THROTTLE_CONFIG,
                   brokerId, currentThrottle);
          done.add(brokerId);
          continue;
        }
        String originalThrottle = originalThrottleByBroker.get(brokerId);
        if (originalThrottle == null) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, null), AlterConfigOp.OpType.DELETE));
        } else if (!originalThrottle.equals(currentThrottle == null ? null : currentThrottle.value())) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, originalThrottle), AlterConfigOp.OpType.SET));
        } else {
          // Already at its original value.
          done.add(brokerId);
        }
      }
      LOG.info("Restoring {} on brokers {}.", LOG_DIR_THROTTLE_CONFIG, throttledBrokers);
      List<ConfigResource> failedToRestore = alterAndVerifyConfigs(adminClient, opByBroker, retries);
      for (Map.Entry<ConfigResource, AlterConfigOp> entry : opByBroker.entrySet()) {
        if (!failedToRestore.contains(entry.getKey())) {
          done.add(Integer.parseInt(entry.getKey().name()));
        }
      }
      if (!failedToRestore.isEmpty()) {
        LOG.warn("Failed to restore {} on {}. The removal will be retried while no execution is in progress.",
                 LOG_DIR_THROTTLE_CONFIG, failedToRestore);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while restoring {} on brokers {}.", LOG_DIR_THROTTLE_CONFIG, throttledBrokers, e);
    } catch (RuntimeException e) {
      LOG.warn("Failed to restore {} on brokers {}. The removal will be retried while no execution is in progress.",
               LOG_DIR_THROTTLE_CONFIG, throttledBrokers, e);
    } finally {
      originalThrottleByBroker.keySet().removeAll(done);
    }
  }

  /**
   * Alter the given broker configs with a batch of requests, and wait until the cluster reflects the altered configs.
   *
   * @param adminClient The adminClient to describe and alter broker configs.
   * @param opByBroker The operation to apply to each broker config.
   * @param retries The number of retries while verifying that the cluster reflects the altered configs.
   * @return Broker configs that failed to be altered or that the cluster did not reflect within the time limit.
   */
  private static List<ConfigResource> alterAndVerifyConfigs(Admin adminClient,
                                                            Map<ConfigResource, AlterConfigOp> opByBroker,
                                                            int retries) throws InterruptedException {
    List<ConfigResource> failed = new ArrayList<>();
    if (opByBroker.isEmpty()) {
      return failed;
    }
    Map<ConfigResource, KafkaFuture<Void>> alterFutures = new HashMap<>();
    List<ConfigResource> resources = new ArrayList<>(opByBroker.keySet());
    for (int i = 0; i < resources.size(); i += MAX_RESOURCES_PER_ADMIN_REQUEST) {
      Map<ConfigResource, Collection<AlterConfigOp>> chunk = new HashMap<>();
      resources.subList(i, Math.min(resources.size(), i + MAX_RESOURCES_PER_ADMIN_REQUEST))
               .forEach(cf -> chunk.put(cf, Collections.singletonList(opByBroker.get(cf))));
      alterFutures.putAll(adminClient.incrementalAlterConfigs(chunk).values());
    }

    // Expected value of the config for each altered broker -- null for removing the per-broker value.
    Map<ConfigResource, String> pending = new HashMap<>();
    for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : alterFutures.entrySet()) {
      try {
        entry.getValue().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        pending.put(entry.getKey(), opByBroker.get(entry.getKey()).configEntry().value());
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Failed to alter {} on {}.", LOG_DIR_THROTTLE_CONFIG, entry.getKey(), e);
        failed.add(entry.getKey());
      }
    }

    // Retry until the cluster reflects the altered configs.
    boolean verified = CruiseControlMetricsUtils.retry(() -> {
      Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsInChunks(adminClient, new ArrayList<>(pending.keySet()));
      pending.entrySet().removeIf(entry -> isApplied(configFutures.get(entry.getKey()), entry.getValue()));
      return !pending.isEmpty();
    }, RETRY_BACKOFF_SCALE_MS, RETRY_BACKOFF_BASE, retries, MAX_RETRY_SLEEP_MS);
    if (!verified) {
      LOG.warn("The cluster did not reflect the altered {} on {} within the time limit.", LOG_DIR_THROTTLE_CONFIG, pending.keySet());
      failed.addAll(pending.keySet());
    }
    return failed;
  }

  /**
   * @param configFuture Future of the described broker config.
   * @param expectedValue The expected per-broker value of the config, or null if the per-broker value is expected to be removed.
   * @return {@code true} if the described broker config reflects the expected per-broker value, {@code false} otherwise.
   */
  private static boolean isApplied(KafkaFuture<Config> configFuture, String expectedValue) {
    try {
      return isApplied(configFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS), expectedValue);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException | TimeoutException e) {
      LOG.debug("Failed to describe broker configs to verify {}, will retry.", LOG_DIR_THROTTLE_CONFIG, e);
      return false;
    }
  }

  static boolean isApplied(Config brokerConfig, String expectedValue) {
    ConfigEntry currentThrottle = brokerConfig.get(LOG_DIR_THROTTLE_CONFIG);
    if (expectedValue == null) {
      // Once the per-broker value is removed, the config resolves from a cluster-wide default, a static config, or Kafka default.
      return !isPerBrokerValue(currentThrottle);
    }
    return isPerBrokerValue(currentThrottle) && expectedValue.equals(currentThrottle.value());
  }

  private static boolean isPerBrokerValue(ConfigEntry configEntry) {
    return configEntry != null && configEntry.value() != null && configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
  }

  private static Map<ConfigResource, KafkaFuture<Config>> describeConfigsInChunks(Admin adminClient, List<ConfigResource> resources) {
    Map<ConfigResource, KafkaFuture<Config>> configFutures = new HashMap<>();
    for (int i = 0; i < resources.size(); i += MAX_RESOURCES_PER_ADMIN_REQUEST) {
      configFutures.putAll(adminClient.describeConfigs(resources.subList(i, Math.min(resources.size(), i + MAX_RESOURCES_PER_ADMIN_REQUEST)))
                                      .values());
    }
    return configFutures;
  }

  private static List<ConfigResource> brokerResources(Collection<Integer> brokerIds) {
    List<ConfigResource> resources = new ArrayList<>(brokerIds.size());
    brokerIds.forEach(brokerId -> resources.add(brokerResource(brokerId)));
    return resources;
  }

  private static ConfigResource brokerResource(int brokerId) {
    return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
  }
}
