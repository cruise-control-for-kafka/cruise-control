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
import java.util.TreeMap;
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
 * <p>The throttle rate of an ongoing execution can be updated dynamically (see {@link #updateThrottleRate(long, Collection)}).
 * This class is thread safe.</p>
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
  private final int _retries;
  // Guarded by this. The throttle rate (null if no throttling is applied), which may be updated during the execution.
  private Long _throttleRate;
  // Guarded by this. Brokers throttled by this helper, mapped to their original per-broker value of the config (null if they had
  // none). Owned by the caller, so that a throttle this helper fails to restore can be removed later.
  private final Map<Integer, String> _originalThrottleByBroker;
  // Guarded by this. Brokers throttled by this helper, mapped to the per-broker value of the config this helper last applied.
  private final Map<Integer, String> _appliedThrottleByBroker;
  // Guarded by this. Whether the throttles have been restored -- i.e. this helper must not throttle any broker afterwards.
  private boolean _closed;

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
    _appliedThrottleByBroker = new HashMap<>();
    _closed = false;
  }

  /**
   * @return The brokers throttled by this helper, whose throttle has not been restored yet.
   */
  synchronized Set<Integer> throttledBrokers() {
    return Collections.unmodifiableSet(new TreeSet<>(_originalThrottleByBroker.keySet()));
  }

  /**
   * Set the throttle on the brokers of the given intra-broker replica movement tasks that have not been throttled yet. The configs
   * of all such brokers are described, altered, and verified with a single batch of requests.
   *
   * @param intraBrokerReplicaMovementTasks Intra-broker replica movement tasks to be executed.
   * @throws IllegalStateException If the throttle cannot be set.
   */
  synchronized void setThrottles(Collection<ExecutionTask> intraBrokerReplicaMovementTasks) {
    if (_throttleRate == null || _closed) {
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
        _appliedThrottleByBroker.put(brokerId, throttleRate);
        if (!hasPerBrokerThrottle || !throttleRate.equals(currentThrottle.value())) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, throttleRate), AlterConfigOp.OpType.SET));
        }
      }
      List<ConfigResource> failedToAlter = alterAndVerifyConfigs(_adminClient, opByBroker, _appliedThrottleByBroker, _retries);
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
   * Update the throttle rate, and apply the new rate to (1) all brokers already throttled by this helper using a single batch
   * of requests, and (2) the brokers of the given in-progress intra-broker replica movement tasks that have not been throttled
   * yet -- e.g. if the execution started without a log dir throttle. Brokers of upcoming intra-broker replica movement tasks are
   * throttled with the new rate as their tasks start.
   *
   * @param throttleRate The new throttle (bytes/second) to apply to intra-broker replica movements.
   * @param inProgressIntraBrokerTasks In-progress intra-broker replica movement tasks.
   * @return {@code true} if the throttle rate has been updated, {@code false} if the throttles have already been restored.
   * @throws IllegalStateException If the new throttle cannot be applied.
   */
  synchronized boolean updateThrottleRate(long throttleRate, Collection<ExecutionTask> inProgressIntraBrokerTasks) {
    if (_closed) {
      LOG.info("Skip updating log dir throttle to {} bytes/sec, since the throttles have already been restored.", throttleRate);
      return false;
    }
    _throttleRate = throttleRate;
    String newThrottleRate = String.valueOf(throttleRate);
    Set<Integer> throttledBrokers = new TreeSet<>(_originalThrottleByBroker.keySet());
    if (!throttledBrokers.isEmpty()) {
      LOG.info("Updating log dir throttle to {} bytes/sec on brokers {}.", throttleRate, throttledBrokers);
      Map<ConfigResource, AlterConfigOp> opByBroker = new HashMap<>();
      for (int brokerId : throttledBrokers) {
        opByBroker.put(brokerResource(brokerId),
                       new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, newThrottleRate), AlterConfigOp.OpType.SET));
      }
      try {
        List<ConfigResource> failedToAlter = alterAndVerifyConfigs(_adminClient, opByBroker, _appliedThrottleByBroker, _retries);
        if (!failedToAlter.isEmpty()) {
          // Say what the cluster is left with: the rate for the upcoming movements HAS changed, and the brokers named here
          // still carry the rate they had, so "the update failed" and "nothing changed" are not the same thing.
          throw new IllegalStateException(String.format("Failed to update %s to %d on %s. Upcoming intra-broker replica "
                                                        + "movements use %d bytes/sec, while these brokers still carry the rate "
                                                        + "they had.", LOG_DIR_THROTTLE_CONFIG, throttleRate, failedToAlter,
                                                        throttleRate));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(String.format("Interrupted while updating %s on brokers %s.", LOG_DIR_THROTTLE_CONFIG,
                                                      throttledBrokers), e);
      }
    }
    setThrottles(inProgressIntraBrokerTasks);
    return true;
  }

  /**
   * Restore the original per-broker value of the config (or remove the per-broker value if there was none) on all brokers
   * throttled by this helper, using a single batch of requests. If the config of a broker has been changed since this helper last
   * applied the throttle to it, the config of that broker is left as is. This method is best-effort and does not throw. A broker
   * whose throttle could not be restored is kept in the applied throttles, so that the removal can be retried afterwards. This
   * helper does not throttle any broker afterwards.
   */
  /**
   * @return The per-broker value of {@value #LOG_DIR_THROTTLE_CONFIG} this helper last applied to each broker it has
   * throttled and not restored yet.
   */
  synchronized Map<Integer, String> appliedThrottleByBroker() {
    return Collections.unmodifiableMap(new TreeMap<>(_appliedThrottleByBroker));
  }

  /**
   * Remove the throttle of an ongoing execution: restore the original per-broker value of {@value #LOG_DIR_THROTTLE_CONFIG}
   * (or remove the per-broker value if there was none) on every broker this helper has throttled, and stop throttling the
   * upcoming intra-broker replica movements.
   *
   * <p>Unlike {@link #clearAllThrottles()} this does NOT close the helper: the throttle of the same execution can be set
   * again afterwards by another update. This is what an {@code admin} request asking for a {@code null} throttle means.</p>
   *
   * @return {@code true} if a throttle was removed, {@code false} if the execution was not throttled, or if the throttles
   * have already been restored.
   */
  synchronized boolean removeThrottleRate() {
    if (_closed) {
      LOG.info("Skip removing the log dir throttle, since the throttles have already been restored.");
      return false;
    }
    if (_throttleRate == null) {
      LOG.info("Skip removing the log dir throttle, since the ongoing execution is not throttled.");
      return false;
    }
    // Clear the rate first, so that the upcoming task batches are not throttled even if the restore below fails for a broker.
    _throttleRate = null;
    LOG.info("Removing the log dir throttle of the ongoing execution from brokers {}.", _originalThrottleByBroker.keySet());
    removeThrottles(_adminClient, _originalThrottleByBroker, _appliedThrottleByBroker, _retries);
    _appliedThrottleByBroker.keySet().retainAll(_originalThrottleByBroker.keySet());
    return true;
  }

  synchronized void clearAllThrottles() {
    _closed = true;
    removeThrottles(_adminClient, _originalThrottleByBroker, _appliedThrottleByBroker, _retries);
    // Keep the last applied value of the brokers whose throttle could not be restored, and forget the rest.
    _appliedThrottleByBroker.keySet().retainAll(_originalThrottleByBroker.keySet());
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
   * @param appliedThrottleByBroker The value that was last applied to each of these brokers, or null not to check the current
   *                                values.
   * @param retries The number of retries while verifying that the cluster reflects the altered configs.
   */
  static void removeThrottles(Admin adminClient, Map<Integer, String> originalThrottleByBroker,
                              Map<Integer, String> appliedThrottleByBroker, int retries) {
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
        if (appliedThrottleByBroker != null
            && (!isPerBrokerValue(currentThrottle) || !currentThrottle.value().equals(appliedThrottleByBroker.get(brokerId)))) {
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
      List<ConfigResource> failedToRestore = alterAndVerifyConfigs(adminClient, opByBroker, null, retries);
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
   * Alter the given broker configs with a batch of requests, and wait until the cluster reflects the altered configs. Records the
   * value applied to each broker whose config is altered successfully.
   *
   * @param adminClient The adminClient to describe and alter broker configs.
   * @param opByBroker The operation to apply to each broker config.
   * @param appliedThrottleByBroker Record of the value applied to each broker, to populate, or null not to record anything.
   * @param retries The number of retries while verifying that the cluster reflects the altered configs.
   * @return Broker configs that failed to be altered or that the cluster did not reflect within the time limit.
   */
  private static List<ConfigResource> alterAndVerifyConfigs(Admin adminClient,
                                                            Map<ConfigResource, AlterConfigOp> opByBroker,
                                                            Map<Integer, String> appliedThrottleByBroker,
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
        String appliedValue = opByBroker.get(entry.getKey()).configEntry().value();
        if (appliedThrottleByBroker != null) {
          appliedThrottleByBroker.put(Integer.parseInt(entry.getKey().name()), appliedValue);
        }
        pending.put(entry.getKey(), appliedValue);
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
