/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.waitUntilTrue;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_DEADLINE_MS;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_SHORT_CHECK_MS;
import static com.linkedin.kafka.cruisecontrol.executor.LogDirThrottleHelper.LOG_DIR_THROTTLE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link LogDirThrottleHelper}.
 */
public class LogDirThrottleHelperTest extends CCKafkaIntegrationTestHarness {
  private static final long THROTTLE_RATE = 100L;
  private static final String THROTTLE_RATE_STRING = String.valueOf(THROTTLE_RATE);
  private static final String LOGDIR_0 = "d0";
  private static final String LOGDIR_1 = "d1";
  private static final String CLUSTER_WIDE_DEFAULT = "";
  private static final int RETRIES_FOR_FAKE_CLUSTER = 2;
  private Admin _adminClient;

  @Override
  public int clusterSize() {
    return 3;
  }

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    super.setUp();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
  }

  /**
   * Teardown the test.
   */
  @After
  public void tearDown() {
    super.tearDown();
    if (_adminClient != null) {
      _adminClient.close(Duration.ofMillis(1000L));
    }
  }

  @Test
  public void testSetAndRestoreThrottlesWithoutPreExistingThrottles() throws Exception {
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, THROTTLE_RATE, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1)));

    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);
    assertPerBrokerThrottle(1, THROTTLE_RATE_STRING);
    // No throttle on broker 2, because it has no intra-broker replica movement.
    assertPerBrokerThrottle(2, null);
    assertEquals(Set.of(0, 1), throttleHelper.throttledBrokers());

    // Setting the throttles again for already throttled brokers is a no-op.
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(2, 0)));
    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);
    assertEquals(Set.of(0, 1), throttleHelper.throttledBrokers());

    throttleHelper.clearAllThrottles();
    for (int brokerId = 0; brokerId < clusterSize(); brokerId++) {
      assertPerBrokerThrottle(brokerId, null);
    }
    assertTrue(throttleHelper.throttledBrokers().isEmpty());
  }

  @Test
  public void testRestorePreExistingPerBrokerThrottle() throws Exception {
    String preExistingThrottle = "500";
    setThrottleConfig("0", preExistingThrottle);
    waitForThrottle(0, preExistingThrottle, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, THROTTLE_RATE, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(0, 0)));
    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);

    throttleHelper.clearAllThrottles();
    assertPerBrokerThrottle(0, preExistingThrottle);
  }

  @Test
  public void testRestoreToClusterWideDefaultThrottle() throws Exception {
    String clusterWideDefaultThrottle = "700";
    setThrottleConfig(CLUSTER_WIDE_DEFAULT, clusterWideDefaultThrottle);
    waitForThrottle(0, clusterWideDefaultThrottle, ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);

    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, THROTTLE_RATE, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(0, 0)));
    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);

    // Removing the per-broker throttle makes the broker fall back to the cluster-wide default.
    throttleHelper.clearAllThrottles();
    assertPerBrokerThrottle(0, null);
    ConfigEntry throttle = brokerConfig(0).get(LOG_DIR_THROTTLE_CONFIG);
    assertEquals(clusterWideDefaultThrottle, throttle.value());
    assertEquals(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, throttle.source());
  }

  @Test
  public void testKeepThrottleChangedDuringExecution() throws Exception {
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, THROTTLE_RATE, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1)));

    // Another party changes the throttle of broker 0 during the execution.
    String changedThrottle = "300";
    setThrottleConfig("0", changedThrottle);
    waitForThrottle(0, changedThrottle, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

    throttleHelper.clearAllThrottles();
    assertPerBrokerThrottle(0, changedThrottle);
    assertPerBrokerThrottle(1, null);
  }

  @Test
  public void testUpdateThrottleRate() throws Exception {
    String preExistingThrottle = "500";
    setThrottleConfig("0", preExistingThrottle);
    waitForThrottle(0, preExistingThrottle, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, THROTTLE_RATE, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1)));
    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);
    assertPerBrokerThrottle(1, THROTTLE_RATE_STRING);

    // The new rate is applied to the throttled brokers and to broker 2, which has a newly in-progress intra-broker task.
    long newThrottleRate = 200L;
    String newThrottleRateString = String.valueOf(newThrottleRate);
    assertTrue(throttleHelper.updateThrottleRate(newThrottleRate, Arrays.asList(intraBrokerTask(1, 1), intraBrokerTask(2, 2))));
    for (int brokerId = 0; brokerId < clusterSize(); brokerId++) {
      assertPerBrokerThrottle(brokerId, newThrottleRateString);
    }
    assertEquals(Set.of(0, 1, 2), throttleHelper.throttledBrokers());

    // Brokers of upcoming tasks are throttled with the new rate.
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(3, 2)));
    assertPerBrokerThrottle(2, newThrottleRateString);

    throttleHelper.clearAllThrottles();
    assertPerBrokerThrottle(0, preExistingThrottle);
    assertPerBrokerThrottle(1, null);
    assertPerBrokerThrottle(2, null);

    // Once the throttles are restored, an update is refused and throttles no broker.
    assertFalse(throttleHelper.updateThrottleRate(300L, Collections.singletonList(intraBrokerTask(4, 1))));
    assertPerBrokerThrottle(0, preExistingThrottle);
    assertPerBrokerThrottle(1, null);
    assertTrue(throttleHelper.throttledBrokers().isEmpty());
  }

  @Test
  public void testUpdateThrottleRateWithoutInitialThrottle() throws Exception {
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, null, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(0, 0)));
    assertPerBrokerThrottle(0, null);

    // An execution started without a log dir throttle gets throttled by the update.
    assertTrue(throttleHelper.updateThrottleRate(THROTTLE_RATE, Collections.singletonList(intraBrokerTask(0, 0))));
    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(1, 1)));
    assertPerBrokerThrottle(1, THROTTLE_RATE_STRING);

    throttleHelper.clearAllThrottles();
    assertPerBrokerThrottle(0, null);
    assertPerBrokerThrottle(1, null);
  }

  @Test
  public void testRemoveThrottleRate() throws Exception {
    String preExistingThrottle = "500";
    setThrottleConfig("0", preExistingThrottle);
    waitForThrottle(0, preExistingThrottle, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);

    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(_adminClient, THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1)));
    assertPerBrokerThrottle(0, THROTTLE_RATE_STRING);
    assertPerBrokerThrottle(1, THROTTLE_RATE_STRING);

    // Removing the throttle restores what each broker had, with the movements still in progress.
    assertTrue(throttleHelper.removeThrottleRate());
    assertPerBrokerThrottle(0, preExistingThrottle);
    assertPerBrokerThrottle(1, null);
    assertTrue(throttleHelper.throttledBrokers().isEmpty());

    // Upcoming tasks are not throttled, and a second removal is a no-op.
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(2, 2)));
    assertPerBrokerThrottle(2, null);
    assertFalse(throttleHelper.removeThrottleRate());

    // Unlike clearAllThrottles, the removal leaves the helper open: the same execution can be throttled again.
    assertTrue(throttleHelper.updateThrottleRate(THROTTLE_RATE, Collections.singletonList(intraBrokerTask(3, 1))));
    assertPerBrokerThrottle(1, THROTTLE_RATE_STRING);

    throttleHelper.clearAllThrottles();
    assertPerBrokerThrottle(0, preExistingThrottle);
    assertPerBrokerThrottle(1, null);
    // Once the throttles are restored the helper is closed, so a removal is refused like an update.
    assertFalse(throttleHelper.removeThrottleRate());
  }

  @Test
  public void testNoAdminRequestWithoutThrottle() {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(mockAdminClient);

    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(mockAdminClient, null, new LogDirThrottleHelper.AppliedThrottles());
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(0, 0)));
    throttleHelper.clearAllThrottles();

    assertTrue(throttleHelper.throttledBrokers().isEmpty());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testBatchAdminRequestsAcrossBrokers() {
    FakeCluster cluster = new FakeCluster();
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles(),
                                                                   RETRIES_FOR_FAKE_CLUSTER);

    // Two tasks on broker 0, and one task on each of brokers 1 and 2.
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 0), intraBrokerTask(2, 1), intraBrokerTask(3, 2)));
    // A single request describes the configs of all brokers, a single request alters them, and a single request verifies them.
    assertEquals(List.of(3, 3), cluster.describeBatchSizes());
    assertEquals(List.of(3), cluster.alterBatchSizes());
    assertEquals(Map.of("0", THROTTLE_RATE_STRING, "1", THROTTLE_RATE_STRING, "2", THROTTLE_RATE_STRING), cluster.perBrokerThrottles());

    // Already throttled brokers incur no request.
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(4, 0), intraBrokerTask(5, 2)));
    assertEquals(List.of(3, 3), cluster.describeBatchSizes());
    assertEquals(List.of(3), cluster.alterBatchSizes());

    // A single batch of requests restores all throttles.
    throttleHelper.clearAllThrottles();
    assertEquals(List.of(3, 3, 3, 3), cluster.describeBatchSizes());
    assertEquals(List.of(3, 3), cluster.alterBatchSizes());
    assertTrue(cluster.perBrokerThrottles().isEmpty());
  }

  @Test
  public void testSetThrottlesFailsIfAlteringConfigsFails() {
    FakeCluster cluster = new FakeCluster();
    cluster.failAlteringConfigsOf("1");
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles(),
                                                                   RETRIES_FOR_FAKE_CLUSTER);

    assertThrows(IllegalStateException.class,
                 () -> throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1))));
    assertEquals(Map.of("0", THROTTLE_RATE_STRING), cluster.perBrokerThrottles());

    // The throttle set before the failure is restored.
    throttleHelper.clearAllThrottles();
    assertTrue(cluster.perBrokerThrottles().isEmpty());
  }

  @Test
  public void testSetThrottlesFailsIfDescribingConfigsFails() {
    FakeCluster cluster = new FakeCluster();
    cluster.failDescribingConfigsOf("0");
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles(),
                                                                   RETRIES_FOR_FAKE_CLUSTER);

    assertThrows(IllegalStateException.class, () -> throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(0, 0))));
    assertTrue(cluster.alterBatchSizes().isEmpty());
    assertTrue(cluster.perBrokerThrottles().isEmpty());
  }

  @Test
  public void testClearAllThrottlesContinuesPastFailingBroker() {
    FakeCluster cluster = new FakeCluster();
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles(),
                                                                   RETRIES_FOR_FAKE_CLUSTER);
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1), intraBrokerTask(2, 2)));

    cluster.failDescribingConfigsOf("1");
    throttleHelper.clearAllThrottles();

    // The throttles of the other brokers are restored with a single alter request.
    assertEquals(List.of(3, 2), cluster.alterBatchSizes());
    assertEquals(Map.of("1", THROTTLE_RATE_STRING), cluster.perBrokerThrottles());
    // The broker that could not be described is KEPT, so that the removal can be retried: the config is a dynamic broker
    // config, so forgetting it here would leave a rate limit on that broker permanently.
    assertEquals(Set.of(1), throttleHelper.throttledBrokers());
  }

  @Test
  public void testLeftoverThrottleIsRemovedByALaterSweep() {
    FakeCluster cluster = new FakeCluster();
    LogDirThrottleHelper.AppliedThrottles appliedThrottles = new LogDirThrottleHelper.AppliedThrottles();
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE, appliedThrottles,
                                                                   RETRIES_FOR_FAKE_CLUSTER);
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1)));
    // The record is the caller's, so it outlives the helper.
    assertEquals(Set.of(0, 1), appliedThrottles.originalThrottleByBroker().keySet());

    cluster.failAlteringConfigsOf("1");
    throttleHelper.clearAllThrottles();
    assertEquals(Map.of("1", THROTTLE_RATE_STRING), cluster.perBrokerThrottles());
    assertEquals(Set.of(1), appliedThrottles.originalThrottleByBroker().keySet());

    // What the executor's sweeper does once the execution has finished and the broker is reachable again.
    cluster.stopFailing();
    LogDirThrottleHelper.removeThrottles(cluster.adminClient(), appliedThrottles.originalThrottleByBroker(), null,
                                         RETRIES_FOR_FAKE_CLUSTER);
    assertEquals(Collections.emptyMap(), cluster.perBrokerThrottles());
    assertTrue(appliedThrottles.isEmpty());
  }

  @Test
  public void testSweepRestoresAnOriginalValueRatherThanDeletingIt() {
    FakeCluster cluster = new FakeCluster();
    cluster.setPerBrokerThrottle("1", "4242");
    LogDirThrottleHelper.AppliedThrottles appliedThrottles = new LogDirThrottleHelper.AppliedThrottles();
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE, appliedThrottles,
                                                                   RETRIES_FOR_FAKE_CLUSTER);
    throttleHelper.setThrottles(Collections.singletonList(intraBrokerTask(0, 1)));
    assertEquals(Map.of("1", THROTTLE_RATE_STRING), cluster.perBrokerThrottles());

    cluster.failAlteringConfigsOf("1");
    throttleHelper.clearAllThrottles();
    assertEquals(Set.of(1), appliedThrottles.originalThrottleByBroker().keySet());

    cluster.stopFailing();
    LogDirThrottleHelper.removeThrottles(cluster.adminClient(), appliedThrottles.originalThrottleByBroker(), null,
                                         RETRIES_FOR_FAKE_CLUSTER);
    // The operator's own value comes back -- a leftover must not be swept into nothing.
    assertEquals(Map.of("1", "4242"), cluster.perBrokerThrottles());
    assertTrue(appliedThrottles.isEmpty());
  }

  @Test
  public void testBatchAdminRequestsForUpdatingThrottleRate() {
    FakeCluster cluster = new FakeCluster();
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles(),
                                                                   RETRIES_FOR_FAKE_CLUSTER);
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1), intraBrokerTask(2, 2)));
    assertEquals(List.of(3, 3), cluster.describeBatchSizes());
    assertEquals(List.of(3), cluster.alterBatchSizes());

    // A single request alters the throttles of all throttled brokers, and a single request verifies them.
    String newThrottleRate = "200";
    assertTrue(throttleHelper.updateThrottleRate(Long.parseLong(newThrottleRate),
                                                 Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1))));
    assertEquals(List.of(3, 3, 3), cluster.describeBatchSizes());
    assertEquals(List.of(3, 3), cluster.alterBatchSizes());
    assertEquals(Map.of("0", newThrottleRate, "1", newThrottleRate, "2", newThrottleRate), cluster.perBrokerThrottles());

    throttleHelper.clearAllThrottles();
    assertTrue(cluster.perBrokerThrottles().isEmpty());
  }

  @Test
  public void testClearAllThrottlesAfterPartiallyFailedUpdate() {
    FakeCluster cluster = new FakeCluster();
    LogDirThrottleHelper throttleHelper = new LogDirThrottleHelper(cluster.adminClient(), THROTTLE_RATE,
                                                                   new LogDirThrottleHelper.AppliedThrottles(),
                                                                   RETRIES_FOR_FAKE_CLUSTER);
    throttleHelper.setThrottles(Arrays.asList(intraBrokerTask(0, 0), intraBrokerTask(1, 1)));

    // Updating the throttle of broker 1 fails.
    cluster.failAlteringConfigsOf("1");
    String newThrottleRate = "200";
    assertThrows(IllegalStateException.class,
                 () -> throttleHelper.updateThrottleRate(Long.parseLong(newThrottleRate), Collections.emptyList()));
    assertEquals(Map.of("0", newThrottleRate, "1", THROTTLE_RATE_STRING), cluster.perBrokerThrottles());

    // Both the updated and the non-updated throttles are restored.
    cluster.stopFailing();
    throttleHelper.clearAllThrottles();
    assertTrue(cluster.perBrokerThrottles().isEmpty());
  }

  private static ExecutionTask intraBrokerTask(long executionId, int brokerId) {
    TopicPartition tp = new TopicPartition(TOPIC0, (int) executionId);
    ExecutionProposal proposal = new ExecutionProposal(tp, 10, new ReplicaPlacementInfo(brokerId, LOGDIR_0),
                                                       Collections.singletonList(new ReplicaPlacementInfo(brokerId, LOGDIR_0)),
                                                       Collections.singletonList(new ReplicaPlacementInfo(brokerId, LOGDIR_1)));
    return new ExecutionTask(executionId, proposal, brokerId, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION,
                             EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private Config brokerConfig(int brokerId) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    return _adminClient.describeConfigs(Collections.singletonList(cf)).all().get().get(cf);
  }

  private void assertPerBrokerThrottle(int brokerId, String expectedThrottle) throws ExecutionException, InterruptedException {
    ConfigEntry throttle = brokerConfig(brokerId).get(LOG_DIR_THROTTLE_CONFIG);
    if (expectedThrottle == null) {
      assertTrue(String.format("Unexpected per-broker throttle on broker %d: %s", brokerId, throttle),
                 throttle == null || throttle.source() != ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
    } else {
      assertNotNull(throttle);
      assertEquals(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, throttle.source());
      assertEquals(expectedThrottle, throttle.value());
    }
  }

  private void setThrottleConfig(String broker, String throttle) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, broker);
    AlterConfigOp op = new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, throttle), AlterConfigOp.OpType.SET);
    _adminClient.incrementalAlterConfigs(Collections.singletonMap(cf, Collections.singletonList(op))).all().get();
  }

  private void waitForThrottle(int brokerId, String expectedThrottle, ConfigEntry.ConfigSource expectedSource) {
    waitUntilTrue(() -> {
      try {
        ConfigEntry throttle = brokerConfig(brokerId).get(LOG_DIR_THROTTLE_CONFIG);
        return throttle != null && expectedThrottle.equals(throttle.value()) && throttle.source() == expectedSource;
      } catch (ExecutionException | InterruptedException e) {
        return false;
      }
    }, String.format("Broker %d did not reflect the throttle %s from %s", brokerId, expectedThrottle, expectedSource),
                  EXECUTION_DEADLINE_MS, EXECUTION_SHORT_CHECK_MS);
  }

  private static <T> KafkaFuture<T> failedFuture() {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.completeExceptionally(new KafkaException("Injected failure"));
    return future;
  }

  /**
   * An in-memory stand-in for the broker configs of a cluster, which serves the admin requests of {@link LogDirThrottleHelper}
   * and records the number of resources in each request.
   */
  private static final class FakeCluster {
    private final Map<String, String> _perBrokerThrottles = new HashMap<>();
    private final Set<String> _brokersFailingDescribe = new HashSet<>();
    private final Set<String> _brokersFailingAlter = new HashSet<>();
    private final List<Integer> _describeBatchSizes = new ArrayList<>();
    private final List<Integer> _alterBatchSizes = new ArrayList<>();
    private final AdminClient _adminClient;

    @SuppressWarnings("unchecked")
    FakeCluster() {
      _adminClient = EasyMock.mock(AdminClient.class);
      EasyMock.expect(_adminClient.describeConfigs(EasyMock.<Collection<ConfigResource>>anyObject()))
              .andAnswer(() -> describe((Collection<ConfigResource>) EasyMock.getCurrentArguments()[0])).anyTimes();
      EasyMock.expect(_adminClient.incrementalAlterConfigs(EasyMock.<Map<ConfigResource, Collection<AlterConfigOp>>>anyObject()))
              .andAnswer(() -> alter((Map<ConfigResource, Collection<AlterConfigOp>>) EasyMock.getCurrentArguments()[0])).anyTimes();
      EasyMock.replay(_adminClient);
    }

    AdminClient adminClient() {
      return _adminClient;
    }

    Map<String, String> perBrokerThrottles() {
      return _perBrokerThrottles;
    }

    List<Integer> describeBatchSizes() {
      return _describeBatchSizes;
    }

    List<Integer> alterBatchSizes() {
      return _alterBatchSizes;
    }

    void failDescribingConfigsOf(String broker) {
      _brokersFailingDescribe.add(broker);
    }

    void failAlteringConfigsOf(String broker) {
      _brokersFailingAlter.add(broker);
    }

    void stopFailing() {
      _brokersFailingDescribe.clear();
      _brokersFailingAlter.clear();
    }

    void setPerBrokerThrottle(String broker, String value) {
      _perBrokerThrottles.put(broker, value);
    }

    private DescribeConfigsResult describe(Collection<ConfigResource> resources) {
      _describeBatchSizes.add(resources.size());
      Map<ConfigResource, KafkaFuture<Config>> futures = new HashMap<>();
      for (ConfigResource cf : resources) {
        if (_brokersFailingDescribe.contains(cf.name())) {
          futures.put(cf, failedFuture());
          continue;
        }
        String perBrokerThrottle = _perBrokerThrottles.get(cf.name());
        ConfigEntry throttle = perBrokerThrottle == null
                               ? throttleEntry(String.valueOf(Long.MAX_VALUE), ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                               : throttleEntry(perBrokerThrottle, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
        futures.put(cf, KafkaFuture.completedFuture(new Config(Collections.singletonList(throttle))));
      }
      DescribeConfigsResult result = EasyMock.mock(DescribeConfigsResult.class);
      EasyMock.expect(result.values()).andReturn(futures).anyTimes();
      EasyMock.replay(result);
      return result;
    }

    private AlterConfigsResult alter(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
      _alterBatchSizes.add(configs.size());
      Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>();
      for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : configs.entrySet()) {
        String broker = entry.getKey().name();
        if (_brokersFailingAlter.contains(broker)) {
          futures.put(entry.getKey(), failedFuture());
          continue;
        }
        for (AlterConfigOp op : entry.getValue()) {
          if (op.opType() == AlterConfigOp.OpType.DELETE) {
            _perBrokerThrottles.remove(broker);
          } else {
            _perBrokerThrottles.put(broker, op.configEntry().value());
          }
        }
        futures.put(entry.getKey(), KafkaFuture.completedFuture(null));
      }
      AlterConfigsResult result = EasyMock.mock(AlterConfigsResult.class);
      EasyMock.expect(result.values()).andReturn(futures).anyTimes();
      EasyMock.replay(result);
      return result;
    }

    private static ConfigEntry throttleEntry(String value, ConfigEntry.ConfigSource source) {
      return new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, value, source, false, false, Collections.emptyList(),
                             ConfigEntry.ConfigType.LONG, null);
    }
  }
}
