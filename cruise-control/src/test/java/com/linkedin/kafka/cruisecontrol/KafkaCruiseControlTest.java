/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.detector.NoopProvisioner;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertNotNull;


public class KafkaCruiseControlTest extends CruiseControlIntegrationTestHarness {
  private static final TopicPartition DUMMY_TOPIC_PARTITION = new TopicPartition(TOPIC0, 0);
  private static final Set<TopicPartition> DUMMY_ONGOING_PARTITION_REASSIGNMENTS = Collections.singleton(DUMMY_TOPIC_PARTITION);

  @Test
  public void testSanityCheckDryRun() throws InterruptedException, ExecutionException, TimeoutException {
    Time time = EasyMock.mock(Time.class);
    AnomalyDetectorManager anomalyDetectorManager = EasyMock.mock(AnomalyDetectorManager.class);
    Executor executor = EasyMock.strictMock(Executor.class);
    LoadMonitor loadMonitor = EasyMock.mock(LoadMonitor.class);
    ExecutorService goalOptimizerExecutor = EasyMock.mock(ExecutorService.class);
    GoalOptimizer goalOptimizer = EasyMock.mock(GoalOptimizer.class);

    // For sanityCheckDryRun(false, true) and sanityCheckDryRun(false, false) (see #1 and #2 below).
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(true).times(2);
    // For sanityCheckDryRun(false, XXX) (see #3 below)
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.listPartitionsBeingReassigned()).andReturn(DUMMY_ONGOING_PARTITION_REASSIGNMENTS);
    EasyMock.expect(executor.maybeStopExternalAgent()).andReturn(true);
    // For sanityCheckDryRun(false, XXX) (see #4 below)
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.listPartitionsBeingReassigned()).andReturn(Collections.emptySet());
    // For sanityCheckDryRun(false, XXX) (see #5 below)
    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.listPartitionsBeingReassigned()).andThrow(new TimeoutException()).once();

    EasyMock.replay(time, anomalyDetectorManager, executor, loadMonitor, goalOptimizerExecutor, goalOptimizer);
    KafkaCruiseControl kafkaCruiseControl = new KafkaCruiseControl(_config, time, anomalyDetectorManager, executor,
                                                                   loadMonitor, goalOptimizerExecutor, goalOptimizer, new NoopProvisioner());

    // Expect no failure (dryrun = true) regardless of ongoing executions.
    kafkaCruiseControl.sanityCheckDryRun(true, false);
    kafkaCruiseControl.sanityCheckDryRun(true, true);
    // 1. Expect no failure (dryrun = false), if there is ongoing execution started by CC, it must be requested to stop.
    kafkaCruiseControl.sanityCheckDryRun(false, true);
    // 2. Expect failure (dryrun = false), if there is ongoing execution started by CC, not requested to stop.
    assertThrows(IllegalStateException.class, () -> kafkaCruiseControl.sanityCheckDryRun(false, false));
    // 3. Expect no failure (dryrun = false), there is no execution started by CC, but ongoing replica reassignment, request to stop is irrelevant.
    kafkaCruiseControl.sanityCheckDryRun(false, false);
    // 4. Expect no failure (dryrun = false), there is no execution started by CC or other tools, request to stop is irrelevant.
    kafkaCruiseControl.sanityCheckDryRun(false, false);
    // 5. Expect failure (dryrun = false), there is no execution started by CC, but checking ongoing executions started
    // by other tools timed out, request to stop is irrelevant.
    assertThrows(IllegalStateException.class, () -> kafkaCruiseControl.sanityCheckDryRun(false, false));

    EasyMock.verify(time, anomalyDetectorManager, executor, loadMonitor, goalOptimizerExecutor, goalOptimizer);

    // Verify initialization and functioning of Admin Client
    Admin adminClient = kafkaCruiseControl.adminClient();
    assertNotNull(adminClient);
    assertEquals(clusterSize(), adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).size());
  }

  @Test
  public void testDisableAutoStopExternalAgent() throws InterruptedException, ExecutionException, TimeoutException {
    Time time = EasyMock.mock(Time.class);
    AnomalyDetectorManager anomalyDetectorManager = EasyMock.mock(AnomalyDetectorManager.class);
    Executor executor = EasyMock.strictMock(Executor.class);
    LoadMonitor loadMonitor = EasyMock.mock(LoadMonitor.class);
    ExecutorService goalOptimizerExecutor = EasyMock.mock(ExecutorService.class);
    GoalOptimizer goalOptimizer = EasyMock.mock(GoalOptimizer.class);

    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    properties.put(ExecutorConfig.AUTO_STOP_EXTERNAL_AGENT_CONFIG, false);
    properties.put(KafkaSampleStore.PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__partition_samples");
    properties.put(KafkaSampleStore.BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG, "__broker_samples");
    _config = new KafkaCruiseControlConfig(properties);

    EasyMock.expect(executor.hasOngoingExecution()).andReturn(false).once();
    EasyMock.expect(executor.listPartitionsBeingReassigned()).andReturn(DUMMY_ONGOING_PARTITION_REASSIGNMENTS);

    EasyMock.replay(time, anomalyDetectorManager, executor, loadMonitor, goalOptimizerExecutor, goalOptimizer);
    KafkaCruiseControl kafkaCruiseControl = new KafkaCruiseControl(_config, time, anomalyDetectorManager, executor,
                                                                   loadMonitor, goalOptimizerExecutor, goalOptimizer, new NoopProvisioner());

    // Expect failure (dryrun = false), if there is no execution started by CC, but ongoing replica reassignment, request to stop is irrelevant.
    assertThrows(IllegalStateException.class, () -> kafkaCruiseControl.sanityCheckDryRun(false, false));

    EasyMock.verify(time, anomalyDetectorManager, executor, loadMonitor, goalOptimizerExecutor, goalOptimizer);

    // Verify initialization and functioning of Admin Client
    Admin adminClient = kafkaCruiseControl.adminClient();
    assertNotNull(adminClient);
    assertEquals(clusterSize(), adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).size());
  }
}
