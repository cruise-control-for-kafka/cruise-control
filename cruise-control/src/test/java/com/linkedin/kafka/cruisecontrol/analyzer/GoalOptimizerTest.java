/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import junit.framework.AssertionFailedError;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;


public class GoalOptimizerTest {

  @Test
  public void testNoPreComputingThread() {
    GoalOptimizer goalOptimizer = createGoalOptimizer();
    // Should exit immediately.
    goalOptimizer.run();
  }

  @Test
  public void testGetNoExcludedTopics() {
    GoalOptimizer goalOptimizer = createGoalOptimizer();
    ClusterModel clusterModel = EasyMock.mock(ClusterModel.class);
    EasyMock.expect(clusterModel.topics()).andThrow(new AssertionFailedError("Not expect this method gets called")).anyTimes();
    EasyMock.replay(clusterModel);
    Set<String> excludedTopics = goalOptimizer.excludedTopics(clusterModel, null);
    Assert.assertTrue(excludedTopics.isEmpty());
    EasyMock.verify(clusterModel);

    EasyMock.reset(clusterModel);
    EasyMock.replay(clusterModel);
    Pattern matchNothingPattern = Pattern.compile("");
    excludedTopics = goalOptimizer.excludedTopics(clusterModel, matchNothingPattern);
    Assert.assertTrue(excludedTopics.isEmpty());
    EasyMock.verify(clusterModel);
  }

  @Test
  public void testGetExcludedTopics() {
    // 4 topics in total and 2 of them are excluded
    String excludedTopicPrefix = "excluded_topic_";
    String notExcludedTopicPrefix = "not_excluded_topic_";
    Set<String> expectedExcludedTopics = Set.of(excludedTopicPrefix + 1, excludedTopicPrefix + 2);
    Set<String> allTopics = new HashSet<>(expectedExcludedTopics);
    allTopics.add(notExcludedTopicPrefix + 1);
    allTopics.add(notExcludedTopicPrefix + 2);
    allTopics = Collections.unmodifiableSet(allTopics);
    String topicExcludedFromPartitionMovementPattern = excludedTopicPrefix + ".*";

    // Verify case 1: use default topic name pattern from config
    Properties properties = new Properties();
    properties.put(AnalyzerConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG, topicExcludedFromPartitionMovementPattern);
    GoalOptimizer goalOptimizer = createGoalOptimizer(properties);
    ClusterModel clusterModel = EasyMock.mock(ClusterModel.class);
    EasyMock.expect(clusterModel.topics()).andReturn(allTopics).once();
    EasyMock.replay(clusterModel);
    Set<String> excludedTopics = goalOptimizer.excludedTopics(clusterModel, null);

    Assert.assertEquals(expectedExcludedTopics, excludedTopics);
    EasyMock.verify(clusterModel);
    EasyMock.reset(clusterModel);

    // Verify case 2: use topic name pattern passed as argument
    EasyMock.expect(clusterModel.topics()).andReturn(allTopics).once();
    EasyMock.replay(clusterModel);
    goalOptimizer = createGoalOptimizer();
    excludedTopics = goalOptimizer.excludedTopics(clusterModel, Pattern.compile(topicExcludedFromPartitionMovementPattern));
    Assert.assertEquals(expectedExcludedTopics, excludedTopics);
    EasyMock.verify(clusterModel);
  }

  private GoalOptimizer createGoalOptimizer() {
    return createGoalOptimizer(new Properties());
  }

  private GoalOptimizer createGoalOptimizer(Properties overrideProps) {
    Properties props = new Properties();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(AnalyzerConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, "0");
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    String brokerSetsDataFile = Objects.requireNonNull(KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource(
            TestConstants.DEFAULT_BROKER_SET_RESOLVER_FILE)).getFile();
    props.setProperty(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, brokerSetsDataFile);
    props.putAll(overrideProps);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    return new GoalOptimizer(config, EasyMock.mock(LoadMonitor.class), Time.SYSTEM, new MetricRegistry(),
                             EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
  }
}
