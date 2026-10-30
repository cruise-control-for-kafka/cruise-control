/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeMinIsrWithOfflineReplicasStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.StrategyOptions;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ThrottleAwareReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartitionReplica;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC3;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

/**
 * Unit test class for execution task planner
 */
public class ExecutionTaskPlannerTest {
  private static final long BYTES_PER_MB = 1024L * 1024L;
  private static final int MAX_BROKER_CONCURRENCY = ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG - 1;
  private static final int MIN_BROKER_CONCURRENCY = 1;
  private final ReplicaPlacementInfo _r0 = new ReplicaPlacementInfo(0);
  private final ReplicaPlacementInfo _r1 = new ReplicaPlacementInfo(1);
  private final ReplicaPlacementInfo _r2 = new ReplicaPlacementInfo(2);
  private final ReplicaPlacementInfo _r3 = new ReplicaPlacementInfo(3);
  private final ReplicaPlacementInfo _r4 = new ReplicaPlacementInfo(4);
  private final ReplicaPlacementInfo _r5 = new ReplicaPlacementInfo(5);

  private final ExecutionProposal _leaderMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 0), 0, _r1, Arrays.asList(_r1, _r0), Arrays.asList(_r0, _r1));
  private final ExecutionProposal _leaderMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 1), 0, _r1, Arrays.asList(_r1, _r0), Arrays.asList(_r0, _r1));
  private final ExecutionProposal _leaderMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 2), 0, _r1, Arrays.asList(_r1, _r2), Arrays.asList(_r2, _r1));
  private final ExecutionProposal _leaderMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 3), 0, _r3, Arrays.asList(_r3, _r2), Arrays.asList(_r2, _r3));

  private final ExecutionProposal _partitionMovement0 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 0), 10, _r0, Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1));
  private final ExecutionProposal _partitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 1), 30, _r1, Arrays.asList(_r1, _r3), Arrays.asList(_r3, _r2));
  private final ExecutionProposal _partitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 2), 20, _r2, Arrays.asList(_r2, _r1), Arrays.asList(_r1, _r3));
  private final ExecutionProposal _partitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 3), 10, _r3, Arrays.asList(_r3, _r2), Arrays.asList(_r2, _r0));

  private final ExecutionProposal _rf4PartitionMovement0 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 0), 10, _r0, List.of(_r0, _r2, _r4, _r3), List.of(_r2, _r1, _r3, _r5));
  private final ExecutionProposal _rf4PartitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 1), 30, _r1, List.of(_r1, _r3, _r5, _r4), List.of(_r3, _r2, _r0, _r4));
  private final ExecutionProposal _rf4PartitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 2), 20, _r2, List.of(_r2, _r1, _r4, _r5), List.of(_r1, _r3, _r4, _r0));
  private final ExecutionProposal _rf4PartitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 3), 10, _r3, List.of(_r3, _r2, _r5, _r1), List.of(_r2, _r0, _r5, _r1));

  private final List<Node> _expectedNodes = Arrays.asList(new Node(0, "null", -1),
                                                          new Node(1, "null", -1),
                                                          new Node(2, "null", -1),
                                                          new Node(3, "null", -1));
  private final List<Node> _rf4ExpectedNodes = List.of(new Node(0, "null", -1),
                                                       new Node(1, "null", -1),
                                                       new Node(2, "null", -1),
                                                       new Node(3, "null", -1),
                                                       new Node(4, "null", -1),
                                                       new Node(5, "null", -1));
  private final int _defaultPartitionsMaxCap = ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG;

  @Test
  public void testGetLeaderMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG, "");

    Set<PartitionInfo> partitions = new HashSet<>();
    for (ExecutionProposal proposal : proposals) {
      partitions.add(generatePartitionInfo(proposal, false));
    }

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.emptySet(),
                                          Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();
    ExecutionConcurrencyManager manager = new ExecutionConcurrencyManager(new KafkaCruiseControlConfig(props));

    // 1: there should be no throttle if there is enough concurrency at both cluster level and broker level.
    manager.setExecutionConcurrencyForAllBrokersOrCluster(4, ConcurrencyType.LEADERSHIP_CLUSTER);
    manager.setExecutionConcurrencyForBroker(0, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(1, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(2, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(3, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("4 of the leader movements should return in one batch", 4, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement2);
    assertEquals(leaderMovementTasks.get(2).proposal(), _leaderMovement3);
    assertEquals(leaderMovementTasks.get(3).proposal(), _leaderMovement4);

    // 2: task is throttled by cluster concurrency:
    // Cluster concurrency is 3 and there are 4 tasks. Only 3 tasks can be executed at once, and 1 task should be throttled.
    manager.setExecutionConcurrencyForAllBrokersOrCluster(3, ConcurrencyType.LEADERSHIP_CLUSTER);
    manager.setExecutionConcurrencyForBroker(0, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(1, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(2, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(3, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("3 of the leader movements should return in one batch", 3, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement2);
    assertEquals(leaderMovementTasks.get(2).proposal(), _leaderMovement3);

    // 3: task is throttled by broker concurrency
    // The broker 1 have concurrency 1, but 3 of the tasks involves this broker. In this case, 2 tasks should be throttled.
    manager.setExecutionConcurrencyForAllBrokersOrCluster(3, ConcurrencyType.LEADERSHIP_CLUSTER);
    manager.setExecutionConcurrencyForBroker(0, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(1, MIN_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(2, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(3, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);

    planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement4);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("1 of the leader movements should return in one batch", 1, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement2);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("1 of the leader movements should return in one batch", 1, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement3);
  }

  @Test
  public void testGetInterBrokerPartitionMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_partitionMovement0);
    proposals.add(_partitionMovement1);
    proposals.add(_partitionMovement2);
    proposals.add(_partitionMovement3);
    // Test different execution strategies.
    ExecutionTaskPlanner basePlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    // Create postponeUrpPlanner
    Properties postponeUrpProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    postponeUrpProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                 PostponeUrpReplicaMovementStrategy.class.getName());
    ExecutionTaskPlanner postponeUrpPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(postponeUrpProps));

    // Create prioritizeLargeMovementPlanner
    Properties prioritizeLargeMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeLargeMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                             String.format("%s,%s", PrioritizeLargeReplicaMovementStrategy.class.getName(),
                                                           BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeLargeMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeLargeMovementProps));

    // Create prioritizeSmallMovementPlanner
    Properties prioritizeSmallMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeSmallMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                             String.format("%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                           BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeSmallMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeSmallMovementProps));

    // Create smallUrpMovementPlanner
    Properties smallUrpMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    smallUrpMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                      String.format("%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                    PostponeUrpReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner smallUrpMovementPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(smallUrpMovementProps));

    // Create contradictingMovementPlanner containing both small and large replica movements
    Properties contradictingMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    contradictingMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                           String.format("%s,%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                         PostponeUrpReplicaMovementStrategy.class.getName(),
                                                         PrioritizeLargeReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner contradictingMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(contradictingMovementProps));

    // Create prioritizeMinIsrMovementPlanner
    Properties prioritizeMinIsrMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeMinIsrMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                              PrioritizeMinIsrWithOfflineReplicasStrategy.class.getName());
    ExecutionTaskPlanner prioritizeMinIsrMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeMinIsrMovementProps));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_partitionMovement0, true));
    partitions.add(generatePartitionInfo(_partitionMovement1, false));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_partitionMovement2, true));
    partitions.add(generatePartitionInfo(_partitionMovement3, false));

    Cluster expectedCluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    // This ensures that the _partitionMovement0 and _partitionMovement2 are AtMinISR, while the other partitions are not.
    Map<String, MinIsrWithTime> minIsrWithTimeByTopic
        = Collections.singletonMap(TOPIC2, new MinIsrWithTime((short) (_partitionMovement0.oldReplicas().size() - 1), 0));
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).minIsrWithTimeByTopic(minIsrWithTimeByTopic).build();

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 14);
    readyBrokers.put(1, 14);
    readyBrokers.put(2, 14);
    readyBrokers.put(3, 14);

    basePlanner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> partitionMovementTasks = basePlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                                _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement1, partitionMovementTasks.get(2).proposal());

    postponeUrpPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = postponeUrpPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                   _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());

    prioritizeLargeMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = prioritizeLargeMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                               _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement2, partitionMovementTasks.get(2).proposal());

    prioritizeSmallMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = prioritizeSmallMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                               _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement3, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement1, partitionMovementTasks.get(3).proposal());

    smallUrpMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = smallUrpMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                        _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement3, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement1, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement2, partitionMovementTasks.get(3).proposal());

    contradictingMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = contradictingMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                             _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement3, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement1, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement2, partitionMovementTasks.get(3).proposal());

    prioritizeMinIsrMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = prioritizeMinIsrMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                                _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement1, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement3, partitionMovementTasks.get(3).proposal());
  }

  @Test
  public void testGetInterBrokerPartitionMovementWithMinIsrTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_rf4PartitionMovement0);
    proposals.add(_rf4PartitionMovement1);
    proposals.add(_rf4PartitionMovement2);
    proposals.add(_rf4PartitionMovement3);
    // Test PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy execution strategies.
    // Create prioritizeOneAboveMinIsrMovementPlanner, chain after prioritizeMinIsr strategy
    Properties prioritizeOneAboveMinIsrMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeOneAboveMinIsrMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                                      String.format("%s,%s", PrioritizeMinIsrWithOfflineReplicasStrategy.class.getName(),
                                                                    PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeOneAboveMinIsrMovementPlanner
        = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeOneAboveMinIsrMovementProps));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(_rf4PartitionMovement0, false));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_rf4PartitionMovement1, 1));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_rf4PartitionMovement2, 3));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_rf4PartitionMovement3, 2));

    Cluster expectedCluster = new Cluster(null, _rf4ExpectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    // Setting topic min ISR to 2
    Map<String, MinIsrWithTime> minIsrWithTimeByTopic
        = Collections.singletonMap(TOPIC3, new MinIsrWithTime((short) 2, 0));
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).minIsrWithTimeByTopic(minIsrWithTimeByTopic).build();

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 5);
    readyBrokers.put(1, 6);
    readyBrokers.put(2, 6);
    readyBrokers.put(3, 6);
    readyBrokers.put(4, 5);
    readyBrokers.put(5, 6);
    prioritizeOneAboveMinIsrMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> partitionMovementTasks
        = prioritizeOneAboveMinIsrMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(), _defaultPartitionsMaxCap);
    assertEquals("First task", _rf4PartitionMovement2, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _rf4PartitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _rf4PartitionMovement1, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _rf4PartitionMovement0, partitionMovementTasks.get(3).proposal());
  }

  @Test
  public void testDynamicConfigReplicaMovementStrategy() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_partitionMovement0);
    proposals.add(_partitionMovement1);
    proposals.add(_partitionMovement2);
    proposals.add(_partitionMovement3);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(_partitionMovement0, true));
    partitions.add(generatePartitionInfo(_partitionMovement1, false));
    partitions.add(generatePartitionInfo(_partitionMovement2, true));
    partitions.add(generatePartitionInfo(_partitionMovement3, false));

    Cluster expectedCluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 8);
    readyBrokers.put(1, 8);
    readyBrokers.put(2, 8);
    readyBrokers.put(3, 8);
    planner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                            _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement1, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, strategyOptions, new PostponeUrpReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(), _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, strategyOptions, new PrioritizeLargeReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(), _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement2, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, strategyOptions, new PrioritizeSmallReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(), _defaultPartitionsMaxCap);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement3, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement1, partitionMovementTasks.get(3).proposal());
  }

  @Test
  public void testNumRemainingInterBrokerReplicaMovementsByBrokerId() {
    ExecutionProposal p0 = new ExecutionProposal(new TopicPartition(TOPIC1, 0), 10, _r0, List.of(_r0, _r1), List.of(_r0, _r2));
    ExecutionProposal p1 = new ExecutionProposal(new TopicPartition(TOPIC1, 1), 10, _r0, List.of(_r0, _r1), List.of(_r0, _r2));
    ExecutionProposal p2 = new ExecutionProposal(new TopicPartition(TOPIC1, 2), 10, _r3, List.of(_r3, _r4), List.of(_r3, _r5));
    List<ExecutionProposal> proposals = List.of(p0, p1, p2);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    planner.addExecutionProposals(proposals, strategyOptionsFor(proposals), null);

    // Brokers 0 and 2 are involved in two movements each, brokers 3 and 5 in one movement each.
    Map<Integer, Integer> numMovementsByBrokerId = planner.numRemainingInterBrokerReplicaMovementsByBrokerId();
    assertEquals(Map.of(0, 2, 2, 2, 3, 1, 5, 1), numMovementsByBrokerId);
    assertEquals(List.of(2, 2, 1, 1), new ArrayList<>(numMovementsByBrokerId.values()));
    assertEquals(List.of(p0, p1, p2), getInterBrokerReplicaMovementsOneByOne(planner, proposals.size()));
    assertTrue(planner.numRemainingInterBrokerReplicaMovementsByBrokerId().isEmpty());
  }

  @Test
  public void testDynamicConfigThrottleAwareStrategy() {
    ExecutionProposal p0 = new ExecutionProposal(new TopicPartition(TOPIC1, 0), 10, _r0, List.of(_r0, _r1), List.of(_r0, _r2));
    ExecutionProposal p1 = new ExecutionProposal(new TopicPartition(TOPIC1, 1), 10, _r0, List.of(_r0, _r1), List.of(_r0, _r2));
    ExecutionProposal p2 = new ExecutionProposal(new TopicPartition(TOPIC1, 2), 10, _r3, List.of(_r3, _r4), List.of(_r3, _r5));
    List<ExecutionProposal> proposals = List.of(p0, p1, p2);
    StrategyOptions strategyOptions = strategyOptionsFor(proposals);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    // Throttle-aware strategy chained after another strategy, which still determines the order of tasks.
    ReplicaMovementStrategy chainedStrategy = new PrioritizeLargeReplicaMovementStrategy().chain(new ThrottleAwareReplicaMovementStrategy());
    assertTrue(ThrottleAwareReplicaMovementStrategy.isEnabledIn(chainedStrategy.chainBaseReplicaMovementStrategyIfAbsent()));
    planner.addExecutionProposals(proposals, strategyOptions, chainedStrategy);
    assertTrue(planner.isThrottleAware());
    assertEquals(List.of(p0, p1, p2), getInterBrokerReplicaMovementsOneByOne(planner, proposals.size()));
    planner.clear();

    // Falls back to the default strategy for the next execution.
    assertFalse(ThrottleAwareReplicaMovementStrategy.isEnabledIn(new PrioritizeLargeReplicaMovementStrategy()));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    assertFalse(planner.isThrottleAware());
    assertEquals(List.of(p0, p1, p2), getInterBrokerReplicaMovementsOneByOne(planner, proposals.size()));

    // Throttle-aware strategy as the default strategy.
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG, ThrottleAwareReplicaMovementStrategy.class.getName());
    ExecutionTaskPlanner throttleAwarePlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    throttleAwarePlanner.addExecutionProposals(proposals, strategyOptions, null);
    assertTrue(throttleAwarePlanner.isThrottleAware());
  }

  @Test
  public void testThrottleAwareStrategyRespectsReplicationThrottle() {
    long throttle = 50L * BYTES_PER_MB;
    ReplicaPlacementInfo r6 = new ReplicaPlacementInfo(6);
    ReplicaPlacementInfo r7 = new ReplicaPlacementInfo(7);
    // Three hot partitions (p0, p1, p2) and a cool partition (p3) moving to broker 2 from different leaders.
    ExecutionProposal p0 = new ExecutionProposal(new TopicPartition(TOPIC1, 0), 10, _r0, List.of(_r0, _r1), List.of(_r0, _r2));
    ExecutionProposal p1 = new ExecutionProposal(new TopicPartition(TOPIC1, 1), 10, _r3, List.of(_r3, _r4), List.of(_r3, _r2));
    ExecutionProposal p2 = new ExecutionProposal(new TopicPartition(TOPIC1, 2), 10, _r5, List.of(_r5, r6), List.of(_r5, _r2));
    ExecutionProposal p3 = new ExecutionProposal(new TopicPartition(TOPIC1, 3), 10, r7, List.of(r7, _r1), List.of(r7, _r2));
    List<ExecutionProposal> proposals = List.of(p0, p1, p2, p3);
    Map<TopicPartition, Double> writeRateByPartition = Map.of(p0.topicPartition(), 30.0 * BYTES_PER_MB,
                                                              p1.topicPartition(), 30.0 * BYTES_PER_MB,
                                                              p2.topicPartition(), 30.0 * BYTES_PER_MB,
                                                              p3.topicPartition(), 5.0 * BYTES_PER_MB);
    StrategyOptions strategyOptions = strategyOptionsFor(proposals);
    int maxInterBrokerPartitionMovements = proposals.size();

    // Only one hot partition starts per call, and the cool partition fills the remaining budget of broker 2.
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    planner.addExecutionProposals(proposals, strategyOptions, new ThrottleAwareReplicaMovementStrategy());
    List<ExecutionTask> inProgressTasks = planner.getInterBrokerReplicaMovementTasks(
        readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements,
        budget(Collections.emptyList(), writeRateByPartition, throttle));
    assertEquals(Set.of(p0, p3), new HashSet<>(proposalsOf(inProgressTasks)));
    inProgressTasks.forEach(task -> task.inProgress(0L));
    // The other hot partitions wait while the movements to broker 2 are in progress.
    assertTrue(planner.getInterBrokerReplicaMovementTasks(readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements,
                                                          budget(inProgressTasks, writeRateByPartition, throttle)).isEmpty());
    // Once these movements are finished, the next hot partition starts.
    List<ExecutionTask> tasks = planner.getInterBrokerReplicaMovementTasks(
        readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements,
        budget(Collections.emptyList(), writeRateByPartition, throttle));
    assertEquals(1, tasks.size());
    assertEquals(1, planner.remainingInterBrokerReplicaMovements().size());

    // Without the throttle-aware strategy, the replication throttle budget is ignored.
    ExecutionTaskPlanner basePlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    basePlanner.addExecutionProposals(proposals, strategyOptions, null);
    tasks = basePlanner.getInterBrokerReplicaMovementTasks(readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements,
                                                           budget(Collections.emptyList(), writeRateByPartition, throttle));
    assertEquals(Set.copyOf(proposals), new HashSet<>(proposalsOf(tasks)));

    // Without a replication throttle budget, all movements start.
    planner.clear();
    planner.addExecutionProposals(proposals, strategyOptions, new ThrottleAwareReplicaMovementStrategy());
    tasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements, null);
    assertEquals(Set.copyOf(proposals), new HashSet<>(proposalsOf(tasks)));

    // A partition with a write rate higher than the throttle waits while another movement is in progress on its brokers ...
    ExecutionProposal hot = new ExecutionProposal(new TopicPartition(TOPIC2, 0), 10, _r3, List.of(_r3, _r4), List.of(_r3, _r5));
    Map<TopicPartition, Double> hotWriteRateByPartition = Map.of(hot.topicPartition(), 60.0 * BYTES_PER_MB,
                                                                 p0.topicPartition(), 5.0 * BYTES_PER_MB);
    ExecutionTask inProgressTask = new ExecutionTask(0L, new ExecutionProposal(p0.topicPartition(), 10, _r0, List.of(_r0, _r1),
                                                                               List.of(_r0, _r5)),
                                                     ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 100L);
    inProgressTask.inProgress(0L);
    planner.clear();
    planner.addExecutionProposals(List.of(hot), strategyOptionsFor(List.of(hot)), new ThrottleAwareReplicaMovementStrategy());
    assertTrue(planner.getInterBrokerReplicaMovementTasks(readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements,
                                                          budget(List.of(inProgressTask), hotWriteRateByPartition, throttle)).isEmpty());
    // ... and starts alone when nothing is in progress on its brokers.
    tasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers(), Collections.emptySet(), maxInterBrokerPartitionMovements,
                                                       budget(Collections.emptyList(), hotWriteRateByPartition, throttle));
    assertEquals(List.of(hot), proposalsOf(tasks));
  }

  @Test
  public void testThrottleAwareStrategyStartsMovementsThatFitFirst() {
    long throttle = 50L * BYTES_PER_MB;
    // A hot partition (p0), whose write rate exceeds the throttle, and two cool partitions (p1, p2) move to broker 2 from
    // different leaders, the hot partition first in execution-id order.
    ExecutionProposal p0 = new ExecutionProposal(new TopicPartition(TOPIC1, 0), 10, _r0, List.of(_r0, _r1), List.of(_r0, _r2));
    ExecutionProposal p1 = new ExecutionProposal(new TopicPartition(TOPIC1, 1), 10, _r3, List.of(_r3, _r4), List.of(_r3, _r2));
    ExecutionProposal p2 = new ExecutionProposal(new TopicPartition(TOPIC1, 2), 10, _r5, List.of(_r5, _r4), List.of(_r5, _r2));
    Map<TopicPartition, Double> writeRateByPartition = Map.of(p0.topicPartition(), 60.0 * BYTES_PER_MB,
                                                              p1.topicPartition(), 10.0 * BYTES_PER_MB,
                                                              p2.topicPartition(), 10.0 * BYTES_PER_MB);
    // The cool partitions start together, and the hot partition starts alone once they are finished.
    assertThrottleAwareStartOrder(List.of(p0, p1, p2), writeRateByPartition, throttle, Set.of(p1, p2), Set.of(p0));

    // Same if the hot partition (p3) is checked for its source broker 0 before the cool partition (p4) is checked for the
    // destination broker 2 they share. Broker 0 is checked first as another hot partition (p5) leaves it, too. p5 starts alone
    // on the brokers left idle by the cool partition.
    ExecutionProposal p3 = new ExecutionProposal(new TopicPartition(TOPIC2, 0), 10, _r0, List.of(_r0, _r4), List.of(_r0, _r2));
    ExecutionProposal p4 = new ExecutionProposal(new TopicPartition(TOPIC2, 1), 10, _r1, List.of(_r1, _r4), List.of(_r1, _r2));
    ExecutionProposal p5 = new ExecutionProposal(new TopicPartition(TOPIC2, 2), 10, _r0, List.of(_r0, _r4), List.of(_r0, _r3));
    writeRateByPartition = Map.of(p3.topicPartition(), 60.0 * BYTES_PER_MB,
                                  p4.topicPartition(), 10.0 * BYTES_PER_MB,
                                  p5.topicPartition(), 60.0 * BYTES_PER_MB);
    assertThrottleAwareStartOrder(List.of(p3, p4, p5), writeRateByPartition, throttle, Set.of(p4, p5), Set.of(p3));
  }

  @Test
  public void testGetIntraBrokerPartitionMovementTasks() {
    ReplicaPlacementInfo r0d0 = new ReplicaPlacementInfo(0, "d0");
    ReplicaPlacementInfo r0d1 = new ReplicaPlacementInfo(0, "d1");
    ReplicaPlacementInfo r1d0 = new ReplicaPlacementInfo(1, "d0");
    ReplicaPlacementInfo r1d1 = new ReplicaPlacementInfo(1, "d1");

    List<ExecutionProposal> proposals = Collections.singletonList(new ExecutionProposal(new TopicPartition(TOPIC2, 0),
                                                                                        4,
                                                                                        r0d0,
                                                                                        Arrays.asList(r0d0, r1d1),
                                                                                        Arrays.asList(r1d0, r0d1)));

    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TOPIC2, 0, 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TOPIC2, 0, 1);

    //Mock adminClient
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    try {
      // Reflectively set constructors from package private to public.
      Constructor<DescribeReplicaLogDirsResult> constructor1 = DescribeReplicaLogDirsResult.class.getDeclaredConstructor(Map.class);
      constructor1.setAccessible(true);
      Constructor<ReplicaLogDirInfo> constructor2 =
          ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
      constructor2.setAccessible(true);

      Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
      futureByReplica.put(tpr0, completedFuture(constructor2.newInstance("d0", 0L, null, -1L)));
      futureByReplica.put(tpr1, completedFuture(constructor2.newInstance("d1", 0L, null, -1L)));

      EasyMock.expect(mockAdminClient.describeReplicaLogDirs(anyObject()))
              .andReturn(constructor1.newInstance(futureByReplica))
              .anyTimes();
      EasyMock.replay(mockAdminClient);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      // Let it go.
    }

    Set<PartitionInfo> partitions = new HashSet<>();
    Node[] isrArray = generateExpectedReplicas(proposals.get(0));
    partitions.add(new PartitionInfo(proposals.get(0).topicPartition().topic(),
                                     proposals.get(0).topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.emptySet(),
                                          Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();

    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(mockAdminClient, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    assertEquals(1, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingIntraBrokerReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingIntraBrokerReplicaMovements().size());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClear() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_partitionMovement0);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Set<PartitionInfo> partitions = new HashSet<>();

    partitions.add(generatePartitionInfo(_leaderMovement1, false));
    partitions.add(generatePartitionInfo(_partitionMovement0, false));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.emptySet(),
                                          Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();

    planner.addExecutionProposals(proposals, strategyOptions, null);
    assertEquals(2, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingInterBrokerReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingInterBrokerReplicaMovements().size());
  }

  /**
   * Get the inter-broker replica movements from the given planner one at a time (i.e. max one partition movement per call).
   *
   * @param planner The execution task planner to get the inter-broker replica movements from.
   * @param numMovements The number of inter-broker replica movements to get.
   * @return The proposals of the inter-broker replica movements in the order they are picked by the planner.
   */
  private List<ExecutionProposal> getInterBrokerReplicaMovementsOneByOne(ExecutionTaskPlanner planner, int numMovements) {
    List<ExecutionProposal> movements = new ArrayList<>();
    Map<Integer, Integer> readyBrokers = readyBrokers();
    for (int i = 0; i < numMovements; i++) {
      List<ExecutionTask> tasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(), 1);
      assertEquals(1, tasks.size());
      movements.add(tasks.get(0).proposal());
    }
    assertEquals(0, planner.remainingInterBrokerReplicaMovements().size());
    return movements;
  }

  /**
   * Assert that with the throttle-aware strategy and the given replication throttle, the given movements start in two steps:
   * first the given movements, then, once these are finished, the other given movements.
   *
   * @param proposals The proposals to execute, in execution-id order.
   * @param writeRateByPartition The write rate of partitions in bytes per second.
   * @param throttle The replication throttle of all brokers in bytes per second.
   * @param startedFirst The proposals expected to start first.
   * @param startedNext The proposals expected to start once the first ones are finished.
   */
  private void assertThrottleAwareStartOrder(List<ExecutionProposal> proposals,
                                             Map<TopicPartition, Double> writeRateByPartition,
                                             long throttle,
                                             Set<ExecutionProposal> startedFirst,
                                             Set<ExecutionProposal> startedNext) {
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    planner.addExecutionProposals(proposals, strategyOptionsFor(proposals), new ThrottleAwareReplicaMovementStrategy());
    List<ExecutionTask> inProgressTasks = planner.getInterBrokerReplicaMovementTasks(
        readyBrokers(), Collections.emptySet(), proposals.size(), budget(Collections.emptyList(), writeRateByPartition, throttle));
    assertEquals(startedFirst, new HashSet<>(proposalsOf(inProgressTasks)));
    inProgressTasks.forEach(task -> task.inProgress(0L));
    assertTrue(planner.getInterBrokerReplicaMovementTasks(readyBrokers(), Collections.emptySet(), proposals.size(),
                                                          budget(inProgressTasks, writeRateByPartition, throttle)).isEmpty());
    List<ExecutionTask> tasks = planner.getInterBrokerReplicaMovementTasks(
        readyBrokers(), Collections.emptySet(), proposals.size(), budget(Collections.emptyList(), writeRateByPartition, throttle));
    assertEquals(startedNext, new HashSet<>(proposalsOf(tasks)));
    assertEquals(0, planner.remainingInterBrokerReplicaMovements().size());
  }

  private static ReplicationThrottleBudget budget(List<ExecutionTask> inProgressTasks,
                                                  Map<TopicPartition, Double> writeRateByPartition,
                                                  long throttle) {
    return new ReplicationThrottleBudget(inProgressTasks, writeRateByPartition, task -> null,
                                         brokerId -> throttle, brokerId -> throttle,
                                         ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_THROTTLE_WRITE_RATE_MARGIN);
  }

  private static Map<Integer, Integer> readyBrokers() {
    Map<Integer, Integer> readyBrokers = new HashMap<>();
    for (int brokerId = 0; brokerId < 8; brokerId++) {
      readyBrokers.put(brokerId, MAX_BROKER_CONCURRENCY);
    }
    return readyBrokers;
  }

  private static List<ExecutionProposal> proposalsOf(List<ExecutionTask> tasks) {
    List<ExecutionProposal> proposals = new ArrayList<>(tasks.size());
    tasks.forEach(task -> proposals.add(task.proposal()));
    return proposals;
  }

  private StrategyOptions strategyOptionsFor(List<ExecutionProposal> proposals) {
    Set<PartitionInfo> partitions = new HashSet<>();
    proposals.forEach(proposal -> partitions.add(generatePartitionInfo(proposal, false)));
    List<Node> nodes = new ArrayList<>();
    for (int brokerId = 0; brokerId < 8; brokerId++) {
      nodes.add(new Node(brokerId, "null", -1));
    }
    Cluster cluster = new Cluster(null, nodes, partitions, Collections.emptySet(), Collections.emptySet());
    return new StrategyOptions.Builder(cluster).build();
  }

  private Node[] generateExpectedReplicas(ExecutionProposal proposal) {
    int i = 0;
    Node[] expectedProposalReplicas = new Node[proposal.oldReplicas().size()];
    for (ReplicaPlacementInfo oldId: proposal.oldReplicas()) {
      expectedProposalReplicas[i++] = new Node(oldId.brokerId(), "null", -1);
    }
    return expectedProposalReplicas;
  }

  private PartitionInfo generatePartitionInfoWithUrpHavingOfflineReplica(ExecutionProposal proposal, int urpCount) {
    Node[] isrArray = generateExpectedReplicas(proposal);

    Node[] offlineReplicas;
    if (urpCount > 0) {
      offlineReplicas = new Node[urpCount];
      System.arraycopy(isrArray, isrArray.length - urpCount, offlineReplicas, 0, urpCount);
    } else {
      offlineReplicas = new Node[0];
    }

    return new PartitionInfo(proposal.topicPartition().topic(),
                             proposal.topicPartition().partition(), isrArray[0], isrArray,
                             urpCount > 0 ? Arrays.copyOf(isrArray, isrArray.length - urpCount) : isrArray,
                             offlineReplicas);
  }

  private PartitionInfo generatePartitionInfoWithUrpHavingOfflineReplica(ExecutionProposal proposal, boolean isPartitionURP) {
    return generatePartitionInfoWithUrpHavingOfflineReplica(proposal, isPartitionURP ? 1 : 0);
  }

  private PartitionInfo generatePartitionInfo(ExecutionProposal proposal, boolean isPartitionURP) {
    Node[] isrArray = generateExpectedReplicas(proposal);
    return new PartitionInfo(proposal.topicPartition().topic(),
                             proposal.topicPartition().partition(), isrArray[0], isrArray,
                             isPartitionURP ? Arrays.copyOf(isrArray, 1) : isrArray);
  }
}
