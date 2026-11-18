/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.BrokerSetAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Host;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Rack;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils.goal;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.smallClusterModel;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.mediumClusterModel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for testing modifying topic replication factor with different requirement under fixed cluster properties.
 */
@RunWith(Parameterized.class)
public class ReplicationFactorChangeTest {
  private static final short SMALL_REPLICATION_FACTOR = 1;
  private static final short LARGE_REPLICATION_FACTOR = 3;

  private final int _testId;
  private final Set<String> _topics;
  private final short _replicationFactor;
  private final Goal _goal;
  private final OptimizationOptions _optimizationOptions;
  private final Class<Throwable> _exceptionClass;
  private final ClusterModel _clusterModel;
  private final Boolean _expectedToOptimize;
  private Map<String, List<Integer>> _brokersByRack;
  private Map<Integer, String> _rackByBroker;
  private Cluster _cluster;

  /**
   * Constructor of Replication Factor Change Test.
   *
   * @param testId The test id.
   * @param topics Topics to modify replication factor.
   * @param replicationFactor The target replication factor.
   * @param goal Goal to be used to further tune location of new replicas.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   */
  public ReplicationFactorChangeTest(int testId,
                                     Set<String> topics,
                                     short replicationFactor,
                                     Goal goal,
                                     Class<Throwable> exceptionClass,
                                     ClusterModel clusterModel,
                                     Boolean expectedToOptimize) {
    _testId = testId;
    _topics = topics;
    _replicationFactor = replicationFactor;
    _goal = goal;
    _optimizationOptions = new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                                                   false, Collections.emptySet(), true);
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
  }

  /**
   * Populate parameters for the parametrized test.
   * @return Populated parameters.
   */
  @Parameterized.Parameters(name = "test-{0}-replica_factor={2}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    int tid = 0;

    for (short replicationFactor : Arrays.asList(SMALL_REPLICATION_FACTOR, LARGE_REPLICATION_FACTOR)) {
      for (boolean isSmallCluster: Arrays.asList(true, false)) {
        for (Class<? extends Goal> goalClass : Arrays.asList(BrokerSetAwareGoal.class,
                                                             RackAwareGoal.class,
                                                             RackAwareDistributionGoal.class,
                                                             MinTopicLeadersPerBrokerGoal.class,
                                                             ReplicaCapacityGoal.class,
                                                             DiskCapacityGoal.class,
                                                             NetworkInboundCapacityGoal.class,
                                                             NetworkOutboundCapacityGoal.class,
                                                             CpuCapacityGoal.class,
                                                             ReplicaDistributionGoal.class,
                                                             PotentialNwOutGoal.class,
                                                             DiskUsageDistributionGoal.class,
                                                             NetworkInboundUsageDistributionGoal.class,
                                                             NetworkOutboundUsageDistributionGoal.class,
                                                             CpuUsageDistributionGoal.class,
                                                             LeaderReplicaDistributionGoal.class,
                                                             LeaderBytesInDistributionGoal.class,
                                                             TopicReplicaDistributionGoal.class)) {
          ClusterModel clusterModel = isSmallCluster ? smallClusterModel(TestConstants.BROKER_CAPACITY)
                                                     : mediumClusterModel(TestConstants.BROKER_CAPACITY);
          p.add(params(tid++, clusterModel.topics(), replicationFactor, goalClass,
                       expectedExceptionClass(replicationFactor, goalClass, isSmallCluster),
                       clusterModel, expectedToOptimize(replicationFactor, goalClass, isSmallCluster)));
        }
      }
    }
    return p;
  }

  private static Class<? extends Throwable> expectedExceptionClass(short replicationFactor,
                                                                   Class<? extends Goal> goalClass,
                                                                   boolean smallCluster) {
    if ((replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == RackAwareGoal.class)
        || (replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == ReplicaCapacityGoal.class && !smallCluster)
        || (replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == MinTopicLeadersPerBrokerGoal.class && !smallCluster)) {
      return OptimizationFailureException.class;
    }
    return null;
  }

  /**
   * Check whether the given goal is expected to be optimized with the given replication factor and cluster model.
   * This function returns {@code false} if the given parameters together fall into any exceptional case, and {@code true} otherwise.
   *
   * Each exceptional case for the given set of parameters is indicated individually. For example, {@code ReplicaDistributionGoal} is not
   * expected to be optimized when replication factor and the cluster is small.
   *
   * @param replicationFactor Target replication factor.
   * @param goalClass The class corresponding to the goal to be optimized.
   * @param smallCluster {@code true} for optimization under small cluster model, {@code false} otherwise.
   * @return {@code true} if the goal is expected to be optimized with the given cluster model and replication factor, {@code false} otherwise.
   */
  private static boolean expectedToOptimize(short replicationFactor, Class<? extends Goal> goalClass, boolean smallCluster) {
    // Each line indicates an exceptional case. If parameters satisfy any exceptional case, the response will be false.
    return !(replicationFactor == SMALL_REPLICATION_FACTOR && goalClass == ReplicaDistributionGoal.class && smallCluster
             || replicationFactor == SMALL_REPLICATION_FACTOR && goalClass == DiskUsageDistributionGoal.class
             || replicationFactor == SMALL_REPLICATION_FACTOR && goalClass == NetworkInboundUsageDistributionGoal.class
             || replicationFactor == SMALL_REPLICATION_FACTOR && goalClass == NetworkOutboundUsageDistributionGoal.class
             || replicationFactor == SMALL_REPLICATION_FACTOR && goalClass == CpuUsageDistributionGoal.class
             || replicationFactor == SMALL_REPLICATION_FACTOR && goalClass == LeaderReplicaDistributionGoal.class && smallCluster
             || replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == NetworkOutboundUsageDistributionGoal.class && smallCluster
             || goalClass == LeaderBytesInDistributionGoal.class && !(replicationFactor != SMALL_REPLICATION_FACTOR && !smallCluster)
             || replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == DiskUsageDistributionGoal.class && !smallCluster
             || replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == NetworkInboundUsageDistributionGoal.class && !smallCluster
             || replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == CpuUsageDistributionGoal.class
             || replicationFactor == LARGE_REPLICATION_FACTOR && goalClass == MinTopicLeadersPerBrokerGoal.class && !smallCluster);
  }

  @Test
  public void test() throws Exception {
    prepareContext();
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = _clusterModel.getLeaderDistribution();

    _clusterModel.createOrDeleteReplicas(Collections.singletonMap(_replicationFactor, _topics), _brokersByRack, _rackByBroker, _cluster);
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, _goal.provisionResponse().status());
    if (_exceptionClass == null) {
      if (_expectedToOptimize) {
        assertTrue("Replication factor change test with goal " + _goal.name() + " failed.",
                   _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      } else {
        assertFalse("Replication factor change test with goal " + _goal.name() + " should not succeed.",
                    _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      }
      // The cluster cannot be underprovisioned, because _exceptionClass was null.
      assertNotEquals(ProvisionStatus.UNDER_PROVISIONED, _goal.provisionResponse().status());
      Set<ExecutionProposal> goalProposals =
          AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel, true);

      for (ExecutionProposal proposal : goalProposals) {
        // Replication factor change should only be applied to specified topics.
        if (!_topics.contains(proposal.topic())) {
          fail("Replication factor change should not apply to topic %s." + proposal.topic());
        }
        if (proposal.newReplicas().size() != _replicationFactor) {
          fail(String.format("Topic partition %s's replication factor is not changed to %d.", proposal.topicPartition(), _replicationFactor));
        }
        // Increase replication factor should not touch the existing replicas.
        if (_replicationFactor >= proposal.oldReplicas().size() && !proposal.replicasToRemove().isEmpty()) {
          fail(String.format("Increasing topic partition %s's replication factor to %d should not move existing replicas.",
                             proposal.topicPartition(), _replicationFactor));
        }
      }

      // Ensure all the specified topic has target replication factor.
      for (String topic : _topics) {
        for (PartitionInfo partitioninfo : _cluster.partitionsForTopic(topic)) {
          TopicPartition tp = new TopicPartition(topic, partitioninfo.partition());
          if (_clusterModel.partition(tp).replicas().size() != _replicationFactor) {
            fail(String.format("Topic partition %s's replication factor is not changed to %d", tp, _replicationFactor));
          }
        }
      }
    } else {
      assertThrows(_exceptionClass, () -> _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
    }
  }

  /**
   * Initialize necessary metadata from cluster model.
   */
  private void prepareContext() {
    _brokersByRack = new HashMap<>();
    _rackByBroker = new HashMap<>();
    Map<Integer, Node> nodesById = new HashMap<>();
    Set<PartitionInfo> partitionInfos = new HashSet<>();

    for (Broker broker : _clusterModel.brokers()) {
      Host host = broker.host();
      Rack rack = host.rack();
      nodesById.put(broker.id(), new Node(broker.id(), host.name(), 0, rack.id()));
      _brokersByRack.putIfAbsent(rack.id(), new ArrayList<>());
      _brokersByRack.get(rack.id()).add(broker.id());
      _rackByBroker.put(broker.id(), rack.id());
    }

    for (Map.Entry<String, List<Partition>> entry :_clusterModel.getPartitionsByTopic().entrySet()) {
      String topic = entry.getKey();
      for (Partition p : entry.getValue()) {
        Node [] replicas = new Node [p.replicas().size()];
        int i = 0;
        for (Replica replica : p.replicas()) {
          replicas[i++] = nodesById.get(replica.broker().id());
        }
        partitionInfos.add(new PartitionInfo(topic, p.topicPartition().partition(), nodesById.get(p.leader().broker().id()),
                                             replicas, replicas));
      }
    }
    _cluster = new Cluster("cluster", nodesById.values(), partitionInfos, Collections.emptySet(), Collections.emptySet());
  }

  private static Object[] params(int tid,
                                 Set<String> topics,
                                 short replicationFactor,
                                 Class<? extends Goal> goalClass,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Boolean expectedToOptimize) throws Exception {
    Properties configOverrides = new Properties();
    configOverrides.put(AnalyzerConfig.MIN_TOPIC_LEADERS_PER_BROKER_CONFIG, "1");
    if (topics.contains(DeterministicCluster.TOPIC_A)) {
      // It implies the cluster model is the medium cluster model
      configOverrides.put(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, DeterministicCluster.TOPIC_A);
    } else if (topics.contains(DeterministicCluster.T2)) {
      // It implies the cluster model is the small cluster model
      configOverrides.put(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG, DeterministicCluster.T2);
    } else {
      fail("Cannot figure out which topic to use to test the MinTopicLeadersPerBrokerGoal with model: " + clusterModel);
    }

    String brokerSetsDataFile = Objects.requireNonNull(KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource(
        TestConstants.BROKER_SET_RESOLVER_FILE_1)).getFile();
    configOverrides.setProperty(AnalyzerConfig.BROKER_SET_CONFIG_FILE_CONFIG, brokerSetsDataFile);

    return new Object[]{tid, topics, replicationFactor, goal(goalClass, configOverrides), exceptionClass, clusterModel, expectedToOptimize};
  }
}
