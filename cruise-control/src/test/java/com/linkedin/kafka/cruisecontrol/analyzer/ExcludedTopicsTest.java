/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

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
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.ReplicaToOriginalBrokerSetMappingPolicy;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUnitTestUtils.goal;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced2;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced3;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced5;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for testing goals with excluded topics under fixed cluster properties.
 * Generated proposals cannot have the excluded topic.
 */
@RunWith(Parameterized.class)
public class ExcludedTopicsTest {

  @Rule
  public ExpectedException _expected = ExpectedException.none();

  private final int _testId;
  private final Goal _goal;
  private final OptimizationOptions _optimizationOptions;
  private final Class<Throwable> _exceptionClass;
  private final ClusterModel _clusterModel;
  private final Boolean _expectedToOptimize;
  private final Boolean _expectedToGenerateProposals;
  private static final Properties NO_CONFIG_OVERRIDE = new Properties();

  /**
   * Constructor of Excluded Topics Test.
   *
   * @param testId the test id
   * @param goal Goal to be tested.
   * @param excludedTopics Topics to be excluded from the goal.
   * @param exceptionClass Expected exception class (if any).
   * @param clusterModel Cluster model to be used for the test.
   * @param expectedToOptimize The expectation on whether the cluster state will be considered optimized or not.
   * @param expectedToGenerateProposals The expectation on whether the optimization process will generate proposals or not,
   *                                   or {@code null} if proposal generation is irrelevant due to exception.
   */
  public ExcludedTopicsTest(int testId,
                            Goal goal,
                            Set<String> excludedTopics,
                            Class<Throwable> exceptionClass,
                            ClusterModel clusterModel,
                            Boolean expectedToOptimize,
                            Boolean expectedToGenerateProposals) {
    _testId = testId;
    _goal = goal;
    _optimizationOptions = new OptimizationOptions(excludedTopics, Collections.emptySet(), Collections.emptySet());
    _exceptionClass = exceptionClass;
    _clusterModel = clusterModel;
    _expectedToOptimize = expectedToOptimize;
    _expectedToGenerateProposals = expectedToGenerateProposals;
  }

  /**
   * Populate parameters for the parametrized test.
   * @return Populated parameters.
   */
  @Parameterized.Parameters(name = "{1}-{0}")
  public static Collection<Object[]> data() throws Exception {
    Collection<Object[]> p = new ArrayList<>();

    Set<String> noExclusion = Collections.emptySet();
    Set<String> excludeT1 = Collections.singleton(T1);
    Set<String> excludeAllTopics = Set.of(T1, T2);
    Set<String> excludeTopic0 = Collections.singleton(TestConstants.TOPIC0);
    Set<String> excludeTopic1 = Collections.singleton(TestConstants.TOPIC1);
    Set<Integer> noDeadBroker = Collections.emptySet();
    Set<Integer> deadBroker0 = Collections.singleton(0);

    // ============RackAwareGoal============
    // With excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, RackAwareGoal.class, excludeT1, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true, false));
    // With excluded topics, rack aware satisfiable cluster, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, RackAwareGoal.class, excludeT1, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true, true));
    // Without excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(2, RackAwareGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true, true));
    // Without excluded topics, rack aware satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, RackAwareGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true, true));
    // With excluded topics, rack aware unsatisfiable cluster, no dead broker (No exception, No proposal, Expected to look optimized)
    p.add(params(4, RackAwareGoal.class, excludeT1, null, DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, true, false));
    // With excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(5, RackAwareGoal.class, excludeT1, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(),
                 deadBroker0, null, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, no dead brokers (Exception expected)
    p.add(params(6, RackAwareGoal.class, noExclusion, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(),
                 noDeadBroker, null, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception expected)
    p.add(params(7, RackAwareGoal.class, noExclusion, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(),
                 deadBroker0, null, null));

    // ============RackAwareDistributionGoal============
    // With excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, RackAwareDistributionGoal.class, excludeT1, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true, false));
    // With excluded topics, rack aware satisfiable cluster, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, RackAwareDistributionGoal.class, excludeT1, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true, true));
    // Without excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(2, RackAwareDistributionGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true, true));
    // Without excluded topics, rack aware satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, RackAwareDistributionGoal.class, noExclusion, null, DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true, true));
    // With excluded topics, rack aware unsatisfiable cluster, no dead broker (No exception, No proposal, Expected to look optimized)
    p.add(params(4, RackAwareDistributionGoal.class, excludeT1, null, DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, true, false));
    // With excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(5, RackAwareDistributionGoal.class, excludeT1, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(),
                 deadBroker0, null, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, no dead brokers (No exception, Generates proposals,
    // Expected to look optimized)
    p.add(params(6, RackAwareDistributionGoal.class, noExclusion, null, DeterministicCluster.rackAwareUnsatisfiable(),
                 noDeadBroker, true, true));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception expected)
    p.add(params(7, RackAwareDistributionGoal.class, noExclusion, OptimizationFailureException.class, DeterministicCluster.rackAwareUnsatisfiable(),
                 deadBroker0, null, null));

    // ============MinTopicLeadersPerBrokerGoal============
    Properties configOverrides = new Properties();
    configOverrides.setProperty(AnalyzerConfig.TOPICS_WITH_MIN_LEADERS_PER_BROKER_CONFIG,
                                TestConstants.TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS);
    // With one excluded but irrelevant topic, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, MinTopicLeadersPerBrokerGoal.class, excludeT1, null,
                 DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable(), noDeadBroker, true, true, configOverrides));
    // With one excluded and relevant topic, no dead brokers (No exception, No proposal, Expected to look optimized: the excluded
    // topic is held rather than enforced, so a broker below its minimum does not make the goal violated)
    p.add(params(1, MinTopicLeadersPerBrokerGoal.class, Collections.singleton(TestConstants.TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS),
                 null, DeterministicCluster.minLeaderReplicaPerBrokerSatisfiable(), noDeadBroker, true, false, configOverrides));

    // ============ReplicaCapacityGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, ReplicaCapacityGoal.class, excludeT1, null, unbalanced(), noDeadBroker, true, false));
    // Test: With single excluded topic, satisfiable cluster, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, ReplicaCapacityGoal.class, excludeT1, null, unbalanced(), deadBroker0, true, true));
    // Test: With all topics excluded, no dead brokers, not satisfiable (No exception, No proposal, Expected to look optimized)
    p.add(params(2, ReplicaCapacityGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, true, false));
    // Test: With all topics excluded, one dead brokers, Generates proposals, Expected to look optimized
    p.add(params(3, ReplicaCapacityGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true, true));

    for (Class<? extends Goal> goalClass : Arrays.asList(CpuCapacityGoal.class,
                                                         DiskCapacityGoal.class,
                                                         NetworkInboundCapacityGoal.class,
                                                         NetworkOutboundCapacityGoal.class)) {
      // Test: With single excluded topic, satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
      p.add(params(0, goalClass, excludeT1, null, unbalanced(), noDeadBroker, true, true));
      // Test: With single excluded topic, satisfiable cluster, one dead brokers (No exception, Generates proposals, Expected to look optimized)
      p.add(params(1, goalClass, excludeT1, null, unbalanced(), deadBroker0, true, true));
      // Test: With all topics excluded, no dead brokers, not satisfiable (Exception)
      p.add(params(2, goalClass, excludeAllTopics, OptimizationFailureException.class, unbalanced(), noDeadBroker, null, null));
      // Test: With all topics excluded, one dead brokers, Generates proposals, Expected to look optimized.
      p.add(params(3, goalClass, excludeAllTopics, null, unbalanced(), deadBroker0, true, true));
    }

    for (Class<? extends Goal> goalClass : Arrays.asList(DiskUsageDistributionGoal.class,
                                                         NetworkInboundUsageDistributionGoal.class,
                                                         NetworkOutboundUsageDistributionGoal.class,
                                                         CpuUsageDistributionGoal.class)) {
      // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal,
      // Not expected to look optimized)
      p.add(params(0, goalClass, excludeT1, null, unbalanced(), noDeadBroker, false, false));
      // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, Generates proposals,
      // Not expected to look optimized)
      p.add(params(1, goalClass, excludeT1, null, unbalanced(), deadBroker0, true, true));
      // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal, Not expected to look optimized)
      p.add(params(2, goalClass, excludeAllTopics, null, unbalanced(), noDeadBroker, false, false));
      // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, Generates proposals, Expected to look optimized)
      p.add(params(3, goalClass, excludeAllTopics, null, unbalanced(), deadBroker0, true, true));
    }

    // ============LeaderBytesInDistributionGoal============
    // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal, Not expected to look optimized)
    p.add(params(0, LeaderBytesInDistributionGoal.class, excludeT1, null, unbalanced(), noDeadBroker, false, false));
    // Test: With single excluded topic, balance not satisfiable cluster, no dead broker (No exception, No proposal, Not expected to look optimized)
    p.add(params(1, LeaderBytesInDistributionGoal.class, excludeT1, null, unbalanced(), deadBroker0, false, false));
    // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal, Not expected to look optimized)
    p.add(params(2, LeaderBytesInDistributionGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, false, false));
    // Test: With all topics excluded, no dead brokers, balance not satisfiable (No exception, No proposal, Not expected to look optimized)
    p.add(params(3, LeaderBytesInDistributionGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, false, false));

    // ============PotentialNwOutGoal============
    // Test: With single excluded topic, balance satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, PotentialNwOutGoal.class, excludeT1, null, unbalanced(), noDeadBroker, true, true));
    // Test: With single excluded topic, balance satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, PotentialNwOutGoal.class, excludeT1, null, unbalanced(), deadBroker0, true, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal, Not expected to look optimized)
    p.add(params(2, PotentialNwOutGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, false, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, PotentialNwOutGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true, true));

    // ============TopicReplicaDistributionGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, No proposal, Large min gap, Expected to look optimized)
    configOverrides = new Properties();
    configOverrides.setProperty(AnalyzerConfig.TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, "40");
    p.add(params(0, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced5(), noDeadBroker, true, false, configOverrides));
    // Test: With single excluded topic, satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced(), deadBroker0, true, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(2, TopicReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced(), noDeadBroker, true, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, TopicReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced(), deadBroker0, true, true));
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(4, TopicReplicaDistributionGoal.class, excludeT1, null, unbalanced5(), noDeadBroker, true, true));

    // ============ReplicaDistributionGoal============
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, ReplicaDistributionGoal.class, excludeT1, null, unbalanced2(), noDeadBroker, true, true));
    // Test: With single excluded topic, satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, ReplicaDistributionGoal.class, excludeT1, null, unbalanced2(), deadBroker0, true, true));
    // Test: With all topics excluded, balance not satisfiable, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(2, ReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced2(), noDeadBroker, false, false));
    // Test: With all topics excluded, balance not satisfiable, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, ReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced2(), deadBroker0, true, true));

    // ============LeaderReplicaDistributionGoal============
    // Test: With no topic excluded, satisfiable cluster, no dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(0, LeaderReplicaDistributionGoal.class, noExclusion, null, unbalanced3(), noDeadBroker, true, true));
    // Test: With single excluded topic, satisfiable cluster, no dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, LeaderReplicaDistributionGoal.class, excludeT1, null, unbalanced3(), noDeadBroker, true, true));
    // Test: With all topics excluded, balance not satisfiable, no dead broker (No exception, No proposal, Not expected to look optimized)
    p.add(params(2, LeaderReplicaDistributionGoal.class, excludeAllTopics, null, unbalanced3(), noDeadBroker, false, false));
    // Test: With no topic excluded, satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, LeaderReplicaDistributionGoal.class, noExclusion, null, unbalanced3(), deadBroker0, true, true));

    // ============KafkaAssignerEvenRackAwareGoal============
    // With excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true, false));
    // With excluded topics, rack aware satisfiable cluster, one dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(1, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true, true));
    // Without excluded topics, rack aware satisfiable cluster, no dead brokers (No exception, Generates proposals, Expected to look optimized)
    p.add(params(2, KafkaAssignerEvenRackAwareGoal.class, noExclusion, null,
                 DeterministicCluster.rackAwareSatisfiable(), noDeadBroker, true, true));
    // Without excluded topics, rack aware satisfiable cluster, one dead broker (No exception, Generates proposals, Expected to look optimized)
    p.add(params(3, KafkaAssignerEvenRackAwareGoal.class, noExclusion, null,
                 DeterministicCluster.rackAwareSatisfiable(), deadBroker0, true, true));
    // With excluded topics, rack aware unsatisfiable cluster, no dead broker (No exception, No proposal, Expected to look optimized)
    p.add(params(4, KafkaAssignerEvenRackAwareGoal.class, excludeT1, null,
                 DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, true, false));
    // With excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception)
    p.add(params(5, KafkaAssignerEvenRackAwareGoal.class, excludeT1, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, no dead brokers (Exception expected)
    p.add(params(6, KafkaAssignerEvenRackAwareGoal.class, noExclusion, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), noDeadBroker, null, null));
    // Test: Without excluded topics, rack aware unsatisfiable cluster, one dead broker (Exception expected)
    p.add(params(7, KafkaAssignerEvenRackAwareGoal.class, noExclusion, OptimizationFailureException.class,
                 DeterministicCluster.rackAwareUnsatisfiable(), deadBroker0, null, null));

    // ===============BrokerSetRackAwareGoal===============
    configOverrides.setProperty(AnalyzerConfig.REPLICA_TO_BROKER_SET_MAPPING_POLICY_CLASS_CONFIG,
                                ReplicaToOriginalBrokerSetMappingPolicy.class.getName());

    // With BrokerSetAwareGoal, BrokerSet satisfiable cluster, no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(0, BrokerSetAwareGoal.class, excludeTopic0, null, DeterministicCluster.brokerSetSatisfiable1(), noDeadBroker, true, false,
                 configOverrides));
    p.add(params(1, BrokerSetAwareGoal.class, excludeTopic0, null, DeterministicCluster.brokerSetSatisfiable2(), noDeadBroker, true, false,
                 configOverrides));
    // Test: Without excluded topics, broker set aware unsatisfiable cluster, no dead brokers (Exception expected)
    p.add(
        params(2, BrokerSetAwareGoal.class, noExclusion, OptimizationFailureException.class, DeterministicCluster.brokerSetUnSatisfiable1(),
               noDeadBroker, null, null, configOverrides));
    // With BrokerSetAwareGoal, BrokerSet satisfiable cluster after TOPIC1 exclusion,
    // no dead brokers (No exception, No proposal, Expected to look optimized)
    p.add(params(1, BrokerSetAwareGoal.class, excludeTopic1, null, DeterministicCluster.brokerSetSatisfiableAfterTopicExclusion(),
                 noDeadBroker, true, false, configOverrides));

    return p;
  }

  @Test
  public void test() throws Exception {
    // Before the optimization, goals are expected to be undecided wrt their provision status.
    assertEquals(ProvisionStatus.UNDECIDED, _goal.provisionResponse().status());
    if (_exceptionClass == null) {
      Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = _clusterModel.getReplicaDistribution();
      Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = _clusterModel.getLeaderDistribution();

      Set<String> excludedTopics = _optimizationOptions.excludedTopics();
      if (_expectedToOptimize) {
        assertTrue("Excluded Topics Test failed to optimize " + _goal.name() + " with excluded topics " + excludedTopics,
            _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      } else {
        assertFalse("Excluded Topics Test optimized " + _goal.name() + " with excluded topics " + excludedTopics,
                    _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      }
      // The cluster cannot be underprovisioned, because _exceptionClass was null.
      assertNotEquals(ProvisionStatus.UNDER_PROVISIONED, _goal.provisionResponse().status());
      // Generated proposals cannot have the excluded topic.
      if (!excludedTopics.isEmpty()) {
        Set<ExecutionProposal> goalProposals =
            AnalyzerUtils.getDiff(initReplicaDistribution, initLeaderDistribution, _clusterModel);
        assertEquals(_expectedToGenerateProposals, !goalProposals.isEmpty());
        for (ExecutionProposal proposal : goalProposals) {
          if (excludedTopics.contains(proposal.topic())) {
            for (ReplicaPlacementInfo r : proposal.replicasToRemove()) {
              if (_clusterModel.broker(r.brokerId()).isAlive()) {
                fail(String.format("Proposal %s contains excluded topic %s, but the broker %d is still alive.",
                                   proposal, proposal.topic(), r.brokerId()));
              }
            }
          }
        }
      }
    } else {
      _expected.expect(_exceptionClass);
      assertTrue("Excluded Topics Test failed to optimize with excluded topics.",
          _goal.optimize(_clusterModel, Collections.emptySet(), _optimizationOptions));
      assertEquals(ProvisionStatus.UNDER_PROVISIONED, _goal.provisionResponse().status());
    }
  }

  private static Object[] params(int tid,
                                 Class<? extends Goal> goalClass,
                                 Collection<String> excludedTopics,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Collection<Integer> deadBrokers,
                                 Boolean expectedToOptimize,
                                 Boolean expectedToGenerateProposals,
                                 Properties configOverrides) throws Exception {
    deadBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
    return new Object[]{tid, goal(goalClass, configOverrides), excludedTopics, exceptionClass, clusterModel, expectedToOptimize,
                        expectedToGenerateProposals};
  }

  private static Object[] params(int tid,
                                 Class<? extends Goal> goalClass,
                                 Collection<String> excludedTopics,
                                 Class<? extends Throwable> exceptionClass,
                                 ClusterModel clusterModel,
                                 Collection<Integer> deadBrokers,
                                 Boolean expectedToOptimize,
                                 Boolean expectedToGenerateProposals) throws Exception {
    return params(tid, goalClass, excludedTopics, exceptionClass, clusterModel, deadBrokers, expectedToOptimize, expectedToGenerateProposals,
                  NO_CONFIG_OVERRIDE);
  }
}
