/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;


public final class DeterministicCluster {
  public static final String T1 = "T1";
  public static final String T2 = "T2";
  public static final String TOPIC_A = "A";
  public static final String TOPIC_B = "B";
  public static final String TOPIC_C = "C";
  public static final String TOPIC_D = "D";
  public static final Map<Integer, Integer> RACK_BY_BROKER;
  public static final Map<Integer, Integer> RACK_BY_BROKER2;
  public static final Map<Integer, Integer> RACK_BY_BROKER3;
  public static final Map<Integer, Integer> RACK_BY_BROKER4;
  public static final Map<Integer, Integer> RACK_BY_BROKER5;
  public static final String CLUSTER_ID = "DETERMINISTIC_CLUSTER";
  public static final int PORT = 0;

  static {
    RACK_BY_BROKER = Map.of(0, 0, 1, 0, 2, 1);
  }

  static {
    RACK_BY_BROKER2 = Map.of(0, 0, 1, 1, 2, 1);
  }

  static {
    RACK_BY_BROKER3 = Map.of(0, 0, 1, 1, 2, 1, 3, 1);
  }

  static {
    RACK_BY_BROKER4 = Map.of(0, 0, 1, 1, 2, 2, 3, 0, 4, 1, 5, 2);
  }

  static {
    RACK_BY_BROKER5 = Map.of(0, 0, 1, 1, 2, 2, 3, 0, 4, 1, 5, 2, 6, 0);
  }

  private DeterministicCluster() {

  }

  /**
   * Two brokers on two racks, each has two disks.
   * One topic with eight partitions, each has one replica.
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel unbalanced4() {
    return createUnbalanced(Collections.singleton(T1), 8);
  }

  private static ClusterModel createUnbalanced(Set<String> topics, int numBrokers) {
    Map<Integer, Integer> rackByBrokerId = Map.of(0, 0, 1, 1);
    ClusterModel cluster = getHomogeneousCluster(rackByBrokerId, TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY);

    for (String topic : topics) {
      for (int i = 0; i < numBrokers; i++) {
        // Create topic partitions.
        TopicPartition pInfo = new TopicPartition(topic, i);

        // Create replicas for topic partitions.
        int brokerId = i > 3 ? 1 : 0;
        String logdir = i % 4 < 2 ? TestConstants.LOGDIR0 : TestConstants.LOGDIR1;
        cluster.createReplica(rackByBrokerId.get(brokerId).toString(), brokerId, pInfo, 0, true, false, logdir, false);

        // Create snapshots for replicas.
        AggregatedMetricValues aggregatedMetricValues =
            getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 5 + TestConstants.TYPICAL_CPU_CAPACITY / 50 * (i / 2.0 - 1.5),
                                      TestConstants.LARGE_BROKER_CAPACITY / 5 + TestConstants.LARGE_BROKER_CAPACITY / 50 * (i / 2.0 - 1.5),
                                      TestConstants.MEDIUM_BROKER_CAPACITY / 5 + TestConstants.MEDIUM_BROKER_CAPACITY / 50 * (i / 2.0
                                                                                                                              - 1.5),
                                      TestConstants.LARGE_BROKER_CAPACITY / 5 + TestConstants.LARGE_BROKER_CAPACITY / 50 * (i / 2.0 - 1.5));
        cluster.setReplicaLoad(rackByBrokerId.get(brokerId).toString(), brokerId, pInfo, aggregatedMetricValues,
                               Collections.singletonList(1L));
      }
    }
    return cluster;
  }

  /**
   * The same cluster as {@link #unbalanced4()} with an additional topic with fourteen partitions, each has one replica.
   * @return Cluster model for the tests.
   */
  public static ClusterModel unbalanced5() {
    Set<String> topics = Set.of(T1, T2);
    return createUnbalanced(topics, 14);
  }

  /**
   * Two racks, three brokers, two partitions, two replicas, leaders are in index-1 (not in index-0).
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel unbalanced3() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);
    TopicPartition pInfoT20 = new TopicPartition(T2, 0);

    // Create replicas for topics -- leader is not in the first position!
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT10, 0, false);
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT20, 0, false);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, 1, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT20, 1, true);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT20, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT10, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT20, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Two racks, three brokers, six partitions, one replica.
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel unbalanced2() {

    ClusterModel cluster = unbalanced();
    // Create topic partition.
    TopicPartition pInfoT30 = new TopicPartition(T1, 1);
    TopicPartition pInfoT40 = new TopicPartition(T2, 1);
    TopicPartition pInfoT50 = new TopicPartition(T1, 2);
    TopicPartition pInfoT60 = new TopicPartition(T2, 2);
    // Create replicas for topics.
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT30, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT40, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT50, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT60, 0, true);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT30, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT40, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT50, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT60, aggregatedMetricValues, Collections.singletonList(1L));
    return cluster;
  }

  /**
   * @return {@link #unbalanced()} cluster with an additional follower for an existing partition.
   */
  public static ClusterModel unbalancedWithAFollower() {
    ClusterModel unbalancedWithAFollower = unbalanced();

    TopicPartition pInfoT10 = new TopicPartition(T1, 0);
    unbalancedWithAFollower.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoT10, 1, false);

    unbalancedWithAFollower.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoT10,
                                           createLoad(TestConstants.TYPICAL_CPU_CAPACITY / 8, TestConstants.LARGE_BROKER_CAPACITY / 2, 0.0,
                                                      TestConstants.LARGE_BROKER_CAPACITY / 2), Collections.singletonList(1L));
    return unbalancedWithAFollower;
  }

  /**
   * Two racks, three brokers, two partitions, one replica.
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel unbalanced() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);
    TopicPartition pInfoT20 = new TopicPartition(T2, 0);

    // Create replicas for topics.
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT20, 0, true);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT20, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Two racks, three brokers, one partition, two replicas. Replicas reside on brokers 0 and 1.
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel rackAwareSatisfiable() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);

    // Create replicas for topic: T1.
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT10, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, getAggregatedMetricValues(40.0, 100.0, 130.0, 75.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT10, getAggregatedMetricValues(5.0, 100.0, 0.0, 75.0), windows);

    return cluster;
  }

  /**
   * Two racks, three brokers, one partition, two replicas. Contrary to {@link #rackAwareSatisfiable}, the replicas reside
   * on brokers 0 and 2 and broker to rack distribution is based on {@link #RACK_BY_BROKER2}
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel rackAwareSatisfiable2() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);

    // Create replicas for topic: T1.
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT10, 0, true);
    cluster.createReplica(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT10, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT10, getAggregatedMetricValues(40.0, 100.0, 130.0, 75.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT10, getAggregatedMetricValues(5.0, 100.0, 0.0, 75.0), windows);

    return cluster;
  }

  /**
   * Two racks, three brokers, one partition, three replicas.
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel rackAwareUnsatisfiable() {
    ClusterModel cluster = rackAwareSatisfiable();
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);

    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoT10, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoT10, getAggregatedMetricValues(60.0, 100.0, 130.0, 75.0),
                           Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Three brokers, one topic, one partition, each partition has two replicas.
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T_P0_leader</li>
   * <li>B1: T_P0_follower</li>
   * <li>B2: N/A</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel minLeaderReplicaPerBrokerUnsatisfiable() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create the only one topic partition.
    TopicPartition topicPartition = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 0);

    // Create replicas for topics.
    // T_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topicPartition, 0, true);
    // T_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topicPartition, 1, false);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, topicPartition, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(1).toString(), 1, topicPartition, aggregatedMetricValues, Collections.singletonList(1L));
    return cluster;
  }

  /**
   * Three brokers, one topic, three partitions, each partition has two replicas.
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T_P0_leader, T_P1_leader</li>
   * <li>B1: T_P2_leader, T_P0_follower</li>
   * <li>B2: T_P2_follower, T_P1_follower</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel minLeaderReplicaPerBrokerSatisfiable() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT0 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 0);
    TopicPartition pInfoT1 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 1);
    TopicPartition pInfoT2 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 2);

    // Create replicas for topics.
    // T_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT0, 0, true);
    // T_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT1, 0, true);

    // T_P2_leader
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT2, 0, true);
    // T_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT0, 1, false);

    // T_P2_follower
    cluster.createReplica(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT2, 1, false);
    // T_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT1, 1, false);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT0, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT1, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT0, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT2, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT2, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Three brokers, one topic, three partitions, each partition has 2 replicas.
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T_P0_leader, T_P1_leader, T_P2_leader</li>
   * <li>B1: T_P1_follower</li>
   * <li>B2: T_P0_follower, T_P2_follower</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel minLeaderReplicaPerBrokerSatisfiable2() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT0 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 0);
    TopicPartition pInfoT1 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 1);
    TopicPartition pInfoT2 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 2);

    // Create replicas for topics.
    // T_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT0, 0, true);
    // T_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT1, 0, true);
    // T_P2_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT2, 0, true);

    // T_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT1, 1, false);

    // T_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT0, 1, false);
    // T_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT2, 1, false);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT0, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT0, aggregatedMetricValues, Collections.singletonList(1L));

    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT1, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT1, aggregatedMetricValues, Collections.singletonList(1L));

    cluster.setReplicaLoad(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT2, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT2, aggregatedMetricValues, Collections.singletonList(1L));
    return cluster;
  }

  /**
   * Three brokers, two topics, each topic has three partitions, each partition has 2 replicas.
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader, T0_P1_leader, T0_P2_leader, T1_P0_leader, T1_P1_leader, T1_P2_leader</li>
   * <li>B1: T0_P0_follower, T0_P1_follower, T0_P2_follower, T1_P0_follower, T1_P1_follower, T1_P2_follower</li>
   * <li>B2: N/A</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel minLeaderReplicaPerBrokerSatisfiable4() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);

    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition0, 0, true);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition1, 0, true);
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition2, 0, true);
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition0, 0, true);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition1, 0, true);
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition2, 0, true);

    // T0_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition0, 1, false);
    // T0_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition1, 1, false);
    // T0_P2_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition2, 1, false);
    // T1_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition0, 1, false);
    // T1_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition1, 1, false);
    // T1_P2_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition2, 1, false);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    for (int bId : Arrays.asList(0, 1)) {
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition0, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition1, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition2, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition0, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition1, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition2, aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    return cluster;
  }

  /**
   * Three brokers, two topics, each topic has four partitions, each partition has 2 replicas.
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader, T0_P1_leader, T0_P2_leader, T0_P3_leader, T1_P0_leader, T1_P1_leader, T1_P2_leader, T1_P3_leader</li>
   * <li>B1: T0_P0_follower, T0_P1_follower, T0_P2_follower, T0_P3_follower, T1_P0_follower, T1_P1_follower, T1_P2_follower, T1_P3_follower</li>
   * <li>B2: N/A</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel minLeaderReplicaPerBrokerSatisfiable5() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);
    TopicPartition topic1Partition3 = new TopicPartition(TOPIC1, 3);

    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition0, 0, true);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition1, 0, true);
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition2, 0, true);
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic0Partition3, 0, true);
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition0, 0, true);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition1, 0, true);
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition2, 0, true);
    // T1_P3_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, topic1Partition3, 0, true);

    // T0_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition0, 1, false);
    // T0_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition1, 1, false);
    // T0_P2_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition2, 1, false);
    // T0_P3_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic0Partition3, 1, false);
    // T1_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition0, 1, false);
    // T1_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition1, 1, false);
    // T1_P2_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition2, 1, false);
    // T1_P3_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, topic1Partition3, 1, false);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    for (int bId : Arrays.asList(0, 1)) {
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition0, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition1, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition2, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic0Partition3, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition0, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition1, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition2, aggregatedMetricValues,
                             Collections.singletonList(1L));
      cluster.setReplicaLoad(RACK_BY_BROKER2.get(bId).toString(), bId, topic1Partition3, aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    return cluster;
  }

  /**
   * Four brokers, one topic, 16 partitions, each partition has 2 replicas.
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: N/A </li>
   * <li>B1: T_P0_l, T_P1_l, T_P2_l, T_P3_l, followers of all leaders on this broker</li>
   * <li>B2: T_P4_l, T_P5_l, T_P6_l, T_P7_l, T_P8_l, T_P9_l, followers of all leaders on this broker</li>
   * <li>B3: T_P10_l, T_P11_l, T_P12_l, T_P13_l, T_P14_l, T_P15_l, followers of all leaders on this broker</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel minLeaderReplicaPerBrokerSatisfiable3() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER3, TestConstants.BROKER_CAPACITY, null);
    final int totalTopicPartitionCount = 16;

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create topic partition.
    List<TopicPartition> topicPartitions = new ArrayList<>(totalTopicPartitionCount);
    for (int i = 0; i < totalTopicPartitionCount; i++) {
      topicPartitions.add(new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, i));
    }

    // Add all leader replicas to broker 1
    for (int i = 0; i < 4; i++) {
      // T_P0_l, T_P1_l, T_P2_l, T_P3_l
      cluster.createReplica(RACK_BY_BROKER3.get(1).toString(), 1, topicPartitions.get(i), 0, true);
      cluster.setReplicaLoad(RACK_BY_BROKER3.get(1).toString(), 1, topicPartitions.get(i), aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    // Add all follower replicas to broker 1
    for (int i = 4; i < 10; i++) {
      // T_P4_l, T_P5_l, T_P6_l, T_P7_l, T_P8_l, T_P9_l
      cluster.createReplica(RACK_BY_BROKER3.get(1).toString(), 1, topicPartitions.get(i), 0, false);
      cluster.setReplicaLoad(RACK_BY_BROKER3.get(1).toString(), 1, topicPartitions.get(i), aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    // Add all leader replicas to broker 2
    for (int i = 4; i < 10; i++) {
      // T_P4_l, T_P5_l, T_P6_l, T_P7_l, T_P8_l, T_P9_l
      cluster.createReplica(RACK_BY_BROKER3.get(2).toString(), 2, topicPartitions.get(i), 0, true);
      cluster.setReplicaLoad(RACK_BY_BROKER3.get(2).toString(), 2, topicPartitions.get(i), aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    // Add all follower replicas to broker 2
    for (int i = 10; i < 16; i++) {
      // T_P10_f, T_P11_f, T_P12_f, T_P13_f, T_P14_f, T_P15_f
      cluster.createReplica(RACK_BY_BROKER3.get(2).toString(), 2, topicPartitions.get(i), 0, false);
      cluster.setReplicaLoad(RACK_BY_BROKER3.get(2).toString(), 2, topicPartitions.get(i), aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    // Add all leader replicas to broker 3
    for (int i = 10; i < 16; i++) {
      // T_P10_l, T_P11_l, T_P12_l, T_P13_l, T_P14_l, T_P15_l
      cluster.createReplica(RACK_BY_BROKER3.get(3).toString(), 3, topicPartitions.get(i), 0, true);
      cluster.setReplicaLoad(RACK_BY_BROKER3.get(3).toString(), 3, topicPartitions.get(i), aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    // Add all follower replicas to broker 3
    for (int i = 0; i < 4; i++) {
      // T_P0_f, T_P1_f, T_P2_f, T_P3_f
      cluster.createReplica(RACK_BY_BROKER3.get(3).toString(), 3, topicPartitions.get(i), 0, false);
      cluster.setReplicaLoad(RACK_BY_BROKER3.get(3).toString(), 3, topicPartitions.get(i), aggregatedMetricValues,
                             Collections.singletonList(1L));
    }
    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 2 topics, each topic has four partitions, each partition has 3 replicas.
   * Topics are contained within the boundary fo BrokerSet
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader,     T0_P1_follower_1,  T0_P2_follower_2, T0_P3_leader</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader,     T0_P2_follower_1, T0_P3_follower_1</li>
   * <li>B2: T0_P0_follower_2, T0_P1_follower_2, T0_P2_leader,    T0_P3_follower_2</li>
   * <li>B3: T1_P0_leader,     T1_P1_follower_1,  T1_P2_follower_2, T1_P3_leader</li>
   * <li>B4: T1_P0_follower_1, T1_P1_leader,     T1_P2_follower_1, T1_P3_follower_1</li>
   * <li>B5: T1_P0_follower_2, T1_P1_follower_2, T1_P2_leader,    T1_P3_follower_2</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable1() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);
    TopicPartition topic1Partition3 = new TopicPartition(TOPIC1, 3);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 1 topic that has four partitions, each partition has 3 replicas.
   * Topic is contained within the boundary fo BrokerSet
   *
   * One BrokerSet has topic and another BrokerSet is empty
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader,     T0_P1_follower_1,  T0_P2_follower_2, T0_P3_leader</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader,     T0_P2_follower_1, T0_P3_follower_1</li>
   * <li>B2: T0_P0_follower_2, T0_P1_follower_2, T0_P2_leader,    T0_P3_follower_2</li>
   * <li>B3: </li>
   * <li>B4: </li>
   * <li>B5: </li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable2() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 0 topics
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: </li>
   * <li>B1: </li>
   * <li>B2: </li>
   * <li>B3: </li>
   * <li>B4: </li>
   * <li>B5: </li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable3() {
    return getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 2 topics, each topic has four partitions, each partition has 3 replicas.
   * Topics are contained within the boundary fo BrokerSet
   * <p>
   * The CPU Usage is unbalanced and can be solved with LEADER movement
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader,     T0_P1_follower_1,  T0_P2_follower_2, T0_P3_leader</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader,     T0_P2_follower_1, T0_P3_follower_1</li>
   * <li>B2: T0_P0_follower_2, T0_P1_follower_2, T0_P2_leader,    T0_P3_follower_2</li>
   * <li>B3: T1_P0_leader,     T1_P1_follower_1,  T1_P2_follower_2, T1_P3_leader</li>
   * <li>B4: T1_P0_follower_1, T1_P1_leader,     T1_P2_follower_1, T1_P3_follower_1</li>
   * <li>B5: T1_P0_follower_2, T1_P1_follower_2, T1_P2_leader,    T1_P3_follower_2</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable4() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);
    TopicPartition topic1Partition3 = new TopicPartition(TOPIC1, 3);

    // Create replicas for topics.
    List<Long> windows = Collections.singletonList(1L);
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, createLoad(40.0, 1000.0, 1300.0, 75.0), windows);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, createLoad(40.0, 1000.0, 1300.0, 75.0), windows);
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, createLoad(30.0, 1000.0, 1300.0, 75.0), windows);
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, createLoad(30.0, 1000.0, 1300.0, 75.0), windows);
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);

    // T0_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);

    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 2 topics, each topic has four partitions, each partition has 2 replicas.
   *
   * Since Capacity goals do not allow multiple replicas of a partition on the same broker and we have 3 brokers in a BrokerSet, the RF is 2
   *
   * Topics are contained within the boundary fo BrokerSet
   *
   * The Disk Usage is unbalanced and can be solved with REPLICA movement
   *
   * Disk usage threshold Breach is at 300000 * 0.8 = 240000
   * B0 violates Disk usage in this case and should be fixed by moving replica to B1
   *
   * <p>
   * <h3>Replica Distribution with High Disk Usage</h3>
   * <li>B0: T0_P0_leader(150000.0), T0_P1_follower_1(1.0), T0_P3_leader(100000.0)</li>
   * <li>B1: T0_P1_leader(100.0),     T0_P2_follower_1(1.0), T0_P3_follower_1(1.0)</li>
   * <li>B2: T0_P0_follower_1(1.0), T0_P2_leader(150000.0)</li>
   * <li>B3: T1_P0_leader(1.0),     T1_P1_follower_1(1.0), T1_P3_leader(1.0)</li>
   * <li>B4: T1_P0_follower_1(1.0), T1_P1_leader(1.0), T1_P2_follower_1(1.0), T1_P3_follower_1(1.0)</li>
   * <li>B5: T1_P2_leader(1.0)</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable5() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);
    TopicPartition topic1Partition3 = new TopicPartition(TOPIC1, 3);

    // Create replicas for topics.
    List<Long> windows = Collections.singletonList(1L);
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, createLoad(20.0, 1000.0, 1300.0, 150000.0), windows);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, createLoad(20.0, 1000.0, 1300.0, 100.0), windows);
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, createLoad(30.0, 1000.0, 1300.0, 150000.0), windows);
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, createLoad(30.0, 1000.0, 1300.0, 100000.0), windows);
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T0_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, createLoad(1.0, 1.0, 1.0, 1.0), windows);
    // T1_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, createLoad(1.0, 1.0, 1.0, 1.0), windows);

    return cluster;
  }

  /**
   * 4 brokers in Cluster Model
   * 6 Brokers in BrokerSetResolver
   * <p>
   * No Topics
   * <p>
   * This would be Satisfiable if a broker is missing rack info, the Broker object is null and ClusterModel.brokers() does not count
   * the broker in.
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: </li>
   * <li>B1: </li>
   * <li>B2: </li>
   * <li>B3: </li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable6() {
    return getHomogeneousCluster(RACK_BY_BROKER3, TestConstants.BROKER_CAPACITY, null);
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 2 topics, each topic has 2 partitions, each partition has 2 replicas.
   * Topics are contained within the boundary of a brokerset.
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader, T0_P1_follower_1</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader</li>
   * <li>B2: </li>
   * <li>B3: T1_P0_leader, T1_P1_follower_1</li>
   * <li>B4: T1_P0_follower_1, T1_P1_leader</li>
   * <li>B5: </li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable7() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 8, TestConstants.LARGE_BROKER_CAPACITY / 8,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 8, TestConstants.LARGE_BROKER_CAPACITY / 8);

    List<Long> windows = Collections.singletonList(1L);
    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, aggregatedMetricValues, windows);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, aggregatedMetricValues, windows);

    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, aggregatedMetricValues, windows);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, aggregatedMetricValues, windows);

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, windows);
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, windows);

    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, aggregatedMetricValues, windows);
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, aggregatedMetricValues, windows);

    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers in BrokerSet store
   * Cluster model has extra Broker B6
   * 2 topics, each topic has 2 partitions, each partition has 2 replicas.
   * Topics are not contained within the boundary of a brokerset.
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader, T0_P1_follower_1</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader</li>
   * <li>B2: </li>
   * <li>B3: T1_P0_leader, T1_P1_follower_1</li>
   * <li>B4: T1_P0_follower_1, T1_P1_leader</li>
   * <li>B5: </li>
   * <li>B6: </li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiable8() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER5, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 8,
                                  TestConstants.LARGE_BROKER_CAPACITY / 8,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 8,
                                  TestConstants.LARGE_BROKER_CAPACITY / 8);

    List<Long> windows = Collections.singletonList(1L);
    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, aggregatedMetricValues, windows);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, aggregatedMetricValues, windows);

    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, aggregatedMetricValues, windows);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, aggregatedMetricValues, windows);

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, windows);
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, windows);

    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, aggregatedMetricValues, windows);
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, aggregatedMetricValues, windows);

    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 2 topics, each topic has four partitions, each partition has 3 replicas.
   * Topics are not contained within the boundary of a brokerset. Unless excluded, the prerequisite for BrokerSetAwareGoal is
   * that topics are within a brokerSet.
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T1_P0_leader,     T0_P1_follower_1,  T0_P2_follower_2, T0_P3_leader</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader,     T0_P2_follower_1, T0_P3_follower_1</li>
   * <li>B2: T0_P0_follower_2, T0_P1_follower_2, T0_P2_leader,    T0_P3_follower_2</li>
   * <li>B3: T0_P0_leader,     T1_P1_follower_1,  T1_P2_follower_2, T1_P3_leader</li>
   * <li>B4: T1_P0_follower_1, T1_P1_leader,     T1_P2_follower_1, T1_P3_follower_1</li>
   * <li>B5: T1_P0_follower_2, T1_P1_follower_2, T1_P2_leader,    T1_P3_follower_2</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetUnSatisfiable1() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);
    TopicPartition topic1Partition3 = new TopicPartition(TOPIC1, 3);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 10, TestConstants.LARGE_BROKER_CAPACITY / 10,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 10, TestConstants.LARGE_BROKER_CAPACITY / 10);

    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * 7 brokers in Cluster Model
   * 6 Brokers in BrokerSetResolver
   * <p>
   * 2 Topics
   * <p>
   * This would be unsatisfiable since all brokers in cluster model are different from brokers in BrokerSetResolver
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader, T0_P1_follower_1</li>
   * <li>B1: T0_P0_follower_1</li>
   * <li>B2: T0_P1_leader</li>
   * <li>B3: T1_P0_leader</li>
   * <li>B4: T1_P0_follower_1</li>
   * <li>B5: T1_P1_leader</li>
   * <li>B6: T1_P1_follower_1</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetUnSatisfiable3() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER5, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 8, TestConstants.LARGE_BROKER_CAPACITY / 8,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 8, TestConstants.LARGE_BROKER_CAPACITY / 8);

    List<Long> windows = Collections.singletonList(1L);
    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER5.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(0).toString(), 0, topic0Partition0, aggregatedMetricValues, windows);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER5.get(2).toString(), 2, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(2).toString(), 2, topic0Partition1, aggregatedMetricValues, windows);

    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER5.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(3).toString(), 3, topic1Partition0, aggregatedMetricValues, windows);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER5.get(5).toString(), 5, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(5).toString(), 5, topic1Partition1, aggregatedMetricValues, windows);

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER5.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, windows);
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER5.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, windows);

    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER5.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(4).toString(), 4, topic1Partition0, aggregatedMetricValues, windows);
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER5.get(6).toString(), 6, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER5.get(6).toString(), 6, topic1Partition1, aggregatedMetricValues, windows);

    return cluster;
  }

  /**
   * 3 Brokers in 2 BrokerSets
   * <p>
   * 2 Topics
   * <p>
   * This would be unsatisfiable since the Disk Distribution would try to swap T0_P0_leader and T1_P0_leader but BrokerSetAwareGoal
   * will REPLICA_REJECT it
   *
   * <p>
   * <h3>Replica Distribution and Disk Usage Distribution</h3>
   * <li>B0: T0_P0_leader(90000.0), T0_P1_follower_1(60000.0)</li>
   * <li>B1: T0_P0_follower_1(100000.0)</li>
   * <li>B2: T0_P1_leader(100000.0)</li>
   * <li>B3: T1_P0_leader(10000.0), T1_P1_leader(40000.0)</li>
   * <li>B4: T1_P0_follower_1(100000.0)</li>
   * <li>B5: T1_P1_follower_1(100000.0)</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetUnSatisfiable4() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);

    List<Long> windows = Collections.singletonList(1L);
    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, createLoad(1.0, 1.0, 1.0, 90000.0), windows);
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, createLoad(1.0, 1.0, 1.0, 100000.0), windows);

    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition0, createLoad(1.0, 1.0, 1.0, 10000.0), windows);
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, createLoad(1.0, 1.0, 1.0, 40000.0), windows);

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, createLoad(1.0, 1.0, 1.0, 100000.0), windows);
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, createLoad(1.0, 1.0, 1.0, 60000.0), windows);

    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, createLoad(1.0, 1.0, 1.0, 100000.0), windows);
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, createLoad(1.0, 1.0, 1.0, 100000.0), windows);

    return cluster;
  }

  /**
   * 6 brokers, 2 BrokerSets, each BrokerSet has 3 Brokers
   * 2 topics, each topic has four partitions, each partition has 3 replicas.
   * Topic TOPIC0 is contained within the boundary of a brokerset.
   * Topic TOPIC1 is not contained within the boundary of a brokerSet
   *
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T0_P0_leader, T1_P0_leader, T0_P1_follower_1,  T0_P2_follower_2, T0_P3_leader</li>
   * <li>B1: T0_P0_follower_1, T0_P1_leader,     T0_P2_follower_1, T0_P3_follower_1</li>
   * <li>B2: T0_P0_follower_2, T0_P1_follower_2, T0_P2_leader,    T0_P3_follower_2</li>
   * <li>B3: T1_P1_follower_1,  T1_P2_follower_2, T1_P3_leader</li>
   * <li>B4: T1_P0_follower_1, T1_P1_leader,     T1_P2_follower_1, T1_P3_follower_1</li>
   * <li>B5: T1_P0_follower_2, T1_P1_follower_2, T1_P2_leader,    T1_P3_follower_2</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel brokerSetSatisfiableAfterTopicExclusion() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER4, TestConstants.BROKER_CAPACITY, null);

    // Create topic partitions.
    TopicPartition topic0Partition0 = new TopicPartition(TOPIC0, 0);
    TopicPartition topic0Partition1 = new TopicPartition(TOPIC0, 1);
    TopicPartition topic0Partition2 = new TopicPartition(TOPIC0, 2);
    TopicPartition topic0Partition3 = new TopicPartition(TOPIC0, 3);

    TopicPartition topic1Partition0 = new TopicPartition(TOPIC1, 0);
    TopicPartition topic1Partition1 = new TopicPartition(TOPIC1, 1);
    TopicPartition topic1Partition2 = new TopicPartition(TOPIC1, 2);
    TopicPartition topic1Partition3 = new TopicPartition(TOPIC1, 3);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create replicas for topics.
    // T0_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_leader
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic1Partition0, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_leader
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_leader
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_leader
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, 0, true);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(1).toString(), 1, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_follower_1
    cluster.createReplica(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, 1, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(4).toString(), 4, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    // T0_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(0).toString(), 0, topic0Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T0_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(2).toString(), 2, topic0Partition3, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P0_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition0, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P1_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition1, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P2_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(3).toString(), 3, topic1Partition2, aggregatedMetricValues, Collections.singletonList(1L));
    // T1_P3_follower_2
    cluster.createReplica(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, 2, false);
    cluster.setReplicaLoad(RACK_BY_BROKER4.get(5).toString(), 5, topic1Partition3, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Three brokers, one topic, two partitions, each partition has 2 replicas. There are not enough leader replicas
   * to distribute over all brokers (in this case, 2 leader replicas and 3 brokers)
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T_P0_leader, T_P1_leader</li>
   * <li>B1: T_P1_follower</li>
   * <li>B2: T_P0_follower</li>
   * </p>
   *
   * @return Cluster model for the tests.
   */
  public static ClusterModel leaderReplicaPerBrokerUnsatisfiable() {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER2, TestConstants.BROKER_CAPACITY, null);

    // Create topic partition.
    TopicPartition pInfoT0 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 0);
    TopicPartition pInfoT1 = new TopicPartition(TOPIC_MUST_HAVE_LEADER_REPLICAS_ON_BROKERS, 1);

    // Create replicas for topics.
    // T_P0_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT0, 0, true);
    // T_P1_leader
    cluster.createReplica(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT1, 0, true);
    // T_P1_follower
    cluster.createReplica(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT1, 1, false);
    // T_P0_follower
    cluster.createReplica(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT0, 1, false);

    AggregatedMetricValues aggregatedMetricValues =
        getAggregatedMetricValues(TestConstants.TYPICAL_CPU_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2,
                                  TestConstants.MEDIUM_BROKER_CAPACITY / 2, TestConstants.LARGE_BROKER_CAPACITY / 2);

    // Create snapshots and push them to the cluster.
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT0, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(2).toString(), 2, pInfoT0, aggregatedMetricValues, Collections.singletonList(1L));

    cluster.setReplicaLoad(RACK_BY_BROKER2.get(0).toString(), 0, pInfoT1, aggregatedMetricValues, Collections.singletonList(1L));
    cluster.setReplicaLoad(RACK_BY_BROKER2.get(1).toString(), 1, pInfoT1, aggregatedMetricValues, Collections.singletonList(1L));

    return cluster;
  }

  /**
   * Generates a small scale cluster.
   * <p>
   * <li>Number of Partitions: 5.</li>
   * <li>Topics: T1, T2</li>
   * <li>Replication factor/Topic: T1: 2, T2: 2</li>
   * <li>Partitions/Topic: T1: 2, T2: 3</li>
   * <p>
   * <h3>Replica Distribution</h3>
   * <li>B0: T1_P1_leader, T1_P2_follower, T2_P3_leader, T2_P2_leader</li>
   * <li>B1: T1_P2_leader, T2_P1_leader, T2_P3_follower</li>
   * <li>B2: T2_P2_follower, T1_P1_follower, T2_P1_follower</li>
   * <p>
   * <h3>Load on Partitions</h3>
   * <p>
   * <ul>
   * <li>T1_P1_leader:</li>
   * <ul>
   * <li>CPU: 20.0</li>
   * <li>DISK: 75.0</li>
   * <li>INBOUND NW: 100.0</li>
   * <li>OUTBOUND NW: 130.0</li>
   * </ul>
   * <li>T1_P1_follower:</li>
   * <ul>
   * <li>CPU: 5.0</li>
   * <li>DISK: 75.0</li>
   * <li>INBOUND NW: 100.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T1_P2_leader:</li>
   * <ul>
   * <li>CPU: 15.0</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 90.0</li>
   * <li>OUTBOUND NW: 110.0</li>
   * </ul>
   * <li>T1_P2_follower:</li>
   * <ul>
   * <li>CPU: 4.5</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 90.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T2_P1_leader:</li>
   * <ul>
   * <li>CPU: 5.0</li>
   * <li>DISK: 5.0</li>
   * <li>INBOUND NW: 5.0</li>
   * <li>OUTBOUND NW: 6.0</li>
   * </ul>
   * <li>T2_P1_follower:</li>
   * <ul>
   * <li>CPU: 4.0</li>
   * <li>DISK: 5.0</li>
   * <li>INBOUND NW: 5.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T2_P2_leader:</li>
   * <ul>
   * <li>CPU: 25.0</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 25.0</li>
   * <li>OUTBOUND NW: 45.0</li>
   * </ul>
   * <li>T2_P2_follower:</li>
   * <ul>
   * <li>CPU: 10.0</li>
   * <li>DISK: 55.0</li>
   * <li>INBOUND NW: 25.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * <li>T2_P3_leader:</li>
   * <ul>
   * <li>CPU: 20.0</li>
   * <li>DISK: 95.0</li>
   * <li>INBOUND NW: 45.0</li>
   * <li>OUTBOUND NW: 120.0</li>
   * </ul>
   * <li>T2_P3_follower:</li>
   * <ul>
   * <li>CPU: 8.0</li>
   * <li>DISK: 95.0</li>
   * <li>INBOUND NW: 45.0</li>
   * <li>OUTBOUND NW: 0.0</li>
   * </ul>
   * </ul>
   *
   * @param brokerCapacity Broker capacity.
   * @return Small scale cluster.
   */
  public static ClusterModel smallClusterModel(Map<Resource, Double> brokerCapacity) {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER, brokerCapacity, null);

    // Create topic partition.
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);
    TopicPartition pInfoT11 = new TopicPartition(T1, 1);
    TopicPartition pInfoT20 = new TopicPartition(T2, 0);
    TopicPartition pInfoT21 = new TopicPartition(T2, 1);
    TopicPartition pInfoT22 = new TopicPartition(T2, 2);
    // Create replicas for topic: T1.
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoT10, 1, false);
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT11, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT11, 1, false);
    // Create replicas for topic: T2.
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT20, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoT20, 1, false);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT21, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoT21, 1, false);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoT22, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoT22, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT10, createLoad(20.0, 100.0, 130.0, 75.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoT10, createLoad(5.0, 100.0, 0.0, 75.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT11, createLoad(15.0, 90.0, 110.0, 55.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT11, createLoad(4.5, 90.0, 0.0, 55.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT20, createLoad(5.0, 5.0, 6.0, 5.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoT20, createLoad(4.0, 5.0, 0.0, 5.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT21, createLoad(25.0, 25.0, 45.0, 55.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoT21, createLoad(10.5, 25.0, 0.0, 55.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoT22, createLoad(20.0, 45.0, 120.0, 95.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoT22, createLoad(8.0, 45.0, 0.0, 95.0), windows);

    return cluster;
  }

  /**
   * Generates a test cluster containing a dead broker.
   * <p>
   *   <li>Number of Partitions: 8.</li>
   *   <li>Topics: T1, T2</li>
   *   <li>Replication factor/Topic: T1:2, T2:2</li>
   *   <li>Partitions/Topic: T1:4, T2:4</li>
   * </p>
   *
   * @param brokerCapacity Broker capacity.
   * @return A test cluster containing a dead broker.
   */
  public static ClusterModel deadBroker(Map<Resource, Double> brokerCapacity) {
    Map<Integer, Integer> racksByBrokerIds = new HashMap<>();
    racksByBrokerIds.put(0, 0);
    racksByBrokerIds.put(1, 1);
    racksByBrokerIds.put(2, 2);
    racksByBrokerIds.put(3, 3);
    racksByBrokerIds.put(4, 4);
    ClusterModel cluster = getHomogeneousCluster(racksByBrokerIds, brokerCapacity, null);

    // Create topic partitions.
    TopicPartition pInfoT10 = new TopicPartition(T1, 0);
    TopicPartition pInfoT11 = new TopicPartition(T1, 1);
    TopicPartition pInfoT12 = new TopicPartition(T1, 2);
    TopicPartition pInfoT13 = new TopicPartition(T1, 3);
    TopicPartition pInfoT20 = new TopicPartition(T2, 0);
    TopicPartition pInfoT21 = new TopicPartition(T2, 1);
    TopicPartition pInfoT22 = new TopicPartition(T2, 2);
    TopicPartition pInfoT23 = new TopicPartition(T2, 3);

    // Create replicas for topic: T1.
    cluster.createReplica(racksByBrokerIds.get(1).toString(), 1, pInfoT10, 0, true);
    cluster.createReplica(racksByBrokerIds.get(2).toString(), 2, pInfoT10, 1, false);
    cluster.createReplica(racksByBrokerIds.get(1).toString(), 1, pInfoT11, 0, true);
    cluster.createReplica(racksByBrokerIds.get(3).toString(), 3, pInfoT11, 1, false);
    cluster.createReplica(racksByBrokerIds.get(1).toString(), 1, pInfoT12, 0, true);
    cluster.createReplica(racksByBrokerIds.get(4).toString(), 4, pInfoT12, 1, false);
    cluster.createReplica(racksByBrokerIds.get(2).toString(), 2, pInfoT13, 0, true);
    cluster.createReplica(racksByBrokerIds.get(0).toString(), 0, pInfoT13, 1, false);
    // Create replicas for topic: T2.
    cluster.createReplica(racksByBrokerIds.get(1).toString(), 1, pInfoT20, 0, true);
    cluster.createReplica(racksByBrokerIds.get(2).toString(), 2, pInfoT20, 1, false);
    cluster.createReplica(racksByBrokerIds.get(1).toString(), 1, pInfoT21, 0, true);
    cluster.createReplica(racksByBrokerIds.get(3).toString(), 3, pInfoT21, 1, false);
    cluster.createReplica(racksByBrokerIds.get(1).toString(), 1, pInfoT22, 0, true);
    cluster.createReplica(racksByBrokerIds.get(4).toString(), 4, pInfoT22, 1, false);
    cluster.createReplica(racksByBrokerIds.get(3).toString(), 3, pInfoT23, 0, true);
    cluster.createReplica(racksByBrokerIds.get(0).toString(), 0, pInfoT23, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(racksByBrokerIds.get(1).toString(), 1, pInfoT10, createLoad(20.0, 100.0, 200.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(2).toString(), 2, pInfoT10, createLoad(15.0, 100.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(1).toString(), 1, pInfoT11, createLoad(20.0, 90.0, 180.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(3).toString(), 3, pInfoT11, createLoad(15.0, 90.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(1).toString(), 1, pInfoT12, createLoad(15.0, 75.0, 150.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(4).toString(), 4, pInfoT12, createLoad(12.0, 75.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(2).toString(), 2, pInfoT13, createLoad(15.0, 60.0, 120.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(0).toString(), 0, pInfoT13, createLoad(12.5, 60.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(1).toString(), 1, pInfoT20, createLoad(18.0, 100.0, 200.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(2).toString(), 2, pInfoT20, createLoad(14.0, 100.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(1).toString(), 1, pInfoT21, createLoad(18.0, 90.0, 180.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(3).toString(), 3, pInfoT21, createLoad(14.0, 90.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(1).toString(), 1, pInfoT22, createLoad(12.0, 75.0, 150.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(4).toString(), 4, pInfoT22, createLoad(10.0, 75.0, 0.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(3).toString(), 3, pInfoT23, createLoad(12.0, 60.0, 120.0, 100.0), windows);
    cluster.setReplicaLoad(racksByBrokerIds.get(0).toString(), 0, pInfoT23, createLoad(10.5, 60.0, 0.0, 100.0), windows);

    // Make broker 0 dead.
    cluster.setBrokerState(0, Broker.State.DEAD);
    return cluster;
  }

  /**
   * Generates a medium test cluster.
   * <p>
   * <li>Number of Partitions: 12.</li>
   * <li>Topics: A, B, C, D</li>
   * <li>Replication factor/Topic: A:2, B:2, C:2, D:2</li>
   * <li>Partitions/Topic: A: 3, B:1, C:1, D:1</li>
   *
   * @param brokerCapacity Broker capacity.
   * @return A medium test cluster.
   */
  public static ClusterModel mediumClusterModel(Map<Resource, Double> brokerCapacity) {
    ClusterModel cluster = getHomogeneousCluster(RACK_BY_BROKER, brokerCapacity, null);
    // Create topic partition.
    TopicPartition pInfoA0 = new TopicPartition(TOPIC_A, 0);
    TopicPartition pInfoA1 = new TopicPartition(TOPIC_A, 1);
    TopicPartition pInfoA2 = new TopicPartition(TOPIC_A, 2);
    TopicPartition pInfoB0 = new TopicPartition(TOPIC_B, 0);
    TopicPartition pInfoC0 = new TopicPartition(TOPIC_C, 0);
    TopicPartition pInfoD0 = new TopicPartition(TOPIC_D, 0);

    // Create replicas for TopicA.
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoA0, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoA0, 1, false);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoA1, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoA1, 1, false);
    cluster.createReplica(RACK_BY_BROKER.get(0).toString(), 0, pInfoA2, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoA2, 1, false);
    // Create replicas for TopicB.
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoB0, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoB0, 1, false);
    // Create replicas for TopicC.
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoC0, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoC0, 1, false);
    // Create replicas for TopicD.
    cluster.createReplica(RACK_BY_BROKER.get(1).toString(), 1, pInfoD0, 0, true);
    cluster.createReplica(RACK_BY_BROKER.get(2).toString(), 2, pInfoD0, 1, false);

    // Create snapshots and push them to the cluster.
    List<Long> windows = Collections.singletonList(1L);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoA0, createLoad(5.0, 5.0, 0.0, 4.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoA1, createLoad(5.0, 3.0, 10.0, 8.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(0).toString(), 0, pInfoA2, createLoad(5.0, 2.0, 10.0, 6.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoB0, createLoad(5.0, 4.0, 10.0, 7.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoC0, createLoad(5.0, 6.0, 0.0, 4.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoD0, createLoad(5.0, 5.0, 10.0, 6.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(1).toString(), 1, pInfoA0, createLoad(5.0, 4.0, 10.0, 10.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoB0, createLoad(2.0, 2.0, 0.0, 5.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoC0, createLoad(1.0, 8.0, 10.0, 4.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoD0, createLoad(2.0, 8.0, 0.0, 7.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoA1, createLoad(3.0, 4.0, 0.0, 6.0), windows);
    cluster.setReplicaLoad(RACK_BY_BROKER.get(2).toString(), 2, pInfoA2, createLoad(4.0, 5.0, 0.0, 3.0), windows);

    return cluster;
  }

  private static AggregatedMetricValues createLoad(double cpu, double networkIn, double networkOut, double disk) {
    return getAggregatedMetricValues(cpu, networkIn, networkOut, disk);
  }

  /**
   * Creates a deterministic cluster with the given number of racks and the broker distribution.
   *
   * @param rackByBroker Racks by broker ids.
   * @param brokerCapacity Alive broker capacity.
   * @param diskCapacityByLogDir Disk capacity for each broker.
   * @return Cluster with the specified number of racks and broker distribution.
   */
  public static ClusterModel getHomogeneousCluster(Map<Integer, Integer> rackByBroker, Map<Resource, Double> brokerCapacity,
                                                   Map<String, Double> diskCapacityByLogDir) {
    // Sanity checks.
    if (rackByBroker.size() <= 0 || brokerCapacity.get(Resource.CPU) < 0 || brokerCapacity.get(Resource.DISK) < 0
        || brokerCapacity.get(Resource.NW_IN) < 0 || brokerCapacity.get(Resource.NW_OUT) < 0) {
      throw new IllegalArgumentException("Deterministic cluster generation failed due to bad input.");
    }

    // Create cluster.
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    // Create racks and add them to cluster.
    Set<Integer> racks = new HashSet<>();
    for (int rackId : rackByBroker.values()) {
      if (racks.add(rackId)) {
        cluster.createRack(Integer.toString(rackId));
      }
    }

    BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(brokerCapacity, diskCapacityByLogDir);
    // Create brokers and assign a broker to each rack.
    rackByBroker.forEach((key, value) -> cluster.createBroker(value.toString(), Integer.toString(key), key, commonBrokerCapacityInfo,
                                                              diskCapacityByLogDir != null));
    return cluster;
  }

  /**
   * Generate the cluster metadata from given cluster model.
   * @param clusterModel The cluster model.
   * @return The cluster metadata.
   */
  public static Cluster generateClusterFromClusterModel(ClusterModel clusterModel) {
    Map<Integer, Node> nodes = new HashMap<>();
    clusterModel.brokers().forEach(b -> nodes.put(b.id(), new Node(b.id(), b.host().name(), PORT, b.rack().id())));
    List<PartitionInfo> partitions = new ArrayList<>();
    for (List<Partition> pList : clusterModel.getPartitionsByTopic().values()) {
      for (Partition p : pList) {
        Node[] replicas = new Node[p.replicas().size()];
        for (int i = 0; i < p.replicas().size(); i++) {
          replicas[i] = nodes.get(p.replicas().get(i).broker().id());
        }
        Node[] inSyncReplicas = new Node[p.onlineFollowers().size() + 1];
        inSyncReplicas[0] = nodes.get(p.leader().broker().id());
        for (int i = 0; i < p.onlineFollowers().size(); i++) {
          replicas[i + 1] = nodes.get(p.onlineFollowers().get(i).broker().id());
        }
        partitions.add(
            new PartitionInfo(p.topicPartition().topic(), p.topicPartition().partition(), nodes.get(p.leader().broker().id()), replicas,
                              inSyncReplicas));
      }
    }
    return new Cluster(CLUSTER_ID, nodes.values(), partitions, Collections.emptySet(), Collections.emptySet());
  }
}
