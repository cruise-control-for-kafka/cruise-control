/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertThrows;


public class ConcurrencyConfigTest {
  private static final String GET_INT_METHOD = "getInt";
  private static final String GET_LONG_METHOD = "getLong";

  @Test
  public void testNumConcurrentPartitionMovementsPerBrokerNotSmallerThanMaxNumClusterMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testNumConcurrentIntraBrokerPartitionMovementsNotSmallerThanMaxNumClusterMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testNumConcurrentLeaderMovementsGreaterThanMaxNumClusterMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testClusterLeaderMoveConcurrencyNotSmallerThanBrokerLeaderMoveConcurrency() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testNumConcurrentPartitionMovementsPerBrokerNotSmallerThanConcurrencyAdjusterMaxPartitionMovementsPerBroker() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMaxPartitionMovementsPerBrokerGreaterThanMaxNumClusterMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMinPartitionMovementsPerBrokerGreaterThanNumConcurrentPartitionMovementsPerBroker() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
           .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMinLeadershipMovementsGreaterThanNumConcurrentLeaderMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
           .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMaxLeadershipMovementsSmallerThanNumConcurrentLeaderMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS - 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMaxLeadershipMovementsGreaterThanMaxNumClusterMovements() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
           .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testBrokerConcurrencyAdjusterMinLeaderMoveNotGreaterThanBrokerLeaderMoveConcurrency() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testBrokerConcurrencyAdjusterMaxLeaderMoveNotGreaterClusterConcurrencyAdjusterMaxLeaderMove() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testBrokerConcurrencyAdjusterMaxLeaderMoveNotSmallerBrokerLeaderMoveConcurrency() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
        .addMockedMethod(GET_INT_METHOD)
        .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER - 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testMinExecutionProgressCheckIntervalMsGreaterThanExecutionProgressCheckIntervalMs() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_LONG_METHOD)
                                              .addMockedMethod(GET_LONG_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
        .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS + 1);
    EasyMock.expect(config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMinReplicationThrottleGreaterThanMaxReplicationThrottle() {
    KafkaCruiseControlConfig config = EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class)
                                              .addMockedMethod(GET_INT_METHOD)
                                              .addMockedMethod(GET_LONG_METHOD)
                                              .createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_NUM_CONCURRENT_LEADER_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_PARTITION_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MIN_LEADERSHIP_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getInt(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_LEADERSHIP_MOVEMENTS_PER_BROKER);
    EasyMock.expect(config.getLong(ExecutorConfig.MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MIN_EXECUTION_PROGRESS_CHECK_INTERVAL_MS);
    EasyMock.expect(config.getLong(ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS);
    EasyMock.expect(config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_MIN_REPLICATION_THROTTLE_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_REPLICATION_THROTTLE + 1);
    EasyMock.expect(config.getLong(ExecutorConfig.CONCURRENCY_ADJUSTER_MAX_REPLICATION_THROTTLE_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_CONCURRENCY_ADJUSTER_MAX_REPLICATION_THROTTLE);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }

  @Test
  public void testConcurrencyAdjusterMaxPartitionMovementsGreaterThanMaxNumClusterMovements() {
    KafkaCruiseControlConfig config =
        EasyMock.partialMockBuilder(KafkaCruiseControlConfig.class).addMockedMethod(GET_INT_METHOD).createNiceMock();
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG);
    EasyMock.expect(config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG))
            .andReturn(ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG + 1);

    EasyMock.replay(config);
    assertThrows(ConfigException.class, config::sanityCheckConcurrency);
    EasyMock.verify(config);
  }
}
