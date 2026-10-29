/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.kafka.common.security.auth.SecurityProtocol;


public abstract class CCKafkaIntegrationTestHarness extends CCAbstractKRaftTestHarness {
  protected Map<Integer, CCEmbeddedBroker> _brokers = null;
  protected String _bootstrapUrl;

  @Override
  public void setUp() {
    super.setUp();
    if (_brokers != null) {
      return;
    }

    _brokers = new LinkedHashMap<>();
    List<Map<Object, Object>> brokerConfigs = buildBrokerConfigs();
    if (brokerConfigs == null || brokerConfigs.isEmpty()) {
      throw new AssertionError("Broker configs " + brokerConfigs + " should not be null or empty");
    }
    for (Map<Object, Object> brokerConfig : brokerConfigs) {
      CCEmbeddedBroker broker = new CCEmbeddedBroker(brokerConfig);
      int id = broker.id();
      if (_brokers.putIfAbsent(id, broker) != null) {
        // Will not be picked up by teardown
        CCKafkaTestUtils.quietly(broker::close);
        throw new IllegalStateException("multiple brokers defined with id " + id);
      }
    }

    StringJoiner joiner = new StringJoiner(",");
    _brokers.values().forEach(broker -> joiner.add(broker.addr(securityProtocol())));
    _bootstrapUrl = joiner.toString();
  }

  @Override
  public void tearDown() {
    try {
      if (_brokers != null) {
        for (CCEmbeddedBroker broker : _brokers.values()) {
          CCKafkaTestUtils.quietly(broker::close);
        }
        _brokers.clear();
        _brokers = null;
      }
    } finally {
      super.tearDown();
    }
  }

  protected CCEmbeddedBroker broker(int id) {
    CCEmbeddedBroker broker = _brokers.get(id);
    if (broker == null) {
      throw new IllegalArgumentException("Invalid server id " + id);
    }
    return broker;
  }

  public String bootstrapServers() {
    return _bootstrapUrl;
  }

  /**
   * returns the list of broker configs for all brokers created by this test
   * (as determined by clusterSize()
   * @return list of broker configs, one config map per broker to be created
   */
  protected List<Map<Object, Object>> buildBrokerConfigs() {
    List<Map<Object, Object>> configs = new ArrayList<>();
    for (int i = 0; i < clusterSize(); i++) {
      configs.add(createBrokerConfig(i));
    }
    return configs;
  }

  protected Map<Object, Object> createBrokerConfig(int brokerId) {
    CCEmbeddedBrokerBuilder builder = new CCEmbeddedBrokerBuilder();
    if (_controller != null) {
      builder.kraftConnect(kraftController());
      builder.clusterId(kraftController());
    }
    builder.nodeId(brokerId);
    builder.enable(securityProtocol());
    if (securityProtocol() == SecurityProtocol.SSL) {
      if (trustStoreFile() != null) {
        builder.trustStore(trustStoreFile());
      }
    } else {
      if (trustStoreFile() != null) {
        throw new AssertionError("security protocol not set yet trust store file provided");
      }
    }
    Map<Object, Object> config = builder.buildConfig();
    config.putAll(overridingProps());
    return config;
  }
  
  protected SecurityProtocol securityProtocol() {
    return SecurityProtocol.PLAINTEXT;
  }

  protected File trustStoreFile() {
    return null;
  }

  protected int clusterSize() {
    return 2;
  }

  protected Map<Object, Object> overridingProps() {
    return Collections.emptyMap();
  }
}
