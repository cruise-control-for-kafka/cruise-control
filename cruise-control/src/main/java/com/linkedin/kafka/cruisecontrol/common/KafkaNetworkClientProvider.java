/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import org.apache.kafka.clients.*;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.NoSuchElementException;


public class KafkaNetworkClientProvider implements NetworkClientProvider {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNetworkClientProvider.class);

  public KafkaNetworkClientProvider() {

  }

  @Override
  public NetworkClient createNetworkClient(long connectionMaxIdleMs,
                                           Metrics metrics,
                                           Time time,
                                           String metricGrpPrefix,
                                           ChannelBuilder channelBuilder,
                                           Metadata metadata,
                                           String clientId,
                                           int maxInFlightRequestsPerConnection,
                                           long reconnectBackoffMs,
                                           long reconnectBackoffMax,
                                           int socketSendBuffer,
                                           int socketReceiveBuffer,
                                           int defaultRequestTimeoutMs,
                                           long connectionSetupTimeoutMs,
                                           long connectionSetupTimeoutMaxMs,
                                           boolean discoverBrokerVersions,
                                           ApiVersions apiVersions) {
    NetworkClient networkClient = null;
    try {
      Constructor<?> kafka38PlusCon = NetworkClient.class.getConstructor(Selectable.class, Metadata.class, String.class, int.class, long.class,
                                                                         long.class, int.class, int.class, int.class, long.class, long.class,
                                                                         Time.class, boolean.class, ApiVersions.class, LogContext.class,
                                                                         MetadataRecoveryStrategy.class);
      networkClient = (NetworkClient) kafka38PlusCon.newInstance(new Selector(connectionMaxIdleMs, metrics, time, metricGrpPrefix, channelBuilder,
                                                                              new LogContext()), metadata, clientId,
                                                                 maxInFlightRequestsPerConnection, reconnectBackoffMs, reconnectBackoffMax,
                                                                 socketSendBuffer, socketReceiveBuffer, defaultRequestTimeoutMs,
                                                                 connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, time, discoverBrokerVersions,
                                                                 apiVersions, new LogContext(), MetadataRecoveryStrategy.NONE);
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      LOG.debug("Unable to find Kafka 3.8+ constructor for KafkaSever class", e);
    }
    if (networkClient == null) {
      try {
        Constructor<?> kafka30PlusCon = NetworkClient.class.getConstructor(Selectable.class, Metadata.class, String.class, int.class, long.class,
                long.class, int.class, int.class, int.class, long.class, long.class,
                Time.class, boolean.class, ApiVersions.class, LogContext.class);
        networkClient = (NetworkClient) kafka30PlusCon.newInstance(new Selector(connectionMaxIdleMs, metrics, time, metricGrpPrefix, channelBuilder,
                        new LogContext()), metadata, clientId,
                maxInFlightRequestsPerConnection, reconnectBackoffMs, reconnectBackoffMax,
                socketSendBuffer, socketReceiveBuffer, defaultRequestTimeoutMs,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, time, discoverBrokerVersions,
                apiVersions, new LogContext());
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        LOG.debug("Unable to find Kafka 3.0+ constructor for KafkaSever class", e);
      }
    }
    if (networkClient == null) {
      try {
        Constructor<?> kafka30MinusCon = NetworkClient.class.getConstructor(Selectable.class, Metadata.class, String.class, int.class, long.class,
                                                                            long.class, int.class, int.class, int.class, long.class, long.class,
                                                                            ClientDnsLookup.class, Time.class, boolean.class, ApiVersions.class,
                                                                            LogContext.class);
        networkClient = (NetworkClient) kafka30MinusCon.newInstance(new Selector(connectionMaxIdleMs, metrics, time, metricGrpPrefix, channelBuilder,
                                                                                 new LogContext()), metadata, clientId,
                                                                    maxInFlightRequestsPerConnection, reconnectBackoffMs, reconnectBackoffMax,
                                                                    socketSendBuffer, socketReceiveBuffer, defaultRequestTimeoutMs,
                                                                    connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs,
                                                                    ClientDnsLookup.USE_ALL_DNS_IPS, time, discoverBrokerVersions, apiVersions,
                                                                    new LogContext());
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        LOG.debug("Unable to find Kafka 3.0- constructor for KafkaSever class", e);
      }
    }
    if (networkClient != null) {
      return networkClient;
    } else {
      throw new NoSuchElementException("Unable to find viable constructor for the NetworkClient class");
    }
  }
}
