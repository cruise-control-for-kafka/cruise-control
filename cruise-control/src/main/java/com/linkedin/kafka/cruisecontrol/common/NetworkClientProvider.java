/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.utils.Time;


/**
 * An interface to create network clients supporting custom Kafka client versions.
 */
@InterfaceStability.Evolving
public interface NetworkClientProvider {

    /**
     * Creates a new network client with the given properties.
     *
     * @param connectionMaxIdleMs              Connection max idle time in milliseconds.
     * @param metrics                          Metrics.
     * @param time                             Time.
     * @param metricGrpPrefix                  Metric group prefix.
     * @param channelBuilder                   Channel builder.
     * @param metadata                         Metadata.
     * @param clientId                         Client id.
     * @param maxInFlightRequestsPerConnection Max in flight requests per connection.
     * @param reconnectBackoffMs               Reconnect backoff in milliseconds.
     * @param reconnectBackoffMax              Reconnect backoff max.
     * @param socketSendBuffer                 Socket send buffer.
     * @param socketReceiveBuffer              Socket receive buffer.
     * @param defaultRequestTimeoutMs          Default request timeout in milliseconds.
     * @param connectionSetupTimeoutMs         Connection setup timeout in milliseconds.
     * @param connectionSetupTimeoutMaxMs      Connection setup maximum timeout in milliseconds.
     * @param discoverBrokerVersions           {@code true} to discover broker versions, {@code false} otherwise.
     * @param apiVersions                      API versions.
     * @return A new network client with the given properties.
     */
    NetworkClient createNetworkClient(long connectionMaxIdleMs, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, Metadata metadata, String clientId, int maxInFlightRequestsPerConnection, long reconnectBackoffMs, long reconnectBackoffMax, int socketSendBuffer, int socketReceiveBuffer, int defaultRequestTimeoutMs, long connectionSetupTimeoutMs, long connectionSetupTimeoutMaxMs, boolean discoverBrokerVersions, ApiVersions apiVersions);
}
