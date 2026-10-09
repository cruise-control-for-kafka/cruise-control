/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import java.util.concurrent.TimeoutException;


/**
 * The interface for getting the broker capacity. Users should implement this interface so Cruise Control can
 * optimize the cluster according to the capacity of each broker.
 *
 */
public interface BrokerCapacityConfigResolver extends CruiseControlConfigurable, AutoCloseable {
  /**
   * Get the capacity of a broker based on rack, host and broker id.
   * May estimate the capacity of a broker, if it is not directly available.
   *
   * @param rack The rack of the broker
   * @param host The host of the broker
   * @param brokerId The id of the broker
   * @param timeoutMs The timeout in millisecond.
   * @param allowCapacityEstimation Whether allow resolver to estimate broker capacity if resolver is unable to get
   *                                capacity information of the broker.
   * @return An instance of {@link BrokerCapacityInfo}.
   * @throws TimeoutException if resolver is unable to resolve broker capacity in time.
   * @throws BrokerCapacityResolutionException if resolver fails to resolve broker capacity.
   */
  BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId, long timeoutMs, boolean allowCapacityEstimation)
      throws TimeoutException, BrokerCapacityResolutionException;
  boolean isJbodKafkaCluster();
}
