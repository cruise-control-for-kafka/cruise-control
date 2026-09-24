/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.cruisecontrol.servlet.EndpointType;


/**
 * An enum for categorizing {@link CruiseControlEndPoint}. Endpoints can be roughly grouped into following four types.
 * <ul>
 *  <li>KAFKA_MONITOR: endpoints that get information related to Kafka cluster.</li>
 *  <li>CRUISE_CONTROL_MONITOR: endpoints that get information related to Cruise Control instance.</li>
 *  <li>KAFKA_ADMIN: endpoints that can change state of Kafka cluster.</li>
 *  <li>CRUISE_CONTROL_ADMIN: endpoints that can change state or setting of Cruise Control instance.</li>
 * </ul>
 */
enum CruiseControlEndpointType implements EndpointType {
    KAFKA_MONITOR,
    CRUISE_CONTROL_MONITOR,
    KAFKA_ADMIN,
    CRUISE_CONTROL_ADMIN
}
