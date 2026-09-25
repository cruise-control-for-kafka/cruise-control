/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * The exception indicating something went wrong during the broker capacity resolving.
 */
public class BrokerCapacityResolutionException extends KafkaCruiseControlException {

    public BrokerCapacityResolutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public BrokerCapacityResolutionException(String message) {
        super(message);
    }
}
