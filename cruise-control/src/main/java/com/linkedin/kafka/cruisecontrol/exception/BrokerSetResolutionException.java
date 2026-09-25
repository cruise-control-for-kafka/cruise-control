/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * The exception indicating something went wrong during the broker set resolution.
 */
public class BrokerSetResolutionException extends OptimizationFailureException {

    public BrokerSetResolutionException(String message) {
        super(message);
    }
}

