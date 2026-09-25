/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * The exception indicating something went wrong during mapping a replica to a broker set
 */
public class ReplicaToBrokerSetMappingException extends OptimizationFailureException {

    public ReplicaToBrokerSetMappingException(String message) {
        super(message);
    }
}

