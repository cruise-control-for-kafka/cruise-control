/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * The exception indicating something went wrong while sampling data from a requested source.
 */
public class SamplingException extends KafkaCruiseControlException {

    public SamplingException(String message) {
        super(message);
    }

}
