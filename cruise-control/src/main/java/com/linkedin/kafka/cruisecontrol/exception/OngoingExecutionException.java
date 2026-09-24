/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * An exception thrown when an execution was attempted to be started while there is another ongoing execution.
 */
public class OngoingExecutionException extends KafkaCruiseControlException {

    public OngoingExecutionException(String message) {
        super(message);
    }
}
