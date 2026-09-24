/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.exception;

/**
 * An exception thrown when a partition does not exist.
 */
public class PartitionNotExistsException extends KafkaCruiseControlException {
    public PartitionNotExistsException(String message) {
        super(message);
    }
}
