/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

/**
 * The exception caused by a user request. A 400 error will be returned in this case.
 */
public class UserRequestException extends RuntimeException {

    public UserRequestException(Exception e) {
        super(e);
    }

    public UserRequestException(String message) {
        super(message);
    }
}
