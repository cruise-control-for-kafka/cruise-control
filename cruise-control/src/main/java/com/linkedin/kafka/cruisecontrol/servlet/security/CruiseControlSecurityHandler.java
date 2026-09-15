/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.ee10.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.Constraint;
import org.eclipse.jetty.server.Request;

/**
 * A custom {@link ConstraintSecurityHandler} that converts the request to lowercase to ensure case insensitivity.
 */
public class CruiseControlSecurityHandler extends ConstraintSecurityHandler {

  @Override
  public Constraint getConstraint(String pathInContext, Request request) {
    return super.getConstraint(pathInContext.toLowerCase(), request);
  }
}
