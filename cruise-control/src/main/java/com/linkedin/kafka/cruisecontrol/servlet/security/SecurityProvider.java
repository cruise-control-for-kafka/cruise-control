/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.ee10.servlet.security.ConstraintMapping;
import org.eclipse.jetty.security.LoginService;
import jakarta.servlet.ServletException;
import java.util.List;
import java.util.Set;

/**
 * This class delegates to the Jetty implementation of authentication features and is used for setting
 * up Cruise Control security to provide authentication and authorization rules over the WEB API.
 */
public interface SecurityProvider {

  /**
   * Sets the configuration to allow the initialization of the provider with custom configurable values.
   *
   * @param config is the global {@link KafkaCruiseControlConfig} object.
   * @throws ServletException if any problem occurred during the initialization.
   */
  void init(KafkaCruiseControlConfig config) throws ServletException;

  /**
   * Creates or gets a list of constraints that are put on the API endpoints, for instance only
   * allow certain roles on certain endpoints.
   *
   * @return a list of created {@link ConstraintMapping}s that represent permissions.
   */
  List<ConstraintMapping> constraintMappings();

  /**
   * Associates a username, credentials and roles with a {@link org.eclipse.jetty.security.UserIdentity}
   * that will be used by Jetty to manage the authentication.
   *
   * @return a new {@link LoginService}.
   * @throws ServletException if any problem occurred during the initialization of the LoginService.
   */
  LoginService loginService() throws ServletException;

  /**
   * Defines the request authentication method which is responsible to send challenges
   * according to authentication method and decide if the user has valid credentials according
   * to the authentication method.
   *
   * @return the {@link Authenticator} that'll be used for checking the incoming requests.
   * @throws ServletException if any problem occurred during the initialization of the Authenticator.
   */
  Authenticator authenticator() throws ServletException;

  /**
   * The set of roles defined by this {@link SecurityProvider}.
   *
   * @return list of strings that define the possible roles in Cruise Control.
   */
  Set<String> roles();
}
