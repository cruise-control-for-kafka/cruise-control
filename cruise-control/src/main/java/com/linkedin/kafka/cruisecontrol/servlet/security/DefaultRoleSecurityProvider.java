/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.eclipse.jetty.ee10.servlet.security.ConstraintMapping;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.Constraint;


/**
 * <p>A base class that defines the default role structure for Cruise Control and can be used for implementing custom
 * security providers.</p>
 *
 * This class defines three roles: VIEWER, USER and ADMIN.
 * <ul>
 *   <li>VIEWER role: has access to the most lightweight <code>kafka_cluster_state</code>, <code>user_tasks</code> and
 *     <code>review_board</code> endpoints.
 *   <li>USER role: has access to all the GET endpoints except <code>bootstrap</code> and <code>train</code>.
 *   <li>ADMIN role: has access to all endpoints.
 * </ul>
 *
 */
public abstract class DefaultRoleSecurityProvider implements SecurityProvider {

  public static final String ADMIN = "ADMIN";
  public static final String USER = "USER";
  public static final String VIEWER = "VIEWER";

  private String _webServerApiUrlPrefix;

  @Override
  public void init(KafkaCruiseControlConfig config) {
    this._webServerApiUrlPrefix = config.getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG);
  }

  @Override
  public List<ConstraintMapping> constraintMappings() {
    List<ConstraintMapping> constraintMappings = new ArrayList<>();
    CruiseControlEndPoint.getEndpoints().forEach(ep -> {
      if (ep == CruiseControlEndPoint.KAFKA_CLUSTER_STATE
          || ep == CruiseControlEndPoint.USER_TASKS
          || ep == CruiseControlEndPoint.REVIEW_BOARD
          || ep == CruiseControlEndPoint.PERMISSIONS) {
        constraintMappings.add(mapping(ep, VIEWER, USER, ADMIN));
      } else if (ep == CruiseControlEndPoint.BOOTSTRAP || ep == CruiseControlEndPoint.TRAIN) {
        constraintMappings.add(mapping(ep, ADMIN));
      } else {
        constraintMappings.add(mapping(ep, USER, ADMIN));
      }
    });
    CruiseControlEndPoint.postEndpoints().forEach(ep -> constraintMappings.add(mapping(ep, ADMIN)));
    return constraintMappings;
  }

  @Override
  public Set<String> roles() {
    return Set.of(VIEWER, USER, ADMIN);
  }

  private ConstraintMapping mapping(CruiseControlEndPoint endpoint, String... roles) {
    Constraint constraint = new Constraint.Builder()
        .name(Authenticator.BASIC_AUTH)
        .roles(roles)
        .authorization(Constraint.Authorization.SPECIFIC_ROLE)
        .build();
    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setPathSpec(_webServerApiUrlPrefix.replace("*", endpoint.name().toLowerCase()));
    mapping.setConstraint(constraint);
    return mapping;
  }
}
