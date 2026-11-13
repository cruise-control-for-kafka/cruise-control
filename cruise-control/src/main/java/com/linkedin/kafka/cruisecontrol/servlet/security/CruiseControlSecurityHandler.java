/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.eclipse.jetty.ee10.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import java.util.Locale;

/**
 * A custom {@link ConstraintSecurityHandler} that converts the request to lowercase to ensure case insensitivity.
 *
 * <p>Jetty 12 removed the {@code handle(String pathInContext, ...)} hook, and servlet path matching stays
 * case-sensitive, so the URI must be rewritten before both constraint matching and servlet dispatch. The rewrite
 * is scoped to the configured API prefix; all other paths (e.g. the WebUI {@code DefaultServlet}) are passed
 * through unchanged to avoid breaking case-sensitive static resources.</p>
 */
public class CruiseControlSecurityHandler extends ConstraintSecurityHandler {
  private final String _apiBasePath;
  private final String _apiBaseNoSlash;

  public CruiseControlSecurityHandler() {
    this(WebServerConfig.DEFAULT_WEBSERVER_API_URLPREFIX);
  }

  public CruiseControlSecurityHandler(String apiUrlPrefix) {
    String base = apiUrlPrefix == null ? "" : apiUrlPrefix.trim();
    if (base.endsWith("*")) {
      base = base.substring(0, base.length() - 1);
    }
    if (!base.startsWith("/")) {
      base = "/" + base;
    }
    base = base.toLowerCase(Locale.ROOT);
    if (!base.endsWith("/")) {
      base = base + "/";
    }
    _apiBasePath = base;
    _apiBaseNoSlash = base.substring(0, base.length() - 1);
  }

  @Override
  public boolean handle(Request request, Response response, Callback callback) throws Exception {
    String pathInContext = Request.getPathInContext(request);
    if (pathInContext == null || pathInContext.isEmpty()) {
      return super.handle(request, response, callback);
    }
    String lowercasePath = pathInContext.toLowerCase(Locale.ROOT);
    Request requestToHandle = request;
    if (!lowercasePath.equals(pathInContext) && isApiPath(lowercasePath)) {
      requestToHandle = Request.serveAs(request, Request.newHttpURIFrom(request, lowercasePath));
    }
    return super.handle(requestToHandle, response, callback);
  }

  private boolean isApiPath(String lowercasePathInContext) {
    return lowercasePathInContext.equals(_apiBaseNoSlash) || lowercasePathInContext.startsWith(_apiBasePath);
  }
}
