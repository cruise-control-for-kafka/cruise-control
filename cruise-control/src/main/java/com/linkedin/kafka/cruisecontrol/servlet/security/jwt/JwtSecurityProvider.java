/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import jakarta.servlet.ServletException;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.LoginService;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.List;

/**
 * A security provider implementation for JWT based authentication. It has to be configured with
 * <ul>
 *   <li>{@link WebServerConfig#JWT_AUTHENTICATION_PROVIDER_URL_CONFIG} that is the url of the token issuer,
 *   <li>{@link WebServerConfig#JWT_COOKIE_NAME_CONFIG} that is the name of the cookie that contains the issued token
 *   <li>{@link WebServerConfig#JWT_AUTH_CERTIFICATE_LOCATION_CONFIG} which is shared by the issuer to validate tokens
 *   <li>{@link WebServerConfig#JWT_EXPECTED_AUDIENCES_CONFIG} which is the expected audiences of the token (so a token
 *   will be rejected if it contains anything other than what is listed here)
 *   <li>{@link WebServerConfig#WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG} which contains the username-role associations
 *   without any passwords.
 * </ul>
 */
public class JwtSecurityProvider extends DefaultRoleSecurityProvider {

  private String _authenticationProviderUrl;
  private String _cookieName;
  private String _publicKeyLocation;
  private String _privilegesFilePath;
  private List<String> _audiences;

  @Override
  public void init(KafkaCruiseControlConfig config) {
    super.init(config);
    _authenticationProviderUrl = config.getString(WebServerConfig.JWT_AUTHENTICATION_PROVIDER_URL_CONFIG);
    _cookieName = config.getString(WebServerConfig.JWT_COOKIE_NAME_CONFIG);
    _publicKeyLocation = config.getString(WebServerConfig.JWT_AUTH_CERTIFICATE_LOCATION_CONFIG);
    _audiences = config.getList(WebServerConfig.JWT_EXPECTED_AUDIENCES_CONFIG);
    _privilegesFilePath = config.getString(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG);
  }

  @Override
  public LoginService loginService() throws ServletException {
    try {
      return new JwtLoginService(_privilegesFilePath, _publicKeyLocation, _audiences);
    } catch (IOException | CertificateException e) {
      throw new ServletException(e);
    }
  }

  @Override
  public Authenticator authenticator() {
    return new JwtAuthenticator(_authenticationProviderUrl, _cookieName);
  }
}
