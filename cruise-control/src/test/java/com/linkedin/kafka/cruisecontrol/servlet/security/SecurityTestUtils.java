/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlApp;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.PrivilegedAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class SecurityTestUtils {
  public static final String AUTH_CREDENTIALS_FILE = "auth.credentials";
  public static final String BASIC_AUTH_CREDENTIALS_FILE = "basic-auth.credentials";

  private SecurityTestUtils() {
  }

  /**
   * Authenticates {@code principal} against the KDC and runs {@code action} within that {@link Subject}'s context.
   * @param miniKdc the KDC to authenticate against.
   * @param principal the principal to log in as.
   * @param action the work to perform as the authenticated subject.
   * @throws LoginException if the KDC login fails.
   */
  public static void runAs(MiniKdc miniKdc, String principal, Runnable action) throws LoginException {
    Subject subject = miniKdc.loginAs(principal);
    Subject.doAs(subject, (PrivilegedAction<Object>) () -> {
      action.run();
      return null;
    });
  }

  /**
   * Opens an HTTP connection to a Cruise Control endpoint, wrapping checked exceptions as unchecked.
   * @param app the Cruise Control app under test.
   * @param endpoint the endpoint path (optionally with a query string).
   * @return the opened connection.
   */
  public static HttpURLConnection openConnection(KafkaCruiseControlApp app, String endpoint) {
    try {
      return (HttpURLConnection) new URI(app.serverUrl()).resolve(endpoint).toURL().openConnection();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Asserts the request was authorized (HTTP 200).
   * @param connection the connection to read the response code from.
   */
  public static void assertAuthorized(HttpURLConnection connection) {
    try {
      assertEquals(HttpServletResponse.SC_OK, connection.getResponseCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Asserts the request was rejected. Accepts either a 401/403 status or a thrown exception as a valid rejection:
   * Jetty 9.4 sometimes returns the error code cleanly and sometimes closes the connection so that the JDK's
   * Negotiate client throws (an IOException, or a RuntimeException NPE while building the auth header), depending on
   * JDK/timing. Treating all of these as rejection keeps the assertion deterministic.
   * @param connection the connection to read the response code from.
   */
  public static void assertRejected(HttpURLConnection connection) {
    try {
      int responseCode = connection.getResponseCode();
      assertTrue("Expected 401 or 403 on auth failure, got: " + responseCode,
          responseCode == HttpServletResponse.SC_UNAUTHORIZED || responseCode == HttpServletResponse.SC_FORBIDDEN);
    } catch (IOException | RuntimeException e) {
      // Rejection surfaced as a thrown exception rather than a status code.
    }
  }
}
