/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import jakarta.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SslConnectionIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;
  public static final String HTTPS = "https";
  private final TrustManager[] _trustAllCerts = new TrustManager[]{
      new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }
        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
          fail("checkClientTrusted shouldn't be called");
        }
        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
          assertEquals(1, certs.length);
          assertEquals("CN=" + LOCALHOST, certs[0].getIssuerDN().getName());
        }
      }
  };

  @Before
  public void setup() throws Exception {
    super.start();
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> sslConfigs = new HashMap<>();
    sslConfigs.put(WebServerConfig.WEBSERVER_SSL_ENABLE_CONFIG, true);
    sslConfigs.put(WebServerConfig.WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG, Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("ssl_integration_test.keystore")).toString());
    sslConfigs.put(WebServerConfig.WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG, "jetty");
    sslConfigs.put(WebServerConfig.WEBSERVER_SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
    sslConfigs.put(WebServerConfig.WEBSERVER_SSL_KEY_PASSWORD_CONFIG, "jetty");
    sslConfigs.put(WebServerConfig.WEBSERVER_SSL_STS_ENABLED, true);
    return sslConfigs;
  }

  @Test
  public void testSslConnection() throws Exception {
    assertEquals(HTTPS, new URL(_app.serverUrl()).getProtocol());
    SSLSocketFactory defaultSslSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, _trustAllCerts, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
      HttpURLConnection connection = (HttpURLConnection) new URI(_app.serverUrl())
        .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();

      assertEquals(HttpServletResponse.SC_OK, connection.getResponseCode());

      // check for existence of the Strict-Transport-Security Header
      assertNotNull(connection.getHeaderField("Strict-Transport-Security"));
      assertFalse(connection.getHeaderField("Strict-Transport-Security").isEmpty());
    } finally {
      HttpsURLConnection.setDefaultSSLSocketFactory(defaultSslSocketFactory);
    }
  }
}
