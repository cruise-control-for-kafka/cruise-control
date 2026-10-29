/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoSecurityProvider;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.junit.After;
import org.junit.Before;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.AUTH_CREDENTIALS_FILE;

public abstract class SpnegoIntegrationTestHarness extends CruiseControlIntegrationTestHarness {

  public static final String REALM = "LINKEDINTEST.COM";
  public static final String CC_TEST_ADMIN = "ccTestAdmin";
  public static final String CLIENT_PRINCIPAL = CC_TEST_ADMIN + "/localhost";
  public static final String SOME_OTHER_SERVICE_PRINCIPAL = "someotherservice/localhost";
  public static final String SPNEGO_SERVICE_PRINCIPAL = "HTTP/localhost";

  protected final MiniKdc _miniKdc;

  /**
   * Instantiates a new object.
   * @throws KrbException
   */
  public SpnegoIntegrationTestHarness() throws KrbException {
    _miniKdc = new MiniKdc(REALM, principals());
  }

  /**
   * Collects a list of principals that will be initialized by the KDC.
   * @return a list of Strings representing the principals.
   */
  public List<String> principals() {
    List<String> principals = new ArrayList<>();
    principals.add(CLIENT_PRINCIPAL);
    principals.add(SPNEGO_SERVICE_PRINCIPAL);
    principals.add(SOME_OTHER_SERVICE_PRINCIPAL);
    return principals;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, true);
    configs.put(WebServerConfig.WEBSERVER_SECURITY_PROVIDER_CONFIG, SpnegoSecurityProvider.class);
    configs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
        Objects.requireNonNull(this.getClass().getClassLoader().getResource(AUTH_CREDENTIALS_FILE)).getPath());
    configs.put(WebServerConfig.SPNEGO_PRINCIPAL_CONFIG, SPNEGO_SERVICE_PRINCIPAL + "@" + REALM);
    configs.put(WebServerConfig.SPNEGO_KEYTAB_FILE_CONFIG, _miniKdc.keytab().getAbsolutePath());

    return configs;
  }

  /**
   * Starts the Mini KDC and initializes the test environment.
   */
  @Override
  @Before
  public void setUp() {
    try {
      _miniKdc.start();
    } catch (KrbException | IOException e) {
      throw new RuntimeException(e);
    }
    super.setUp();
  }

  /**
   * Shuts down the containerized cluster, the Cruise Control instance, and Mini KDC.
   */
  @Override
  @After
  public void tearDown() {
    super.tearDown();
    try {
      _miniKdc.stop();
    } catch (KrbException e) {
      throw new RuntimeException(e);
    }
  }
}
