/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.servlet.security.SpnegoIntegrationTestHarness;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.junit.Test;

public class SpnegoSecurityProviderIntegrationTest extends SpnegoIntegrationTestHarness {

  public SpnegoSecurityProviderIntegrationTest() throws KrbException {
  }

  @Test
  public void testSuccessfulAuthentication() throws Exception {
    SpnegoSecurityProviderTestUtils.testSuccessfulAuthentication(_miniKdc, _app, CLIENT_PRINCIPAL);
  }

  @Test
  public void testNotAdminServiceLogin() throws Exception {
    SpnegoSecurityProviderTestUtils.testNotAdminServiceLogin(_miniKdc, _app, SOME_OTHER_SERVICE_PRINCIPAL);
  }

}
