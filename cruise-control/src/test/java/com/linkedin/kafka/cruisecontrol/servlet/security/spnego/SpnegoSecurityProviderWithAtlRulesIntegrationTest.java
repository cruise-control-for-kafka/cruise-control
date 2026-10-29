/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.SpnegoIntegrationTestHarness;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.junit.Test;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SpnegoSecurityProviderWithAtlRulesIntegrationTest extends SpnegoIntegrationTestHarness {

    private static final String AUTH_ATL_RULES_CREDENTIALS_FILE = "auth-atlrules.credentials";
    private static final List<String> ATL_RULES = Collections.singletonList("RULE:[2:$1@$0](.*@.*)s/@.*/foo/");

    public SpnegoSecurityProviderWithAtlRulesIntegrationTest() throws KrbException {
    }

    @Override
    protected Map<String, Object> withConfigs() {
        Map<String, Object> configs = super.withConfigs();
        configs.put(WebServerConfig.SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG, ATL_RULES);
        configs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
                Objects.requireNonNull(getClass().getClassLoader().getResource(AUTH_ATL_RULES_CREDENTIALS_FILE)).getPath());
        return configs;
    }

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        SpnegoSecurityProviderTestUtils.testSuccessfulAuthentication(_miniKdc, _app, CLIENT_PRINCIPAL + '@' + REALM);
    }

    @Test
    public void testNotAdminServiceLogin() throws Exception {
        SpnegoSecurityProviderTestUtils.testNotAdminServiceLogin(_miniKdc, _app, SOME_OTHER_SERVICE_PRINCIPAL + '@' + REALM);
    }

}
