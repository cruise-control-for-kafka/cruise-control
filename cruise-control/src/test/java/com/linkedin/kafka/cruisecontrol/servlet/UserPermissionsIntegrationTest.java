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

package com.linkedin.kafka.cruisecontrol.servlet;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.linkedin.kafka.cruisecontrol.CruiseControlIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.google.gson.JsonParser;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserPermissions;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.Test;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.PERMISSIONS;
import static org.junit.Assert.*;

public class UserPermissionsIntegrationTest extends CruiseControlIntegrationTestHarness {

    private static final String AUTH_CREDENTIALS_FILE = "auth-permissions.credentials";
    private static final String CRUISE_CONTROL_PERMISSIONS_ENDPOINT = "kafkacruisecontrol/" + PERMISSIONS;
    private final JsonElement _adminElement = new JsonPrimitive("ADMIN");
    private final JsonElement _userElement = new JsonPrimitive("USER");
    private final JsonElement _viewerElement = new JsonPrimitive("VIEWER");
    private boolean _isSecurityEnabled;

    // This empty override is a workaround to prevent the inherited SetUp() from starting the cluster before
    // _isSecurityEnabled is set. Each test manually calls setup(boolean) instead. This should be
    // refactored to use @Parameterized in a separate PR.
    @Override
    public void setUp() {
    }

    private void setup(boolean isSecurityEnabled) {
        _isSecurityEnabled = isSecurityEnabled;
        super.setUp();
    }

    @Override
    protected Map<String, Object> withConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG, _isSecurityEnabled);
        configs.put(WebServerConfig.WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
                Objects.requireNonNull(this.getClass().getClassLoader().getResource(AUTH_CREDENTIALS_FILE)).getPath());
        return configs;
    }

    @Test
    public void testGetAdminPermissions() throws Exception {
        setup(true);

        HttpURLConnection permissionsEndpointConnection = setupConnection("ccTestAdmin", "TestPass123", false);
        JsonArray response = JsonParser.parseString(getResponse(permissionsEndpointConnection)).getAsJsonArray();

        assertResponse(permissionsEndpointConnection.getResponseCode(), response, _adminElement, _userElement);
    }

    @Test
    public void testGetAdminPermissionsJson() throws Exception {
        setup(true);

        HttpURLConnection permissionsEndpointConnection = setupConnection("ccTestAdmin", "TestPass123", true);
        JsonArray response = JsonParser
                .parseString(getJsonResponse(permissionsEndpointConnection))
                .getAsJsonObject()
                .get(UserPermissions.ROLES)
                .getAsJsonArray();

        assertResponse(permissionsEndpointConnection.getResponseCode(), response, _adminElement, _userElement);
    }

    @Test
    public void testGetUserPermissions() throws Exception {
        setup(true);

        HttpURLConnection permissionsEndpointConnection = setupConnection("ccTestUser", "TestPass123", false);
        JsonArray response = JsonParser.parseString(getResponse(permissionsEndpointConnection)).getAsJsonArray();

        assertResponse(permissionsEndpointConnection.getResponseCode(), response, _userElement, _viewerElement);
    }

    @Test
    public void testGetUserPermissionsJson() throws Exception {
        setup(true);

        HttpURLConnection permissionsEndpointConnection = setupConnection("ccTestUser", "TestPass123", true);
        JsonArray response = JsonParser
                .parseString(getJsonResponse(permissionsEndpointConnection))
                .getAsJsonObject()
                .get(UserPermissions.ROLES)
                .getAsJsonArray();

        assertResponse(permissionsEndpointConnection.getResponseCode(), response, _userElement, _viewerElement);
    }

    @Test
    public void testGetViewerPermissions() throws Exception {
        setup(true);

        HttpURLConnection permissionsEndpointConnection = setupConnection("ccTestUser2", "TestPass123", false);
        JsonArray response = JsonParser.parseString(getResponse(permissionsEndpointConnection)).getAsJsonArray();

        assertResponse(permissionsEndpointConnection.getResponseCode(), response, _viewerElement);
    }

    @Test
    public void testGetViewerPermissionsJson() throws Exception {
        setup(true);

        HttpURLConnection permissionsEndpointConnection = setupConnection("ccTestUser2", "TestPass123", true);
        JsonArray response = JsonParser
                .parseString(getJsonResponse(permissionsEndpointConnection))
                .getAsJsonObject()
                .get(UserPermissions.ROLES)
                .getAsJsonArray();

        assertResponse(permissionsEndpointConnection.getResponseCode(), response, _viewerElement);
    }

    @Test
    public void testNoSecurity() throws Exception {
        setup(false);

        HttpURLConnection permissionsEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
                .resolve(CRUISE_CONTROL_PERMISSIONS_ENDPOINT).toURL().openConnection();

        assertEquals(HttpServletResponse.SC_BAD_REQUEST, permissionsEndpointConnection.getResponseCode());
        assertThrows("Unable to retrieve privilege information for an unsecure connection",
                IOException.class,
                () -> getResponse(permissionsEndpointConnection));
    }

    @Test
    public void testNoSecurityWithJson() throws Exception {
        setup(false);

        HttpURLConnection permissionsEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
                .resolve(CRUISE_CONTROL_PERMISSIONS_ENDPOINT + "?json=true").toURL().openConnection();

        assertEquals(HttpServletResponse.SC_BAD_REQUEST, permissionsEndpointConnection.getResponseCode());
        assertThrows("Unable to retrieve privilege information for an unsecure connection",
                IOException.class,
                () -> getJsonResponse(permissionsEndpointConnection));
    }

    private HttpURLConnection setupConnection(String username, String password, boolean isJson) throws Exception {
        HttpURLConnection permissionsEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
                .resolve(CRUISE_CONTROL_PERMISSIONS_ENDPOINT + (isJson ? "?json=true" : "")).toURL().openConnection();
        String adminCreds = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        permissionsEndpointConnection.setRequestProperty(HttpHeader.AUTHORIZATION.asString(), "Basic " + adminCreds);
        return permissionsEndpointConnection;
    }

    private String getResponse(HttpURLConnection endpoint) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(endpoint.getInputStream(), StandardCharsets.UTF_8));
        String inputLine;
        StringBuilder response = new StringBuilder();
        response.append("[");
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine + ", ");
        }
        response.append("]");
        in.close();
        return response.toString().replace(", ]", "\"]").
                replace("[", "[\"").replace(",", "\",\"");
    }

    private String getJsonResponse(HttpURLConnection endpoint) throws Exception {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(endpoint.getInputStream(), StandardCharsets.UTF_8))) {
            return in.readLine();
        }
    }

    private void assertResponse(int responseStatusCode, JsonArray response, JsonElement... expectedElements) {
        assertEquals(HttpServletResponse.SC_OK, responseStatusCode);
        for (JsonElement element : expectedElements) {
            assertTrue(response.contains(element));
        }
    }

}
