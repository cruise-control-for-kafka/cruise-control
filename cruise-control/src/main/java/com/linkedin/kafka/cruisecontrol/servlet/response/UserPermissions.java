/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserPermissionsManager;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserPermissionsParameters;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

/**
 * - If security enabled for Cruise Control, then the roles will be the result which were assigned to the user:
 *      e.g. ["ADMIN", "VIEWER"], with 200 HTTP status code.
 * - If security disabled, then it will result a 400 HTTP status code, with the following message:
 *      "Unable to retrieve privilege information for an unsecure connection."
 */
@JsonResponseClass
public class UserPermissions extends AbstractCruiseControlResponse {

    @JsonResponseField(required = false)
    public static final String ROLES = "roles";
    protected Set<String> _roles = new HashSet<>();

    private final boolean _securityEnabled;
    private final UserPermissionsManager _userPermissionsManager;

    public UserPermissions(KafkaCruiseControlConfig config, UserPermissionsManager userPermissionsManager) {
        super(config);
        _userPermissionsManager = userPermissionsManager;
        _securityEnabled = config.getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG);
    }

    protected String getJsonString(CruiseControlParameters parameters) {
        Gson gson = new Gson();
        UserPermissionsParameters params = (UserPermissionsParameters) parameters;
        Map<String, Object> jsonStructure = new HashMap<>();
        jsonStructure.put(ROLES, getJsonStructure(params.getUser()));
        jsonStructure.put(VERSION, JSON_VERSION);
        return gson.toJson(jsonStructure);
    }

    /**
     * @param user the name of the required user.
     * @return An object that can be further used to encode into JSON.
     */
    public Set<String> getJsonStructure(String user) {
        fillPermissions(user);
        return _roles;
    }

    protected String getPlaintext(CruiseControlParameters parameters) {
        fillPermissions(((UserPermissionsParameters) parameters).getUser());
        return StringUtils.join(_roles, ',');
    }

    @Override
    protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
        _cachedResponse = parameters.json() ? getJsonString(parameters) : getPlaintext(parameters);
    }

    private void fillPermissions(String user) {
        if (_securityEnabled) {
            _roles = _userPermissionsManager.getRolesBy(user);
        } else {
            throw new UserRequestException("Unable to retrieve privilege information for an unsecure connection.");
        }
    }
}
