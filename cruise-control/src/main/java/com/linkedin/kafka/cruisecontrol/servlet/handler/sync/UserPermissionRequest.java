/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserPermissionsManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserPermissionsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserPermissions;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PERMISSIONS_PARAMETER_OBJECT_CONFIG;

public class UserPermissionRequest extends AbstractSyncRequest {
    protected KafkaCruiseControlConfig _config;
    protected UserPermissionsParameters _parameters;
    protected UserPermissionsManager _userPermissionsManager;

    public UserPermissionRequest() {
        super();
    }

    @Override
    protected UserPermissions handle() {
        return new UserPermissions(_config, _userPermissionsManager);
    }

    @Override
    public UserPermissionsParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return UserPermissionRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControlEndPoints cruiseControlEndPoints = getCruiseControlEndpoints();
        _config = cruiseControlEndPoints.asyncKafkaCruiseControl().config();
        _userPermissionsManager = cruiseControlEndPoints.getUserPermissionsManager();
        _parameters = (UserPermissionsParameters) validateNotNull(configs.get(PERMISSIONS_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
