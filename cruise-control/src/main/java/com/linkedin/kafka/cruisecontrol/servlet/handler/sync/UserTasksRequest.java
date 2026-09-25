/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserTaskState;

import java.util.List;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.USER_TASKS_PARAMETER_OBJECT_CONFIG;


public class UserTasksRequest extends AbstractSyncRequest {
    protected List<UserTaskManager.UserTaskInfo> _userTasks;
    protected KafkaCruiseControlConfig _config;
    protected UserTasksParameters _parameters;

    public UserTasksRequest() {
        super();
    }

    @Override
    protected UserTaskState handle() {
        return new UserTaskState(_userTasks, _config);
    }

    @Override
    public UserTasksParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return UserTasksRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        KafkaCruiseControlEndPoints cruiseControlEndPoints = getCruiseControlEndpoints();
        _userTasks = cruiseControlEndPoints.getAllUserTasks();
        _config = cruiseControlEndPoints.asyncKafkaCruiseControl().config();
        _parameters = (UserTasksParameters) validateNotNull(configs.get(USER_TASKS_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
