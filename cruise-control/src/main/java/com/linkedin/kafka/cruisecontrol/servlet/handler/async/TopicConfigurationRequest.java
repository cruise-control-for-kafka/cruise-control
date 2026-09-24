/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.UpdateTopicConfigurationRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TOPIC_CONFIGURATION_PARAMETER_OBJECT_CONFIG;


public class TopicConfigurationRequest extends AbstractAsyncRequest {
    protected TopicConfigurationParameters _parameters;

    public TopicConfigurationRequest() {
        super();
    }

    @Override
    protected OperationFuture handle(String uuid) {
        OperationFuture future = new OperationFuture("Update Topic Configuration");
        pending(future.operationProgress());
        _asyncKafkaCruiseControl.sessionExecutor().execute(new UpdateTopicConfigurationRunnable(_asyncKafkaCruiseControl, future, uuid, _parameters));
        return future;
    }

    @Override
    public TopicConfigurationParameters parameters() {
        return _parameters;
    }

    @Override
    public String name() {
        return TopicConfigurationRequest.class.getSimpleName();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        _parameters = (TopicConfigurationParameters) validateNotNull(configs.get(TOPIC_CONFIGURATION_PARAMETER_OBJECT_CONFIG),
                "Parameter configuration is missing from the request.");
    }
}
