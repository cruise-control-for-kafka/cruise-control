/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.cruisecontrol.servlet.EndPoint;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserPermissionsManager;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.Purgatory;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.servers.ServerVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.KAFKA_CRUISE_CONTROL_SERVLET_SENSOR;

@OpenAPIDefinition(
    info = @Info(title = "Cruise Control API", version = "1.0.0",
        description = "Cruise-control is the first of its kind to fully automate the dynamic workload rebalance and self-healing of a kafka cluster. "
            + "It provides great value to Kafka users by simplifying the operation of Kafka clusters."),
    servers = @Server(url = "/{urlPrefix}", variables = @ServerVariable(name = "urlPrefix", defaultValue = "kafkacruisecontrol",
        description = "REST API default url prefix, configured by webserver.api.urlprefix")),
    security = @SecurityRequirement(name = "basicAuth"))
@SecurityScheme(name = "basicAuth", type = SecuritySchemeType.HTTP, scheme = "basic",
    description = "HTTP Basic authentication. Enable via webserver.security.enable=true and configure credentials "
        + "with webserver.auth.credentials.file in cruisecontrol.properties.")
@SecurityScheme(name = "jwtAuth", type = SecuritySchemeType.HTTP, scheme = "bearer", bearerFormat = "JWT",
    description = "JWT authentication. Enable via webserver.security.enable=true and "
        + "webserver.security.provider=com.linkedin.kafka.cruisecontrol.servlet.security.jwt.JwtSecurityProvider.")
public class KafkaCruiseControlEndPoints {
    private final AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
    private final KafkaCruiseControlConfig _config;
    private final UserTaskManager _userTaskManager;
    private final ThreadLocal<Integer> _asyncOperationStep;
    private final Map<EndPoint, Meter> _requestMeter = new HashMap<>();
    private final Map<EndPoint, Timer> _successfulRequestExecutionTimer = new HashMap<>();
    private final boolean _twoStepVerification;
    private final Purgatory _purgatory;
    private final UserPermissionsManager _userPermissionsManager;

    public KafkaCruiseControlEndPoints(AsyncKafkaCruiseControl asynckafkaCruiseControl,
                                       MetricRegistry dropwizardMetricRegistry) {
        _config = asynckafkaCruiseControl.config();
        _asyncKafkaCruiseControl = asynckafkaCruiseControl;
        _twoStepVerification = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
        _purgatory = _twoStepVerification ? new Purgatory(_config) : null;
            _userTaskManager = new UserTaskManager(_config, dropwizardMetricRegistry, _successfulRequestExecutionTimer, _purgatory);
        _asyncKafkaCruiseControl.setUserTaskManagerInExecutor(_userTaskManager);
        _asyncOperationStep = new ThreadLocal<>();
        _asyncOperationStep.set(0);
        _userPermissionsManager = new UserPermissionsManager(_config);

        for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
            _requestMeter.put(endpoint, dropwizardMetricRegistry.meter(
                    MetricRegistry.name(KAFKA_CRUISE_CONTROL_SERVLET_SENSOR, endpoint.name() + "-request-rate")));
            _successfulRequestExecutionTimer.put(endpoint, dropwizardMetricRegistry.timer(
                    MetricRegistry.name(KAFKA_CRUISE_CONTROL_SERVLET_SENSOR, endpoint.name() + "-successful-request-execution-timer")));
        }
    }

    public AsyncKafkaCruiseControl asyncKafkaCruiseControl() {
        return _asyncKafkaCruiseControl;
    }

    public Map<EndPoint, Timer> successfulRequestExecutionTimer() {
        return Collections.unmodifiableMap(_successfulRequestExecutionTimer);
    }

    public ThreadLocal<Integer> asyncOperationStep() {
        return _asyncOperationStep;
    }

    public Purgatory purgatory() {
        return _purgatory;
    }

    public UserTaskManager userTaskManager() {
        return _userTaskManager;
    }

    public KafkaCruiseControlConfig config() {
        return _config;
    }

    public Map<EndPoint, Meter> requestMeter() {
        return _requestMeter;
    }

    /**
     *
     * @return is_twoStepVerification
     */
    public boolean twoStepVerification() {
        return _twoStepVerification;
    }

    /**
     * Destroys the UserTaskManager and the Purgatory.
     */
    public void destroy() {
        _userTaskManager.close();
        if (_purgatory != null) {
            _purgatory.close();
        }
    }

    public List<UserTaskManager.UserTaskInfo> getAllUserTasks() {
        return _userTaskManager.getAllUserTasks();
    }
    /**
     * @return the user permissions manager object
     */
    public UserPermissionsManager getUserPermissionsManager() {
        return _userPermissionsManager;
    }
}
