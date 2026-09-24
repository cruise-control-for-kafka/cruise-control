/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.cruisecontrol.servlet.handler.Request;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.RequestParameterWrapper;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REVIEW_BOARD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.getValidEndpoint;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.handleConfigException;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.handleException;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.handleUserRequestException;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.requestParameterFor;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.hasValidParameterNames;

public class KafkaCruiseControlRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlRequestHandler.class);
    private final KafkaCruiseControlEndPoints _cruiseControlEndPoints;

    public KafkaCruiseControlRequestHandler(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
        _cruiseControlEndPoints = new KafkaCruiseControlEndPoints(asynckafkaCruiseControl, dropwizardMetricRegistry);
    }

    /**
     * Shuts down the handler.
     */
    public void destroy() {
        _cruiseControlEndPoints.destroy();
    }

    /**
     * Handles the request.
     * @param context is the request context that will provide any information for handling it.
     * @throws IOException
     */
    public void doGetOrPost(CruiseControlRequestContext context) throws IOException {
        try {
            _cruiseControlEndPoints.asyncOperationStep().set(0);
            CruiseControlEndPoint endPoint = getValidEndpoint(context);
            if (endPoint != null) {
                _cruiseControlEndPoints.requestMeter().get(endPoint).mark();
                Map<String, Object> requestConfigOverrides = new HashMap<>();
                requestConfigOverrides.put(KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG, this);

                Map<String, Object> parameterConfigOverrides = new HashMap<>();
                parameterConfigOverrides.putAll(context.getParameterConfigOverrides());
                parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG, _cruiseControlEndPoints.config());
                switch (context.getMethod()) {
                    case GET_METHOD:
                        handleGet(context, endPoint, requestConfigOverrides, parameterConfigOverrides);
                        break;
                    case POST_METHOD:
                        handlePost(context, endPoint, requestConfigOverrides, parameterConfigOverrides);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported request method: " + context.getMethod() + ".");
                }
            }
        } catch (UserRequestException ure) {
            String errorMessage = handleUserRequestException(ure, context);
            LOG.error(errorMessage, ure);
        } catch (ConfigException ce) {
            String errorMessage = handleConfigException(ce, context);
            LOG.error(errorMessage, ce);
        } catch (Exception e) {
            String errorMessage = handleException(e, context);
            LOG.error(errorMessage, e);
        }
    }

    private void handleGet(CruiseControlRequestContext requestContext,
                           CruiseControlEndPoint endPoint,
                           Map<String, Object> requestConfigOverrides,
                           Map<String, Object> parameterConfigOverrides)
            throws Exception {
        // Sanity check: if the request is for REVIEW_BOARD, two step verification must be enabled.
        if (endPoint == REVIEW_BOARD && _cruiseControlEndPoints.twoStepVerification()) {
            throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                    endPoint, WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
        }
        RequestParameterWrapper requestParameter = requestParameterFor(endPoint);
        CruiseControlParameters parameters = _cruiseControlEndPoints.config().getConfiguredInstance(requestParameter.parametersClass(),
                CruiseControlParameters.class,
                parameterConfigOverrides);
        if (hasValidParameterNames(requestContext, parameters)) {
            requestConfigOverrides.put(requestParameter.parameterObject(), parameters);
            Request ccRequest = _cruiseControlEndPoints.config().getConfiguredInstance(requestParameter.requestClass(),
                    Request.class, requestConfigOverrides);

            ccRequest.handle(requestContext);
        }
    }

    private void handlePost(CruiseControlRequestContext requestContext,
                            CruiseControlEndPoint endPoint,
                            Map<String, Object> requestConfigOverrides,
                            Map<String, Object> parameterConfigOverrides)
            throws Exception {
        CruiseControlParameters parameters;
        RequestParameterWrapper requestParameter = requestParameterFor(endPoint);
        if (endPoint == REVIEW) {
            // Sanity check: if the request is for REVIEW, two step verification must be enabled.
            if (_cruiseControlEndPoints.twoStepVerification()) {
                throw new ConfigException(String.format("Attempt to access %s endpoint without enabling '%s' config.",
                        endPoint, WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG));
            }

            parameters = _cruiseControlEndPoints.config().getConfiguredInstance(requestParameter.parametersClass(),
                    CruiseControlParameters.class, parameterConfigOverrides);
            if (!hasValidParameterNames(requestContext, parameters)) {
                return;
            }
        } else if (_cruiseControlEndPoints.twoStepVerification()) {
            // Do not add to the purgatory if the two-step verification is disabled.
            parameters = _cruiseControlEndPoints.config().getConfiguredInstance(requestParameter.parametersClass(),
                    CruiseControlParameters.class, parameterConfigOverrides);
            if (!hasValidParameterNames(requestContext, parameters)) {
                return;
            }
        } else {
            // Add to the purgatory if the two-step verification is enabled.
            parameters = _cruiseControlEndPoints.purgatory().maybeAddToPurgatory(requestContext, requestParameter.parametersClass(),
                    parameterConfigOverrides, _cruiseControlEndPoints.userTaskManager());
        }

        Request ccRequest = null;
        if (parameters != null) {
            requestConfigOverrides.put(requestParameter.parameterObject(), parameters);
            ccRequest = _cruiseControlEndPoints.config().getConfiguredInstance(requestParameter.requestClass(),
                    Request.class, requestConfigOverrides);
        }

        if (ccRequest != null) {
            // ccRequest would be null if request is added to Purgatory.
            ccRequest.handle(requestContext);
        }
    }

    public KafkaCruiseControlEndPoints cruiseControlEndPoints() {
        return _cruiseControlEndPoints;
    }
}
