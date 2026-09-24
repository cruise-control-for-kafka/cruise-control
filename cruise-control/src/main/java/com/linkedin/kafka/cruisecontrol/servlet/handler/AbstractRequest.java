/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.cruisecontrol.servlet.handler.Request;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG;


public abstract class AbstractRequest implements Request {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRequest.class);
    protected KafkaCruiseControlRequestHandler _requestHandler;

    /**
     * Handle the request and populate the response.
     *
     */
    @Override
    public void handle(CruiseControlRequestContext requestContext)
            throws Exception {

        if (parameters().parseParameters(requestContext)) {
            LOG.warn("Failed to parse parameters: {} for request: {}.", requestContext.getParameterMap(), requestContext.getPathInfo());
            return;
        }

        CruiseControlResponse ccResponse = getResponse(requestContext);
        ccResponse.writeSuccessResponse(parameters(), requestContext);
    }

    /**
     * Get the response of the request
     *
     * @param requestContext the request context.
     *                       <ul>
     *                         <li>Asynchronous requests return either the final response or the progress of the async request.</li>
     *                         <li>Synchronous requests return the final response of the sync request.</li>
     *                       </ul>
     * @return Response of the requests.
     */
    protected abstract CruiseControlResponse getResponse(CruiseControlRequestContext requestContext)
            throws Exception;

    public abstract CruiseControlParameters parameters();

    @Override
    public void configure(Map<String, ?> configs) {
        _requestHandler = (KafkaCruiseControlRequestHandler) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_REQUEST_HANDLER_OBJECT_CONFIG),
                "Kafka Cruise Control request handler configuration is missing from the request.");
    }
}
