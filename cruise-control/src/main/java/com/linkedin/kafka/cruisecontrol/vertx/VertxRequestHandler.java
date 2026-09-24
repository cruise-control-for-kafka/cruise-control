/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlRequestHandler;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;

public class VertxRequestHandler {

    private final KafkaCruiseControlRequestHandler _requestHandler;

    public VertxRequestHandler(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
        _requestHandler = new KafkaCruiseControlRequestHandler(asynckafkaCruiseControl, dropwizardMetricRegistry);
    }

    /**
     * Shuts down the handler.
     */
    public void destroy() {
        _requestHandler.destroy();
    }

    /**
     * Handles the request
     *
     * @param context is the request
     */
    public void handle(RoutingContext context) {
        try {
            _requestHandler.doGetOrPost(new VertxRequestContext(context, cruiseControlEndPoints().config()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KafkaCruiseControlEndPoints cruiseControlEndPoints() {
        return _requestHandler.cruiseControlEndPoints();
    }
}
