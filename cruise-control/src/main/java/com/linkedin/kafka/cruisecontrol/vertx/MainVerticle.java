/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.vertx;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.sstore.LocalSessionStore;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {

  public static final String APPLICATION_JSON = "application/json";
  private final int _port;
  private final String _host;
  private HttpServer _server;
  private VertxRequestHandler _endPoints;
  private final AsyncKafkaCruiseControl _asynckafkaCruiseControl;
  private final MetricRegistry _dropwizardMetricRegistry;

  public MainVerticle(AsyncKafkaCruiseControl asynckafkaCruiseControl, MetricRegistry dropwizardMetricRegistry, int port, String host) {
    _port = port;
    _host = host;
    _asynckafkaCruiseControl = asynckafkaCruiseControl;
    _dropwizardMetricRegistry = dropwizardMetricRegistry;

  }

  public HttpServer getServer() {
    return _server;
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    _endPoints = new VertxRequestHandler(_asynckafkaCruiseControl, _dropwizardMetricRegistry);

    RouterBuilder.create(vertx, Objects.requireNonNull(this.getClass().getClassLoader().getResource("yaml/base.yaml")).toString(), asyncResult -> {
      if (!asyncResult.succeeded()) {
        throw new RuntimeException(asyncResult.cause());
      } else {
        _server = vertx.createHttpServer(createOptions());
        _server.requestHandler(buildRouter(asyncResult.result()));
        _server.listen(result -> {
          if (result.succeeded()) {
            startPromise.complete();
          } else {
            startPromise.fail(result.cause());
          }
        });
      }
    });
  }

  @Override
  public void stop(Promise<Void> promise) {
    _endPoints.destroy();
    if (_server == null) {
      return;
    }
    _server.close(v -> promise.complete());
  }

  private Router buildRouter(RouterBuilder builder) {
    if (!_asynckafkaCruiseControl.config().getBoolean(WebServerConfig.WEBSERVER_SECURITY_ENABLE_CONFIG)) {
      builder.securityHandler("basicAuth", RoutingContext::next);
      builder.securityHandler("jwtAuth", RoutingContext::next);
    }
    builder.operation("state").handler(_endPoints::handle);
    builder.operation("kafkaClusterState").handler(_endPoints::handle);
    builder.operation("load").handler(_endPoints::handle);
    builder.operation("userTasks").handler(_endPoints::handle);
    builder.operation("partitionload").handler(_endPoints::handle);
    builder.operation("proposals").handler(_endPoints::handle);
    builder.operation("rebalance").handler(_endPoints::handle);
    builder.operation("addBroker").handler(_endPoints::handle);
    builder.operation("removeBroker").handler(_endPoints::handle);
    builder.operation("fixOfflineReplicas").handler(_endPoints::handle);
    builder.operation("demoteBroker").handler(_endPoints::handle);
    builder.operation("stopProposalExecution").handler(_endPoints::handle);
    builder.operation("pauseSampling").handler(_endPoints::handle);
    builder.operation("resumeSampling").handler(_endPoints::handle);
    builder.operation("topicConfiguration").handler(_endPoints::handle);
    builder.operation("admin").handler(_endPoints::handle);
    builder.operation("rightsize").handler(_endPoints::handle);
    builder.operation("permissions").handler(_endPoints::handle);
    builder.rootHandler(StaticHandler
            .create()
            .setCachingEnabled(false)
            .setWebRoot("webroot/"));
    Router router = builder.createRouter();

    Set<String> allowedHeaders = new HashSet<>();
    allowedHeaders.add("auth");
    allowedHeaders.add("Content-Type");

    Set<HttpMethod> allowedMethods = new HashSet<>();
    allowedMethods.add(HttpMethod.GET);
    allowedMethods.add(HttpMethod.POST);
    allowedMethods.add(HttpMethod.OPTIONS);
    allowedMethods.add(HttpMethod.DELETE);
    allowedMethods.add(HttpMethod.PATCH);
    allowedMethods.add(HttpMethod.PUT);

    Router root = Router.router(vertx);

    root.route().consumes(APPLICATION_JSON);
    root.route().produces(APPLICATION_JSON);
    root.route().handler(BodyHandler.create());

    root.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
    root.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
    root.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));

    root.route().handler(RoutingContext::next);
    root.route().failureHandler(ErrorHandler.create(vertx, true));

    String rootPath = _asynckafkaCruiseControl
            .config()
            .getString(WebServerConfig.WEBSERVER_API_URLPREFIX_CONFIG)
            .trim()
            .replace("/*", "/");
    root.mountSubRouter(rootPath, router);

    return root;
  }

  private HttpServerOptions createOptions() {
    HttpServerOptions options = new HttpServerOptions();
    options.setHost(_host);
    options.setPort(_port);
    return options;
  }

  public VertxRequestHandler getEndPoints() {
    return _endPoints;
  }

}
