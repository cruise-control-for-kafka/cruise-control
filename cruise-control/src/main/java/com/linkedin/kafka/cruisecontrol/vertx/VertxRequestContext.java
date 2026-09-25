/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.http.CruiseControlHttpSession;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.ROUTING_CONTEXT_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager.USER_TASK_HEADER_NAME;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.getJsonSchema;

public class VertxRequestContext implements CruiseControlRequestContext {

    protected RoutingContext _context;

    private final CruiseControlHttpSession _session;
    private final KafkaCruiseControlConfig _config;

    public VertxRequestContext(RoutingContext context, KafkaCruiseControlConfig config) {
        super();
        _context = context;
        _config = config;
        _session = new VertxSession(context.session());
    }

    @Override
    public String getRequestURL() {
        return String.format("%s %s", _context.request().method(), _context.request().uri().split("\\?")[0]);
    }

    @Override
    public String getUserTaskIdString() {
        return _context.request().getHeader(USER_TASK_HEADER_NAME);
    }

    @Override
    public String getMethod() {
        return _context.request().method().toString();
    }

    @Override
    public String getPathInfo() {
        String path = _context.request().path();
        String basePath = _context.mountPoint();
        if (basePath != null && path.startsWith(basePath)) {
            path = path.substring(basePath.length());
            if (!path.isEmpty() && !path.startsWith("/")) {
                path = "/" + path;
            }
        }
        return path.isEmpty() ? null : path;
    }

    @Override
    public String getClientIdentity() {
        return getVertxClientIpAddress(_context);
    }

    @Override
    public MultiMap getHeaders() {
        return _context.request().headers();
    }

    @Override
    public String getHeader(String header) {
        return _context.request().getHeader(header);
    }

    @Override
    public String getRemoteAddr() {
        return _context.request().remoteAddress().toString().split(":")[0];
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        return getVertxQueryParamsMap(_context);
    }

    @Override
    public String getRequestUri() {
        return _context.request().uri();
    }

    @Override
    public String getParameter(String parameter) {
        return _context.queryParams().get(parameter);
    }

    @Override
    public void writeResponseToOutputStream(int responseCode, boolean json, boolean wantJsonSchema,
                                            String responseMessage) throws IOException {
        ResponseUtils.setResponseCode(_context, responseCode, _config);
        _context.response().putHeader("Cruise-Control-Version", KafkaCruiseControl.cruiseControlVersion());
        _context.response().putHeader("Cruise-Control-Commit_Id", KafkaCruiseControl.cruiseControlCommitId());
        if (json && wantJsonSchema) {
            _context.response().putHeader("Cruise-Control-JSON-Schema", getJsonSchema(responseMessage));
        }
        _context.response()
                .end(responseMessage);
    }

    @Override
    public CruiseControlHttpSession getSession() {
        return _session;
    }

    @Override
    public String getRequestURI() {
        return _context.request().uri().split("\\?")[0];
    }

    @Override
    public Map<String, Object> getJson() {
        Gson gson = new Gson();
        return gson.fromJson(_context.getBodyAsString(), Map.class);
    }

    @Override
    public void setHeader(String key, String value) {
        _context.response().putHeader(key, value);
    }

    @Override
    public Map<String, Object> getParameterConfigOverrides() {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(KAFKA_CRUISE_CONTROL_HTTP_SERVLET_REQUEST_OBJECT_CONFIG, _context.request());
        overrides.put(ROUTING_CONTEXT_OBJECT_CONFIG, _context);
        return overrides;
    }

    @Override
    public String getUserPrincipal() {
        JsonObject userPrincipal = _context.user().principal();
        return userPrincipal == null
                ? "null"
                : userPrincipal.getString("username");
    }

    /**
     * Makes a Map from the query parameters of RoutingContext.
     *
     * @param context The routing context
     * @return Returns the query parameter map
     */
    public static Map<String, String[]> getVertxQueryParamsMap(RoutingContext context) {
        Map<String, String[]> queryParamsMap = new HashMap<>();
        for (Map.Entry<String, String> entry : context.queryParams().entries()) {
            queryParamsMap.put(entry.getKey(), new String[]{entry.getValue()});
        }
        return queryParamsMap;
    }

    private String getVertxClientIpAddress(RoutingContext context) {
        for (String header : HEADERS_TO_TRY) {
            String ip = context.request().getHeader(header);
            if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
                return "[" + ip + "]";
            }
        }
        return "[" + context.request().remoteAddress().host() + "]";
    }
}
