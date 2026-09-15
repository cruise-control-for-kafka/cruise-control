/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.config.AbstractConfig;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;


/**
 * The util class for Kafka Cruise Control response.
 */
public final class ResponseUtils {
  public static final int JSON_VERSION = 1;
  public static final String VERSION = "version";
  @JsonResponseField
  public static final String MESSAGE = "message";

  private ResponseUtils() {
  }

  /**
   *
   * @param response The servlet response.
   * @param code Status code
   * @param json Write the response in JSON or not
   * @param config CruiseControlConfig
   */
  public static void setResponseCode(HttpServletResponse response, int code, boolean json, KafkaCruiseControlConfig config) {
    response.setStatus(code);
    response.setContentType(json ? "application/json" : "text/plain");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    boolean corsEnabled = config != null && config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    if (corsEnabled) {
      // These headers are exposed to the browser
      response.setHeader("Access-Control-Allow-Origin",
                         config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_ORIGIN_CONFIG));
      response.setHeader("Access-Control-Expose-Headers",
                         config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));
      response.setHeader("Access-Control-Allow-Credentials", "true");
    }
  }

  /**
   *
   * @param context The routing context
   * @param code Status code
   * @param config CruiseControlConfig
   */
  public static void setResponseCode(RoutingContext context, int code, AbstractConfig config) {
    context.response().setStatusCode(code);
    boolean corsEnabled = config != null && config.getBoolean(WebServerConfig.WEBSERVER_HTTP_CORS_ENABLED_CONFIG);
    if (corsEnabled) {
      // These headers are exposed to the browser
      context.response().putHeader("Access-Control-Allow-Origin",
              config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_ORIGIN_CONFIG));
      context.response().putHeader("Access-Control-Expose-Headers",
              config.getString(WebServerConfig.WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG));
      context.response().putHeader("Access-Control-Allow-Credentials", "true");
    }
  }

  static String getBaseJsonString(String message) {
    Map<String, Object> jsonResponse = Map.of(VERSION, JSON_VERSION, MESSAGE, message);
    return new Gson().toJson(jsonResponse);
  }

  /**
   * Retrieve stack trace (if any).
   *
   * @param e Exception from which the stack trace will be retrieved.
   * @return Stack trace if the given exception is not {@code null}, empty string otherwise.
   */
  private static String stackTrace(Exception e) {
    if (e == null) {
      return "";
    }

    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * Write error response to the output stream.
   *
   * @param requestContext The request context
   * @param e Exception (if any) corresponding to the error, {@code null} otherwise.
   * @param errorMessage Error message to return in the response message.
   * @param responseCode HTTP Status code to indicate the error.
   * @param json {@code true} if json, {@code false} otherwise.
   * @param wantJsonSchema {@code true} for json error response, {@code false} otherwise.
   */
  public static void writeErrorResponse(CruiseControlRequestContext requestContext,
                                        Exception e,
                                        String errorMessage,
                                        int responseCode,
                                        boolean json,
                                        boolean wantJsonSchema)
      throws IOException {
    String responseMessage;
    ErrorResponse errorResponse = new ErrorResponse(e, errorMessage);
    if (json) {
      Gson gson = new Gson();
      responseMessage = gson.toJson(errorResponse.getJsonStructure());
    } else {
      responseMessage = errorResponse.toString();
    }
    // Send the CORS Task ID header as part of this error response if 2-step verification is enabled.
    requestContext.writeResponseToOutputStream(responseCode, json, wantJsonSchema, responseMessage);
  }

  /**
   *
   * @param responseMessage The response message string
   * @return the JSON schema of the response
   */
  public static String getJsonSchema(String responseMessage) {
    JsonElement response = new JsonParser().parse(responseMessage);
    return convertNodeToStringSchemaNode(response, null);
  }

  private static String convertNodeToStringSchemaNode(JsonElement node, String key) {
    StringBuilder result = new StringBuilder();

    if (key != null) {
      result.append("\"" + key + "\": { \"type\": \"");
    } else {
      result.append("{ \"type\": \"");
    }
    if (node.isJsonArray()) {
      result.append("array\"");
      JsonArray arr = node.getAsJsonArray();
      if (arr.size() > 0) {
        result.append(", \"items\": [");
        // Generate schema based on the first item of the array, since the schema should be consistent between elements in the array.
        result.append(convertNodeToStringSchemaNode(arr.get(0), null));
        result.append("]");
      }
      result.append("}");
    } else if (node.isJsonPrimitive()) {
      if (node.getAsJsonPrimitive().isBoolean()) {
        result.append("boolean\" }");
      } else if (node.getAsJsonPrimitive().isNumber()) {
        result.append("number\" }");
      } else if (node.getAsJsonPrimitive().isString()) {
        result.append("string\" }");
      }
    } else if (node.isJsonObject()) {
      result.append("object\", \"properties\": ");
      result.append("{");
      for (Iterator<Map.Entry<String, JsonElement>> iterator = node.getAsJsonObject().entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<String, JsonElement> entry = iterator.next();
        JsonElement child = entry.getValue();

        result.append(convertNodeToStringSchemaNode(child, entry.getKey()));
        if (iterator.hasNext()) {
          result.append(",");
        }
      }
      result.append("}}");
    } else if (node.isJsonNull()) {
      result.append("}");
    }

    return result.toString();
  }

  @JsonResponseClass
  public static class ErrorResponse {
    @JsonResponseField
    private static final String STACK_TRACE = "stackTrace";
    @JsonResponseField
    private static final String ERROR_MESSAGE = "errorMessage";
    private final Exception _exception;
    private final String _errorMessage;

    ErrorResponse(Exception exception, String errorMessage) {
      _errorMessage = errorMessage;
      _exception = exception;
    }

    @Override
    public String toString() {
      return _errorMessage == null ? "" : _errorMessage;
    }

    /**
     * @return The map describing the error.
     */
    public Map<String, Object> getJsonStructure() {
      return Map.of(VERSION, JSON_VERSION, STACK_TRACE, stackTrace(_exception), ERROR_MESSAGE, _errorMessage);
    }
  }
}
