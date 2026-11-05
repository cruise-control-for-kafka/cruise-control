/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.kafka.cruisecontrol.openapi;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import com.linkedin.kafka.cruisecontrol.servlet.ServletRequestContext;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.response.ProgressResult;
import com.linkedin.kafka.cruisecontrol.vertx.VertxRequestContext;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.OPENAPI_SPEC_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class OpenApiRuntimeTest {
  @Test
  public void testGeneratedContractRoutesAndValidatesVertxRequests() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      RouterBuilder builder = RouterBuilder.create(vertx, OPENAPI_SPEC_PATH).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      builder.securityHandler("basicAuth", RoutingContext::next);
      builder.securityHandler("jwtAuth", RoutingContext::next);
      builder.operation("load").handler(context -> {
        try {
          progress().writeSuccessResponse(jsonParameters(), new VertxRequestContext(context, null));
        } catch (IOException e) {
          context.fail(e);
        }
      });
      builder.operation("demoteBroker").handler(context -> context.response().setStatusCode(204).end());
      builder.operation("partitionload").handler(context -> context.response().setStatusCode(204).end());
      builder.operation("state").handler(context -> context.response().setStatusCode(204).end());
      builder.operation("admin").handler(context -> context.response().setStatusCode(204).end());
      HttpServer server = vertx.createHttpServer().requestHandler(builder.createRouter()).listen(0, "127.0.0.1")
          .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      String base = "http://127.0.0.1:" + server.actualPort();
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
      HttpResponse<String> load = client.send(HttpRequest.newBuilder(URI.create(base + "/load?json=true&start=0"))
                                                  .timeout(Duration.ofSeconds(10)).GET().build(),
                                              HttpResponse.BodyHandlers.ofString());
      assertEquals(202, load.statusCode());
      assertProgressBody(load.body());
      HttpResponse<String> invalid = client.send(HttpRequest.newBuilder(URI.create(base + "/load?start=-2"))
                                                     .timeout(Duration.ofSeconds(10)).GET().build(),
                                                 HttpResponse.BodyHandlers.ofString());
      assertEquals(400, invalid.statusCode());
      HttpResponse<String> demotion = client.send(
          HttpRequest.newBuilder(URI.create(base + "/demote_broker?brokerid_and_logdirs=101-/tmp/kafka-logs-1,102-/tmp/kafka-logs-2"))
              .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
      assertEquals(204, demotion.statusCode());
      for (String path : List.of("/state?substates=executor", "/partition_load?resource=nw_in")) {
        HttpResponse<String> validEnum = client.send(HttpRequest.newBuilder(URI.create(base + path))
            .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(path, 204, validEnum.statusCode());
      }
      String admin = base + "/admin?enable_concurrency_adjuster_for=LEADERSHIP_BROKER&enable_self_healing_for=MAINTENANCE_EVENT";
      HttpResponse<String> validAdmin = client.send(HttpRequest.newBuilder(URI.create(admin))
          .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
      assertEquals(204, validAdmin.statusCode());
      HttpResponse<String> invalidConcurrency = client.send(
          HttpRequest.newBuilder(URI.create(base + "/admin?enable_concurrency_adjuster_for=LEADERSHIP"))
              .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
      assertEquals(400, invalidConcurrency.statusCode());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testServletProgressMatchesGeneratedContract() throws Exception {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    ServletOutputStream output = new ServletOutputStream() {
      @Override
      public void write(int value) {
        body.write(value);
      }

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setWriteListener(WriteListener listener) {
      }
    };
    HttpServletRequest request = EasyMock.niceMock(HttpServletRequest.class);
    HttpServletResponse response = EasyMock.niceMock(HttpServletResponse.class);
    Capture<Integer> status = Capture.newInstance();
    EasyMock.expect(response.getOutputStream()).andReturn(output);
    response.setStatus(EasyMock.captureInt(status));
    EasyMock.expectLastCall();
    EasyMock.replay(request, response);
    progress().writeSuccessResponse(jsonParameters(), new ServletRequestContext(request, response, null));
    assertEquals(202, status.getValue().intValue());
    assertProgressBody(body.toString(StandardCharsets.UTF_8));
  }

  private static ProgressResult progress() {
    OperationFuture future = new OperationFuture("Getting load");
    future.operationProgress().addStep(new Pending());
    return new ProgressResult(List.of(future), null);
  }

  private static CruiseControlParameters jsonParameters() {
    CruiseControlParameters parameters = EasyMock.niceMock(CruiseControlParameters.class);
    EasyMock.expect(parameters.json()).andStubReturn(true);
    EasyMock.replay(parameters);
    return parameters;
  }

  private static void assertProgressBody(String body) {
    OpenAPI api = new OpenAPIV3Parser().read(OPENAPI_SPEC_PATH);
    JsonObject response = new JsonObject(body);
    assertEquals(api.getComponents().getSchemas().get("ProgressResult").getProperties().keySet(), response.fieldNames());
    JsonObject operation = response.getJsonArray("progress").getJsonObject(0);
    assertEquals("Getting load", operation.getString("operation"));
    assertFalse(operation.containsKey("version"));
    assertEquals(api.getComponents().getSchemas().get("OperationProgress").getProperties().keySet(), operation.fieldNames());
  }
}
