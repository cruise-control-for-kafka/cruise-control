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

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyState;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.purgatory.ReviewStatus;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.ClusterLoadRequest;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverterContextImpl;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.Schema;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class CruiseControlOpenApiReaderTest {
  @Test
  public void testResponseEnumsFollowJavaDefinitions() {
    OpenAPI api = new CruiseControlOpenApiReader().read(Set.of(), Map.of());
    assertResponseEnum(api, "ExecutorState", "state", ExecutorState.State.values());
    assertResponseEnum(api, "ExecutionTask", "type", ExecutionTask.TaskType.values());
    assertResponseEnum(api, "ExecutionTask", "state", ExecutionTaskState.values());
    assertResponseEnum(api, "AnomalyDetails", "status", AnomalyState.Status.values());
    assertResponseEnum(api, "LoadMonitorState", "state", LoadMonitorTaskRunnerState.values());
    assertResponseEnum(api, "OptimizerResult", "provisionStatus", ProvisionStatus.values());
    assertResponseEnum(api, "RequestInfo", "Status", ReviewStatus.values());
    assertResponseEnum(api, "SingleBrokerStats", "BrokerState", Broker.State.values());
    assertResponseEnum(api, "UserTaskInfo", "Status", UserTaskManager.TaskState.values());
  }

  @Test
  public void testParameterEnumsFollowParserDefinitions() {
    OpenAPI api = new CruiseControlOpenApiReader().read(Set.of(), Map.of());
    Operation admin = api.getPaths().get("/admin").getPost();
    assertParameterEnum(admin, "disable_self_healing_for", KafkaAnomalyType.values(), false);
    assertParameterEnum(admin, "enable_self_healing_for", KafkaAnomalyType.values(), false);
    assertParameterEnum(admin, "disable_concurrency_adjuster_for", ConcurrencyType.values(), false);
    assertParameterEnum(admin, "enable_concurrency_adjuster_for", ConcurrencyType.values(), false);
    assertParameterEnum(api.getPaths().get("/state").getGet(), "substates", CruiseControlState.SubState.values(), true);
    assertParameterEnum(api.getPaths().get("/partition_load").getGet(), "resource", Resource.values(), true);
    assertParameterEnum(api.getPaths().get("/proposals").getGet(), "data_from", ParameterUtils.DataFrom.values(), false);
    Schema dataFrom = api.getPaths().get("/proposals").getGet().getParameters().stream()
        .filter(p -> "data_from".equals(p.getName())).findFirst().orElseThrow().getSchema();
    assertEquals(ParameterUtils.DataFrom.VALID_WINDOWS.name(), dataFrom.getDefault());
  }

  @Test
  public void testMissingValueMetadataAndUnknownReferencesFailGeneration() {
    JsonResponseModelConverter converter = new JsonResponseModelConverter(new Components().schemas(new java.util.HashMap<>()));
    ModelConverterContextImpl context = new ModelConverterContextImpl(converter);
    IllegalStateException missing = assertThrows(IllegalStateException.class,
        () -> context.resolve(new AnnotatedType(UndocumentedValue.class)));
    assertTrue(missing.getMessage().contains("Missing JSON value schema"));
    IllegalStateException unknown = assertThrows(IllegalStateException.class,
        () -> converter.resolveReferences(new Schema().$ref("#/components/schemas/UnknownResponse"), context));
    assertTrue(unknown.getMessage().contains("Unknown JSON response schema"));
  }

  @Test
  public void testContextClassLoaderIsRestoredAfterSuccessAndFailure() {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    ClassLoader sentinel = new ClassLoader(previous) { };
    thread.setContextClassLoader(sentinel);
    try {
      CruiseControlOpenApiReader.withContextClassLoader(() -> {
        assertSame(CruiseControlOpenApiReader.class.getClassLoader(), thread.getContextClassLoader());
        return null;
      });
      assertSame(sentinel, thread.getContextClassLoader());
      assertThrows(IllegalStateException.class, () -> CruiseControlOpenApiReader.withContextClassLoader(() -> {
        throw new IllegalStateException("Generation failed");
      }));
      assertSame(sentinel, thread.getContextClassLoader());
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @Test
  public void testParameterWireShapes() {
    OpenAPI api = new CruiseControlOpenApiReader().read(new CruiseControlOpenApiScanner().classes(), Map.of());
    assertEquals(Set.of("brokerid_and_logdirs", "dryrun", "json", "reason", "stop_ongoing_execution"),
                 api.getPaths().get("/remove_disks").getPost().getParameters().stream().filter(p -> "query".equals(p.getIn()))
                     .map(p -> p.getName()).collect(Collectors.toSet()));
    Schema partition = api.getPaths().get("/partition_load").getGet().getParameters().stream()
        .filter(p -> "partition".equals(p.getName())).findFirst().orElseThrow().getSchema();
    assertEquals("integer", ((Schema) partition.getOneOf().get(0)).getType());
    Object goalsExample = api.getPaths().get("/proposals").getGet().getParameters().stream()
        .filter(p -> "goals".equals(p.getName())).findFirst().orElseThrow().getSchema().getExample();
    assertEquals("[\"RackAwareGoal\",\"ReplicaCapacityGoal\",\"ReplicaDistributionGoal\"]", goalsExample.toString());
  }

  @Test
  public void testAllRegisteredEndpointsAreDocumented() {
    OpenAPI api = new CruiseControlOpenApiReader().read(Set.of(), Map.of());
    assertEquals(CruiseControlEndPoint.cachedValues().size(), api.getPaths().size());
    assertEquals(api.getPaths().size(), api.getPaths().values().stream().flatMap(path -> path.readOperations().stream())
        .map(Operation::getOperationId).distinct().count());
    assertEquals("basic", api.getComponents().getSecuritySchemes().get("basicAuth").getScheme());
    assertEquals("bearer", api.getComponents().getSecuritySchemes().get("jwtAuth").getScheme());
    Schema failedDisks = (Schema) api.getComponents().getSchemas().get("AnomalyDetails").getProperties().get("failedDisksByTimeMs");
    Schema failureTimes = (Schema) failedDisks.getAdditionalProperties();
    if (failureTimes.get$ref() != null) {
      failureTimes = api.getComponents().getSchemas().get(failureTimes.get$ref().substring("#/components/schemas/".length()));
    }
    assertEquals("int64", ((Schema) failureTimes.getAdditionalProperties()).getFormat());
    assertTrue(failureTimes.getProperties() == null || failureTimes.getProperties().isEmpty());
    Schema review = (Schema) api.getComponents().getSchemas().get("ReviewResult").getProperties().get("RequestInfo");
    assertEquals("array", review.getType());
  }

  @Test
  public void testLoadContractFromAnnotations() {
    OpenAPI api = new CruiseControlOpenApiReader().read(Set.of(ClusterLoadRequest.class), Map.of());
    Operation load = api.getPaths().get("/load").getGet();
    assertEquals("load", load.getOperationId());
    assertEquals(Set.of("start", "end", "time", "allow_capacity_estimation", "json", "populate_disk_info",
                        "capacity_only", "get_response_schema", "doAs", "reason"),
                 load.getParameters().stream().filter(p -> "query".equals(p.getIn())).map(p -> p.getName()).collect(Collectors.toSet()));
    assertEquals(Set.of("200", "202", "default"), load.getResponses().keySet());
    assertEquals(Boolean.FALSE, load.getParameters().stream().filter(p -> "json".equals(p.getName())).findFirst().orElseThrow()
                                   .getSchema().getDefault());
    assertEquals(-1L, load.getParameters().stream().filter(p -> "start".equals(p.getName())).findFirst().orElseThrow()
                         .getSchema().getDefault());
    assertEquals("uuid", load.getParameters().stream().filter(p -> "header".equals(p.getIn())).findFirst().orElseThrow()
                            .getSchema().getFormat());
    assertTrue(load.getResponses().get("202").getHeaders().containsKey("User-Task-ID"));
    Schema brokers = api.getComponents().getSchemas().get("BrokerStats");
    assertEquals("array", ((Schema) brokers.getProperties().get("brokers")).getType());
    Schema broker = api.getComponents().getSchemas().get("SingleBrokerStats");
    assertEquals("double", ((Schema) broker.getProperties().get("CpuPct")).getFormat());
    Schema progress = api.getComponents().getSchemas().get("OperationProgress");
    assertFalse(progress.getProperties().containsKey("version"));
    assertEquals(Set.of("operation", "operationProgress"), progress.getProperties().keySet());
  }

  @JsonResponseClass
  private static class UndocumentedValue {
    @JsonResponseField
    private static final String VALUE = "value";
  }

  private static void assertResponseEnum(OpenAPI api, String model, String property, Enum<?>[] values) {
    Schema schema = (Schema) api.getComponents().getSchemas().get(model).getProperties().get(property);
    assertEquals(model + "." + property, Arrays.stream(values).map(Object::toString).collect(Collectors.toSet()),
                 schema.getEnum() == null ? null : Set.copyOf(schema.getEnum()));
  }

  private static void assertParameterEnum(Operation operation, String name, Enum<?>[] values, boolean lowercase) {
    Schema schema = operation.getParameters().stream().filter(p -> name.equals(p.getName())).findFirst().orElseThrow().getSchema();
    if ("array".equals(schema.getType())) {
      schema = schema.getItems();
    }
    Set<String> expected = Arrays.stream(values).map(Enum::name)
        .map(value -> lowercase ? value.toLowerCase(Locale.ROOT) : value).collect(Collectors.toSet());
    assertEquals(operation.getOperationId() + "." + name, expected, schema.getEnum() == null ? null : Set.copyOf(schema.getEnum()));
  }
}
