/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.easymock.EasyMock;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.OPENAPI_SPEC_PATH;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.requestParameterFor;
import static com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager.USER_TASK_HEADER_NAME;

public class RequestParameterTest {
  private Map<String, CruiseControlParameters> _endpointToClass;
  private OpenAPI _openAPI;
  /**
   * Specify endpoints to be tested
   */
  @Before
  public void setupParameterClasses() throws Exception {
    _endpointToClass = new HashMap<>();
    KafkaCruiseControlConfig defaultConfig = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
      _endpointToClass.put("/" + endpoint.toString().toLowerCase(),
                           ((CruiseControlParameters) (defaultConfig.getClass(requestParameterFor(endpoint).parametersClass()).newInstance())));
    }
  }

  /**
   * Load the OpenAPI files for endpoints and compares them against the source code
   */
  @Test
  public void checkOpenApiSpec() {
    _openAPI = loadOpenAPI();
    Map<String, Set<String>> schema = parseSchema();
    Assert.assertEquals(schema.keySet(), _endpointToClass.keySet());
    for (Map.Entry<String, Set<String>> endpoint: schema.entrySet()) {
      Assert.assertTrue(_endpointToClass.containsKey(endpoint.getKey()));
      CruiseControlParameters endpointParams = _endpointToClass.get(endpoint.getKey());
      Assert.assertEquals(endpoint.getValue(), endpointParams.caseInsensitiveParameterNames());
    }
  }

  private OpenAPI loadOpenAPI() {
    OpenAPIV3Parser openApiParser = new OpenAPIV3Parser();
    ParseOptions options = new ParseOptions();
    options.setResolveFully(true);
    options.setFlatten(true);

    SwaggerParseResult parseResult = openApiParser.readLocation(OPENAPI_SPEC_PATH, null, options);
    return parseResult.getOpenAPI();
  }

  /**
   * Check that task identifiers are documented separately from query parameters.
   */
  @Test
  public void checkUserTaskHeaders() {
    for (PathItem path : loadOpenAPI().getPaths().values()) {
      for (Operation operation : path.readOperations()) {
        Parameter taskId = operation.getParameters().stream()
            .filter(parameter -> "header".equals(parameter.getIn()) && USER_TASK_HEADER_NAME.equals(parameter.getName()))
            .findFirst().orElse(null);
        Assert.assertNotNull(operation.getOperationId(), taskId);
        Assert.assertFalse(Boolean.TRUE.equals(taskId.getRequired()));
        Assert.assertEquals("string", taskId.getSchema().getType());
        Assert.assertEquals("uuid", taskId.getSchema().getFormat());
        operation.getResponses().forEach((status, response) -> {
          if (status.startsWith("2")) {
            Assert.assertNotNull(operation.getOperationId() + " HTTP " + status, response.getHeaders());
            Header header = response.getHeaders().get(USER_TASK_HEADER_NAME);
            Assert.assertNotNull(operation.getOperationId() + " HTTP " + status, header);
            Assert.assertEquals("string", header.getSchema().getType());
            Assert.assertEquals("uuid", header.getSchema().getFormat());
          }
        });
      }
    }
  }

  /**
   * Check that documented disk-list examples can be decoded by the request parser.
   */
  @Test
  public void checkDiskListWireFormat() {
    OpenAPI api = loadOpenAPI();
    for (String endpoint : List.of("/demote_broker", "/remove_disks")) {
      Parameter disks = api.getPaths().get(endpoint).getPost().getParameters().stream()
          .filter(parameter -> ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM.equals(parameter.getName())).findFirst().orElseThrow();
      Assert.assertEquals(endpoint, "array", disks.getSchema().getType());
      Assert.assertEquals(endpoint, "string", ((ArraySchema) disks.getSchema()).getItems().getType());
      Assert.assertEquals(Parameter.StyleEnum.FORM, disks.getStyle());
      Assert.assertEquals(Boolean.FALSE, disks.getExplode());
      String example = (String) disks.getExample();
      Assert.assertNotNull(endpoint, example);
      CruiseControlRequestContext request = EasyMock.mock(CruiseControlRequestContext.class);
      EasyMock.expect(request.getParameterMap()).andReturn(Map.of(ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM, new String[]{example}));
      EasyMock.expect(request.getParameter(ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM)).andReturn(example);
      EasyMock.replay(request);
      Assert.assertEquals(Map.of(101, Set.of("/tmp/kafka-logs-1", "/tmp/kafka-logs-2")), ParameterUtils.brokerIdAndLogdirs(request));
      EasyMock.verify(request);
    }
  }

  /**
   * Return the list of parameters given the path item (an endpoint)
   *
   * @param pathItem Endpoint defined as a PathItem object
   * @return set of parameters for the specified endpoint
   */
  public static Set<String> parseEndpoint(PathItem pathItem) throws IllegalArgumentException {
    List<Parameter> parameterList;
    Set<String> parameterSet = new TreeSet<>();
    if (pathItem.getGet() != null) {
      parameterList = pathItem.getGet().getParameters();
    } else if (pathItem.getPost() != null) {
      parameterList = pathItem.getPost().getParameters();
    } else {
      throw new IllegalArgumentException("Schema Parser does not support HTTP methods other than GET/POST");
    }

    for (Parameter parameter : parameterList) {
      if (!"query".equals(parameter.getIn())) {
        continue;
      }
      Assert.assertFalse(parameterSet.contains(parameter.getName()));
      parameterSet.add(parameter.getName());
    }
    return parameterSet;
  }

  /**
   * parse the entire schema defined in yaml file and return a map
   *
   * @return a map of endpoints to its corresponding set of parameters
   */
  public Map<String, Set<String>> parseSchema() {
    Map<String, Set<String>> parsedSchema = new HashMap<>();
    Paths paths = _openAPI.getPaths();
    for (Map.Entry<String, PathItem> endpoint : paths.entrySet()) {
      parsedSchema.put(endpoint.getKey(), parseEndpoint(endpoint.getValue()));
    }
    return parsedSchema;
  }
}
