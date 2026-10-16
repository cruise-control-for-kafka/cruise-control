/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.BooleanSchema;
import io.swagger.v3.oas.models.media.ComposedSchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.OPENAPI_SPEC_PATH;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.CRUISE_CONTROL_PACKAGE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.JSON_CONTENT_TYPE;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.PLAIN_TEXT_CONTENT_TYPE;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Test to check the openAPI spec is consistent with layout defined in Java response classes.
 */
public class ResponseTest {

  private Map<String, Class<?>> _schemaToClass;
  private OpenAPI _openAPI;

  /**
   * Scan all the classes in package {@link com.linkedin.kafka.cruisecontrol} for classes which could be used
   * to build JSON response, which are annotated with {@link JsonResponseClass}.
   *
   * @throws ClassNotFoundException If the definition of the class is not found.
   */
  @Before
  public void scanForResponseClass() throws ClassNotFoundException {
    _schemaToClass = new HashMap<>();
    ScanResult scanResult = new ClassGraph().ignoreClassVisibility()
                                            .enableAnnotationInfo()
                                            .whitelistPackages(CRUISE_CONTROL_PACKAGE)
                                            .scan();
    for (ClassInfo classInfo : scanResult.getClassesWithAnnotation(JsonResponseClass.class.getName())) {
      String className = classInfo.getName();
      _schemaToClass.put(extractSimpleClassName(className), Class.forName(className));
    }
  }

  /**
   * Extract the simple class name from canonical class name.
   *
   * @param className The canonical name.
   * @return The simple class name.
   */
  private String extractSimpleClassName(String className) {
    int startIndex = Math.max(className.lastIndexOf("$"), className.lastIndexOf("."));
    return className.substring(startIndex + 1);
  }

  /**
   * Check the consistency for all POST and GET endpoints defined in OpenAPI spec.
   */
  @Test
  public void checkOpenAPISpec() {
    ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setFlatten(true);
    _openAPI = new OpenAPIV3Parser().read(OPENAPI_SPEC_PATH, null, options);
    for (PathItem path : _openAPI.getPaths().values()) {
      if (path.getGet() != null) {
        checkOperation(path.getGet());
      }
      if (path.getPost() != null) {
        checkOperation(path.getPost());
      }
    }
  }

  /**
   * For each endpoint, check all the possible responses defined in OpenAPI spec.
   *
   * @param operation The OpenAPI operation for certain endpoint.
   */
  private void checkOperation(Operation operation) {
    operation.getResponses().forEach((responseStatus, apiResponse) -> apiResponse.getContent().forEach((header, type) -> {
      switch (header) {
        case JSON_CONTENT_TYPE : String refName = type.getSchema().get$ref();
                                 String className = refName.substring(refName.lastIndexOf('/') + 1);
                                 checkSchema(_openAPI.getComponents().getSchemas().get(className), className, true);
                                 break;
        case PLAIN_TEXT_CONTENT_TYPE : assertTrue(type.getSchema() instanceof StringSchema);
                                       break;
        default: fail("Unknown content type " + header);
                 break;
      }
    }));
  }

  /**
   * Check OpenApi's JSON schema definition against with Java response class.
   * The check focus on two things.
   * <ul>
   *   <li>The response hierarchy are the same. In Json response, if a field's value is an array or map, a new layer is
   *   introduced. The check ensures the layout of layer are the same.</li>
   *   <li>Within each layer, the field key set are the same.</li>
   * </ul>
   *
   * @param schema The OpenApi schema
   * @param className The name of corresponding Java response class.
   * @param isOutermost Whether the schema is the outermost layer of the response.
   */
  private void checkSchema(Schema schema, String className, boolean isOutermost) {
    // Get the complete field key set from Java class.
    Map<String, Boolean> fields = extractFieldKeys(_schemaToClass.get(className));

    Map<String, Schema> properties = schema.getProperties();

    // Check version field.
    // If current schema is the schema for the outermost layer in the response, it must contain version field;
    // otherwise it may contain version field because it can be reused as the outermost layer in another response.
    if (isOutermost) {
      assertEquals(properties.size(), fields.size() + 1);
      assertTrue(properties.get(VERSION) instanceof IntegerSchema);
    } else {
      if (properties.size() - fields.size() == 1) {
        assertTrue(properties.get(VERSION) instanceof IntegerSchema);
      } else {
        assertEquals(className, properties.size(), fields.size());
      }
    }

    // Check other fields
    properties.forEach((k, v) -> {
      //Skip version field here.
      if (!k.equals(VERSION)) {
        // Check key exists.
        assertTrue(k + " does not exist in corresponding Java class.", fields.containsKey(k));
        // Check key's necessity.
        assertEquals(className + ":" + k, fields.get(k), schema.getRequired() != null && schema.getRequired().contains(k));
        // Check value
        Schema schemaToCheck = v;
        while (schemaToCheck instanceof ArraySchema || schemaToCheck instanceof MapSchema) {
          if (schemaToCheck instanceof ArraySchema) {
            schemaToCheck = ((ArraySchema) schemaToCheck).getItems();
          } else {
            schemaToCheck = (Schema) schemaToCheck.getAdditionalProperties();
          }
        }

        if (schemaToCheck instanceof StringSchema || schemaToCheck instanceof IntegerSchema || schemaToCheck instanceof NumberSchema
            || schemaToCheck instanceof ComposedSchema || schemaToCheck instanceof BooleanSchema) {
          return;
        }
        assertNotEquals(null, schemaToCheck.get$ref());
        String refName = schemaToCheck.get$ref();
        String schemaName = refName.substring(refName.lastIndexOf('/') + 1);
        checkSchema(_openAPI.getComponents().getSchemas().get(schemaName), schemaName, false);
      }
    });
  }

  /**
   * Extract the JSON field key set from the response class. The keys are identified in two ways.
   * <ul>
   *   <li>Any (static) field in response class or its super class (if exists) and is annotated with {@link JsonResponseField}.</li>
   *   <li>If the response class is annotated with {@link JsonResponseExternalFields} , then any (static) field in the class
   *   referenced by {@link JsonResponseExternalFields#value()} and is annotated with {@link JsonResponseField}.</li>
   * </ul>
   *
   * @param responseClass The Java response class.
   * @return A map of field key to its necessity (i.e. whether it is a required field in JSON response).
   */
  private Map<String, Boolean> extractFieldKeys(Class<?> responseClass) {
    Map<String, Boolean> fields = new HashMap<>();
    Queue<Class<?>> classToScan = new LinkedList<>();
    classToScan.add(responseClass);
    while (!classToScan.isEmpty()) {
      Class<?> currentClass = classToScan.poll();
      Arrays.stream(currentClass.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()) && f.isAnnotationPresent(JsonResponseField.class))
            .forEach(f -> {
              f.setAccessible(true);
              try {
                fields.put(f.get(null).toString(), f.getAnnotation(JsonResponseField.class).required());
              } catch (IllegalAccessException e) {
              // Not reach here.
              }
            });
      // Scan for super class.
      Class<?> superClass = currentClass.getSuperclass();
      if (superClass != null && superClass.isAnnotationPresent(JsonResponseClass.class)) {
        classToScan.offer(superClass);
      }
      // Scan for referenced class.
      if (currentClass.isAnnotationPresent(JsonResponseExternalFields.class)) {
        classToScan.offer(currentClass.getAnnotation(JsonResponseExternalFields.class).value());
      }
    }
    return fields;
  }
}
