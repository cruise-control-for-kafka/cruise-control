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
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.config.RequestParameterWrapper;
import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlParametersConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlRequestConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils;
import com.linkedin.kafka.cruisecontrol.servlet.handler.AbstractRequest;
import io.swagger.v3.core.converter.ModelConverterContextImpl;
import io.swagger.v3.core.converter.ModelConverters;
import io.swagger.v3.core.util.AnnotationsUtils;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.Explode;
import io.swagger.v3.oas.annotations.enums.ParameterStyle;
import io.swagger.v3.oas.integration.api.OpenAPIConfiguration;
import io.swagger.v3.oas.integration.api.OpenApiReader;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigDef;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.RESOURCE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SUBSTATES_PARAM;


/**
 * Swagger reader for Cruise Control's existing dispatch registry, without a JAX-RS facade.
 */
public class CruiseControlOpenApiReader implements OpenApiReader {
  @Override
  public void setConfiguration(OpenAPIConfiguration configuration) {
    // The Java annotations, not an external input specification, own the contract.
  }

  @Override
  public OpenAPI read(Set<Class<?>> classes, Map<String, Object> resources) {
    return withContextClassLoader(() -> readContract(classes));
  }

  static <T> T withContextClassLoader(Supplier<T> action) {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    // Swagger's Gradle task isolates build classes; Kafka resolves CLASS defaults through the thread context loader.
    thread.setContextClassLoader(CruiseControlOpenApiReader.class.getClassLoader());
    try {
      return action.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private OpenAPI readContract(Set<Class<?>> classes) {
    Components components = new Components().schemas(new LinkedHashMap<>());
    JsonResponseModelConverter converter = new JsonResponseModelConverter(components);
    ModelConverters converters = new ModelConverters();
    converters.addConverter(converter);
    ModelConverterContextImpl context = new ModelConverterContextImpl(converters.getConverters());
    OpenAPI api = definition().components(components).paths(new Paths());
    for (io.swagger.v3.oas.annotations.security.SecurityScheme security
        : KafkaCruiseControlEndPoints.class.getAnnotationsByType(io.swagger.v3.oas.annotations.security.SecurityScheme.class)) {
      SecurityScheme scheme = new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme(security.scheme()).description(security.description());
      if (!security.bearerFormat().isEmpty()) {
        scheme.setBearerFormat(security.bearerFormat());
      }
      components.addSecuritySchemes(security.name(), scheme);
    }
    Map<String, ConfigDef.ConfigKey> requests = CruiseControlRequestConfig.define(new ConfigDef()).configKeys();
    Map<String, ConfigDef.ConfigKey> parameters = CruiseControlParametersConfig.define(new ConfigDef()).configKeys();
    io.swagger.v3.oas.annotations.Operation shared = Arrays.stream(AbstractRequest.class.getDeclaredMethods())
        .map(m -> m.getAnnotation(io.swagger.v3.oas.annotations.Operation.class)).filter(a -> a != null).findFirst().orElseThrow();
    for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
      RequestParameterWrapper wrapper = KafkaCruiseControlServletUtils.requestParameterFor(endpoint);
      Class<?> request = defaultClass(requests.get(wrapper.requestClass()));
      if (classes != null && !classes.isEmpty() && !classes.contains(request)) {
        continue;
      }
      io.swagger.v3.oas.annotations.Operation metadata = Arrays.stream(request.getDeclaredMethods())
          .map(m -> m.getAnnotation(io.swagger.v3.oas.annotations.Operation.class)).filter(a -> a != null).findFirst()
          .orElseThrow(() -> new IllegalStateException("Missing endpoint documentation: " + endpoint));
      Operation operation = new Operation().operationId(metadata.operationId()).summary(metadata.summary()).responses(new ApiResponses());
      Map<String, Parameter> query = new LinkedHashMap<>();
      for (Class<?> type = defaultClass(parameters.get(wrapper.parametersClass())); type != null; type = type.getSuperclass()) {
        for (Method method : type.getDeclaredMethods()) {
          Parameters annotation = method.getAnnotation(Parameters.class);
          if (annotation != null) {
            for (io.swagger.v3.oas.annotations.Parameter parameter : annotation.value()) {
              query.putIfAbsent(parameter.name(), parameter(parameter, components));
            }
          }
        }
      }
      Set<String> allowed = parameterNames(defaultClass(parameters.get(wrapper.parametersClass())));
      query.keySet().retainAll(allowed);
      if (!query.keySet().equals(allowed)) {
        throw new IllegalStateException("Query parameter documentation mismatch: " + endpoint
            + " expected " + allowed + ", found " + query.keySet());
      }
      operation.setParameters(new ArrayList<>(query.values()));
      for (io.swagger.v3.oas.annotations.Parameter parameter : shared.parameters()) {
        operation.addParametersItem(parameter(parameter, components));
      }
      for (io.swagger.v3.oas.annotations.responses.ApiResponse response : shared.responses()) {
        if (!"2XX".equals(response.responseCode())) {
          operation.getResponses().addApiResponse(response.responseCode(), response(response, components));
        }
      }
      for (io.swagger.v3.oas.annotations.responses.ApiResponse response : metadata.responses()) {
        ApiResponse result = response(response, components);
        if (response.responseCode().startsWith("2")) {
          for (io.swagger.v3.oas.annotations.responses.ApiResponse common : shared.responses()) {
            if ("2XX".equals(common.responseCode())) {
              AnnotationsUtils.getHeaders(common.headers(), components, null).ifPresent(result::setHeaders);
            }
          }
        }
        operation.getResponses().addApiResponse(response.responseCode(), result);
      }
      converter.resolveReferences(operation, context);
      PathItem item = new PathItem();
      item.operation(CruiseControlEndPoint.getEndpoints().contains(endpoint) ? PathItem.HttpMethod.GET : PathItem.HttpMethod.POST, operation);
      api.getPaths().addPathItem("/" + endpoint.name().toLowerCase(Locale.ROOT), item);
    }
    components.getSchemas().putAll(context.getDefinedModels());
    return api;
  }

  private static Class<?> defaultClass(ConfigDef.ConfigKey key) {
    if (!(key.defaultValue instanceof Class<?>)) {
      throw new IllegalStateException("Expected a default request/parameter class: " + key.name);
    }
    return (Class<?>) key.defaultValue;
  }

  private static Set<String> parameterNames(Class<?> type) {
    try {
      return ((CruiseControlParameters) type.getDeclaredConstructor().newInstance()).caseInsensitiveParameterNames();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot read request parameter names: " + type, e);
    }
  }

  private static Parameter parameter(io.swagger.v3.oas.annotations.Parameter metadata, Components components) {
    Parameter result = new Parameter().name(metadata.name()).in(metadata.in().toString()).description(metadata.description());
    if (metadata.required()) {
      result.setRequired(true);
    }
    if (!metadata.example().isEmpty()) {
      result.setExample(metadata.example());
    }
    if (metadata.style() != ParameterStyle.DEFAULT) {
      result.setStyle(Parameter.StyleEnum.valueOf(metadata.style().name()));
    }
    if (metadata.explode() != Explode.DEFAULT) {
      result.setExplode(metadata.explode() == Explode.TRUE);
    }
    if (AnnotationsUtils.hasArrayAnnotation(metadata.array())) {
      AnnotationsUtils.getArraySchema(metadata.array(), components, null, false, null, true, null).ifPresent(result::setSchema);
    } else {
      AnnotationsUtils.getSchema(metadata.schema(), metadata.array(), false, metadata.schema().implementation(), components, null)
          .ifPresent(result::setSchema);
    }
    if (result.getSchema() == null) {
      throw new IllegalStateException("Missing parameter schema: " + metadata.name());
    }
    if (metadata.schema().implementation().isEnum() && !metadata.schema().defaultValue().isEmpty()) {
      result.getSchema().setDefault(metadata.schema().defaultValue());
    } else if (result.getSchema().getDefault() != null) {
      result.getSchema().setDefault(Json.mapper().convertValue(result.getSchema().getDefault(),
          Json.mapper().constructType(AnnotationsUtils.getSchemaType(metadata.schema()))));
    }
    if ("array".equals(result.getSchema().getType())) {
      AnnotationsUtils.getSchemaFromAnnotation(metadata.array().arraySchema(), components, null)
          .map(schema -> schema.getExample()).ifPresent(result.getSchema()::setExample);
    }
    if (SUBSTATES_PARAM.equals(metadata.name()) || RESOURCE_PARAM.equals(metadata.name())) {
      Schema values = "array".equals(result.getSchema().getType()) ? result.getSchema().getItems() : result.getSchema();
      Class<?> type = "array".equals(result.getSchema().getType()) ? metadata.array().schema().implementation()
                                                                  : metadata.schema().implementation();
      // These case-insensitive parsers document lowercase enum names, not enum display strings.
      values.setEnum(Arrays.stream(type.getEnumConstants())
          .map(value -> ((Enum<?>) value).name().toLowerCase(Locale.ROOT)).toList());
    }
    return result;
  }

  private static ApiResponse response(io.swagger.v3.oas.annotations.responses.ApiResponse metadata, Components components) {
    ApiResponse result = new ApiResponse().description(metadata.description());
    AnnotationsUtils.getContent(metadata.content(), null, null, null, components, null).ifPresent(result::setContent);
    return result;
  }

  private static OpenAPI definition() {
    OpenAPIDefinition metadata = KafkaCruiseControlEndPoints.class.getAnnotation(OpenAPIDefinition.class);
    OpenAPI api = new OpenAPI().openapi("3.0.0");
    AnnotationsUtils.getInfo(metadata.info()).ifPresent(api::setInfo);
    AnnotationsUtils.getServers(metadata.servers()).ifPresent(api::setServers);
    for (io.swagger.v3.oas.annotations.security.SecurityRequirement security : metadata.security()) {
      api.addSecurityItem(new SecurityRequirement().addList(security.name(), List.of(security.scopes())));
    }
    return api;
  }
}
