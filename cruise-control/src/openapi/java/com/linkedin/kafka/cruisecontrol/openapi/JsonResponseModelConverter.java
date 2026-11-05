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

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverter;
import io.swagger.v3.core.converter.ModelConverterContext;
import io.swagger.v3.core.util.AnnotationsUtils;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;


/**
 * Resolves the map serializers' annotated JSON keys instead of their implementation fields.
 */
public class JsonResponseModelConverter implements ModelConverter {
  private static final String SCHEMA_REF = "#/components/schemas/";
  private final Map<String, Class<?>> _models = new HashMap<>();
  private final Components _components;

  public JsonResponseModelConverter(Components components) {
    _components = components;
    try (ScanResult scan = new ClassGraph().ignoreClassVisibility().enableAnnotationInfo()
            .acceptPackages("com.linkedin.kafka.cruisecontrol").scan()) {
      for (ClassInfo info : scan.getClassesWithAnnotation(JsonResponseClass.class.getName())) {
        Class<?> type = info.loadClass();
        if (_models.putIfAbsent(type.getSimpleName(), type) != null) {
          throw new IllegalStateException("Duplicate JSON schema name: " + type.getSimpleName());
        }
      }
    }
  }

  @Override
  public Schema resolve(AnnotatedType annotatedType, ModelConverterContext context, Iterator<ModelConverter> chain) {
    if (!(annotatedType.getType() instanceof Class<?>)
        || !((Class<?>) annotatedType.getType()).isAnnotationPresent(JsonResponseClass.class)) {
      return chain.hasNext() ? chain.next().resolve(annotatedType, context, chain) : null;
    }
    Class<?> type = (Class<?>) annotatedType.getType();
    String name = type.getSimpleName();
    Schema schema = new ObjectSchema();
    context.defineModel(name, schema, type, null);
    io.swagger.v3.oas.annotations.media.Schema metadata = type.getDeclaredAnnotation(io.swagger.v3.oas.annotations.media.Schema.class);
    if (metadata != null) {
      AnnotationsUtils.getSchemaFromAnnotation(metadata, _components, null).ifPresent(s -> {
        schema.setDescription(s.getDescription());
        schema.setRequired(s.getRequired());
      });
    }
    if (type.getAnnotation(JsonResponseClass.class).versioned()) {
      schema.addProperty("version", new IntegerSchema().format("int32"));
    }
    Queue<Class<?>> pending = new ArrayDeque<>();
    Set<Class<?>> visited = new HashSet<>();
    pending.add(type);
    while (!pending.isEmpty()) {
      Class<?> current = pending.remove();
      if (!visited.add(current)) {
        continue;
      }
      for (Field field : current.getDeclaredFields()) {
        JsonResponseField jsonField = field.getAnnotation(JsonResponseField.class);
        if (jsonField != null && Modifier.isStatic(field.getModifiers())) {
          try {
            field.setAccessible(true);
            String key = field.get(null).toString();
            if (schema.getProperties() != null && schema.getProperties().containsKey(key)) {
              continue;
            }
            // External enum keys can have different numeric value types in different owning maps.
            if (field.getAnnotation(io.swagger.v3.oas.annotations.media.Schema.class) == null
                && field.getAnnotation(io.swagger.v3.oas.annotations.media.ArraySchema.class) == null
                && metadata != null && metadata.additionalPropertiesSchema() != Void.class) {
              schema.addProperty(key, context.resolve(new AnnotatedType(metadata.additionalPropertiesSchema())));
            } else {
              schema.addProperty(key, fieldSchema(field, context));
            }
            if (jsonField.required()) {
              schema.addRequiredItem(key);
            }
          } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read JSON key: " + field, e);
          }
        }
      }
      Class<?> parent = current.getSuperclass();
      if (parent != null && parent.isAnnotationPresent(JsonResponseClass.class)) {
        pending.add(parent);
      }
      JsonResponseExternalFields external = current.getAnnotation(JsonResponseExternalFields.class);
      if (external != null) {
        pending.add(external.value());
      }
    }
    resolveReferences(schema, context);
    return new Schema().$ref(SCHEMA_REF + name);
  }

  private Schema fieldSchema(Field field, ModelConverterContext context) {
    io.swagger.v3.oas.annotations.media.ArraySchema array = field.getAnnotation(io.swagger.v3.oas.annotations.media.ArraySchema.class);
    if (array != null) {
      return AnnotationsUtils.getArraySchema(array, _components, null, false, null, true, context)
          .orElseThrow(() -> new IllegalStateException("Missing array schema: " + field));
    }
    io.swagger.v3.oas.annotations.media.Schema metadata = field.getAnnotation(io.swagger.v3.oas.annotations.media.Schema.class);
    if (metadata == null) {
      throw new IllegalStateException("Missing JSON value schema: " + field);
    }
    return AnnotationsUtils.getSchema(metadata, null, false, metadata.implementation(), _components, null, false, context)
        .orElseThrow(() -> new IllegalStateException("Missing JSON value schema: " + field));
  }

  /**
   * Resolve explicit schema references, including references in array and map values.
   * @param value An annotated schema or operation.
   * @param context The isolated conversion context for this generation.
   */
  public void resolveReferences(Object value, ModelConverterContext context) {
    for (String ref : Json.mapper().valueToTree(value).findValuesAsText("$ref")) {
      if (ref.startsWith(SCHEMA_REF)) {
        String name = ref.substring(SCHEMA_REF.length());
        if (!context.getDefinedModels().containsKey(name) && !_components.getSchemas().containsKey(name)) {
          Class<?> type = _models.get(name);
          if (type == null) {
            throw new IllegalStateException("Unknown JSON response schema: " + name);
          }
          context.resolve(new AnnotatedType(type));
        }
      }
    }
  }
}
