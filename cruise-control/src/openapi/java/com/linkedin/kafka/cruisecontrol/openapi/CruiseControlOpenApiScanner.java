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

import com.linkedin.kafka.cruisecontrol.config.constants.CruiseControlRequestConfig;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils;
import io.swagger.v3.oas.integration.api.OpenAPIConfiguration;
import io.swagger.v3.oas.integration.api.OpenApiScanner;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;


/**
 * Discovers actual request handlers from the dispatch registry, not JAX-RS paths.
 */
public class CruiseControlOpenApiScanner implements OpenApiScanner {
  @Override
  public void setConfiguration(OpenAPIConfiguration configuration) {
    // The server's endpoint registry determines the supported operations.
  }

  @Override
  public Set<Class<?>> classes() {
    return CruiseControlOpenApiReader.withContextClassLoader(this::requestClasses);
  }

  private Set<Class<?>> requestClasses() {
    Map<String, ConfigDef.ConfigKey> defaults = CruiseControlRequestConfig.define(new ConfigDef()).configKeys();
    Set<Class<?>> handlers = new HashSet<>();
    for (CruiseControlEndPoint endpoint : CruiseControlEndPoint.cachedValues()) {
      String key = KafkaCruiseControlServletUtils.requestParameterFor(endpoint).requestClass();
      handlers.add((Class<?>) defaults.get(key).defaultValue);
    }
    return handlers;
  }

  @Override
  public Map<String, Object> resources() {
    return Map.of();
  }
}
