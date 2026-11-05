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

package com.linkedin.kafka.cruisecontrol.model;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ComposedSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.util.Arrays;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.OPENAPI_SPEC_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class DiskStatsTest {
  /**
   * Check that live measurements and dead-disk sentinels retain their distinct wire types.
   */
  @Test
  public void testLiveAndDeadDiskWireValuesMatchSchemaAlternatives() {
    OpenAPI api = new OpenAPIV3Parser().read(OPENAPI_SPEC_PATH);
    Schema schema = api.getComponents().getSchemas().get("DiskStats");
    JsonObject live = new Gson().toJsonTree(new DiskStats(1, 2, 12.5, 50.0).getJsonStructure()).getAsJsonObject();
    JsonObject dead = new Gson().toJsonTree(new DiskStats(1, 2, 12.5, 0.0).getJsonStructure()).getAsJsonObject();

    for (String field : Arrays.asList("DiskMB", "DiskPct")) {
      ComposedSchema property = (ComposedSchema) schema.getProperties().get(field);
      assertEquals(2, property.getOneOf().size());
      assertTrue(property.getOneOf().stream().anyMatch(alternative -> "number".equals(alternative.getType())));
      assertTrue(property.getOneOf().stream().anyMatch(alternative -> "string".equals(alternative.getType())
          && alternative.getEnum().contains("DEAD")));
      assertTrue(live.getAsJsonPrimitive(field).isNumber());
      assertTrue(dead.getAsJsonPrimitive(field).isString());
      assertEquals("DEAD", dead.get(field).getAsString());
    }
    assertEquals(12.5, live.get("DiskMB").getAsDouble(), 0.0);
    assertEquals(25.0, live.get("DiskPct").getAsDouble(), 0.0);
  }
}
