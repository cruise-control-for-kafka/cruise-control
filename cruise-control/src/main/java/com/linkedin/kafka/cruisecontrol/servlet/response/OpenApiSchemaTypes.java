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
package com.linkedin.kafka.cruisecontrol.servlet.response;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;


/**
 * Container schemas used where Swagger annotation attributes require class literals.
 */
public final class OpenApiSchemaTypes {
  private OpenApiSchemaTypes() {
  }

  @ArraySchema(schema = @Schema(type = "string"))
  public static final class StringList {
    private StringList() {
    }
  }

  @Schema(type = "object", additionalPropertiesSchema = Long.class)
  public static final class LongMap {
    private LongMap() {
    }
  }

  public enum DeadDiskValue {
    DEAD
  }

  @Schema(type = "string", pattern = "^[0-9]+-[0-9]+$",
      example = "[{value=0-9}, {summary=0 is the start index and 9 is the end index of the partition boundaries.}]")
  public static final class PartitionRange {
    private PartitionRange() {
    }
  }
}
