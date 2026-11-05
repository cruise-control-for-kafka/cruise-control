/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */
package com.linkedin.kafka.cruisecontrol.analyzer;

import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Map;

@JsonResponseClass
public class GoalReadinessRecord {
  @JsonResponseField
  @Schema(type = "string")
  protected static final String NAME = "name";
  @JsonResponseField
  @Schema(ref = "#/components/schemas/ModelCompletenessRequirements")
  protected static final String MODEL_COMPLETE_REQUIREMENT = "modelCompleteRequirement";
  @JsonResponseField
  @Schema(type = "string", allowableValues = {"ready", "notReady"})
  protected static final String STATUS = "status";

  protected Goal _goal;
  protected String _status;

  GoalReadinessRecord(Goal goal, String goalReadyStatus) {
    _goal = goal;
    _status = goalReadyStatus;
  }

  protected Map<String, Object> getJsonStructure() {
    return Map.of(NAME, _goal.getClass().getSimpleName(), MODEL_COMPLETE_REQUIREMENT, _goal.clusterModelCompletenessRequirements().getJsonStructure(),
                  STATUS, _status);
  }
}
