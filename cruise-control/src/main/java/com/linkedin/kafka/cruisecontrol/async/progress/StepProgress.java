/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import io.swagger.v3.oas.annotations.media.Schema;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;


@JsonResponseClass
public class StepProgress {
  @JsonResponseField
  @Schema(type = "string")
  protected static final String STEP = "step";
  @JsonResponseField
  @Schema(type = "string")
  protected static final String DESCRIPTION = "description";
  @JsonResponseField
  @Schema(type = "integer",
      format = "int64")
  protected static final String TIME_IN_MS = "time-in-ms";
  @JsonResponseField
  @Schema(type = "number",
      format = "double")
  protected static final String COMPLETION_PERCENTAGE = "completionPercentage";
  protected final OperationStep _step;
  protected final long _duration;

  public StepProgress(OperationStep step, long duration) {
    _step = step;
    _duration = duration;
  }

  protected Map<String, Object> getJsonStructure() {
    return Map.of(STEP, _step.name(), DESCRIPTION, _step.description(), TIME_IN_MS, _duration,
                  COMPLETION_PERCENTAGE, _step.completionPercentage() * UNIT_INTERVAL_TO_PERCENTAGE);
  }

  @Override
  public String toString() {
    return String.format("(%6d ms) - (%3.1f%%) %s: %s%n", _duration,
                         _step.completionPercentage() * UNIT_INTERVAL_TO_PERCENTAGE, _step.name(), _step.description());
  }
}
