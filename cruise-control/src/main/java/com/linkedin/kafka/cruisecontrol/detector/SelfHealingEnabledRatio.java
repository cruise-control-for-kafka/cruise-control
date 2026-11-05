/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;
import java.util.Map;
import java.util.HashMap;


@io.swagger.v3.oas.annotations.media.Schema(additionalPropertiesSchema = Float.class)
@JsonResponseClass
@JsonResponseExternalFields(KafkaAnomalyType.class)
public class SelfHealingEnabledRatio {
    protected Map<String, Float> _selfHealingEnabledRatioMap;

    SelfHealingEnabledRatio(int size) {
        _selfHealingEnabledRatioMap = new HashMap<>();
    }

    public void put(AnomalyType anomalyType, Float value) {
        _selfHealingEnabledRatioMap.put(anomalyType.toString(), value);
    }

    protected Map<String, Float> getJsonStructure() {
        return _selfHealingEnabledRatioMap;
    }
}
