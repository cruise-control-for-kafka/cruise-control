/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseExternalFields;

import java.util.Map;

@JsonResponseClass
@JsonResponseExternalFields(KafkaAnomalyType.class)
public class MeanTimeBetweenAnomaliesMs {
    protected Map<AnomalyType, Double> _meanTimeBetweenAnomaliesMs;

    MeanTimeBetweenAnomaliesMs(Map<AnomalyType, Double> meanTimes) {
        _meanTimeBetweenAnomaliesMs = meanTimes;
    }

    protected Map<AnomalyType, Double> getJsonStructure() {
        return _meanTimeBetweenAnomaliesMs;
    }
}
