/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.config.CruiseControlConfig;
import com.linkedin.cruisecontrol.detector.metricanomaly.PercentileMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.MetricAnomalyDetector.METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_50TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_50TH;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH;


/**
 * Identifies whether there are metric anomalies in brokers for the selected metric ids.
 */
public class KafkaMetricAnomalyFinder extends PercentileMetricAnomalyFinder<BrokerEntity> {
    private static final String DEFAULT_METRICS =
            new StringJoiner(",").add(BROKER_PRODUCE_LOCAL_TIME_MS_50TH.toString())
                    .add(BROKER_PRODUCE_LOCAL_TIME_MS_999TH.toString())
                    .add(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH.toString())
                    .add(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH.toString())
                    .add(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH.toString())
                    .add(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH.toString())
                    .add(BROKER_LOG_FLUSH_TIME_MS_50TH.toString())
                    .add(BROKER_LOG_FLUSH_TIME_MS_999TH.toString()).toString();
    private KafkaCruiseControl _kafkaCruiseControl;

    @Override
    protected String toMetricName(Short metricId) {
        return KafkaMetricDef.brokerMetricDef().metricInfo(metricId).name();
    }

    @Override
    public KafkaMetricAnomaly createMetricAnomaly(String description, BrokerEntity entity, Short metricId, List<Long> windows) {
        Map<String, Object> parameterConfigOverrides =
                Map.of(METRIC_ANOMALY_DESCRIPTION_OBJECT_CONFIG, description,
                        METRIC_ANOMALY_BROKER_ENTITIES_OBJECT_CONFIG, Collections.singletonMap(entity, _kafkaCruiseControl.timeMs()),
                        ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs(), METRIC_ANOMALY_FIXABLE_OBJECT_CONFIG, false);
        return _kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.METRIC_ANOMALY_CLASS_CONFIG,
                KafkaMetricAnomaly.class,
                parameterConfigOverrides);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        String interestedMetrics = (String) configs.get(CruiseControlConfig.METRIC_ANOMALY_FINDER_METRICS_CONFIG);
        if (interestedMetrics == null) {
            ((Map<String, Object>) configs).put(CruiseControlConfig.METRIC_ANOMALY_FINDER_METRICS_CONFIG, DEFAULT_METRICS);
        }
        super.configure(configs);
        _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        if (_kafkaCruiseControl == null) {
            throw new IllegalArgumentException("Kafka metric anomaly finder is missing " + KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        }
    }
}
