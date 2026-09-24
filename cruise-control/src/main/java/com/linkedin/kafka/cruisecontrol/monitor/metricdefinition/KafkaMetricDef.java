/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.metricdefinition;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import java.util.*;

import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.AVG;
import static com.linkedin.cruisecontrol.metricdef.ValueComputingStrategy.LATEST;
import static com.linkedin.kafka.cruisecontrol.common.Resource.*;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.DefScope.BROKER_ONLY;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.DefScope.COMMON;


/**
 * The metric definitions of Kafka Cruise Control.
 * <p>
 * The class maps the raw metric types to metric definitions to be used by Kafka Cruise Control, i.e. analyzer and
 * linear regression model (in development).
 * <p>
 * The metrics in KafkaCruiseControl fall into two categories.
 * <ul>
 * <li>Broker only metrics (i.e. {@link DefScope#BROKER_ONLY}) -- e.g. request queue size.</li>
 * <li>Common metrics share by brokers and replicas (i.e. {@link DefScope#COMMON}) -- e.g. bytes in/out rate.</li>
 * </ul>
 * As of now, we do not have replica/partition only metrics.
 */
public enum KafkaMetricDef {
    // Ideally CPU usage should be defined as broker only, for legacy reason we are defining it as common metric.
    CPU_USAGE(AVG, COMMON, CPU, true),
    DISK_USAGE(LATEST, COMMON, DISK, false),
    LEADER_BYTES_IN(AVG, COMMON, NW_IN, false),
    LEADER_BYTES_OUT(AVG, COMMON, NW_OUT, false),
    PRODUCE_RATE(AVG, COMMON, null, false),
    FETCH_RATE(AVG, COMMON, null, false),
    MESSAGE_IN_RATE(AVG, COMMON, null, false),
    // Not available at topic level yet.
    REPLICATION_BYTES_IN_RATE(AVG, COMMON, NW_IN, false),
    // Not available at topic level yet.
    REPLICATION_BYTES_OUT_RATE(AVG, COMMON, NW_OUT, false),

    BROKER_PRODUCE_REQUEST_RATE(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_REQUEST_RATE(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_REQUEST_RATE(AVG, BROKER_ONLY, null, false),
    BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT(AVG, BROKER_ONLY, null, false),
    BROKER_REQUEST_QUEUE_SIZE(AVG, BROKER_ONLY, null, false),
    BROKER_RESPONSE_QUEUE_SIZE(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_TOTAL_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_TOTAL_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_LOCAL_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_LOCAL_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_LOG_FLUSH_RATE(AVG, BROKER_ONLY, null, false),
    BROKER_LOG_FLUSH_TIME_MS_MAX(AVG, BROKER_ONLY, null, false),
    BROKER_LOG_FLUSH_TIME_MS_MEAN(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_TOTAL_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_TOTAL_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_LOCAL_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_PRODUCE_LOCAL_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH(AVG, BROKER_ONLY, null, false),
    BROKER_LOG_FLUSH_TIME_MS_50TH(AVG, BROKER_ONLY, null, false),
    BROKER_LOG_FLUSH_TIME_MS_999TH(AVG, BROKER_ONLY, null, false);

    private final ValueComputingStrategy _valueComputingStrategy;
    private final String _group;
    private final DefScope _defScope;
    private final boolean _toPredict;
    private static final Map<RawMetricType, KafkaMetricDef> TYPE_TO_DEF = new HashMap<>();
    private static final List<KafkaMetricDef> CACHED_VALUES = List.of(KafkaMetricDef.values());
    private static final List<KafkaMetricDef> CACHED_COMMON_DEF_VALUES = buildCachedCommonDefValues();
    private static final List<KafkaMetricDef> CACHED_BROKER_DEF_VALUES = CACHED_VALUES;
    // Ensure that COMMON_METRIC_DEF and BROKER_METRIC_DEF are created after CACHED_COMMON_DEF_VALUES and CACHED_BROKER_DEF_VALUES
    private static final MetricDef COMMON_METRIC_DEF = buildCommonMetricDef();
    private static final MetricDef BROKER_METRIC_DEF = buildBrokerMetricDef();

    static {
        // Topic raw metrics
        TYPE_TO_DEF.put(RawMetricType.TOPIC_BYTES_IN, LEADER_BYTES_IN);
        TYPE_TO_DEF.put(RawMetricType.TOPIC_BYTES_OUT, LEADER_BYTES_OUT);
        // TODO: We do not have replication information at topic level yet.
        TYPE_TO_DEF.put(RawMetricType.TOPIC_REPLICATION_BYTES_IN, REPLICATION_BYTES_IN_RATE);
        TYPE_TO_DEF.put(RawMetricType.TOPIC_REPLICATION_BYTES_OUT, REPLICATION_BYTES_OUT_RATE);
        TYPE_TO_DEF.put(RawMetricType.TOPIC_PRODUCE_REQUEST_RATE, PRODUCE_RATE);
        TYPE_TO_DEF.put(RawMetricType.TOPIC_FETCH_REQUEST_RATE, FETCH_RATE);
        TYPE_TO_DEF.put(RawMetricType.TOPIC_MESSAGES_IN_PER_SEC, MESSAGE_IN_RATE);
        // Partition raw metrics
        TYPE_TO_DEF.put(RawMetricType.PARTITION_SIZE, DISK_USAGE);
        // Broker raw metrics
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_BYTES_IN, LEADER_BYTES_IN);
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_BYTES_OUT, LEADER_BYTES_OUT);
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_REPLICATION_BYTES_IN, REPLICATION_BYTES_IN_RATE);
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_REPLICATION_BYTES_OUT, REPLICATION_BYTES_OUT_RATE);
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_PRODUCE_REQUEST_RATE, PRODUCE_RATE);
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_FETCH_REQUEST_RATE, FETCH_RATE);
        TYPE_TO_DEF.put(RawMetricType.ALL_TOPIC_MESSAGES_IN_PER_SEC, MESSAGE_IN_RATE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CPU_UTIL, CPU_USAGE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_RATE, BROKER_PRODUCE_REQUEST_RATE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_RATE, BROKER_CONSUMER_FETCH_REQUEST_RATE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_RATE, BROKER_FOLLOWER_FETCH_REQUEST_RATE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT);
        TYPE_TO_DEF.put(RawMetricType.BROKER_REQUEST_QUEUE_SIZE, BROKER_REQUEST_QUEUE_SIZE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_RESPONSE_QUEUE_SIZE, BROKER_RESPONSE_QUEUE_SIZE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX, BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN, BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX, BROKER_PRODUCE_TOTAL_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN, BROKER_PRODUCE_TOTAL_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX, BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN, BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX, BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN, BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MAX, BROKER_PRODUCE_LOCAL_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_MEAN, BROKER_PRODUCE_LOCAL_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX, BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN, BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX, BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN, BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_RATE, BROKER_LOG_FLUSH_RATE);
        TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MAX, BROKER_LOG_FLUSH_TIME_MS_MAX);
        TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_MEAN, BROKER_LOG_FLUSH_TIME_MS_MEAN);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH, BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH, BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_50TH, BROKER_PRODUCE_TOTAL_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_999TH, BROKER_PRODUCE_TOTAL_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH, BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH, BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH, BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH, BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_50TH, BROKER_PRODUCE_LOCAL_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_PRODUCE_LOCAL_TIME_MS_999TH, BROKER_PRODUCE_LOCAL_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH, BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH, BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH, BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH, BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_50TH, BROKER_LOG_FLUSH_TIME_MS_50TH);
        TYPE_TO_DEF.put(RawMetricType.BROKER_LOG_FLUSH_TIME_MS_999TH, BROKER_LOG_FLUSH_TIME_MS_999TH);
    }

    /**
     * Construct the Kafka metric definition.
     *
     * @param strategy  the way to pick values among samples in a window.
     * @param defScope  the scope the metric applies to. It is either BROKER_ONLY or COMMON. See {@link KafkaMetricDef}
     *                  class doc for more details.
     * @param group     the metric group of the metric. We are using resource as the groups.
     * @param toPredict Whether the metric is a metrics that should be predicted by the linear regression model (in
     *                  development).
     */
    KafkaMetricDef(ValueComputingStrategy strategy,
                   DefScope defScope,
                   Resource group,
                   boolean toPredict) {
        _valueComputingStrategy = strategy;
        _defScope = defScope;
        _group = group == null ? "" : group.name();
        _toPredict = toPredict;
    }

    public ValueComputingStrategy valueComputingStrategy() {
        return _valueComputingStrategy;
    }

    public DefScope defScope() {
        return _defScope;
    }

    /**
     * @return The metric group of the Kafka metric def.
     * @see MetricDef
     */
    public String group() {
        return _group;
    }

    public static List<KafkaMetricDef> cachedCommonDefValues() {
        return Collections.unmodifiableList(CACHED_COMMON_DEF_VALUES);
    }

    public static List<KafkaMetricDef> cachedBrokerDefValues() {
        return Collections.unmodifiableList(CACHED_BROKER_DEF_VALUES);
    }

    public static KafkaMetricDef forRawMetricType(RawMetricType type) {
        return TYPE_TO_DEF.get(type);
    }

    public static short commonMetricDefId(KafkaMetricDef def) {
        return COMMON_METRIC_DEF.metricInfo(def.name()).id();
    }

    public static MetricInfo commonMetricDefInfo(KafkaMetricDef def) {
        return BROKER_METRIC_DEF.metricInfo(def.name());
    }

    public static MetricDef commonMetricDef() {
        return COMMON_METRIC_DEF;
    }

    public static MetricDef brokerMetricDef() {
        return BROKER_METRIC_DEF;
    }

    public static List<MetricInfo> resourceToMetricInfo(Resource resource) {
        return commonMetricDef().metricInfoForGroup(resource.name());
    }

    /**
     * Get a list of metric ids corresponding to the given resource.
     *
     * @param resource The resource type.
     * @return A list of metric ids corresponding to the given resource.
     */
    public static List<Short> resourceToMetricIds(Resource resource) {
        List<Short> metricIds = new ArrayList<>();
        resourceToMetricInfo(resource).forEach(info -> metricIds.add(info.id()));
        return metricIds;
    }

    public enum DefScope {
        BROKER_ONLY, COMMON
    }

    private static List<KafkaMetricDef> buildCachedCommonDefValues() {
        List<KafkaMetricDef> commonDefs = new ArrayList<>();
        for (KafkaMetricDef def : KafkaMetricDef.values()) {
            // All the metrics definitions are applicable to broker.
            if (def._defScope == COMMON) {
                // Add the common metrics definitions to the common metric def.
                commonDefs.add(def);
            }
        }
        return Collections.unmodifiableList(commonDefs);
    }

    private static MetricDef buildCommonMetricDef() {
        MetricDef metricDef = new MetricDef();
        for (KafkaMetricDef def : CACHED_COMMON_DEF_VALUES) {
            // Add the common metrics definitions to the common metric def.
            metricDef.define(def.name(), def.group(), def.valueComputingStrategy().name(), def._toPredict);
        }
        return metricDef;
    }

    private static MetricDef buildBrokerMetricDef() {
        MetricDef metricDef = new MetricDef();
        for (KafkaMetricDef def : CACHED_BROKER_DEF_VALUES) {
            // Add the all metrics definitions to the broker metric def.
            metricDef.define(def.name(), def.group(), def.valueComputingStrategy().name(), def._toPredict);
        }
        return metricDef;
    }
}
