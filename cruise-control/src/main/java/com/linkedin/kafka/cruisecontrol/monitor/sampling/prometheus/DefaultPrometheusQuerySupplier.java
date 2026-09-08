/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;

/**
 * Default prometheus query supplier. This can be used if the Prometheus JMX and node exporters on the
 * Kafka brokers are running with standard vanilla configuration, and do not have any special rules for
 * transformation of metric names.
 *
 * See {@link PrometheusQuerySupplier}
 */
public class DefaultPrometheusQuerySupplier implements CruiseControlConfigurable, PrometheusQuerySupplier {
    private final Map<RawMetricType, String> _typeToQuery = new HashMap<>();
    // Config name visible to tests
    static final String PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS = "prometheus.broker.metrics.scraping.frequency.seconds";
    private static final Integer DEFAULT_PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS = 60;

    private int _brokerCpuUtilQueryMinutes;

    /**
     * Configure this class with the given key-value pairs
     * @param configs Configurations.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        Integer scrapingIntervalSeconds = DEFAULT_PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS;
        if (configs.containsKey(PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS)) {
            String scrapingIntervalSecondsString = (String) configs.get(PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS);
            try {
                scrapingIntervalSeconds = Integer.parseInt(scrapingIntervalSecondsString);
            } catch (NumberFormatException e) {
                throw new ConfigException(
                    String.format("%s config should be a positive number, provided %s", PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS,
                                  scrapingIntervalSecondsString), e);
            }

            if (scrapingIntervalSeconds <= 0) {
                throw new ConfigException(String.format("%s config should be set to positive," + " provided %d.",
                                                        PROMETHEUS_BROKER_METRICS_SCRAPING_INTERVAL_SECONDS, scrapingIntervalSeconds));
            }
        }
        // iRate query requires at least two data points to be available
        _brokerCpuUtilQueryMinutes = convertScrapingIntervalToCPUUtilQueryMinutes(scrapingIntervalSeconds);
        buildTypeToQueryMap();
    }

    private int convertScrapingIntervalToCPUUtilQueryMinutes(int scrapingIntervalSeconds) {
        return (int) (Math.ceil(scrapingIntervalSeconds * 2.0 / 60));
    }

    private void buildTypeToQueryMap() {
        // broker metrics
        _typeToQuery.put(BROKER_CPU_UTIL,
            String.format("avg by (instance) (rate(process_cpu_seconds_total[%dm]))",
                                                        _brokerCpuUtilQueryMinutes));
        _typeToQuery.put(ALL_TOPIC_BYTES_IN,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesInPerSec\",topic=\"\"}");
        _typeToQuery.put(ALL_TOPIC_BYTES_OUT,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}");
        _typeToQuery.put(ALL_TOPIC_REPLICATION_BYTES_IN,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesInPerSec\",topic=\"\"}");
        _typeToQuery.put(ALL_TOPIC_REPLICATION_BYTES_OUT,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesOutPerSec\",topic=\"\"}");
        _typeToQuery.put(ALL_TOPIC_FETCH_REQUEST_RATE,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalFetchRequestsPerSec\",topic=\"\"}");
        _typeToQuery.put(ALL_TOPIC_PRODUCE_REQUEST_RATE,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalProduceRequestsPerSec\",topic=\"\"}");
        _typeToQuery.put(ALL_TOPIC_MESSAGES_IN_PER_SEC,
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"MessagesInPerSec\",topic=\"\"}");
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_RATE, "sum by (instance) "
                + "(kafka_network_RequestMetrics_OneMinuteRate{name=\"RequestsPerSec\",request=\"Produce\"})");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_RATE, "sum by (instance) "
                + "(kafka_network_RequestMetrics_OneMinuteRate{name=\"RequestsPerSec\",request=\"FetchConsumer\"})");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_RATE, "sum by (instance) "
                + "(kafka_network_RequestMetrics_OneMinuteRate{name=\"RequestsPerSec\",request=\"FetchFollower\"})");
        _typeToQuery.put(BROKER_REQUEST_QUEUE_SIZE,
            "kafka_network_RequestChannel_Value{name=\"RequestQueueSize\"}");
        _typeToQuery.put(BROKER_RESPONSE_QUEUE_SIZE,
            "kafka_network_RequestChannel_Value{name=\"ResponseQueueSize\"}");
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"RequestQueueTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"RequestQueueTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"LocalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"LocalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"LocalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"LocalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"LocalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"LocalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"TotalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"TotalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"TotalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"TotalTimeMs\",request=\"Produce\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"TotalTimeMs\",request=\"FetchConsumer\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX,
            "kafka_network_RequestMetrics_Max{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN,
            "kafka_network_RequestMetrics_Mean{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH,
            "kafka_network_RequestMetrics_50thPercentile{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH,
            "kafka_network_RequestMetrics_999thPercentile{name=\"TotalTimeMs\",request=\"FetchFollower\"}");
        _typeToQuery.put(BROKER_LOG_FLUSH_RATE,
            "kafka_log_LogFlushStats_OneMinuteRate{name=\"LogFlushRateAndTimeMs\"}");
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_MAX,
            "kafka_log_LogFlushStats_Max{name=\"LogFlushRateAndTimeMs\"}");
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_MEAN,
            "kafka_log_LogFlushStats_Mean{name=\"LogFlushRateAndTimeMs\"}");
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_50TH,
            "kafka_log_LogFlushStats_50thPercentile{name=\"LogFlushRateAndTimeMs\"}");
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_999TH,
            "kafka_log_LogFlushStats_999thPercentile{name=\"LogFlushRateAndTimeMs\"}");
        _typeToQuery.put(BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT,
            "kafka_server_KafkaRequestHandlerPool_OneMinuteRate{name=\"RequestHandlerAvgIdlePercent\"}");

        // topic metrics
        _typeToQuery.put(TOPIC_BYTES_IN,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesInPerSec\",topic!=\"\"}");
        _typeToQuery.put(TOPIC_BYTES_OUT,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic!=\"\"}");
        _typeToQuery.put(TOPIC_REPLICATION_BYTES_IN,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesInPerSec\",topic!=\"\"}");
        _typeToQuery.put(TOPIC_REPLICATION_BYTES_OUT,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"ReplicationBytesOutPerSec\",topic!=\"\"}");
        _typeToQuery.put(TOPIC_FETCH_REQUEST_RATE,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalFetchRequestsPerSec\",topic!=\"\"}");
        _typeToQuery.put(TOPIC_PRODUCE_REQUEST_RATE,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"TotalProduceRequestsPerSec\",topic!=\"\"}");
        _typeToQuery.put(TOPIC_MESSAGES_IN_PER_SEC,
                         "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"MessagesInPerSec\",topic!=\"\"}");

        // partition metrics
        _typeToQuery.put(PARTITION_SIZE,
                         "kafka_log_Log_Value{name=\"Size\",topic!=\"\",partition!=\"\"}");
    }

    @Override
    public Map<RawMetricType, String> get() {
        return Collections.unmodifiableMap(_typeToQuery);
    }
}
