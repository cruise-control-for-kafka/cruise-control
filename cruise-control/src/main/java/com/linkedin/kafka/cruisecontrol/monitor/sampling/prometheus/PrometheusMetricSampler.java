/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.cruisecontrol.common.config.ConfigDef;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfigUtils;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.AbstractMetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSamplerOptions;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusValue;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Type.CLASS;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.SEC_TO_MS;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.replaceDotsWithUnderscores;

/**
 * Metric sampler that fetches Kafka metrics from a Prometheus server and converts them to samples.
 *
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #PROMETHEUS_SERVER_ENDPOINT_CONFIG}: The config for the HTTP endpoint of the Prometheus server
 *   which is to be used as a source for sampling metrics.</li>
 *   <li>{@link #PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG}: The config for the resolution of the Prometheus
 *   query made to the server (default: {@link #DEFAULT_PROMETHEUS_QUERY_RESOLUTION_STEP_MS}).
 *   If this is set to 30 seconds for a 2 minutes query interval, the query returns with 4 values, which are
 *   then aggregated into the metric sample.</li>
 *   <li>{@link #PROMETHEUS_QUERY_SUPPLIER_CONFIG}: The config for the class that supplies the Prometheus queries
 *   corresponding to Kafka raw metrics (default: {@link #DEFAULT_PROMETHEUS_QUERY_SUPPLIER}). If there are no
 *   customizations done when configuring Prometheus node exporter, the default class should work fine.</li>
 * </ul>
 */
public class PrometheusMetricSampler extends AbstractMetricSampler {
    // Config name visible to tests
    static final String PROMETHEUS_SERVER_ENDPOINT_CONFIG = "prometheus.server.endpoint";

    // Config name visible to tests
    static final String PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG = "prometheus.query.resolution.step.ms";
    private static final Integer DEFAULT_PROMETHEUS_QUERY_RESOLUTION_STEP_MS = (int) TimeUnit.MINUTES.toMillis(1);

    // Config name visible to tests
    static final String PROMETHEUS_QUERY_SUPPLIER_CONFIG = "prometheus.query.supplier";
    static final String PROMETHEUS_QUERY_FILE_CONFIG = "prometheus.query.file";

    private static final Class<?> DEFAULT_PROMETHEUS_QUERY_SUPPLIER = DefaultPrometheusQuerySupplier.class;
    private static final Class<?> PROMETHEUS_QUERY_FILE_SUPPLIER = ConfigurablePrometheusQuerySupplier.class;

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricSampler.class);

    protected int _samplingIntervalMs;
    protected Map<String, Integer> _hostToBrokerIdMap = new HashMap<>();
    protected PrometheusAdapter _prometheusAdapter;
    protected Map<RawMetricType, String> _metricToPrometheusQueryMap;
    private CloseableHttpClient _httpClient;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        configureSamplingInterval(configs);
        configurePrometheusAdapter(configs);
        configureQueryMap(configs);
    }

    private void configureSamplingInterval(Map<String, ?> configs) {
        _samplingIntervalMs = DEFAULT_PROMETHEUS_QUERY_RESOLUTION_STEP_MS;
        if (configs.containsKey(PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG)) {
            String samplingIntervalMsString = (String) configs.get(PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG);
            try {
                _samplingIntervalMs = Integer.parseInt(samplingIntervalMsString);
            } catch (NumberFormatException e) {
                throw new ConfigException(
                    String.format("%s config should be a positive number, provided %s", PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG,
                                  samplingIntervalMsString), e);
            }

            if (_samplingIntervalMs <= 0) {
                throw new ConfigException(String.format("%s config should be set to positive,"
                                                        + " provided %d.",
                                                        PROMETHEUS_QUERY_RESOLUTION_STEP_MS_CONFIG,
                                                        _samplingIntervalMs));
            }
        }
    }

    private void configurePrometheusAdapter(Map<String, ?> configs) {
        final String endpoint = (String) configs.get(PROMETHEUS_SERVER_ENDPOINT_CONFIG);
        if (endpoint == null) {
            throw new ConfigException(String.format(
                "%s config is required by Prometheus metric sampler", PROMETHEUS_SERVER_ENDPOINT_CONFIG));
        }

        try {
            HttpHost host = HttpHost.create(endpoint);
            if (host.getPort() < 0) {
                throw new IllegalArgumentException();
            }
            _httpClient = HttpClients.createDefault();
            _prometheusAdapter = new PrometheusAdapter(_httpClient, host, _samplingIntervalMs);
        } catch (IllegalArgumentException ex) {
            throw new ConfigException(
                String.format("Prometheus endpoint URI is malformed, "
                              + "expected schema://host:port, provided %s", endpoint), ex);
        }
    }

    private void configureQueryMap(Map<String, ?> configs) {
        String prometheusQueryFileName = (String) configs.get(PROMETHEUS_QUERY_FILE_CONFIG);
        String prometheusQuerySupplierClassName = (String) configs.get(PROMETHEUS_QUERY_SUPPLIER_CONFIG);
        Class<?> prometheusQuerySupplierClass = DEFAULT_PROMETHEUS_QUERY_SUPPLIER;
        // prometheus configuration file is first over other custom or default query supplier class
        if (null != prometheusQueryFileName) {
            prometheusQuerySupplierClass = PROMETHEUS_QUERY_FILE_SUPPLIER;
        } else {
            if (prometheusQuerySupplierClassName != null) {
                prometheusQuerySupplierClass = (Class<?>) ConfigDef.parseType(PROMETHEUS_QUERY_SUPPLIER_CONFIG,
                        prometheusQuerySupplierClassName, CLASS);
                if (!PrometheusQuerySupplier.class.isAssignableFrom(prometheusQuerySupplierClass)) {
                    throw new ConfigException(String.format(
                            "Invalid %s is provided to prometheus metric sampler, provided %s",
                            PROMETHEUS_QUERY_SUPPLIER_CONFIG, prometheusQuerySupplierClass));
                }
            }
        }
        PrometheusQuerySupplier prometheusQuerySupplier = KafkaCruiseControlConfigUtils.getConfiguredInstance(
            prometheusQuerySupplierClass, PrometheusQuerySupplier.class, (Map<String, Object>) configs);
        _metricToPrometheusQueryMap = prometheusQuerySupplier.get();
    }

    @Override
    public void close() throws IOException {
        _httpClient.close();
    }

    private Integer getBrokerIdForHostName(String host, Cluster cluster) {
        Integer cachedId = _hostToBrokerIdMap.get(host);
        if (cachedId != null) {
            return cachedId;
        }
        mapNodesToClusterId(cluster);
        return _hostToBrokerIdMap.get(host);
    }

    private void mapNodesToClusterId(Cluster cluster) {
        for (Node node : cluster.nodes()) {
            _hostToBrokerIdMap.put(node.host(), node.id());
        }
    }

    @Override
    protected int retrieveMetricsForProcessing(MetricSamplerOptions metricSamplerOptions) throws SamplingException {
        int metricsAdded = 0;
        int resultsSkipped = 0;
        for (Map.Entry<RawMetricType, String> metricToQueryEntry : _metricToPrometheusQueryMap.entrySet()) {
            final RawMetricType metricType = metricToQueryEntry.getKey();
            final String prometheusQuery = metricToQueryEntry.getValue();
            final List<PrometheusQueryResult> prometheusQueryResults;
            try {
                prometheusQueryResults = _prometheusAdapter.queryMetric(prometheusQuery,
                                                                        metricSamplerOptions.startTimeMs(),
                                                                        metricSamplerOptions.endTimeMs());
            } catch (IOException e) {
                LOG.error("Error when attempting to query Prometheus metrics", e);
                throw new SamplingException("Could not query metrics from Prometheus");
            }
            for (PrometheusQueryResult result : prometheusQueryResults) {
                try {
                    switch (metricType.metricScope()) {
                        case BROKER:
                            metricsAdded += addBrokerMetrics(metricSamplerOptions.cluster(), metricType, result);
                            break;
                        case TOPIC:
                            metricsAdded += addTopicMetrics(metricSamplerOptions.cluster(), metricType, result);
                            break;
                        case PARTITION:
                            metricsAdded += addPartitionMetrics(metricSamplerOptions.cluster(), metricType, result);
                            break;
                        default:
                            // Not supported.
                            break;
                    }
                } catch (InvalidPrometheusResultException e) {
                    /* We can ignore invalid or malformed Prometheus results, for example one which has a hostname
                    that could not be matched to any broker, or one where the topic name is null. Such records
                    will not be converted to metrics. There are valid use cases where this may occur - for instance,
                    when a Prometheus server store metrics from multiple Kafka clusters, in which case the hostname
                    may not correspond to any of this cluster's broker hosts.

                    This can be really frequent, and hence, we are only going to log them at trace level.
                     */
                    LOG.trace("Invalid query result received from Prometheus for query {}", prometheusQuery, e);
                    resultsSkipped++;
                }
            }
        }
        LOG.info("Added {} metric values. Skipped {} invalid query results.", metricsAdded, resultsSkipped);
        return metricsAdded;
    }

    private int addBrokerMetrics(Cluster cluster, RawMetricType metricType, PrometheusQueryResult queryResult)
        throws InvalidPrometheusResultException {
        int brokerId = getBrokerId(cluster, queryResult);

        int metricsAdded = 0;
        for (PrometheusValue value : queryResult.values()) {
            addMetricForProcessing(new BrokerMetric(metricType, value.epochSeconds() * SEC_TO_MS,
                                   brokerId, value.value()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    private int addTopicMetrics(Cluster cluster, RawMetricType metricType, PrometheusQueryResult queryResult)
        throws InvalidPrometheusResultException {
        int brokerId = getBrokerId(cluster, queryResult);
        String topic = getTopic(queryResult);

        int metricsAdded = 0;
        for (PrometheusValue value : queryResult.values()) {
            addMetricForProcessing(new TopicMetric(metricType, value.epochSeconds() * SEC_TO_MS,
                                   brokerId, topic, value.value()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    private int addPartitionMetrics(Cluster cluster, RawMetricType metricType, PrometheusQueryResult queryResult)
        throws InvalidPrometheusResultException {
        int brokerId = getBrokerId(cluster, queryResult);
        String topic = getTopic(queryResult);
        int partition = getPartition(queryResult);

        int metricsAdded = 0;
        for (PrometheusValue value : queryResult.values()) {
            addMetricForProcessing(new PartitionMetric(metricType, value.epochSeconds() * SEC_TO_MS,
                                   brokerId, topic, partition, value.value()));
            metricsAdded++;
        }
        return metricsAdded;
    }

    private int getBrokerId(Cluster cluster, PrometheusQueryResult queryResult) throws
        InvalidPrometheusResultException {
        String hostPort = queryResult.metric().instance();
        if (hostPort == null) {
            throw new InvalidPrometheusResultException("Instance returned as part of Prometheus API response is null.");
        }
        Integer brokerId;

        String hostName = hostPort.split(":")[0];
        brokerId = getBrokerIdForHostName(hostName, cluster);
        if (brokerId == null) {
            throw new InvalidPrometheusResultException(String.format(
                "Unexpected host %s, does not map to any of broker found from Kafka cluster metadata."
                    + " Brokers found in Kafka cluster metadata = %s",
                hostName, _hostToBrokerIdMap.keySet()));
        }
        return brokerId;
    }

    private String getTopic(PrometheusQueryResult queryResult) throws InvalidPrometheusResultException {
        String topic = queryResult.metric().topic();
        if (topic == null) {
            throw new InvalidPrometheusResultException("Topic was not returned as part of Prometheus API response.");
        }
        return replaceDotsWithUnderscores(topic);
    }

    private int getPartition(PrometheusQueryResult queryResult) throws InvalidPrometheusResultException {
        String partitionString = queryResult.metric().partition();
        if (partitionString == null) {
            throw new InvalidPrometheusResultException("Partition was not returned as part of Prometheus API response.");
        }
        try {
            return Integer.parseInt(partitionString);
        } catch (NumberFormatException e) {
            throw new InvalidPrometheusResultException("Partition returned as part of Prometheus API response was not a number.", e);
        }
    }
}
