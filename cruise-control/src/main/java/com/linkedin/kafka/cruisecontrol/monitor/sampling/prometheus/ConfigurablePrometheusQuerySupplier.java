package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.PrometheusMetricSampler.PROMETHEUS_QUERY_FILE_CONFIG;

/**
 * Configurable prometheus query supplier. This needs a configuration file to specify the different
 * prometheus metrics.
 * <p>
 * See {@link PrometheusQuerySupplier}
 */
public class ConfigurablePrometheusQuerySupplier implements CruiseControlConfigurable, PrometheusQuerySupplier {
    private static final Map<RawMetricType, String> TYPE_TO_QUERY = new HashMap<>();

    @Override
    public Map<RawMetricType, String> get() {
        return TYPE_TO_QUERY;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String configFileName = validateNotNull((String) configs.get(PROMETHEUS_QUERY_FILE_CONFIG),
                "Prometheus configuration file is missing.");

        internalParse(configFileName);
    }

    /**
     * Parse the config file to fill in the map
     * @param configFileName the name of the input config file
     */
    private void internalParse(String configFileName) {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(configFileName)) {
            props.load(propStream);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        // load each entry of the properties into the internal map
        props.forEach((key, value) -> loadEntry((String) key, (String) value));
    }

    private void loadEntry(String rawMetricTypeName, String query) {
        // will throw a IllegalArgumentException if the name is unknown in the RawMetricType enum
        TYPE_TO_QUERY.put(RawMetricType.valueOf(rawMetricTypeName), query);
    }
}
