/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import com.linkedin.kafka.cruisecontrol.common.KafkaNetworkClientProvider;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaAdminTopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.DefaultMetricSamplerPartitionAssignor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;


/**
 * A class to keep Cruise Control Monitor Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class MonitorConfig {

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    public static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

    /**
     * <code>security.protocol</code>
     */
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    public static final String DEFAULT_SECURITY_PROTOCOL = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
    public static final String SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;

    /**
     * <code>metadata.max.age.ms</code>
     */
    public static final String METADATA_MAX_AGE_MS_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    public static final long DEFAULT_METADATA_MAX_AGE_MS = TimeUnit.SECONDS.toMillis(55);
    public static final String METADATA_MAX_AGE_MS_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /**
     * <code>client.id</code>
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
    public static final String DEFAULT_CLIENT_ID = "kafka-cruise-control";
    public static final String CLIENT_ID_DOC = CommonClientConfigs.CLIENT_ID_DOC;

    /**
     * <code>send.buffer.bytes</code>
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;
    public static final int DEFAULT_SEND_BUFFER = 128 * 1024;
    public static final String SEND_BUFFER_DOC = CommonClientConfigs.SEND_BUFFER_DOC;

    /**
     * <code>socket.connection.setup.timeout.ms</code>
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public static final long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;

    /**
     * <code>socket.connection.setup.timeout.max.ms</code>
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;
    public static final long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;

    /**
     * <code>receive.buffer.bytes</code>
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;
    public static final int DEFAULT_RECEIVE_BUFFER = 32 * 1024;
    public static final String RECEIVE_BUFFER_DOC = CommonClientConfigs.RECEIVE_BUFFER_DOC;

    /**
     * <code>connections.max.idle.ms</code>
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
    public static final long DEFAULT_CONNECTIONS_MAX_IDLE_MS = TimeUnit.MINUTES.toMillis(9);
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC;

    /**
     * <code>reconnect.backoff.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;
    public static final long DEFAULT_RECONNECT_BACKOFF_MS = 50L;
    public static final String RECONNECT_BACKOFF_MS_DOC = CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC;

    /**
     * <code>request.timeout.ms</code>
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(30);
    public static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /**
     * <code>partition.metrics.windows.ms</code>
     */
    public static final String PARTITION_METRICS_WINDOW_MS_CONFIG = "partition.metrics.window.ms";
    public static final long DEFAULT_PARTITION_METRICS_WINDOW_MS = TimeUnit.HOURS.toMillis(1);
    public static final String PARTITION_METRICS_WINDOW_MS_DOC = "The size of the window in milliseconds to aggregate "
            + "the Kafka partition metrics.";

    /**
     * <code>num.partition.metrics.windows</code>
     */
    public static final String NUM_PARTITION_METRICS_WINDOWS_CONFIG = "num.partition.metrics.windows";
    public static final int DEFAULT_NUM_PARTITION_METRICS_WINDOWS = 5;
    public static final String NUM_PARTITION_METRICS_WINDOWS_DOC = "The total number of windows to keep for partition "
            + "metric samples";

    /**
     * <code>network.client.provider.class</code>
     */
    public static final String NETWORK_CLIENT_PROVIDER_CLASS_CONFIG = "network.client.provider.class";
    // We have to define this to support the use of network clients with different Kafka client versions.
    public static final String DEFAULT_NETWORK_CLIENT_PROVIDER_CLASS = KafkaNetworkClientProvider.class.getName();
    public static final String NETWORK_CLIENT_PROVIDER_CLASS_DOC = "The network client provider class to generate a "
            + "network client with given properties.";

    /**
     * <code>skip.loading.samples</code>
     */
    public static final String SKIP_LOADING_SAMPLES_CONFIG = "skip.loading.samples";
    public static final boolean DEFAULT_SKIP_LOADING_SAMPLES = false;
    public static final String SKIP_LOADING_SAMPLES_DOC = "Specify if sample loading will be skipped upon startup.";

    /**
     * <code>min.samples.per.partition.metrics.window</code>
     */
    public static final String MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG = "min.samples.per.partition.metrics.window";
    public static final int DEFAULT_MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW = 3;
    public static final String MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_DOC = "The minimum number of "
            + "PartitionMetricSamples needed to make a partition metrics window valid without extrapolation.";

    /**
     * <code>max.allowed.extrapolations.per.partition</code>
     */
    public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG = "max.allowed.extrapolations.per.partition";
    public static final int DEFAULT_MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION = 5;
    public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_DOC = "The maximum allowed number of extrapolations "
            + "for each partition. A partition will be considered as invalid if the total number extrapolations in all the "
            + "windows goes above this number.";

    /**
     * <code>partition.metric.sample.aggregator.completeness.cache.size</code>
     */
    public static final String PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
            "partition.metric.sample.aggregator.completeness.cache.size";
    public static final int DEFAULT_PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE = 5;
    public static final String PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample "
            + "aggregator caches the completeness metadata for fast query. The completeness describes the confidence "
            + "level of the data in the metric sample aggregator. It is primarily measured by the validity of the metrics"
            + "samples in different windows. This configuration configures The number of completeness cache slots to "
            + "maintain.";

    /**
     * <code>broker.metrics.window.ms</code>
     */
    public static final String BROKER_METRICS_WINDOW_MS_CONFIG = "broker.metrics.window.ms";
    public static final long DEFAULT_BROKER_METRICS_WINDOW_MS = TimeUnit.HOURS.toMillis(1);
    public static final String BROKER_METRICS_WINDOW_MS_DOC = "The size of the window in milliseconds to aggregate the"
            + " Kafka broker metrics.";

    /**
     * <code>num.broker.metrics.windows</code>
     */
    public static final String NUM_BROKER_METRICS_WINDOWS_CONFIG = "num.broker.metrics.windows";
    public static final int DEFAULT_NUM_BROKER_METRICS_WINDOWS = 5;
    public static final String NUM_BROKER_METRICS_WINDOWS_DOC = "The total number of windows to keep for broker metric samples";

    /**
     * <code>min.samples.per.broker.metrics.window</code>
     */
    public static final String MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG = "min.samples.per.broker.metrics.window";
    public static final int DEFAULT_MIN_SAMPLES_PER_BROKER_METRICS_WINDOW = 3;
    public static final String MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_DOC = "The minimum number of BrokerMetricSamples "
            + "needed to make a broker metrics window valid without extrapolation.";

    /**
     * <code>max.allowed.extrapolations.per.broker</code>
     */
    public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG = "max.allowed.extrapolations.per.broker";
    public static final int DEFAULT_MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER = 5;
    public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_DOC = "The maximum allowed number of extrapolations "
            + "for each broker. A broker will be considered as invalid if the total number extrapolations in all the windows"
            + " goes above this number.";

    /**
     * <code>broker.metric.sample.aggregator.completeness.cache.size</code>
     */
    public static final String BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
            "broker.metric.sample.aggregator.completeness.cache.size";
    public static final int DEFAULT_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG = 5;
    public static final String BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample "
            + "aggregator caches the completeness metadata for fast query. The completeness describes the confidence "
            + "level of the data in the metric sample aggregator. It is primarily measured by the validity of the metrics"
            + "samples in different windows. This configuration configures The number of completeness cache slots to "
            + "maintain.";

    /**
     * <code>metric.sampler.class</code>
     */
    public static final String METRIC_SAMPLER_CLASS_CONFIG = "metric.sampler.class";
    public static final String DEFAULT_METRIC_SAMPLER_CLASS = CruiseControlMetricsReporterSampler.class.getName();
    public static final String METRIC_SAMPLER_CLASS_DOC = "The class name of the metric sampler";

    /**
     * <code>metric.sampler.partition.assignor.class</code>
     */
    public static final String METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG = "metric.sampler.partition.assignor.class";
    public static final String DEFAULT_METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS = DefaultMetricSamplerPartitionAssignor.class.getName();
    public static final String METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_DOC = "The class used to assign the partitions to the "
            + "metric samplers.";

    /**
     * <code>metric.sampling.interval.ms</code>
     */
    public static final String METRIC_SAMPLING_INTERVAL_MS_CONFIG = "metric.sampling.interval.ms";
    public static final long DEFAULT_METRIC_SAMPLING_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
    public static final String METRIC_SAMPLING_INTERVAL_MS_DOC = "The interval of metric sampling.";

    /**
     * <code>min.valid.partition.ratio</code>
     */
    public static final String MIN_VALID_PARTITION_RATIO_CONFIG = "min.valid.partition.ratio";
    public static final double DEFAULT_MIN_VALID_PARTITION_RATIO = 0.995;
    public static final String MIN_VALID_PARTITION_RATIO_DOC = "The minimum percentage of the total partitions "
            + "required to be monitored in order to generate a valid load model. Because the topic and partitions in a "
            + "Kafka cluster are dynamically changing. The load monitor will exclude some of the topics that does not have "
            + "sufficient metric samples. This configuration defines the minimum required percentage of the partitions that "
            + "must be included in the load model.";

    /**
     * <code>leader.network.inbound.weight.for.cpu.util</code>
     */
    public static final String LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "leader.network.inbound.weight.for.cpu.util";
    public static final double DEFAULT_LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL = 0.7;
    public static final String LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the following model to "
            + "derive replica level CPU utilization: REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + "
            + "c * FOLLOWER_BYTES_IN_RATE. This configuration will be used as the weight for LEADER_BYTES_IN_RATE.";

    /**
     * <code>leader.network.outbound.weight.for.cpu.util</code>
     */
    public static final String LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "leader.network.outbound.weight.for.cpu.util";
    public static final double DEFAULT_LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL = 0.15;
    public static final String LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the following model to "
            + "derive replica level CPU utilization: REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + "
            + "c * FOLLOWER_BYTES_IN_RATE. This configuration will be used as the weight for LEADER_BYTES_OUT_RATE.";

    /**
     * <code>follower.network.inbound.weight.for.cpu.util</code>
     */
    public static final String FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "follower.network.inbound.weight.for.cpu.util";
    public static final double DEFAULT_FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL = 0.15;
    public static final String FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the following model to "
            + "derive replica level CPU utilization: REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + "
            + "c * FOLLOWER_BYTES_IN_RATE. This configuration will be used as the weight for FOLLOWER_BYTES_IN_RATE.";

    /**
     * <code>linear.regression.model.cpu.util.bucket.size</code>
     */
    public static final String LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG = "linear.regression.model.cpu.util.bucket.size";
    public static final int DEFAULT_LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE = 5;
    public static final String LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_DOC = "The CPU utilization bucket size for linear regression "
            + "model training data. The unit is percents.";

    /**
     * <code>linear.regression.model.required.samples.per.bucket</code>
     */
    public static final String LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG =
            "linear.regression.model.required.samples.per.bucket";
    public static final int DEFAULT_LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET = 100;
    public static final String LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_DOC = "The number of training samples"
            + " required in each CPU utilization bucket specified by linear.regression.model.cpu.util.bucket";

    /**
     * <code>linear.regression.model.min.num.cpu.util.buckets</code>
     */
    public static final String LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG =
            "linear.regression.model.min.num.cpu.util.buckets";
    public static final int DEFAULT_LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS = 5;
    public static final String LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_DOC = "The minimum number of full CPU"
            + " utilization buckets required to generate a linear regression model.";

    /**
     * <code>fetch.metric.samples.max.retry.count</code>
     */
    public static final String FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_CONFIG = "fetch.metric.samples.max.retry.count";
    public static final int DEFAULT_FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT = 5;
    public static final String FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_DOC = "The maximum number of retries during the "
            + "fetch of the desired replication factor of sample store topics. This config value should be at least 1.";

    /**
     * <code>sampling.allow.cpu.capacity.estimation</code>
     */
    public static final String SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG = "sampling.allow.cpu.capacity.estimation";
    public static final boolean DEFAULT_SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION = true;
    public static final String SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_DOC = "The flag to indicate whether sampling "
            + "process allows CPU capacity estimation of brokers used for CPU utilization estimation.";

    /**
     * <code>use.linear.regression.model</code>
     */
    public static final String USE_LINEAR_REGRESSION_MODEL_CONFIG = "use.linear.regression.model";
    public static final boolean DEFAULT_USE_LINEAR_REGRESSION_MODEL_CONFIG = false;
    public static final String USE_LINEAR_REGRESSION_MODEL_DOC = "Use the linear regression model to estimate the "
            + "cpu utilization.";

    /**
     * <code>sample.store.class</code>
     */
    public static final String SAMPLE_STORE_CLASS_CONFIG = "sample.store.class";
    public static final String DEFAULT_SAMPLE_STORE_CLASS = KafkaSampleStore.class.getName();
    public static final String SAMPLE_STORE_CLASS_DOC = "The sample store class name. User may configure a sample store "
            + "that persists the metric samples that have already been aggregated into Kafka Cruise Control. Later on the "
            + "persisted samples can be reloaded from the sample store to Kafka Cruise Control.";

    /**
     * <code>sample.partition.metrics.store.on.execution.class</code>
     */
    public static final String SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS_CONFIG = "sample.partition.metric.store.on.execution.class";
    public static final String DEFAULT_SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS = NoopSampleStore.class.getName();
    public static final String SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS_DOC = "The sample store during execution class name. "
            + "User may configure a sample store that persists the partition metric samples collected while there is an ongoing execution. "
            + "This config is not intended for enhancing the fault tolerance of the system through recovery of the historical load information.";

    /**
     * <code>topic.config.provider.class</code>
     */
    public static final String TOPIC_CONFIG_PROVIDER_CLASS_CONFIG = "topic.config.provider.class";
    public static final String DEFAULT_TOPIC_CONFIG_PROVIDER_CLASS = KafkaAdminTopicConfigProvider.class.getName();
    public static final String TOPIC_CONFIG_PROVIDER_CLASS_DOC = "The provider class that reports the active configuration of topics.";

    /**
     * <code>broker.capacity.config.resolver.class</code>
     */
    public static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG = "broker.capacity.config.resolver.class";
    public static final String DEFAULT_BROKER_CAPACITY_CONFIG_RESOLVER_CLASS = BrokerCapacityConfigFileResolver.class.getName();
    public static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC = "The broker capacity configuration resolver "
            + "class name. The broker capacity configuration resolver is responsible for getting the broker capacity. The "
            + "default implementation is a file based solution.";

    /**
     * <code>monitor.state.update.interval.ms</code>
     */
    public static final String MONITOR_STATE_UPDATE_INTERVAL_MS_CONFIG = "monitor.state.update.interval.ms";
    public static final long DEFAULT_MONITOR_STATE_UPDATE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);
    public static final String MONITOR_STATE_UPDATE_INTERVAL_MS_DOC = "The load monitor interval to refresh the monitor state.";

    /**
     * <code>metadata.factor.exponent</code>
     */
    public static final String METADATA_FACTOR_EXPONENT_CONFIG = "metadata.factor.exponent";
    public static final double DEFAULT_METADATA_FACTOR_EXPONENT = 1.0;
    public static final String METADATA_FACTOR_EXPONENT_DOC = "The exponent for the metadata factor, which corresponds to "
            + "(number of replicas) * (number of brokers with replicas) ^ exponent.";

    private MonitorConfig() {
    }

    /**
     * Define configs for Monitor.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the configs for Monitor.
     */
    public static ConfigDef define(ConfigDef configDef) {
        return configDef.define(BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        BOOTSTRAP_SERVERS_DOC)
                .define(SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        SECURITY_PROTOCOL_DOC)
                .define(METADATA_MAX_AGE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_METADATA_MAX_AGE_MS,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        METADATA_MAX_AGE_MS_DOC)
                .define(CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_CLIENT_ID,
                        ConfigDef.Importance.MEDIUM,
                        CLIENT_ID_DOC)
                .define(SEND_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_SEND_BUFFER,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        SEND_BUFFER_DOC)
                .define(RECEIVE_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_RECEIVE_BUFFER,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        RECEIVE_BUFFER_DOC)
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_CONNECTIONS_MAX_IDLE_MS,
                        ConfigDef.Importance.MEDIUM,
                        CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_RECONNECT_BACKOFF_MS,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        RECONNECT_BACKOFF_MS_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_REQUEST_TIMEOUT_MS,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        REQUEST_TIMEOUT_MS_DOC)
                .define(PARTITION_METRICS_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_PARTITION_METRICS_WINDOW_MS,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        PARTITION_METRICS_WINDOW_MS_DOC)
                .define(NUM_PARTITION_METRICS_WINDOWS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_NUM_PARTITION_METRICS_WINDOWS,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        NUM_PARTITION_METRICS_WINDOWS_DOC)
                .define(NETWORK_CLIENT_PROVIDER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_NETWORK_CLIENT_PROVIDER_CLASS,
                        ConfigDef.Importance.LOW,
                        NETWORK_CLIENT_PROVIDER_CLASS_DOC)
                .define(SKIP_LOADING_SAMPLES_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_SKIP_LOADING_SAMPLES,
                        ConfigDef.Importance.MEDIUM,
                        SKIP_LOADING_SAMPLES_DOC)
                .define(MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_DOC)
                .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_DOC)
                .define(PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
                .define(BROKER_METRICS_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_BROKER_METRICS_WINDOW_MS,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        BROKER_METRICS_WINDOW_MS_DOC)
                .define(NUM_BROKER_METRICS_WINDOWS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_NUM_BROKER_METRICS_WINDOWS,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        NUM_BROKER_METRICS_WINDOWS_DOC)
                .define(MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MIN_SAMPLES_PER_BROKER_METRICS_WINDOW,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_DOC)
                .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_DOC)
                .define(BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
                .define(METRIC_SAMPLER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_METRIC_SAMPLER_CLASS,
                        ConfigDef.Importance.HIGH,
                        METRIC_SAMPLER_CLASS_DOC)
                .define(METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS,
                        ConfigDef.Importance.LOW,
                        METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_DOC)
                .define(METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_METRIC_SAMPLING_INTERVAL_MS,
                        atLeast(0),
                        ConfigDef.Importance.HIGH,
                        METRIC_SAMPLING_INTERVAL_MS_DOC)
                .define(MIN_VALID_PARTITION_RATIO_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        DEFAULT_MIN_VALID_PARTITION_RATIO,
                        between(0, 1),
                        ConfigDef.Importance.HIGH,
                        MIN_VALID_PARTITION_RATIO_DOC)
                .define(LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        DEFAULT_LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL,
                        between(0, 1),
                        ConfigDef.Importance.MEDIUM,
                        LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
                .define(LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        DEFAULT_LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL,
                        between(0, 1),
                        ConfigDef.Importance.MEDIUM,
                        LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
                .define(FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        DEFAULT_FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL,
                        between(0, 1),
                        ConfigDef.Importance.MEDIUM,
                        FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
                .define(LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE,
                        between(0, 100),
                        ConfigDef.Importance.MEDIUM,
                        LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_DOC)
                .define(LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_DOC)
                .define(LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS,
                        ConfigDef.Importance.MEDIUM,
                        LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_DOC)
                .define(FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_DOC)
                .define(SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION,
                        ConfigDef.Importance.LOW,
                        SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_DOC)
                .define(USE_LINEAR_REGRESSION_MODEL_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_USE_LINEAR_REGRESSION_MODEL_CONFIG,
                        ConfigDef.Importance.MEDIUM,
                        USE_LINEAR_REGRESSION_MODEL_DOC)
                .define(SAMPLE_STORE_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_SAMPLE_STORE_CLASS,
                        ConfigDef.Importance.LOW,
                        SAMPLE_STORE_CLASS_DOC)
                .define(SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS_CONFIG,
                        ConfigDef.Type.CLASS, DEFAULT_SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS,
                        ConfigDef.Importance.LOW, SAMPLE_PARTITION_METRIC_STORE_ON_EXECUTION_CLASS_DOC)
                .define(TOPIC_CONFIG_PROVIDER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_TOPIC_CONFIG_PROVIDER_CLASS,
                        ConfigDef.Importance.LOW,
                        TOPIC_CONFIG_PROVIDER_CLASS_DOC)
                .define(BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_BROKER_CAPACITY_CONFIG_RESOLVER_CLASS,
                        ConfigDef.Importance.MEDIUM,
                        BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC)
                .define(MONITOR_STATE_UPDATE_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_MONITOR_STATE_UPDATE_INTERVAL_MS,
                        ConfigDef.Importance.LOW,
                        MONITOR_STATE_UPDATE_INTERVAL_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                .define(METADATA_FACTOR_EXPONENT_CONFIG,
                        ConfigDef.Type.DOUBLE,
                        DEFAULT_METADATA_FACTOR_EXPONENT,
                        atLeast(1.0),
                        ConfigDef.Importance.LOW,
                        METADATA_FACTOR_EXPONENT_DOC);
    }
}
