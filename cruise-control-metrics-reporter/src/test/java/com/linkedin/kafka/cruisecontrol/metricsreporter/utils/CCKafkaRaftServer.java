/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker$;
import kafka.server.KafkaConfig;
import kafka.server.Server;
import kafka.server.Server$;
import kafka.server.SharedServer;
import kafka.server.StandardFaultHandlerFactory;
import kafka.utils.Mx4jLoader;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaProperties.Builder;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.Copier;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.Loader;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.ServerSocketFactory;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.config.ServerTopicConfigSynonyms;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.KafkaServerConfigs.METADATA_LOG_DIR_CONFIG;

/**
 * This class implements the KRaft (Kafka Raft) mode server which relies on a KRaft quorum for maintaining cluster
 * metadata. It is responsible for constructing the controller and/or broker based on the `process.roles` configuration
 * and for managing their basic lifecycle (startup and shutdown).
 */
public class CCKafkaRaftServer implements Server {
    public static final String CLUSTER_ID_CONFIG = "cluster.id";
    private static final Logger LOG = LoggerFactory.getLogger(CCKafkaRaftServer.class);
    private static final KafkaConfigSchema CONFIG_SCHEMA = new KafkaConfigSchema(Map.of(
            Type.BROKER, new ConfigDef(KafkaConfig.configDef()),
            Type.TOPIC, LogConfig.configDefCopy()
    ), ServerTopicConfigSynonyms.ALL_TOPIC_CONFIG_SYNONYMS);
    private static final String VERSION_CONFIG = "version";
    private final KafkaConfig _config;
    private final String _clusterId;
    private final Time _time;
    private final Metrics _metrics;
    private final Optional<BrokerServer> _broker;
    private final Optional<ControllerServer> _controller;

    public CCKafkaRaftServer(KafkaConfig config, String clusterId, Time time) {
        _config = config;
        _clusterId = clusterId;
        _time = time;
        String logIdent = "[CCKafkaRaftServer nodeId=" + config.nodeId() + "] ";

        initializeMetaData(_config);
        KafkaMetricsReporter.startReporters(new VerifiableProperties(parseConfigs(config.originals())));
        KafkaYammerMetrics.INSTANCE.configure(config.originals());

        MetaTuple metaTuple = initializeLogDirs(config, logIdent);
        MetaPropertiesEnsemble metaPropsEnsemble = metaTuple.getMetaPropertiesEnsemble();

        if (metaPropsEnsemble.clusterId().isEmpty()) {
            throw new RuntimeException("MetaPropertiesEnsemble contains empty cluster ID.");
        }
        _metrics = Server.initializeMetrics(config, time, metaPropsEnsemble.clusterId().get());
        SharedServer sharedServer = new SharedServer(
                config,
                metaPropsEnsemble,
                time,
                _metrics,
                CompletableFuture.completedFuture(QuorumConfig.parseVoterConnections(config.quorumConfig().voters())),
                new ArrayList<>(),
                new StandardFaultHandlerFactory(),
                new ServerSocketFactory.KafkaServerSocketFactory()
        );

        _broker = config.processRoles().contains(ProcessRole.BrokerRole)
                ? Optional.of(new BrokerServer(sharedServer))
                : Optional.empty();
        _controller = config.processRoles().contains(ProcessRole.ControllerRole)
                ? Optional.of(new ControllerServer(sharedServer, CONFIG_SCHEMA, metaTuple.getBootstrapMetadata()))
                : Optional.empty();
    }

    @Override
    public void startup() {
        Mx4jLoader.maybeLoad();
        _controller.ifPresent(ControllerServer::startup);
        _broker.ifPresent(BrokerServer::startup);
        AppInfoParser.registerAppInfo(Server$.MODULE$.MetricsPrefix(), Integer.toString(_config.brokerId()), _metrics, _time.milliseconds());
        LOG.info(KafkaBroker$.MODULE$.STARTED_MESSAGE());
    }

    @Override
    public void shutdown() {
        _broker.ifPresent(BrokerServer::shutdown);
        _controller.ifPresent(ControllerServer::shutdown);
        try {
            AppInfoParser.unregisterAppInfo(Server$.MODULE$.MetricsPrefix(), Integer.toString(_config.brokerId()), _metrics);
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    @Override
    public void awaitShutdown() {
        _broker.ifPresent(BrokerServer::awaitShutdown);
        _controller.ifPresent(ControllerServer::awaitShutdown);
    }

    /**
     * Bound broker port for the specified listener. Throws expection if no broker was initialized.
     * @param listenerName name of the listener
     * @return port for the specified listener of the broker
     */
    public int boundBrokerPort(ListenerName listenerName) {
        return _broker.orElseThrow(() -> new RuntimeException("Cannot bound broker port when broker role is not configured."))
                .boundPort(listenerName);
    }

    /**
     * Bound controller port for the specified listener. Throws expection if no controller was initialized.
     * @return port for the controller
     */
    public int boundControllerPort() throws ExecutionException, InterruptedException {
        return _controller.orElseThrow(() -> new RuntimeException("Cannot bound controller port when controller role is not configured."))
                .socketServerFirstBoundPortFuture().get();
    }

    private static MetaTuple initializeLogDirs(KafkaConfig config, String logPrefix) {
        Loader loader = new Loader();
        loader.addMetadataLogDir(config.metadataLogDir());
        loader.addLogDirs(config.logDirs());

        // Load the MetaPropertiesEnsemble
        MetaPropertiesEnsemble initialMetaPropsEnsemble;
        try {
            initialMetaPropsEnsemble = loader.load();
        } catch (IOException e) {
            throw new RuntimeException("MetaPropertiesEnsemble loading failed.", e);
        }
        initialMetaPropsEnsemble.verify(
                Optional.empty(),
                OptionalInt.of(config.nodeId()),
                EnumSet.of(VerificationFlag.REQUIRE_AT_LEAST_ONE_VALID, VerificationFlag.REQUIRE_METADATA_LOG_DIR)
        );
        initialMetaPropsEnsemble.logDirProps().keySet().forEach(logDir -> {
            if (!logDir.equals(config.metadataLogDir())) {
                TopicPartition tp = Topic.CLUSTER_METADATA_TOPIC_PARTITION;
                File clusterMetadataTopic = new File(logDir, tp.topic() + "-" + tp.partition());
                if (clusterMetadataTopic.exists()) {
                    throw new KafkaException("Unexpected metadata directory (" + config.metadataLogDir() + ") found for " + clusterMetadataTopic);
                }
            }
        });

        Copier copier = new Copier(initialMetaPropsEnsemble);
        initialMetaPropsEnsemble.nonFailedDirectoryProps().forEachRemaining(ensemble -> {
            String logDir = ensemble.getKey();
            MetaProperties metaProps = ensemble.getValue().orElseThrow(() -> new RuntimeException("No `meta.properties` found in " + logDir));

            Builder builder = new Builder(metaProps);
            if (builder.directoryId().isEmpty()) {
                builder.setDirectoryId(copier.generateValidDirectoryId());
            }

            copier.setLogDirProps(logDir, builder.build());
            copier.setPreWriteHandler((dir, isNew, properties) -> LOG.info("{}Rewriting {}{}meta.properties", logPrefix, dir, File.separator));
            try {
                copier.writeLogDirChanges();
            } catch (IOException e) {
                throw new RuntimeException("Exception during write of the log directory changes.", e);
            }
        });
        MetaPropertiesEnsemble metaPropsEnsemble = copier.copy();

        // Load the BootstrapMetadata
        BootstrapDirectory bootstrapDirectory = new BootstrapDirectory(config.metadataLogDir());
        BootstrapMetadata bootstrapMetadata;
        try {
            bootstrapMetadata = bootstrapDirectory.read();
        } catch (Exception e) {
            throw new RuntimeException("BootstrapMetadata reading failed.", e);
        }

        return new MetaTuple(metaPropsEnsemble, bootstrapMetadata);
    }

    private void initializeMetaData(KafkaConfig config) {
        Set<File> allLogDirs = new HashSet<>(readLogDirs(config));
        allLogDirs.add(new File(config.getString(METADATA_LOG_DIR_CONFIG)));
        for (File logDir : allLogDirs) {
            File metaPropsFile = new File(logDir, "meta.properties");
            if (!metaPropsFile.exists()) {
                try {
                    if (!metaPropsFile.createNewFile()) {
                        throw new RuntimeException("File cannot be created - " + metaPropsFile);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }

            Properties properties = new Properties();
            properties.setProperty(VERSION_CONFIG, MetaPropertiesVersion.V1.numberString());
            properties.setProperty(CLUSTER_ID_CONFIG, _clusterId);
            properties.setProperty(KRaftConfigs.NODE_ID_CONFIG, config.get(KRaftConfigs.NODE_ID_CONFIG).toString());
            // Kafka 4.3 changed log.dir(s) to a LIST config; join it back to the comma-separated form meta.properties expects.
            properties.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, String.join(",", config.getList(ServerLogConfigs.LOG_DIR_CONFIG)));
            properties.setProperty(METADATA_LOG_DIR_CONFIG, config.getString(METADATA_LOG_DIR_CONFIG));

            try (FileOutputStream out = new FileOutputStream(metaPropsFile)) {
                properties.store(out, "Meta Properties");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private Set<File> readLogDirs(KafkaConfig config) {
        Set<File> logDirs = new HashSet<>();
        for (String path : config.getList(ServerLogConfigs.LOG_DIR_CONFIG)) {
            logDirs.add(new File(path.trim()));
        }
        return logDirs;
    }

    private Properties parseConfigs(Map<String, ?> configMap) {
        return configMap.entrySet().stream().collect(Collectors.toMap(
                Entry::getKey,
                Entry::getValue,
                (prev, next) -> next, Properties::new
        ));
    }

    private static final class MetaTuple {
        private final MetaPropertiesEnsemble _metaPropertiesEnsemble;
        private final BootstrapMetadata _bootstrapMetadata;

        private MetaTuple(MetaPropertiesEnsemble metaPropertiesEnsemble, BootstrapMetadata bootstrapMetadata) {
            _metaPropertiesEnsemble = metaPropertiesEnsemble;
            _bootstrapMetadata = bootstrapMetadata;
        }

        public MetaPropertiesEnsemble getMetaPropertiesEnsemble() {
            return _metaPropertiesEnsemble;
        }

        public BootstrapMetadata getBootstrapMetadata() {
            return _bootstrapMetadata;
        }
    }
}
