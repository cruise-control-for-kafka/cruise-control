/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaRaftServer.CLUSTER_ID_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.KafkaServerConfigs.METADATA_LOG_DIR_CONFIG;


public class CCEmbeddedBrokerBuilder {
  private static final AtomicInteger BROKER_ID_COUNTER = new AtomicInteger();

  //mandatory fields
  private int _nodeId = BROKER_ID_COUNTER.incrementAndGet();
  private String _kraftConnect;
  private String _clusterId;
  //storage config
  private File _logDirectory;
  private File _metadataLogDirectory;
  //networking config
  private int _plaintextPort = -1;
  private int _sslPort = -1;
  private File _trustStore;
  private long _socketTimeoutMs = 1500;
  //feature control
  private boolean _enableControlledShutdown;
  private boolean _enableDeleteTopic;
  //resource management
  // 2MB
  private long _logCleanerDedupBufferSize = 2097152;
  private String _rack;

  public CCEmbeddedBrokerBuilder() {
  }

  /**
   * Set node id.
   *
   * @param nodeId Node id to set.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder nodeId(int nodeId) {
    _nodeId = nodeId;
    return this;
  }

  /**
   * Set the KRaft quorum voters string
   * @param kraftConnect quorum voters string
   * @return This.
   */
  public CCEmbeddedBrokerBuilder kraftConnect(String kraftConnect) {
    _kraftConnect = kraftConnect;
    return this;
  }

  /**
   * Set the KRaft quorum voters string
   * @param kraft {@link CCEmbeddedBrokerBuilder} instance
   * @return This.
   */
  public CCEmbeddedBrokerBuilder kraftConnect(CCEmbeddedKRaftController kraft) {
    return kraftConnect(kraft.quorumVoters());
  }

  /**
   * Set the Kafka cluster ID
   * @param clusterId the ID of the Kafka cluster
   * @return This.
   */
  public CCEmbeddedBrokerBuilder clusterId(String clusterId) {
    _clusterId = clusterId;
    return this;
  }

  /**
   * Set the Kafka cluster ID
   * @param kraft {@link CCEmbeddedBrokerBuilder} instance
   * @return This.
   */
  public CCEmbeddedBrokerBuilder clusterId(CCEmbeddedKRaftController kraft) {
    return clusterId(kraft.clusterId());
  }

  /**
   * Set log directory.
   *
   * @param logDirectory Log directory to set.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder logDirectory(File logDirectory) {
    _logDirectory = logDirectory;
    return this;
  }

  /**
   * Enable security protocol.
   *
   * @param protocol Security protocol to enable.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder enable(SecurityProtocol protocol) {
    switch (protocol) {
      case PLAINTEXT -> enablePlaintext();
      case SSL -> enableSsl();
      default -> throw new IllegalStateException("unhandled: " + protocol);
    }
    return this;
  }

  /**
   * Set plaintext port.
   *
   * @param plaintextPort Plaintext port.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder plaintextPort(int plaintextPort) {
    _plaintextPort = plaintextPort;
    return this;
  }

  /**
   * Enable plaintext by setting its port to 0.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder enablePlaintext() {
    return plaintextPort(0);
  }

  /**
   * Set SSL port.
   * @param sslPort SSL port to set.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder sslPort(int sslPort) {
    _sslPort = sslPort;
    return this;
  }

  /**
   * Enable SSL by setting ssl port to 0.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder enableSsl() {
    return sslPort(0);
  }

  /**
   * Set trust store.
   *
   * @param trustStore Trust store.
   * @return This
   */
  public CCEmbeddedBrokerBuilder trustStore(File trustStore) {
    _trustStore = trustStore;
    return this;
  }

  /**
   * Set socket timeout
   * @param socketTimeoutMs Socket timeout.
   * @return This
   */
  public CCEmbeddedBrokerBuilder socketTimeoutMs(long socketTimeoutMs) {
    _socketTimeoutMs = socketTimeoutMs;
    return this;
  }

  /**
   * Set enableControlledShutdown.
   *
   * @param enableControlledShutdown {@code true} to enable controlled shutdown, {@code false} otherwise.
   * @return This
   */
  public CCEmbeddedBrokerBuilder enableControlledShutdown(boolean enableControlledShutdown) {
    _enableControlledShutdown = enableControlledShutdown;
    return this;
  }

  /**
   * Enable delete topic.
   *
   * @param enableDeleteTopic {@code true} to enable delete topic, {@code false} otherwise.
   * @return This
   */
  public CCEmbeddedBrokerBuilder enableDeleteTopic(boolean enableDeleteTopic) {
    _enableDeleteTopic = enableDeleteTopic;
    return this;
  }

  /**
   * Set log cleaner dedup buffer size.
   * @param logCleanerDedupBufferSize log cleaner dedup buffer size.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder logCleanerDedupBufferSize(long logCleanerDedupBufferSize) {
    _logCleanerDedupBufferSize = logCleanerDedupBufferSize;
    return this;
  }

  /**
   * @param rack Rack to set.
   * @return This.
   */
  public CCEmbeddedBrokerBuilder rack(String rack) {
    _rack = rack;
    return this;
  }

  private void applyDefaults() {
    if (_logDirectory == null) {
      _logDirectory = CCKafkaTestUtils.newTempDir();
    }
    if (_metadataLogDirectory == null) {
      _metadataLogDirectory = CCKafkaTestUtils.newTempDir();
    }
  }

  private void validate() throws IllegalArgumentException {
    if (_plaintextPort < 0 && _sslPort < 0) {
      throw new IllegalArgumentException("at least one protocol must be used");
    }
    if (_logDirectory == null) {
      throw new IllegalArgumentException("log directory must be specified");
    }
    if (_metadataLogDirectory == null) {
      throw new IllegalArgumentException("metadata log directory must be specified");
    }
  }

  /**
   * @return Config properties.
   */
  public Map<Object, Object> buildConfig() {
    applyDefaults();
    validate();

    Map<Object, Object> props = new HashMap<>();

    StringJoiner csvJoiner = new StringJoiner(",");
    if (_plaintextPort >= 0) {
      csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:" + _plaintextPort);
    }
    if (_sslPort >= 0) {
      csvJoiner.add(SecurityProtocol.SSL.name + "://localhost:" + _sslPort);
    }
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
    props.put(KRaftConfigs.NODE_ID_CONFIG, Integer.toString(_nodeId));
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, _kraftConnect);
    props.put(CLUSTER_ID_CONFIG, _clusterId);
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL");
    props.put(SocketServerConfigs.LISTENERS_CONFIG, csvJoiner.toString());
    props.put(ServerLogConfigs.LOG_DIR_CONFIG, _logDirectory.getAbsolutePath());
    props.put(METADATA_LOG_DIR_CONFIG, _metadataLogDirectory.getAbsolutePath());
    props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, Long.toString(_socketTimeoutMs));
    props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, Long.toString(_socketTimeoutMs));
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, Boolean.toString(_enableControlledShutdown));
    props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, Boolean.toString(_enableDeleteTopic));
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, Long.toString(_logCleanerDedupBufferSize));
    props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    if (_rack != null) {
      props.put(ServerConfigs.BROKER_RACK_CONFIG, _rack);
    }
    if (_trustStore != null || _sslPort > 0) {
      try {
        props.putAll(CCSslTestUtils.createSslConfig(false, true,
            CCSslTestUtils.ConnectionMode.SERVER, _trustStore, "server" + _nodeId));
        // Switch interbroker to ssl
        props.put(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    return props;
  }

  public CCEmbeddedBroker build() {
    return new CCEmbeddedBroker(buildConfig());
  }
}
