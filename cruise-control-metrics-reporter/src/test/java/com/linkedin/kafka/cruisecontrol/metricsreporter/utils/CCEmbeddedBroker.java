/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.server.KafkaConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaRaftServer.CLUSTER_ID_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.KafkaServerConfigs.METADATA_LOG_DIR_CONFIG;

public class CCEmbeddedBroker implements AutoCloseable {
  private final Map<SecurityProtocol, Integer> _ports;
  private final Map<SecurityProtocol, String> _hosts;
  private final CCKafkaRaftServer _kafkaServer;
  private int _id;
  private final List<File> _logDirs;
  private File _metadataLogDir;

  public CCEmbeddedBroker(Map<Object, Object> config) {
    _ports = new HashMap<>();
    _hosts = new HashMap<>();
    _logDirs = new ArrayList<>();

    try {
      // Also validates the config
      KafkaConfig kafkaConfig = new KafkaConfig(config, true);
      parseConfigs(config);

      _kafkaServer = new CCKafkaRaftServer(kafkaConfig, config.get(CLUSTER_ID_CONFIG).toString(), Time.SYSTEM);

      startup();
      _ports.replaceAll((securityProtocol, port) -> {
        try {
          return _kafkaServer.boundBrokerPort(ListenerName.forSecurityProtocol(securityProtocol));
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      });
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void parseConfigs(Map<Object, Object> config) {
    readLogDirs(config);
    _id = Integer.parseInt((String) config.get(KRaftConfigs.NODE_ID_CONFIG));
    _metadataLogDir = new File((String) config.get(METADATA_LOG_DIR_CONFIG));

    // Bind addresses
    String listenersString = (String) config.get(SocketServerConfigs.LISTENERS_CONFIG);
    for (String protocolAddr : listenersString.split("\\s*,\\s*")) {
      try {
        URI uri = new URI(protocolAddr.trim());
        SecurityProtocol protocol = SecurityProtocol.forName(uri.getScheme());
        _hosts.put(protocol, uri.getHost());
        // We get the value after boot
        _ports.put(protocol, null);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private void readLogDirs(Map<Object, Object> config) {
    String logdir = (String) config.get(ServerLogConfigs.LOG_DIR_CONFIG);
    String[] paths = logdir.split(",");
    for (String path : paths) {
      _logDirs.add(new File(path));
    }
  }

  public int id() {
    return _id;
  }

  /**
   * @param protocol Security protocol.
   * @return Address containing host and port.
   */
  public String addr(SecurityProtocol protocol) {
    if (!_hosts.containsKey(protocol)) {
      return null;
    }
    return _hosts.get(protocol) + ":" + _ports.get(protocol);
  }

  public String plaintextAddr() {
    return addr(SecurityProtocol.PLAINTEXT);
  }

  public String sslAddr() {
    return addr(SecurityProtocol.SSL);
  }

  public void startup() {
    _kafkaServer.startup();
  }

  public void shutdown() {
    _kafkaServer.shutdown();
  }

  public void awaitShutdown() {
    _kafkaServer.awaitShutdown();
  }

  @Override
  public void close() {
    CCKafkaTestUtils.quietly(this::shutdown);
    CCKafkaTestUtils.quietly(this::awaitShutdown);
    CCKafkaTestUtils.quietly(() -> FileUtils.forceDelete(_metadataLogDir));
    for (File logDir : _logDirs) {
      CCKafkaTestUtils.quietly(() -> FileUtils.forceDelete(logDir));
    }
  }
}
