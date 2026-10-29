/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.MountableFile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.DEFAULT_CRUISE_CONTROL_METRICS_TOPIC;

/**
 * The {@code CCContainerizedKraftCluster} class creates a containerized KRaft Kafka cluster used for testing purposes.
 *
 * <p>This configuration sets up multiple listeners for different internal and external communication paths.
 * <ul>
 *   <li><b>CONTROLLER://0.0.0.0:9094</b>: Used for KRaft controller communication.</li>
 *   <li><b>INTERNAL://0.0.0.0:9095</b>: Used for client communication within the container network.</li>
 *   <li><b>EXTERNAL://0.0.0.0:9096</b>: Used for client communication outside the container network.</li>
 * </ul>
 */
public class CCContainerizedKraftCluster implements Startable {
  private static final String KAFKA_IMAGE = System.getenv().getOrDefault("KAFKA_IMAGE", "apache/kafka:4.3.2");
  /**
   * Determines the hostname used by containers to connect to services running on the host machine.
   * Required for CI environments like CircleCI, where the Docker executor relies on a specific hostname
   * for the CircleCI job to communicate with TestContainers services.
   */
  private static final String CONTAINER_HOST = System.getenv().getOrDefault("CONTAINER_HOST", "localhost");
  /**
   * Selects or creates the Docker network used for TestContainers communication.
   * Required for CI environments like CircleCI, where the Docker executor requires sharing the Docker network
   * to allow the CircleCI job to communicate with TestContainers services.
   */
  private static final Network NETWORK = System.getenv("NETWORK_NAME") != null
    ? Network.builder().id(System.getenv("NETWORK_NAME")).build()
    : Network.newNetwork();

  private static final String NETWORK_ALIAS_PREFIX = "broker";
  public static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
  public static final String INTERNAL_LISTENER_NAME = "INTERNAL";
  public static final String EXTERNAL_LISTENER_NAME = "EXTERNAL";

  private static final int CONTAINER_CONTROLLER_LISTENER_PORT = 9094;
  public static final int CONTAINER_INTERNAL_LISTENER_PORT = 9095;
  private static final int CONTAINER_EXTERNAL_LISTENER_PORT = 9096;

  private final List<KafkaContainer> _brokers;
  private final String _clusterId;
  private final String _controllerQuorumVoters;
  private String _bootstrapServers;
  private final List<String> _brokerAddressList;
  private final Admin _adminClient;

  public CCContainerizedKraftCluster(int numOfBrokers, List<Map<Object, Object>> brokerConfigs, Properties adminClientProps) {
    if (numOfBrokers <= 0) {
      throw new IllegalArgumentException("numOfBrokers '" + numOfBrokers + "' must be greater than 0");
    }

    this._clusterId = Uuid.randomUuid().toString();
    this._controllerQuorumVoters = getControllerQuorumVoters(numOfBrokers);
    List<Integer> containerHostPorts = getContainerHostPorts(numOfBrokers);
    this._brokerAddressList = new ArrayList<>(generateServerAddressList(numOfBrokers, containerHostPorts));
    this._bootstrapServers = String.join(",", _brokerAddressList);

    /*
     * Ideally, we would construct the admin client properties directly in this constructor. However, due to the current
     * inheritance structure in the test classes, we rely on externally provided properties to preserve required security
     * configurations. Cloning and augmenting the provided properties here is the simplest and safest way to handle this
     * without breaking existing test setups.
     */
    Properties adminClientPropsWithBootstrapAddress = new Properties();
    adminClientPropsWithBootstrapAddress.putAll(adminClientProps);
    adminClientPropsWithBootstrapAddress.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    adminClientPropsWithBootstrapAddress.setProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10000");
    this._adminClient = Admin.create(adminClientPropsWithBootstrapAddress);

    this._brokers = IntStream.range(0, numOfBrokers)
      .mapToObj(brokerNum -> createBrokerContainer(brokerNum, brokerConfigs.get(brokerNum), "broker,controller",
        containerHostPorts.get(brokerNum)))
      .collect(Collectors.toCollection(ArrayList::new));
  }

  private KafkaContainer createBrokerContainer(int brokerNum, Map<Object, Object> brokerConfig, String processRoles, int containerHostPort) {
    brokerConfig.put("node.id", brokerNum + "");
    brokerConfig.put("process.roles", processRoles);

    List<String> listeners = new ArrayList<>();
    listeners.add(INTERNAL_LISTENER_NAME + "://0.0.0.0:" + CONTAINER_INTERNAL_LISTENER_PORT);
    listeners.add(EXTERNAL_LISTENER_NAME + "://0.0.0.0:" + CONTAINER_EXTERNAL_LISTENER_PORT);
    if (processRoles.contains("controller")) {
      listeners.add(CONTROLLER_LISTENER_NAME + "://0.0.0.0:" + CONTAINER_CONTROLLER_LISTENER_PORT);
    }
    brokerConfig.put("listeners", String.join(",", listeners));

    brokerConfig.put("controller.quorum.voters", this._controllerQuorumVoters);
    brokerConfig.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    brokerConfig.put(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      "127.0.0.1:" + CONTAINER_INTERNAL_LISTENER_PORT);
    brokerConfig.put(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    brokerConfig.putIfAbsent(CRUISE_CONTROL_METRICS_TOPIC_CONFIG, DEFAULT_CRUISE_CONTROL_METRICS_TOPIC);

    // TestContainers automatically sets `inter.broker.listener.name` so we must disable `security.inter.broker.protocol`
    // https://kafka.apache.org/documentation/#brokerconfigs_inter.broker.listener.name
    brokerConfig.put("inter.broker.listener.name", INTERNAL_LISTENER_NAME);
    brokerConfig.remove("security.inter.broker.protocol");

    String networkAlias = String.format("%s-%d", NETWORK_ALIAS_PREFIX, brokerNum);
    String metricsTopic = (String) brokerConfig.get(CRUISE_CONTROL_METRICS_TOPIC_CONFIG);

    KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE) {
      // Overridden to allow specifying listeners in `advertised.listeners` list.
      // Tracking issue in TestContainer's Kafka module here: https://github.com/testcontainers/testcontainers-java/issues/10718
      @Override
      protected void containerIsStarting(InspectContainerResponse containerInfo) {
        String advertisedListeners = String.join(",",
          INTERNAL_LISTENER_NAME + "://" + containerInfo.getConfig().getHostName() + ":" + CONTAINER_INTERNAL_LISTENER_PORT,
          EXTERNAL_LISTENER_NAME + "://" + CONTAINER_HOST + ":" + containerHostPort);

        String command = String.format("#!/bin/bash%n"
          + "export KAFKA_ADVERTISED_LISTENERS=%s%n"
          + "/etc/kafka/docker/run%n", advertisedListeners);
        copyFileToContainer(Transferable.of(command, 0777), "/tmp/testcontainers_start.sh");
      }
    }
    .withNetwork(NETWORK)
    .withNetworkAliases(networkAlias)
    .withExposedPorts(CONTAINER_EXTERNAL_LISTENER_PORT)
    // Run as root to avoid permission issues when copying test resources
    // (e.g., metrics reporter JAR) into the image's Kafka install directories.
    .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
    .withEnv("CLUSTER_ID", this._clusterId)
    // Uncomment the following line when debugging Kafka cluster problems.
    //.withLogConsumer(outputFrame -> System.out.print(networkAlias + " | " + outputFrame.getUtf8String()))
    // Uncomment the following line when debugging SSL connection problems.
    //.withEnv("KAFKA_OPTS", "-Djavax.net.debug=ssl,handshake")
    .waitingFor(new BrokerWaitStrategy(brokerNum, metricsTopic, _adminClient).withStartupTimeout(Duration.ofMinutes(1)));
    kafkaContainer.setPortBindings(List.of(containerHostPort + ":" + CONTAINER_EXTERNAL_LISTENER_PORT));

    overrideBrokerConfig(kafkaContainer, brokerConfig);

    // Mount metrics reporter and copy generated certs into container
    setupContainerResources(kafkaContainer, brokerConfig);

    return kafkaContainer;
  }

  private String getControllerQuorumVoters(int numOfBrokers) {
    return IntStream
      .range(0, numOfBrokers)
      .mapToObj(brokerNum -> String.format("%d@%s-%d:%d", brokerNum, NETWORK_ALIAS_PREFIX, brokerNum, CONTAINER_CONTROLLER_LISTENER_PORT))
      .collect(Collectors.joining(","));
  }

  private List<Integer> getContainerHostPorts(int numOfBrokers) {
    return IntStream.range(0, numOfBrokers)
      .mapToObj(i -> CCKafkaTestUtils.findLocalPort())
      .toList();
  }

  private List<String> generateServerAddressList(int numOfBrokers, List<Integer> containerHostPorts) {
    return IntStream.range(0, numOfBrokers)
      .mapToObj(i -> String.format("%s:%s", CONTAINER_HOST, containerHostPorts.get(i)))
      .toList();
  }

  private void copyCertToContainer(KafkaContainer container, Map<Object, Object> config, String key) {
    if (config.containsKey(key)) {
      String path = config.get(key).toString();
      container.withCopyToContainer(MountableFile.forHostPath(path, 0644), path);
    }
  }

  /**
   * Add new broker to existing containerized Kraft cluster.
   *
   * @param brokerId     the node ID for the new broker
   * @param brokerConfig the broker configuration properties
   */
  public void addBroker(int brokerId, Map<Object, Object> brokerConfig) {
    int hostPort = CCKafkaTestUtils.findLocalPort();
    KafkaContainer broker = createBrokerContainer(brokerId, brokerConfig, "broker", hostPort);
    String brokerAddress = String.format("%s:%s", CONTAINER_HOST, hostPort);
    broker.start();

    _brokers.add(broker);
    _brokerAddressList.add(brokerAddress);
    _bootstrapServers = String.join(",", _brokerAddressList);
  }

  /**
   * Sets up required resources inside the Kafka container, including the Cruise Control Metrics Reporter JAR.
   *
   * <p>Note: This requires the 'cruise-control-metrics-reporter' project to be built beforehand.
   * The JAR is expected to be located in '../cruise-control-metrics-reporter/build/libs/'.</p>
   *
   * @param kafkaContainer the Kafka test container
   * @param brokerConfig   the Kafka broker configuration
   */
  private void setupContainerResources(KafkaContainer kafkaContainer, Map<Object, Object> brokerConfig) {
    Path projectRoot = Paths.get("").toAbsolutePath().normalize();

    Path metricsReporterJarPath = projectRoot
      .resolveSibling("cruise-control-metrics-reporter")
      .resolve("build")
      .resolve("libs");

    try {
      Path jarPath = Files.list(metricsReporterJarPath)
        .filter(path -> path.getFileName().toString().startsWith("cruise-control-metrics-reporter"))
        .filter(path -> path.getFileName().toString().endsWith(".jar"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Cruise Control Metrics Reporter jar not found in: " + metricsReporterJarPath
          + "The 'cruise-control-metrics-reporter' project must be built first."));

      kafkaContainer.withCopyToContainer(
        MountableFile.forHostPath(jarPath.toString(), 0755),
        "/opt/kafka/libs/cruise-control-metrics-reporter.jar"
      );

      copyCertToContainer(kafkaContainer, brokerConfig, "ssl.truststore.location");
      copyCertToContainer(kafkaContainer, brokerConfig, "ssl.keystore.location");
      copyCertToContainer(kafkaContainer, brokerConfig, "cruise.control.metrics.reporter.ssl.keystore.location");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to mount Kafka container resources", e);
    }
  }

  /**
   * Updates the broker configuration of the Kafka container.
   *
   * @param kafkaContainer The Kafka container to be updated.
   * @param brokerConfig A map of properties that that will be translated into environment variables which the TestContainers
   *                     scripts will use to override the default broker configuration.
   */
  public void overrideBrokerConfig(KafkaContainer kafkaContainer, Map<Object, Object> brokerConfig) {
    for (Map.Entry<Object, Object> entry : brokerConfig.entrySet()) {
      String key = String.valueOf(entry.getKey());
      Object rawValue = entry.getValue();
      String value;

      if (rawValue instanceof Collection<?> col) {
        value = String.join(",", col.stream()
          .map(Object::toString)
          .toArray(String[]::new));
      } else if (rawValue instanceof String[] arr) {
        value = String.join(",", arr);
      } else if (rawValue instanceof Password pass) {
        value = pass.value();
      } else if (rawValue instanceof String str) {
        value = str;
      } else {
        value = String.valueOf(rawValue);
      }

      // Convert Kafka broker properties key to env var format: e.g., log.retention.hours -> KAFKA_LOG_RETENTION_HOURS
      String envKey = "KAFKA_" + key.toUpperCase().replace('.', '_');
      kafkaContainer.withEnv(envKey, value);
    }
  }

  /**
   * Returns the {@link Admin} client used by this cluster instance.
   *
   * @return the {@link Admin} client associated with this cluster
   */
  public Admin adminClient() {
    return _adminClient;
  }

  /**
   * Returns list of KafkaContainer broker objects within the TestContainer Kafka cluster.
   *
   * @return List of KafkaContainer broker objects within the TestContainer Kafka cluster.
   */
  public List<KafkaContainer> getBrokers() {
    return this._brokers;
  }

  /**
   * Returns a comma-separated list of bootstrap server addresses that are reachable from container host.
   *
   * @return A comma-separated list of external bootstrap server addresses in the form {@code host:port}.
   */
  public String getExternalBootstrapAddress() {
    return _bootstrapServers;
  }

  /**
   * Returns the list of broker addresses.
   *
   * @return a list of broker address strings.
   */
  public List<String> getBrokerAddressList() {
    return _brokerAddressList;
  }

  @Override
  public void start() {
    _brokers.parallelStream().forEach(GenericContainer::start);
  }

  @Override
  public void stop() {
    this._adminClient.close();
    this._brokers.stream().parallel().forEach(GenericContainer::close);
  }

  private static <T> T waitUntil(Supplier<T> supplier, Predicate<T> condition, Duration timeout, String timeoutMessage) {
    long deadline = System.currentTimeMillis() + timeout.toMillis();

    while (System.currentTimeMillis() < deadline) {
      T value;
      try {
        value = supplier.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (condition.test(value)) {
        return value;
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    throw new RuntimeException(timeoutMessage);
  }

  /**
   * Waits until the metadata for the listed Kafka topics meet the specified condition,
   * or until the given timeout period elapses.
   *
   * @param topicNames the names of the topics whose metadata should be monitored
   * @param fetchTimeout the timeout duration for the metadata retrieval call
   * @param waitTimeout the maximum duration to wait for the condition to be satisfied
   * @param condition a {@link Predicate} that tests the {@link TopicDescription} for readiness
   * @return the {@link Map}{@code <String, TopicDescription>} once the condition evaluates to {@code true}
   */
  public Map<String, TopicDescription> waitForTopicMetadata(List<String> topicNames,
                                               Duration fetchTimeout,
                                               Duration waitTimeout,
                                               Predicate<TopicDescription> condition) {
    return waitUntil(
      () -> {
        try {
          Map<String, TopicDescription> descriptions =
            _adminClient.describeTopics(topicNames)
              .allTopicNames()
              .get(fetchTimeout.toSeconds(), TimeUnit.SECONDS);

          boolean topicMetadataReady = descriptions.values().stream().allMatch(td -> td != null && condition.test(td));

          return topicMetadataReady ? descriptions : null;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for broker to become ready", e);
        } catch (ExecutionException e) {
          if (e.getCause() instanceof UnknownTopicOrPartitionException || e.getCause() instanceof RetriableException) {
            // Topic doesn't exist yet or transient error, retry
            return null;
          }
          throw new RuntimeException("Failed to describe topics: " + topicNames, e);
        } catch (TimeoutException e) {
          // Timeout fetching metadata, treat as transient and retry
          return null;
        }
      },
      Objects::nonNull,
      waitTimeout,
      String.format("Timeout waiting for topics %s metadata to be ready.", topicNames)
    );
  }

  /**
   * Shuts down the Kafka broker with the given broker ID and waits for it
   * to be removed from the cluster metadata.
   *
   * @param brokerId the ID of the broker to wait for shutdown.
   */
  public void shutDownBroker(int brokerId) {
    Duration timeout = Duration.ofSeconds(30);
    _brokers.get(brokerId).stop();

    // Wait until brokerId is no longer in the cluster metadata.
    waitUntil(
      () -> {
        try {
          return _adminClient.describeCluster().nodes().get(10, TimeUnit.SECONDS)
            .stream().noneMatch(node -> node.id() == brokerId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for broker removal", e);
        } catch (ExecutionException e) {
          if (e.getCause() instanceof RetriableException) {
            return false;
          }
          throw new RuntimeException("Failed to describe cluster: ", e);
        } catch (TimeoutException e) {
          // Timeout fetching metadata, treat as transient and retry
          return false;
        }
      },
      Boolean::booleanValue,
      timeout,
      String.format("Broker %s did not shutdown properly.", brokerId)
    );
  }

  public static class BrokerWaitStrategy extends AbstractWaitStrategy {
    private final int _brokerId;
    private final String _metricsTopic;
    private final Admin _adminClient;

    public BrokerWaitStrategy(int brokerId, String metricsTopic, Admin adminClient) {
      this._brokerId = brokerId;
      this._metricsTopic = metricsTopic;
      this._adminClient = adminClient;
    }

    @Override
    protected void waitUntilReady() {
      waitUntil(
        () -> {
          // Returns true if broker is online and metrics topic exists
          try {
            boolean brokerOnline = _adminClient.describeCluster().nodes().get().stream().anyMatch(node -> node.id() == _brokerId);

            if (!brokerOnline) {
              return false;
            }

            return _adminClient.describeTopics(Collections.singleton(_metricsTopic))
              .allTopicNames().get(5, TimeUnit.SECONDS).containsKey(_metricsTopic);

          } catch (InterruptedException e) {
            // Restore interrupt status.
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for broker to become ready", e);
          } catch (ExecutionException | TimeoutException e) {
            // Kafka broker is not ready yet, ignore and retry.
            return false;
          }
        },
        ready -> ready,
        startupTimeout,
        String.format("Broker %d did not become ready within timeout of %d ms", _brokerId, startupTimeout.toMillis())
      );
    }
  }
}
