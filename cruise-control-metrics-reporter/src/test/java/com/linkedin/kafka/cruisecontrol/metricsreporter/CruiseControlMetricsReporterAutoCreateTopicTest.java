/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.KafkaTopicDescriptionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.KafkaServerConfigs;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.getTopicDescription;
import static org.junit.Assert.assertEquals;

public class CruiseControlMetricsReporterAutoCreateTopicTest extends CCKafkaClientsIntegrationTestHarness {
    protected static final String TOPIC = "CruiseControlMetricsReporterTest";
    protected static final String TEST_TOPIC = "TestTopic";
    private CCContainerizedKraftCluster _cluster;

    /**
     * Setup the unit test.
     */
    @Before
    public void setUp() {
        Properties adminClientProps = new Properties();
        setSecurityConfigs(adminClientProps, "admin");

        _cluster = new CCContainerizedKraftCluster(2, buildBrokerConfigs(), adminClientProps);
        _cluster.start();
        _bootstrapUrl = _cluster.getExternalBootstrapAddress();

        // creating the "TestTopic" explicitly because the topic auto-creation is disabled on the broker
        Properties adminProps = new Properties();
        adminProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        AtomicInteger adminFailed = new AtomicInteger(0);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic testTopic = new NewTopic(TEST_TOPIC, 1, (short) 1);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Set.of(testTopic));
            createTopicsResult.all().whenComplete((v, e) -> {
                if (e != null) {
                    adminFailed.incrementAndGet();
                }
            });
        }
        assertEquals(0, adminFailed.get());

        // starting producer to verify that Kafka cluster is working fine
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        AtomicInteger producerFailed = new AtomicInteger(0);
        try (Producer<String, String> producer = createProducer(producerProps)) {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(TEST_TOPIC, Integer.toString(i)),
                        (m, e) -> {
                            if (e != null) {
                                producerFailed.incrementAndGet();
                            }
                        });
            }
        }
        assertEquals(0, producerFailed.get());
    }

    /**
     * Tear down the unit test.
     */
    @After
    public void tearDown() {
        if (_cluster != null) {
            _cluster.close();
        }
    }

    @Override
    public Properties overridingProps() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
        props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
          "127.0.0.1:" + CCContainerizedKraftCluster.CONTAINER_INTERNAL_LISTENER_PORT);
        props.put("listener.security.protocol.map", String.join(",",
          CCContainerizedKraftCluster.CONTROLLER_LISTENER_NAME + ":PLAINTEXT",
          CCContainerizedKraftCluster.INTERNAL_LISTENER_NAME + ":PLAINTEXT",
          CCContainerizedKraftCluster.EXTERNAL_LISTENER_NAME + ":PLAINTEXT"));
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
        // configure metrics topic auto-creation by the metrics reporter
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_CONFIG, "5000");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_CONFIG, "1");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        // disable topic auto-creation to leave the metrics reporter to create the metrics topic
        props.setProperty(KafkaServerConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false");
        props.setProperty(KafkaServerConfigs.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        props.setProperty(KafkaServerConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2");
        props.setProperty(KafkaServerConfigs.NUM_PARTITIONS_CONFIG, "2");
        return props;
    }

    @Test
    public void testAutoCreateMetricsTopic() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        AdminClient adminClient = AdminClient.create(props);

        // For compatibility with Kafka 4.0 and beyond we must use new API methods.
        TopicDescription topicDescription;
        try {
            topicDescription = getTopicDescription(adminClient, TOPIC);
        } catch (KafkaTopicDescriptionException e) {
            throw new RuntimeException(e);
        }
        // assert that the metrics topic was created with partitions and replicas as configured for the metrics report auto-creation
        assertEquals(1, topicDescription.partitions().size());
        assertEquals(1, topicDescription.partitions().get(0).replicas().size());
    }
}
