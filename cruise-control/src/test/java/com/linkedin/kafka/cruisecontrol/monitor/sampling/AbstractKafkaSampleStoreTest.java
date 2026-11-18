/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Unit test for {@link AbstractKafkaSampleStore}
 */
public class AbstractKafkaSampleStoreTest {

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueAlreadyExists() {
        short expected = 1;
        Map<String, ?> config = Collections.emptyMap();
        Admin adminClient = EasyMock.mock(Admin.class);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        kafkaSampleStore._sampleStoreTopicReplicationFactor = expected;
        EasyMock.replay(adminClient, kafkaSampleStore);

        short actual = kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient);

        assertEquals(expected, actual);
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndNodeCountIsOne() {
        Map<String, Object> config = createFilledConfigMap();
        Admin adminClient = EasyMock.mock(Admin.class);
        prepareForNumberOfBrokersCall(adminClient, false);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        EasyMock.replay(adminClient, kafkaSampleStore);

        assertThrows(IllegalStateException.class,
                () -> kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient));
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndNodeCountIsTwo() {
        short expected = 2;
        Map<String, Object> config = createFilledConfigMap();
        Admin adminClient = EasyMock.mock(Admin.class);
        prepareForNumberOfBrokersCall(adminClient, true);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        EasyMock.replay(adminClient, kafkaSampleStore);

        short actual = kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient);

        assertEquals(expected, actual);
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    @Test
    public void testSampleStoreTopicReplicationFactorWhenValueNotExistsAndDescribeOfClusterFails() {
        Map<String, Object> config = createFilledConfigMap();
        Admin adminClient = EasyMock.mock(Admin.class);
        KafkaFutureImpl<Collection<Node>> nodesFuture = new KafkaFutureImpl<>();
        nodesFuture.completeExceptionally(new TimeoutException());
        // Describing the cluster is retried up to the max retry count.
        expectDescribeClusterNodes(adminClient, nodesFuture, 2);
        AbstractKafkaSampleStore kafkaSampleStore = EasyMock.partialMockBuilder(AbstractKafkaSampleStore.class).createMock();
        EasyMock.replay(adminClient, kafkaSampleStore);

        assertThrows(IllegalStateException.class,
                () -> kafkaSampleStore.sampleStoreTopicReplicationFactor(config, adminClient));
        EasyMock.verify(adminClient, kafkaSampleStore);
    }

    private Map<String, Object> createFilledConfigMap() {
        Map<String, Object> config = new HashMap<>();
        config.put(MonitorConfig.FETCH_METRIC_SAMPLES_MAX_RETRY_COUNT_CONFIG, 2);
        return config;
    }

    private void prepareForNumberOfBrokersCall(Admin adminClient, boolean isNodeCountEnough) {
        Node node = new Node(0, "host", 9092);
        Collection<Node> nodes = isNodeCountEnough ? Arrays.asList(node, node) : Collections.singletonList(node);
        expectDescribeClusterNodes(adminClient, KafkaFuture.completedFuture(nodes), null);
    }

    /**
     * Uses real futures rather than class-mocking {@link KafkaFuture}: a class mock of {@link KafkaFuture} created under the
     * PowerMock class loader conflicts with one created by an earlier test in the same JVM (i.e. depends on the test order).
     *
     * @param adminClient Mock admin client.
     * @param nodesFuture Future to return as the nodes of the cluster.
     * @param times Expected number of describe cluster calls, or {@code null} for any number of calls.
     */
    private void expectDescribeClusterNodes(Admin adminClient, KafkaFuture<Collection<Node>> nodesFuture, Integer times) {
        DescribeClusterResult describeClusterResult = EasyMock.mock(DescribeClusterResult.class);
        EasyMock.expect(describeClusterResult.nodes()).andReturn(nodesFuture).anyTimes();
        EasyMock.replay(describeClusterResult);
        if (times == null) {
            EasyMock.expect(adminClient.describeCluster()).andReturn(describeClusterResult).anyTimes();
        } else {
            EasyMock.expect(adminClient.describeCluster()).andReturn(describeClusterResult).times(times);
        }
    }

}
