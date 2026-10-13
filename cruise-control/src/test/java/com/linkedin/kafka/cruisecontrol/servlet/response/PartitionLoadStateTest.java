/*
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

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PartitionLoadStateTest {
  private static final String TOPIC = "topic";
  private static final int TOPIC_NAME_LENGTH = 20;
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  /**
   * Three JBOD brokers, with the replicas of two partitions:
   * <ul>
   *   <li>{@code topic-0}: leader on broker 0 {@link TestConstants#LOGDIR0}, followers on broker 1 {@link TestConstants#LOGDIR1}
   *   and on broker 2 -- the latter on a failed disk, hence with no known logdir.</li>
   *   <li>{@code topic-1}: leader on broker 2 with no known logdir, follower on broker 0 {@link TestConstants#LOGDIR1}.</li>
   * </ul>
   *
   * @return The partitions, sorted by partition number.
   */
  private static List<Partition> partitions() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo capacity = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY);
    for (int brokerId = 0; brokerId < 3; brokerId++) {
      clusterModel.createRack(Integer.toString(brokerId));
      clusterModel.createBroker(Integer.toString(brokerId), Integer.toString(brokerId), brokerId, capacity, true);
    }
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    createReplica(clusterModel, tp0, 0, 0, true, false, LOGDIR0);
    createReplica(clusterModel, tp0, 1, 1, false, false, LOGDIR1);
    createReplica(clusterModel, tp0, 2, 2, false, true, null);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    createReplica(clusterModel, tp1, 2, 0, true, false, null);
    createReplica(clusterModel, tp1, 0, 1, false, false, LOGDIR1);
    // PartitionLoadState discards the partitions once it caches its response.
    return new ArrayList<>(Arrays.asList(clusterModel.partition(tp0), clusterModel.partition(tp1)));
  }

  private static void createReplica(ClusterModel clusterModel, TopicPartition tp, int brokerId, int index, boolean isLeader,
                                    boolean isOffline, String logdir) {
    String rackId = Integer.toString(brokerId);
    clusterModel.createReplica(rackId, brokerId, tp, index, isLeader, isOffline, logdir, false);
    AggregatedMetricValues load = getAggregatedMetricValues(1.0, 10.0, 10.0, 100.0 * (tp.partition() + 1));
    MetricValues messageInRate = new MetricValues(1);
    messageInRate.set(0, 5.0);
    load.add(KafkaMetricDef.commonMetricDefId(KafkaMetricDef.MESSAGE_IN_RATE), messageInRate);
    clusterModel.setReplicaLoad(rackId, brokerId, tp, load, Collections.singletonList(1L));
  }

  private static PartitionLoadState partitionLoadState(boolean populateDiskInfo, int entries) {
    return new PartitionLoadState(partitions(), false, false, entries, Integer.MAX_VALUE, 0, null, TOPIC_NAME_LENGTH,
                                  populateDiskInfo, CONFIG);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> records(PartitionLoadState partitionLoadState) {
    return (List<Map<String, Object>>) new Gson().fromJson(partitionLoadState.getJsonString(), Map.class).get("records");
  }

  @Test
  public void testJsonWithDiskInfo() {
    List<Map<String, Object>> records = records(partitionLoadState(true, Integer.MAX_VALUE));

    assertEquals(2, records.size());
    Map<String, Object> record = records.get(0);
    assertEquals(Arrays.asList(1.0, 2.0), record.get("followers"));
    assertEquals(LOGDIR0, record.get("leaderLogdir"));
    // The logdirs of the followers are in the order of the followers, with null for the follower on a failed disk.
    assertEquals(Arrays.asList(LOGDIR1, null), record.get("followerLogdirs"));

    record = records.get(1);
    assertEquals(2.0, record.get("leader"));
    assertFalse(record.containsKey("leaderLogdir"));
    assertEquals(Collections.singletonList(LOGDIR1), record.get("followerLogdirs"));
  }

  @Test
  public void testJsonWithoutDiskInfo() {
    for (Map<String, Object> record : records(partitionLoadState(false, Integer.MAX_VALUE))) {
      assertFalse(record.containsKey("leaderLogdir"));
      assertFalse(record.containsKey("followerLogdirs"));
      assertTrue(record.containsKey("followers"));
    }
  }

  @Test
  public void testPlaintextWithDiskInfo() {
    String[] lines = partitionLoadState(true, Integer.MAX_VALUE).getPlaintext().split(System.lineSeparator());

    assertEquals(3, lines.length);
    String header = lines[0];
    assertTrue(header, header.endsWith("MSG_IN (#/s)  LEADER_LOGDIR  FOLLOWER_LOGDIRS"));
    // A logdir that is not known is shown as "-".
    String logdirs = String.format("%-13s  [%s, -]", LOGDIR0, LOGDIR1);
    assertTrue(lines[1], lines[1].endsWith(logdirs));
    // The logdir columns line up with their header, though the load values are narrower than their header.
    assertEquals(header.indexOf("LEADER_LOGDIR"), lines[1].length() - logdirs.length());
    assertEquals(header.indexOf("FOLLOWER_LOGDIRS"), lines[1].indexOf("[" + LOGDIR1 + ", -]"));
    logdirs = String.format("%-13s  [%s]", "-", LOGDIR1);
    assertTrue(lines[2], lines[2].endsWith(logdirs));
    assertEquals(header.indexOf("LEADER_LOGDIR"), lines[2].length() - logdirs.length());
  }

  @Test
  public void testPlaintextWithoutDiskInfo() {
    String[] lines = partitionLoadState(false, Integer.MAX_VALUE).getPlaintext().split(System.lineSeparator());

    assertEquals(3, lines.length);
    assertTrue(lines[0], lines[0].endsWith("MSG_IN (#/s)"));
    assertFalse(lines[1], lines[1].contains(LOGDIR0));
    assertTrue(lines[1], lines[1].startsWith(String.format("%" + TOPIC_NAME_LENGTH + "s%10s%30s", TOPIC + "-0", 0, "[1, 2]")));
  }

  @Test
  public void testEntries() {
    PartitionLoadState partitionLoadState = partitionLoadState(true, 1);

    assertEquals(1, records(partitionLoadState).size());
    assertEquals(0.0, records(partitionLoadState).get(0).get("partition"));
    assertEquals(2, partitionLoadState.getPlaintext().split(System.lineSeparator()).length);
  }
}
