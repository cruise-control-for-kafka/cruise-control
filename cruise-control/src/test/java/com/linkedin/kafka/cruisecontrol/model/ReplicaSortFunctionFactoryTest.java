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

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ReplicaSortFunctionFactoryTest {
  private static final String RACK = "r0";
  private static final int BROKER_ID = 0;
  private static final String EXCLUDED_TOPIC = "excluded";
  private static final TopicPartition EXCLUDED_TP = new TopicPartition(EXCLUDED_TOPIC, 0);
  private static final TopicPartition INCLUDED_TP = new TopicPartition("included", 0);

  @Test
  public void testSelectReplicasBasedOnExcludedTopicsAndDisksToRemove() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    clusterModel.createRack(RACK);
    clusterModel.createBroker(RACK, Integer.toString(BROKER_ID), BROKER_ID,
                              new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY), true);
    Replica excludedReplica = createReplica(clusterModel, EXCLUDED_TP);
    Replica includedReplica = createReplica(clusterModel, INCLUDED_TP);
    Set<String> excludedTopics = Collections.singleton(EXCLUDED_TOPIC);
    Function<Replica, Boolean> selection = ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopicsAndDisksToRemove(excludedTopics);

    assertFalse(selection.apply(excludedReplica));
    assertTrue(selection.apply(includedReplica));

    // While on a disk marked for removal, the replica of an excluded topic is selected, so that the disk can be emptied.
    clusterModel.markDiskForRemoval(BROKER_ID, LOGDIR0);
    assertTrue(selection.apply(excludedReplica));
    assertTrue(selection.apply(includedReplica));
    // The selection function that the other goals use keeps excluding it.
    assertFalse(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics).apply(excludedReplica));

    // Once moved off the disk to remove, it is excluded again.
    clusterModel.relocateReplica(EXCLUDED_TP, BROKER_ID, LOGDIR1);
    assertFalse(selection.apply(excludedReplica));
  }

  private static Replica createReplica(ClusterModel clusterModel, TopicPartition tp) {
    Replica replica = clusterModel.createReplica(RACK, BROKER_ID, tp, 0, true, false, LOGDIR0, false);
    clusterModel.setReplicaLoad(RACK, BROKER_ID, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, 100.0), Collections.singletonList(1L));
    return replica;
  }
}
