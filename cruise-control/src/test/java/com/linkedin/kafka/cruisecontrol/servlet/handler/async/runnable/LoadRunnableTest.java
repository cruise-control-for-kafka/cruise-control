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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.List;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static org.junit.Assert.assertTrue;


public class LoadRunnableTest {
  private static final long END_MS = 1000L;

  /**
   * With {@code populate_disk_info=true}, the load is computed from a cluster model built with the requested capacity estimation,
   * without resolving the capacity of the cluster separately: that used to fail for brokers whose capacity comes from the default
   * broker capacity, and for dead brokers.
   */
  @Test
  public void testLoadWithDiskInfoUsesRequestedCapacityEstimation() throws Exception {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    ModelCompletenessRequirements requirements = new ModelCompletenessRequirements(1, 0.0, true);
    for (boolean allowCapacityEstimation : List.of(true, false)) {
      ClusterLoadParameters parameters = EasyMock.niceMock(ClusterLoadParameters.class);
      EasyMock.expect(parameters.startMs()).andReturn(DEFAULT_START_TIME_FOR_CLUSTER_MODEL).anyTimes();
      EasyMock.expect(parameters.endMs()).andReturn(END_MS).anyTimes();
      EasyMock.expect(parameters.requirements()).andReturn(requirements).anyTimes();
      EasyMock.expect(parameters.allowCapacityEstimation()).andReturn(allowCapacityEstimation).anyTimes();
      EasyMock.expect(parameters.populateDiskInfo()).andReturn(true).anyTimes();

      // A strict mock: any other call, e.g. to get the load monitor, fails the test.
      KafkaCruiseControl kafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
      EasyMock.expect(kafkaCruiseControl.config()).andReturn(config).anyTimes();
      EasyMock.expect(kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
      EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL), EasyMock.eq(END_MS),
                                                      EasyMock.eq(requirements), EasyMock.eq(true), EasyMock.eq(allowCapacityEstimation),
                                                      EasyMock.anyObject()))
              .andReturn(DeterministicCluster.unbalanced4());
      EasyMock.replay(parameters, kafkaCruiseControl);

      BrokerStats brokerStats = new LoadRunnable(kafkaCruiseControl, new OperationFuture("Get load"), parameters).getResult();

      EasyMock.verify(kafkaCruiseControl);
      String plaintext = brokerStats.toString();
      assertTrue(plaintext, plaintext.contains(LOGDIR0));
    }
  }
}
