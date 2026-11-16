/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CruiseControlStateParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.Cluster;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.shouldRefreshClusterAndGeneration;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.*;


/**
 * The async runnable for Cruise Control state.
 */
public class GetStateRunnable extends OperationRunnable {
  protected final Set<CruiseControlState.SubState> _substates;

  public GetStateRunnable(KafkaCruiseControl kafkaCruiseControl,
                          OperationFuture future,
                          CruiseControlStateParameters parameters) {
    super(kafkaCruiseControl, future);
    _substates = parameters.substates();
  }

  /**
   * Get the state with selected substates for Kafka Cruise Control.
   */
  @Override
  public CruiseControlState getResult() {
    // In case no substate is specified, return all substates.
    Set<CruiseControlState.SubState> substates = !_substates.isEmpty() ? _substates
                                                                       : new HashSet<>(Arrays.asList(CruiseControlState.SubState.values()));

    Cluster cluster = null;
    if (shouldRefreshClusterAndGeneration(substates)) {
      cluster = _kafkaCruiseControl.refreshClusterAndGeneration();
    }

    return new CruiseControlState(substates.contains(EXECUTOR) ? _kafkaCruiseControl.executorState() : null,
                                  substates.contains(MONITOR) ? _kafkaCruiseControl.monitorState(cluster) : null,
                                  substates.contains(ANALYZER) ? _kafkaCruiseControl.analyzerState(cluster) : null,
                                  substates.contains(ANOMALY_DETECTOR) ? _kafkaCruiseControl.anomalyDetectorState() : null,
                                  _kafkaCruiseControl.config());
  }
}
