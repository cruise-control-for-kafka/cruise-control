/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingIntraBrokerGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION;


/**
 * A class that holds all the intra-broker goal violations -- i.e. imbalance across the disks of brokers in a JBOD deployment,
 * which are fixed by rebalancing the disks of brokers (i.e. moving replicas between the disks of the same broker).
 */
public class IntraBrokerGoalViolations extends AbstractGoalViolations {

  /**
   * An anomaly to indicate intra-broker goal violation(s).
   */
  public IntraBrokerGoalViolations() {
  }

  @Override
  public AnomalyType anomalyType() {
    return INTRA_BROKER_GOAL_VIOLATION;
  }

  @Override
  protected RebalanceRunnable rebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                                                KafkaCruiseControlConfig config,
                                                boolean allowCapacityEstimation) {
    return new RebalanceRunnable(kafkaCruiseControl,
                                 getSelfHealingIntraBrokerGoalNames(config),
                                 allowCapacityEstimation,
                                 _excludeRecentlyDemotedBrokers,
                                 _excludeRecentlyRemovedBrokers,
                                 _anomalyId.toString(),
                                 reasonSupplier(),
                                 stopOngoingExecution(),
                                 true);
  }
}
