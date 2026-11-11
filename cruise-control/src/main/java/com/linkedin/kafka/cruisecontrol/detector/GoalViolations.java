/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.GOAL_VIOLATION;


/**
 * A class that holds all the goal violations.
 */
public class GoalViolations extends AbstractGoalViolations {

  /**
   * An anomaly to indicate goal violation(s).
   */
  public GoalViolations() {
  }

  @Override
  public AnomalyType anomalyType() {
    return GOAL_VIOLATION;
  }

  @Override
  protected RebalanceRunnable rebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                                                KafkaCruiseControlConfig config,
                                                boolean allowCapacityEstimation) {
    return new RebalanceRunnable(kafkaCruiseControl,
                                 getSelfHealingGoalNames(config),
                                 allowCapacityEstimation,
                                 _excludeRecentlyDemotedBrokers,
                                 _excludeRecentlyRemovedBrokers,
                                 _anomalyId.toString(),
                                 reasonSupplier(),
                                 stopOngoingExecution());
  }
}
