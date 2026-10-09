/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.common.Utils;
import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptionsGenerator;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ADMIN_CLIENT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.ANOMALY_DETECTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.balancednessCostByGoal;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.MAX_BALANCEDNESS_SCORE;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG;


/**
 * This class will be scheduled to run periodically to check if the given goals are violated or not. An alert will be
 * triggered if one of the goals is not met.
 *
 * This detector handles both inter-broker and intra-broker goal violations.
 */
public class GoalViolationDetector extends AbstractAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalViolationDetector.class);
  private final List<Goal> _detectionGoals;
  private final List<Goal> _intraBrokerDetectionGoals;
  private ModelGeneration _lastCheckedModelGeneration;
  private final Pattern _excludedTopics;
  private final boolean _allowCapacityEstimation;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final Map<String, Double> _balancednessCostByGoal;
  private volatile double _balancednessScore;
  private volatile ProvisionResponse _provisionResponse;
  private volatile boolean _hasPartitionsWithRFGreaterThanNumRacks;
  private final OptimizationOptionsGenerator _optimizationOptionsGenerator;
  private final Timer _goalViolationDetectionTimer;
  private final Timer _intraBrokerGoalViolationDetectionTimer;
  private final Meter _automatedRightsizingMeter;
  protected static final double BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS = -1.0;
  protected final Provisioner _provisioner;
  protected final Boolean _isProvisionerEnabled;
  private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;

  public GoalViolationDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl, MetricRegistry dropwizardMetricRegistry) {
    super(anomalies, kafkaCruiseControl);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    // Notice that we use a separate set of Goal instances for anomaly detector to avoid interference.
    _detectionGoals = config.getConfiguredInstances(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, Goal.class);
    _intraBrokerDetectionGoals = config.getConfiguredInstances(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG, Goal.class);
    _excludedTopics = Pattern.compile(config.getString(AnalyzerConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _allowCapacityEstimation = config.getBoolean(AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    // Combine both inter-broker and intra-broker goals for balancedness calculation
    List<Goal> allDetectionGoals = new ArrayList<>();
    allDetectionGoals.addAll(_detectionGoals);
    allDetectionGoals.addAll(_intraBrokerDetectionGoals);
    _balancednessCostByGoal = balancednessCostByGoal(allDetectionGoals,
                                                     config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG),
                                                     config.getDouble(AnalyzerConfig.GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG));
    _balancednessScore = MAX_BALANCEDNESS_SCORE;
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
    _hasPartitionsWithRFGreaterThanNumRacks = false;
    Map<String, Object> overrideConfigs = Map.of(KAFKA_CRUISE_CONTROL_CONFIG_OBJECT_CONFIG, config,
                                                 ADMIN_CLIENT_CONFIG, _kafkaCruiseControl.adminClient());
    _optimizationOptionsGenerator = config.getConfiguredInstance(AnalyzerConfig.OPTIMIZATION_OPTIONS_GENERATOR_CLASS_CONFIG,
                                                                 OptimizationOptionsGenerator.class,
                                                                 overrideConfigs);
    _goalViolationDetectionTimer = dropwizardMetricRegistry.timer(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR,
                                                                                      "goal-violation-detection-timer"));
    _intraBrokerGoalViolationDetectionTimer = dropwizardMetricRegistry.timer(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR,
                                                                                                  "intra-broker-goal-violation-detection-timer"));
    _automatedRightsizingMeter = dropwizardMetricRegistry.meter(MetricRegistry.name(ANOMALY_DETECTOR_SENSOR, "automated-rightsizing-rate"));
    _provisioner = kafkaCruiseControl.provisioner();
    _isProvisionerEnabled = config.getBoolean(AnomalyDetectorConfig.PROVISIONER_ENABLE_CONFIG);
    _brokerCapacityConfigResolver = config.getConfiguredInstance(
            MonitorConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
            BrokerCapacityConfigResolver.class);
  }

  /**
   * @return A metric to quantify how well the load distribution on a cluster satisfies the {@link #_detectionGoals}.
   */
  public double balancednessScore() {
    return _balancednessScore;
  }

  /**
   * @return Provision status of the cluster based on the latest goal violation check.
   */
  public ProvisionStatus provisionStatus() {
    return _provisionResponse.status();
  }

  /**
   * @return {@code true} if the goal violation detector identified partitions with a replication factor (RF) greater than the number of
   * racks that contain brokers that are eligible to host replicas (i.e. not excluded for replica moves), {@code false} otherwise.
   */
  public boolean hasPartitionsWithRFGreaterThanNumRacks() {
    return _hasPartitionsWithRFGreaterThanNumRacks;
  }

  /**
   * Retrieve the {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the goal violation detector
   * is ready to check for an anomaly.
   *
   * <ul>
   *   <li>Skips detection if cluster model generation has not changed since the last goal violation check.</li>
   *   <li>In case the cluster has offline replicas, this function skips goal violation check and calls
   *   {@link #setBalancednessWithOfflineReplicas}.</li>
   *   <li>See {@link AnomalyDetectionStatus} for details.</li>
   * </ul>
   *
   * @return The {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the anomaly detector is ready.
   */
  protected AnomalyDetectionStatus getGoalViolationDetectionStatus() {
    if (_kafkaCruiseControl.loadMonitor().clusterModelGeneration().equals(_lastCheckedModelGeneration)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping goal violation detection because the model generation hasn't changed. Current model generation {}",
                  _kafkaCruiseControl.loadMonitor().clusterModelGeneration());
      }
      return AnomalyDetectionStatus.SKIP_MODEL_GENERATION_NOT_CHANGED;
    }

    AnomalyDetectionStatus detectionStatus = getAnomalyDetectionStatus(_kafkaCruiseControl, true, true);
    if (detectionStatus == AnomalyDetectionStatus.SKIP_HAS_OFFLINE_REPLICAS) {
      setBalancednessWithOfflineReplicas();
    } else if (detectionStatus == AnomalyDetectionStatus.SKIP_EXECUTOR_NOT_READY) {
      // An ongoing execution might indicate a cluster expansion/shrinking. Hence, the detector avoids reporting a stale provision status.
      _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      // An ongoing execution may modify the replication factor of partitions; hence, the detector avoids reporting potential false positives.
      _hasPartitionsWithRFGreaterThanNumRacks = false;
    }

    return detectionStatus;
  }

  @Override
  public void run() {
    if (getGoalViolationDetectionStatus() != AnomalyDetectionStatus.READY) {
      return;
    }

    // Run inter-broker goal violation detection
    detectInterBrokerGoalViolations();

    // Run intra-broker goal violation detection (only for JBOD clusters)
    if (_brokerCapacityConfigResolver.isJbodKafkaCluster()) {
      detectIntraBrokerGoalViolations();
    }
  }

  /**
   * Detect inter-broker goal violations.
   */
  private void detectInterBrokerGoalViolations() {
    AutoCloseable clusterModelSemaphore = null;
    try {
      Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl,
                                                            ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
      GoalViolations goalViolations = _kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.GOAL_VIOLATIONS_CLASS_CONFIG,
                                                                                         GoalViolations.class,
                                                                                         parameterConfigOverrides);
      boolean newModelNeeded = true;
      ClusterModel clusterModel = null;

      // Retrieve excluded brokers for leadership and replica move.
      ExecutorState executorState = null;
      if (_excludeRecentlyDemotedBrokers || _excludeRecentlyRemovedBrokers) {
        executorState = _kafkaCruiseControl.executorState();
      }

      Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                 : Collections.emptySet();

      Set<Integer> excludedBrokersForReplicaMove = _excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                  : Collections.emptySet();

      ProvisionResponse provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      boolean checkPartitionsWithRFGreaterThanNumRacks = true;
      final Timer.Context ctx = _goalViolationDetectionTimer.time();
      try {
        for (Goal goal : _detectionGoals) {
          if (_kafkaCruiseControl.loadMonitor().meetCompletenessRequirements(goal.clusterModelCompletenessRequirements())) {
            LOG.debug("Detecting if {} is violated.", goal.name());
            // Because the model generation could be slow, We only get new cluster model if needed.
            if (newModelNeeded) {
              if (clusterModelSemaphore != null) {
                clusterModelSemaphore.close();
              }
              clusterModelSemaphore = _kafkaCruiseControl.acquireForModelGeneration(new OperationProgress());
              // Make cluster model null before generating a new cluster model so the current one can be GCed.
              clusterModel = null;
              clusterModel = _kafkaCruiseControl.clusterModel(goal.clusterModelCompletenessRequirements(),
                                                              _allowCapacityEstimation,
                                                              new OperationProgress());

              // If the clusterModel contains dead brokers or disks, goal violation detector will ignore any goal violations.
              // Detection and fix for dead brokers/disks is the responsibility of broker/disk failure detector.
              if (skipDueToOfflineReplicas(clusterModel)) {
                return;
              }
              _lastCheckedModelGeneration = clusterModel.generation();
            }
            newModelNeeded = optimizeForGoal(clusterModel, goal, goalViolations, excludedBrokersForLeadership, excludedBrokersForReplicaMove,
                                             checkPartitionsWithRFGreaterThanNumRacks, false);
            // CC will check for partitions with RF greater than number of eligible racks just once, because regardless of the goal, the cluster
            // will have the same (1) maximum replication factor and (2) rack count containing brokers that are eligible to host replicas.
            checkPartitionsWithRFGreaterThanNumRacks = false;
          } else {
            LOG.warn("Skipping goal violation detection for {} because load completeness requirement is not met.", goal);
          }
          provisionResponse.aggregate(goal.provisionResponse());
        }
      } finally {
        ctx.stop();
      }
      _provisionResponse = provisionResponse;
      if (_isProvisionerEnabled) {
        // Rightsize the cluster (if needed)
        ProvisionerState provisionerState = _provisioner.rightsize(_provisionResponse.recommendationByRecommender(), new RightsizeOptions());
        if (provisionerState != null) {
          LOG.info("Provisioner state: {}.", provisionerState);
          _automatedRightsizingMeter.mark();
        }
      }
      Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
      if (!violatedGoalsByFixability.isEmpty()) {
        goalViolations.setProvisionResponse(_provisionResponse);
        _anomalies.add(goalViolations);
      }
      refreshBalancednessScore(violatedGoalsByFixability);
    } catch (NotEnoughValidWindowsException nevwe) {
      LOG.debug("Skipping goal violation detection because there are not enough valid windows.", nevwe);
    } catch (KafkaCruiseControlException kcce) {
      LOG.warn("Goal violation detector received exception", kcce);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      if (clusterModelSemaphore != null) {
        try {
          clusterModelSemaphore.close();
        } catch (Exception e) {
          LOG.error("Received exception when closing auto closable semaphore", e);
        }
      }
      LOG.debug("Goal violation detection finished.");
    }
  }

  /**
   * Detect intra-broker goal violations (for JBOD clusters).
   */
  private void detectIntraBrokerGoalViolations() {
    AutoCloseable clusterModelSemaphore = null;
    try {
      Map<String, Object> parameterConfigOverrides = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl,
                                                            ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
      IntraBrokerGoalViolations goalViolations = _kafkaCruiseControl.config().getConfiguredInstance(
              AnomalyDetectorConfig.INTRA_BROKER_GOAL_VIOLATIONS_CLASS_CONFIG,
              IntraBrokerGoalViolations.class,
              parameterConfigOverrides);

      boolean newModelNeeded = true;
      ClusterModel clusterModel = null;

      // Retrieve excluded brokers for leadership and replica move.
      ExecutorState executorState = null;
      if (_excludeRecentlyDemotedBrokers || _excludeRecentlyRemovedBrokers) {
        executorState = _kafkaCruiseControl.executorState();
      }

      Set<Integer> excludedBrokersForLeadership = _excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                 : Collections.emptySet();

      Set<Integer> excludedBrokersForReplicaMove = _excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                  : Collections.emptySet();

      ProvisionResponse provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
      boolean checkPartitionsWithRFGreaterThanNumRacks = true;
      final Timer.Context ctx = _intraBrokerGoalViolationDetectionTimer.time();
      try {
        for (Goal goal : _intraBrokerDetectionGoals) {
          if (_kafkaCruiseControl.loadMonitor().meetCompletenessRequirements(goal.clusterModelCompletenessRequirements())) {
            LOG.debug("Detecting if {} is violated.", goal.name());
            // Because the model generation could be slow, We only get new cluster model if needed.
            if (newModelNeeded) {
              if (clusterModelSemaphore != null) {
                clusterModelSemaphore.close();
              }
              clusterModelSemaphore = _kafkaCruiseControl.acquireForModelGeneration(new OperationProgress());
              // Make cluster model null before generating a new cluster model so the current one can be GCed.
              clusterModel = null;
              clusterModel = _kafkaCruiseControl.clusterModel(goal.clusterModelCompletenessRequirements(),
                                                              _allowCapacityEstimation,
                                                              new OperationProgress(),
                                                              true);

              // If the clusterModel contains dead brokers or disks, goal violation detector will ignore any goal violations.
              // Detection and fix for dead brokers/disks is the responsibility of broker/disk failure detector.
              if (skipDueToOfflineReplicas(clusterModel)) {
                return;
              }
              _lastCheckedModelGeneration = clusterModel.generation();
            }
            newModelNeeded = optimizeForGoal(clusterModel, goal, goalViolations, excludedBrokersForLeadership,
                                             excludedBrokersForReplicaMove, checkPartitionsWithRFGreaterThanNumRacks, true);
            // CC will check for partitions with RF greater than number of eligible racks just once,
            // because regardless of the goal, the cluster will have the same
            // (1) maximum replication factor and
            // (2) rack count containing brokers that are eligible to host replicas.
            checkPartitionsWithRFGreaterThanNumRacks = false;
          } else {
            LOG.warn("Skipping goal violation detection for {} because load completeness requirement is not met.", goal);
          }
          provisionResponse.aggregate(goal.provisionResponse());
        }
      } finally {
        ctx.stop();
      }
      _provisionResponse = provisionResponse;
      if (_isProvisionerEnabled) {
        // Rightsize the cluster (if needed)
        ProvisionerState provisionerState = _provisioner.rightsize(_provisionResponse.recommendationByRecommender(), new RightsizeOptions());
        if (provisionerState != null) {
          LOG.info("Provisioner state: {}.", provisionerState);
          _automatedRightsizingMeter.mark();
        }
      }
      Map<Boolean, List<String>> violatedGoalsByFixability = goalViolations.violatedGoalsByFixability();
      if (!violatedGoalsByFixability.isEmpty()) {
        goalViolations.setProvisionResponse(_provisionResponse);
        _anomalies.add(goalViolations);
      }
      refreshBalancednessScore(violatedGoalsByFixability);
    } catch (NotEnoughValidWindowsException nevwe) {
      LOG.debug("Skipping intra-broker goal violation detection because there are not enough valid windows.", nevwe);
    } catch (KafkaCruiseControlException kcce) {
      LOG.warn("Intra-broker goal violation detector received exception", kcce);
    } catch (Exception e) {
      LOG.error("Unexpected exception in intra-broker goal violation detection", e);
    } finally {
      if (clusterModelSemaphore != null) {
        try {
          clusterModelSemaphore.close();
        } catch (Exception e) {
          LOG.error("Received exception when closing auto closable semaphore", e);
        }
      }
      LOG.debug("Intra-broker goal violation detection finished.");
    }
  }

  /**
   * @param clusterModel The state of the cluster.
   * @return {@code true} to skip goal violation detection due to offline replicas in the cluster model.
   */
  protected boolean skipDueToOfflineReplicas(ClusterModel clusterModel) {
    if (!clusterModel.deadBrokers().isEmpty()) {
      LOG.info("Skipping goal violation detection due to dead brokers {}, which are reported by broker failure "
               + "detector, and fixed if its self healing configuration is enabled.", clusterModel.deadBrokers());
      setBalancednessWithOfflineReplicas();
      return true;
    } else if (!clusterModel.brokersWithBadDisks().isEmpty()) {
      LOG.info("Skipping goal violation detection due to brokers with bad disks {}, which are reported by disk failure "
               + "detector, and fixed if its self healing configuration is enabled.", clusterModel.brokersWithBadDisks());
      setBalancednessWithOfflineReplicas();
      return true;
    }

    return false;
  }

  protected void setBalancednessWithOfflineReplicas() {
    _balancednessScore = BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS;
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
  }

  protected void refreshBalancednessScore(Map<Boolean, List<String>> violatedGoalsByFixability) {
    _balancednessScore = MAX_BALANCEDNESS_SCORE;
    for (List<String> violatedGoals : violatedGoalsByFixability.values()) {
      violatedGoals.forEach(violatedGoal -> _balancednessScore -= _balancednessCostByGoal.get(violatedGoal));
    }
  }

  protected Set<String> excludedTopics(ClusterModel clusterModel) {
    return Utils.getTopicNamesMatchedWithPattern(_excludedTopics, clusterModel::topics);
  }

  protected boolean optimizeForGoal(ClusterModel clusterModel,
                                    Goal goal,
                                    GoalViolations goalViolations,
                                    Set<Integer> excludedBrokersForLeadership,
                                    Set<Integer> excludedBrokersForReplicaMove,
                                    boolean checkPartitionsWithRFGreaterThanNumRacks,
                                    boolean isIntraBroker)
      throws KafkaCruiseControlException {
    if (clusterModel.topics().isEmpty()) {
      LOG.info("Skipping {} goal violation detection because the cluster model does not have any topic.",
               isIntraBroker ? "intra-broker" : "inter-broker");
      return false;
    }
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    try {
      OptimizationOptions options = _optimizationOptionsGenerator.optimizationOptionsForGoalViolationDetection(clusterModel,
                                                                                                               excludedTopics(clusterModel),
                                                                                                               excludedBrokersForLeadership,
                                                                                                               excludedBrokersForReplicaMove);
      if (checkPartitionsWithRFGreaterThanNumRacks) {
        if (isIntraBroker) {
          _hasPartitionsWithRFGreaterThanNumRacks = clusterModel.maxReplicationFactor() > clusterModel.numAliveRacksAllowedReplicaMoves(options);
        } else {
          _hasPartitionsWithRFGreaterThanNumRacks = clusterModel.maxReplicationFactor() > clusterModel.aliveRacksAllowedReplicaMoves(options).size();
        }
      }
      goal.optimize(clusterModel, Collections.emptySet(), options);
    } catch (OptimizationFailureException ofe) {
      // An OptimizationFailureException indicates (1) a hard goal violation that cannot be fixed typically due to
      // lack of physical hardware (e.g. insufficient number of racks to satisfy rack awareness, insufficient number
      // of brokers to satisfy Replica Capacity Goal, or insufficient number of resources to satisfy resource
      // capacity goals), or (2) a failure to move offline replicas away from dead brokers/disks.
      goalViolations.addViolation(goal.name(), false);
      return true;
    }
    boolean hasDiff = AnalyzerUtils.hasDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    LOG.trace("{} generated {} proposals", goal.name(), hasDiff ? "some" : "no");
    if (hasDiff) {
      // A goal violation that can be optimized by applying the generated proposals.
      goalViolations.addViolation(goal.name(), true);
      return true;
    } else {
      // The goal is already satisfied.
      return false;
    }
  }

  protected boolean optimizeForGoal(ClusterModel clusterModel,
                                    Goal goal,
                                    IntraBrokerGoalViolations goalViolations,
                                    Set<Integer> excludedBrokersForLeadership,
                                    Set<Integer> excludedBrokersForReplicaMove,
                                    boolean checkPartitionsWithRFGreaterThanNumRacks,
                                    boolean isIntraBroker)
      throws KafkaCruiseControlException {
    if (clusterModel.topics().isEmpty()) {
      LOG.info("Skipping the intra broker goal violation detection because the cluster model does not have any topic.");
      return false;
    }

    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initLeaderDistribution = clusterModel.getLeaderDistribution();
    try {
      OptimizationOptions options = _optimizationOptionsGenerator.optimizationOptionsForGoalViolationDetection(clusterModel,
                                                                                                               excludedTopics(clusterModel),
                                                                                                               excludedBrokersForLeadership,
                                                                                                               excludedBrokersForReplicaMove);
      if (checkPartitionsWithRFGreaterThanNumRacks) {
        _hasPartitionsWithRFGreaterThanNumRacks = clusterModel.maxReplicationFactor() > clusterModel.numAliveRacksAllowedReplicaMoves(options);
      }
      goal.optimize(clusterModel, Collections.emptySet(), options);
    } catch (OptimizationFailureException ofe) {
      // An OptimizationFailureException indicates (1) a hard goal violation that cannot be fixed typically due to
      // lack of physical hardware (e.g. insufficient number of racks to satisfy rack awareness, insufficient number
      // of brokers to satisfy Replica Capacity Goal, or insufficient number of resources to satisfy resource
      // capacity goals), or (2) a failure to move offline replicas away from dead brokers/disks.
      goalViolations.addViolation(goal.name(), false);
      return true;
    }
    boolean hasDiff = AnalyzerUtils.hasDiff(initReplicaDistribution, initLeaderDistribution, clusterModel);
    LOG.trace("{} generated {} proposals", goal.name(), hasDiff ? "some" : "no");
    if (hasDiff) {
      // A goal violation that can be optimized by applying the generated proposals.
      goalViolations.addViolation(goal.name(), true);
      return true;
    } else {
      // The goal is already satisfied.
      return false;
    }
  }
}
