/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerState;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.BrokerSetResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorState;
import com.linkedin.kafka.cruisecontrol.detector.Provisioner;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitorState;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.closeAdminClientWithTimeout;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createAdminClient;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.partitionWithOfflineReplicas;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;


/**
 * The main class of Cruise Control.
 */
public class KafkaCruiseControl {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControl.class);
  protected final KafkaCruiseControlConfig _config;
  private final LoadMonitor _loadMonitor;
  private final GoalOptimizer _goalOptimizer;
  private final ExecutorService _goalOptimizerExecutor;
  private final Executor _executor;
  private final AnomalyDetectorManager _anomalyDetectorManager;
  private final Time _time;
  private final AdminClient _adminClient;
  private final Provisioner _provisioner;

  private static final String VERSION;
  private static final String COMMIT_ID;
  private static final boolean FORCE_PAUSE_SAMPLING = false;

  // Referenced similar method to get software version in Kafka code.
  static {
    Properties props = new Properties();
    try (InputStream resourceStream = KafkaCruiseControl.class.getResourceAsStream("/cruise-control/cruise-control-version.properties")) {
      props.load(resourceStream);
    } catch (Exception e) {
      LOG.warn("Error while loading cruise-control-version.properties :" + e.getMessage());
    }
    VERSION = props.getProperty("version", "unknown").trim();
    COMMIT_ID = props.getProperty("commitId", "unknown").trim();
    LOG.info("COMMIT INFO: " + VERSION + "---" + COMMIT_ID);
  }

  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   */
  public KafkaCruiseControl(KafkaCruiseControlConfig config, MetricRegistry dropwizardMetricRegistry) {
    _config = config;
    _time = Time.SYSTEM;
    // initialize some of the static state of Kafka Cruise Control;
    ModelUtils.init(config);
    ModelParameters.init(config);

    // Instantiate the components.
    _adminClient = createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _provisioner = config.getConfiguredInstance(AnomalyDetectorConfig.PROVISIONER_CLASS_CONFIG,
                                                Provisioner.class,
                                                Collections.singletonMap(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, this));
    _anomalyDetectorManager = new AnomalyDetectorManager(this, _time, dropwizardMetricRegistry);
    _executor = new Executor(config, _time, dropwizardMetricRegistry, _anomalyDetectorManager);
    _loadMonitor = new LoadMonitor(config, _time, dropwizardMetricRegistry, KafkaMetricDef.commonMetricDef());
    _goalOptimizerExecutor = Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("GoalOptimizerExecutor"));
    _goalOptimizer = new GoalOptimizer(config, _loadMonitor, _time, dropwizardMetricRegistry, _executor, _adminClient);
  }

  /**
   * Package private constructor for unit tests w/o static state initialization.
   */
  KafkaCruiseControl(KafkaCruiseControlConfig config,
                     Time time,
                     AnomalyDetectorManager anomalyDetectorManager,
                     Executor executor,
                     LoadMonitor loadMonitor,
                     ExecutorService goalOptimizerExecutor,
                     GoalOptimizer goalOptimizer,
                     Provisioner provisioner) {
    _config = config;
    _time = time;
    _adminClient = createAdminClient(KafkaCruiseControlUtils.parseAdminClientConfigs(config));
    _anomalyDetectorManager = anomalyDetectorManager;
    _executor = executor;
    _loadMonitor = loadMonitor;
    _goalOptimizerExecutor = goalOptimizerExecutor;
    _goalOptimizer = goalOptimizer;
    _provisioner = provisioner;
  }

  /**
   * @return The load monitor.
   */
  public LoadMonitor loadMonitor() {
    return _loadMonitor;
  }

  /**
   * Refresh the cluster metadata and get the corresponding cluster and generation information.
   *
   * @return Cluster and generation information after refreshing the cluster metadata.
   */
  public MetadataClient.ClusterAndGeneration refreshClusterAndGeneration() {
    return _loadMonitor.refreshClusterAndGeneration();
  }

  /**
   * @return The state of load monitor's task runner.
   */
  public LoadMonitorTaskRunner.LoadMonitorTaskRunnerState getLoadMonitorTaskRunnerState() {
    return _loadMonitor.taskRunnerState();
  }

  /**
   * Retrieve the Admin Client (expected to be thread-safe).
   * @return Admin Client of Cruise Control.
   */
  public AdminClient adminClient() {
    return _adminClient;
  }

  /**
   * Retrieve the Provisioner (expected to be thread-safe).
   * @return Provisioner of Cruise Control.
   */
  public Provisioner provisioner() {
    return _provisioner;
  }

  /**
   * Acquire the semaphore for the cluster model generation.
   *
   * @param operationProgress the progress for the job.
   * @return A new auto closeable semaphore for the cluster model generation.
   */
  public LoadMonitor.AutoCloseableSemaphore acquireForModelGeneration(OperationProgress operationProgress)
      throws InterruptedException {
    return _loadMonitor.acquireForModelGeneration(operationProgress);
  }

  /**
   * @return The current time in milliseconds.
   */
  public long timeMs() {
    return _time.milliseconds();
  }

  /**
   * Make the caller thread sleep for the given number of milliseconds.
   * @param ms Time to sleep in milliseconds.
   */
  public void sleep(long ms) {
    _time.sleep(ms);
  }

  /**
   * Start up the Cruise Control.
   */
  public void startUp() {
    LOG.info("Starting Kafka Cruise Control...");
    _loadMonitor.startUp();
    _anomalyDetectorManager.startDetection();
    _goalOptimizerExecutor.execute(_goalOptimizer);
    LOG.info("Kafka Cruise Control started.");
  }

  /**
   * Shutdown Cruise Control.
   */
  public void shutdown() {
    Thread t = new Thread(() -> {
      LOG.info("Shutting down Kafka Cruise Control...");
      _loadMonitor.shutdown();
      _executor.shutdown();
      _anomalyDetectorManager.shutdown();
      _goalOptimizer.shutdown();
      closeAdminClientWithTimeout(_adminClient);
      LOG.info("Kafka Cruise Control shutdown completed.");
    });
    t.setDaemon(true);
    t.start();
    try {
      t.join(30000);
    } catch (InterruptedException e) {
      LOG.warn("Cruise Control failed to shutdown in 30 seconds. Exit.");
    }
  }

  /**
   * Allow a reference to {@link UserTaskManager} to be passed to {@link Executor}
   * @param userTaskManager a reference to {@link UserTaskManager}
   */
  public void setUserTaskManagerInExecutor(UserTaskManager userTaskManager) {
    _executor.setUserTaskManager(userTaskManager);
  }

  /**
   * Sanity check that if current request is not a dryrun, then there is no ongoing
   * <ol>
   *   <li>execution in current Cruise Control deployment and user does not require stopping the ongoing execution,</li>
   *   <li>partition reassignment triggered by other admin tools or previous Cruise Control deployment.</li>
   * </ol>
   *
   * This method helps to fail fast if a user attempts to start an execution during an ongoing execution.
   *
   * @param dryRun {@code true} if the request is just a dryrun, {@code false} if the intention is to start an execution.
   * @param stopOngoingExecution {@code true} to stop the ongoing execution (if any) and start executing the given proposals,
   *                             {@code false} otherwise.
   */
  public void sanityCheckDryRun(boolean dryRun, boolean stopOngoingExecution) {
    if (dryRun) {
      return;
    }
    if (hasOngoingExecution()) {
      if (!stopOngoingExecution) {
        throw new IllegalStateException(String.format("Cannot start a new execution while there is an ongoing execution. "
                                                      + "Please use %s=true to stop ongoing execution and start a new one.",
                                                      STOP_ONGOING_EXECUTION_PARAM));
      }
    } else {
      Set<TopicPartition> partitionsBeingReassigned;
      try {
        partitionsBeingReassigned = _executor.listPartitionsBeingReassigned();
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
        // This may indicate transient (e.g. network) issues.
        throw new IllegalStateException("Cannot execute new proposals due to failure to retrieve whether the Kafka cluster has "
                                        + "an already ongoing partition reassignment.", e);
      }
      if (!partitionsBeingReassigned.isEmpty()) {
        if (_config.getBoolean(ExecutorConfig.AUTO_STOP_EXTERNAL_AGENT_CONFIG)) {
          // Stop the external agent reassignment.
          if (_executor.maybeStopExternalAgent()) {
            LOG.info("External agent is reassigning partitions. "
                     + "The request to stop it is submitted successfully: {}", partitionsBeingReassigned);
          }
        } else {
          throw new IllegalStateException(String.format("Cannot execute new proposals while there are ongoing partition reassignments "
                                                        + "initiated by external agent: %s", partitionsBeingReassigned));
        }
      }
    }
  }

  /**
   * @return {@code true} if there is an ongoing execution started by Cruise Control.
   */
  public boolean hasOngoingExecution() {
    return _executor.hasOngoingExecution();
  }

  /**
   * Let executor know the intention regarding modifying the ongoing execution. Only one request at a given time is
   * allowed to modify the ongoing execution.
   *
   * @param modify {@code true} to indicate, {@code false} to cancel the intention to modify
   * @return {@code true} if the intention changes the state known by executor, {@code false} otherwise.
   */
  public boolean modifyOngoingExecution(boolean modify) {
    return _executor.modifyOngoingExecution(modify);
  }

  /**
   * Get the broker load stats from the cache. null will be returned if their is no cached broker load stats.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The cached broker load statistics.
   */
  public BrokerStats cachedBrokerLoadStats(boolean allowCapacityEstimation) {
    return _loadMonitor.cachedBrokerLoadStats(allowCapacityEstimation);
  }

  /**
   * Get the cluster model cutting off at the current timestamp.
   * @param requirements the model completeness requirements.
   * @param allowCapacityEstimation whether allow capacity estimation in cluster model if the underlying live broker capacity is unavailable.
   * @param operationProgress the progress of the job to report.
   * @return The cluster workload model.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   * @throws TimeoutException If broker capacity resolver is unable to resolve broker capacity in time.
   * @throws BrokerCapacityResolutionException If broker capacity resolver fails to resolve broker capacity.
   */
  public ClusterModel clusterModel(ModelCompletenessRequirements requirements,
                                   boolean allowCapacityEstimation,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException, TimeoutException, BrokerCapacityResolutionException {
    return _loadMonitor.clusterModel(timeMs(), requirements, allowCapacityEstimation, operationProgress);
  }

  /**
   * Get the cluster model cutting off at the current timestamp with replica placement info.
   * @param requirements the model completeness requirements.
   * @param allowCapacityEstimation whether allow capacity estimation in cluster model if the underlying live broker capacity is unavailable.
   * @param operationProgress the progress of the job to report.
   * @param populateReplicaPlacementInfo whether populate replica placement information.
   * @return The cluster workload model.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   * @throws TimeoutException If broker capacity resolver is unable to resolve broker capacity in time.
   * @throws BrokerCapacityResolutionException If broker capacity resolver fails to resolve broker capacity.
   */
  public ClusterModel clusterModel(ModelCompletenessRequirements requirements,
                                   boolean allowCapacityEstimation,
                                   OperationProgress operationProgress,
                                   boolean populateReplicaPlacementInfo)
          throws NotEnoughValidWindowsException, TimeoutException, BrokerCapacityResolutionException {
    return _loadMonitor.clusterModel(timeMs(), requirements, populateReplicaPlacementInfo, allowCapacityEstimation, operationProgress);
  }

  /**
   * Get the cluster model for a given time window.
   * @param from the start time of the window
   * @param to the end time of the window
   * @param requirements the load completeness requirements.
   * @param populateReplicaPlacementInfo whether populate replica placement information.
   * @param allowCapacityEstimation whether allow capacity estimation in cluster model if the underlying live broker capacity is unavailable.
   * @param operationProgress the progress of the job to report.
   * @return The cluster workload model.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   * @throws TimeoutException If broker capacity resolver is unable to resolve broker capacity in time.
   * @throws BrokerCapacityResolutionException If broker capacity resolver fails to resolve broker capacity.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   boolean populateReplicaPlacementInfo,
                                   boolean allowCapacityEstimation,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException, TimeoutException, BrokerCapacityResolutionException {
    return _loadMonitor.clusterModel(from, to, requirements, populateReplicaPlacementInfo, allowCapacityEstimation, operationProgress);
  }

  /**
   * Get cluster capacity, and skip populating cluster load. Enables quick retrieval of capacity without the load.
   * @return Cluster capacity without cluster load.
   */
  public ClusterModel clusterCapacity() throws TimeoutException, BrokerCapacityResolutionException {
    return _loadMonitor.clusterCapacity();
  }

  /**
   * Bootstrap the load monitor for a given period.
   *
   * @param startMs the starting time of the bootstrap period, or null if no time period will be used.
   * @param endMs the end time of the bootstrap period, or null if no end time is specified.
   * @param clearMetrics clear the existing metric samples.
   */
  public void bootstrap(Long startMs, Long endMs, boolean clearMetrics) {
    if (startMs != null && endMs != null) {
      // Bootstrap the load monitor for a given period.
      _loadMonitor.bootstrap(startMs, endMs, clearMetrics);
    } else if (startMs != null) {
      // Bootstrap the load monitor from the given timestamp until it catches up -- i.e. clears all metric samples.
      _loadMonitor.bootstrap(startMs, clearMetrics);
    } else {
      // Bootstrap the load monitor with the most recent metric samples until it catches up -- clears all metric samples.
      _loadMonitor.bootstrap(clearMetrics);
    }
  }

  /**
   * Pause all the activities of the load monitor. The load monitor can only be paused when it is in
   * RUNNING state.
   *
   * @param reason The reason for pausing metric sampling.
   */
  public void pauseMetricSampling(String reason) {
    _loadMonitor.pauseMetricSampling(reason, FORCE_PAUSE_SAMPLING);
  }

  /**
   * Train the load model with metric samples.
   * @param startMs training period starting time.
   * @param endMs training period end time.
   */
  public void train(Long startMs, Long endMs) {
    _loadMonitor.train(startMs, endMs);
  }

  /**
   * Enable or disable self healing for the given anomaly type in the anomaly detector.
   *
   * @param anomalyType Type of anomaly for which to enable or disable self healing.
   * @param isSelfHealingEnabled {@code true} if self healing is enabled for the given anomaly type, {@code false} otherwise.
   * @return The old value of self healing for the given anomaly type.
   */
  public boolean setSelfHealingFor(AnomalyType anomalyType, boolean isSelfHealingEnabled) {
    return _anomalyDetectorManager.setSelfHealingFor(anomalyType, isSelfHealingEnabled);
  }

  /**
   * Enable or disable concurrency adjuster for the given concurrency type in the executor.
   *
   * @param concurrencyType Type of concurrency for which to enable or disable concurrency adjuster.
   * @param isConcurrencyAdjusterEnabled {@code true} if concurrency adjuster is enabled for the given type, {@code false} otherwise.
   * @return {@code true} if concurrency adjuster was enabled before for the given concurrency type, {@code false} otherwise.
   */
  public boolean setConcurrencyAdjusterFor(ConcurrencyType concurrencyType, boolean isConcurrencyAdjusterEnabled) {
    return _executor.setConcurrencyAdjusterFor(concurrencyType, isConcurrencyAdjusterEnabled);
  }

  /**
   * Enable or disable (At/Under)MinISR-based concurrency adjustment.
   *
   * @param isMinIsrBasedConcurrencyAdjustmentEnabled {@code true} to enable (At/Under)MinISR-based concurrency adjustment, {@code false} otherwise.
   * @return {@code true} if (At/Under)MinISR-based concurrency adjustment was enabled before, {@code false} otherwise.
   */
  public boolean setConcurrencyAdjusterMinIsrCheck(boolean isMinIsrBasedConcurrencyAdjustmentEnabled) {
    return _executor.setConcurrencyAdjusterMinIsrCheck(isMinIsrBasedConcurrencyAdjustmentEnabled);
  }

  /**
   * Drop the given brokers from the recently removed/demoted brokers.
   *
   * @param brokersToDrop Brokers to drop from the recently removed or demoted brokers.
   * @param isRemoved {@code true} to drop recently removed brokers, {@code false} to drop recently demoted brokers
   * @return {@code true} if any elements were removed from the requested set of brokers.
   */
  public boolean dropRecentBrokers(Set<Integer> brokersToDrop, boolean isRemoved) {
    return isRemoved ? _executor.dropRecentlyRemovedBrokers(brokersToDrop) : _executor.dropRecentlyDemotedBrokers(brokersToDrop);
  }

  /**
   * Add the given brokers to the recently removed/demoted brokers permanently -- i.e. until they are explicitly dropped by user.
   *
   * @param brokersToAdd Brokers to add to the recently removed or demoted brokers.
   * @param isRemoved {@code true} to add to recently removed brokers, {@code false} to add recently demoted brokers
   */
  public void addRecentBrokersPermanently(Set<Integer> brokersToAdd, boolean isRemoved) {
    if (isRemoved) {
      _executor.addRecentlyRemovedBrokers(brokersToAdd);
    } else {
      _executor.addRecentlyDemotedBrokers(brokersToAdd);
    }
  }

  /**
   * Get {@link Executor#recentlyRemovedBrokers()} if isRemoved is true, {@link Executor#recentlyDemotedBrokers()} otherwise.
   *
   * @param isRemoved {@code true} to get recently removed brokers, {@code false} to get recently demoted brokers
   * @return IDs of requested brokers.
   */
  public Set<Integer> recentBrokers(boolean isRemoved) {
    return isRemoved ? _executor.recentlyRemovedBrokers() : _executor.recentlyDemotedBrokers();
  }

  /**
   * Dynamically set the interval between checking and updating (if needed) the progress of an initiated execution.
   *
   * @param requestedExecutionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                                  execution (if null, use the default execution progress check interval
   *                                                  of Executor).
   */
  public void setRequestedExecutionProgressCheckIntervalMs(Long requestedExecutionProgressCheckIntervalMs) {
    _executor.setRequestedExecutionProgressCheckIntervalMs(requestedExecutionProgressCheckIntervalMs);
  }

  /**
   * Dynamically set the per broker execution concurrency for all brokers or set the cluster concurrency.
   * Notice that if concurrency type is LEADERSHIP, this value will also be used as max allowed concurrency at cluster level.
   *
   * @param requestedExecutionConcurrency The maximum number of allowed concurrent execution.
   * @param concurrencyType The type of the execution concurrency to set value for.
   */
  public void setExecutionConcurrencyForAllBrokersOrCluster(Integer requestedExecutionConcurrency, ConcurrencyType concurrencyType) {
    _executor.setExecutionConcurrencyForAllBrokersOrCluster(requestedExecutionConcurrency, concurrencyType);
  }

  /**
   * Dynamically set the max inter-broker partition movements per cluster.
   *
   * @param requestedMaxInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per cluster.
   */
  public void setRequestedMaxInterBrokerPartitionMovements(Integer requestedMaxInterBrokerPartitionMovements) {
    _executor.setRequestedMaxInterBrokerPartitionMovements(requestedMaxInterBrokerPartitionMovements);
  }

  /**
   * Resume the activities of the load monitor.
   *
   * @param reason The reason for resuming metric sampling.
   */
  public void resumeMetricSampling(String reason) {
    _loadMonitor.resumeMetricSampling(reason);
  }

  /**
   * Get the optimization proposals from the current cluster. The result would be served from the cached result if
   * it is still valid.
   * @param operationProgress the job progress to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The optimization result.
   */
  public OptimizerResult getProposals(OperationProgress operationProgress, boolean allowCapacityEstimation)
      throws KafkaCruiseControlException {
    try {
      return _goalOptimizer.optimizations(operationProgress, allowCapacityEstimation);
    } catch (InterruptedException ie) {
      throw new KafkaCruiseControlException("Interrupted when getting the optimization proposals", ie);
    }
  }

  /**
   * Ignore the cached best proposals when:
   * <ul>
   *   <li>The caller specified goals, excluded topics, or requested to exclude brokers (e.g. recently removed brokers).</li>
   *   <li>Provided completeness requirements contain a weaker requirement than what is used by the cached proposal.</li>
   *   <li>There is an ongoing execution.</li>
   *   <li>The request is triggered by goal violation detector.</li>
   *   <li>The request involves explicitly requested destination broker Ids.</li>
   *   <li>The caller wants to rebalance across disks within the brokers.</li>
   *   <li>There are offline replicas in the cluster.</li>
   * </ul>
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to optimize. When empty all goals will be used.
   * @param requirements Model completeness requirements.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param excludeBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param ignoreProposalCache {@code true} to explicitly ignore the proposal cache, {@code false} otherwise.
   * @param isTriggeredByGoalViolation {@code true} if proposals is triggered by goal violation, {@code false} otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @param isRebalanceDiskMode {@code true} to generate proposal to rebalance between disks within the brokers, {@code false} otherwise.
   * @return {@code true} to ignore proposal cache, {@code false} otherwise.
   */
  public boolean ignoreProposalCache(List<String> goals,
                                     ModelCompletenessRequirements requirements,
                                     Pattern excludedTopics,
                                     boolean excludeBrokers,
                                     boolean ignoreProposalCache,
                                     boolean isTriggeredByGoalViolation,
                                     Set<Integer> requestedDestinationBrokerIds,
                                     boolean isRebalanceDiskMode) {
    ModelCompletenessRequirements requirementsForCache = _goalOptimizer.modelCompletenessRequirementsForPrecomputing();
    boolean hasWeakerRequirement =
        requirementsForCache.minMonitoredPartitionsPercentage() > requirements.minMonitoredPartitionsPercentage()
        || requirementsForCache.minRequiredNumWindows() > requirements.minRequiredNumWindows()
        || (requirementsForCache.includeAllTopics() && !requirements.includeAllTopics());

    return hasOngoingExecution() || ignoreProposalCache || (goals != null && !goals.isEmpty())
           || hasWeakerRequirement || excludedTopics != null || excludeBrokers || isTriggeredByGoalViolation
           || !requestedDestinationBrokerIds.isEmpty() || isRebalanceDiskMode
           || partitionWithOfflineReplicas(kafkaCluster()) != null;
  }

  /**
   * See {@link GoalOptimizer#optimizations(ClusterModel, List, OperationProgress, Map, OptimizationOptions)}.
   *
   * @param clusterModel The state of the cluster.
   * @param goalsByPriority the goals ordered by priority.
   * @param operationProgress to report the job progress.
   * @param initReplicaDistribution The initial replica distribution of the cluster. This is only needed if the passed in clusterModel is not
   *                                the original cluster model so that initial replica distribution can not be deducted from that cluster model,
   *                                otherwise it is null. One case explicitly specifying initial replica distribution needed is to increase/decrease
   *                                specific topic partition's replication factor, in this case some replicas are tentatively deleted/added in
   *                                cluster model before passing it in to generate proposals.
   * @param optimizationOptions Optimization options.
   * @return Results of optimization containing the proposals and stats.
   */
  public synchronized OptimizerResult optimizations(ClusterModel clusterModel,
                                                    List<Goal> goalsByPriority,
                                                    OperationProgress operationProgress,
                                                    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution,
                                                    OptimizationOptions optimizationOptions)
      throws KafkaCruiseControlException {
    return _goalOptimizer.optimizations(clusterModel, goalsByPriority, operationProgress, initReplicaDistribution, optimizationOptions);
  }

  /**
   * See {@link GoalOptimizer#excludedTopics(ClusterModel, Pattern)}.
   * @param clusterModel The state of the cluster.
   * @param requestedExcludedTopics Pattern used to exclude topics, or {@code null} to use the default excluded topics.
   * @return Set of excluded topics in the given cluster model.
   */
  public Set<String> excludedTopics(ClusterModel clusterModel, Pattern requestedExcludedTopics) {
    return _goalOptimizer.excludedTopics(clusterModel, requestedExcludedTopics);
  }

  /**
   * @return Kafka Cruise Control config.
   */
  public KafkaCruiseControlConfig config() {
    return _config;
  }

  private static boolean hasProposalsToExecute(Collection<ExecutionProposal> proposals, String uuid) {
    if (proposals.isEmpty()) {
      LOG.info("Goals used in proposal generation for UUID {} are already satisfied.", uuid);
      return false;
    }
    return true;
  }

  /**
   * Execute the given balancing proposals for non-(demote/remove) operations.
   * @param proposals the given balancing proposals
   * @param unthrottledBrokers Brokers for which the rate of replica movements from/to will not be throttled.
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param maxInterBrokerPartitionMovements The upper bound of concurrent inter-broker partition movements in cluster
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentIntraBrokerPartitionMovements The maximum number of concurrent intra-broker partition movements
   *                                                (if null, use num.concurrent.intra.broker.partition.movements).
   * @param clusterConcurrentLeaderMovements The maximum number of concurrent leader movements in a cluster
   *                                  (if null, use num.concurrent.leader.movements).
   * @param brokerConcurrentLeaderMovements The maximum number of concurrent leader movements involved in a broker
   *                                  (if null, use num.concurrent.leader.movements.per.broker).
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing proposals (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   * @param skipInterBrokerReplicaConcurrencyAdjustment {@code true} to skip auto adjusting concurrency of inter-broker
   * replica movements even if the concurrency adjuster is enabled, {@code false} otherwise.
   */
  public void executeProposals(Set<ExecutionProposal> proposals,
                               Set<Integer> unthrottledBrokers,
                               boolean isKafkaAssignerMode,
                               Integer concurrentInterBrokerPartitionMovements,
                               Integer maxInterBrokerPartitionMovements,
                               Integer concurrentIntraBrokerPartitionMovements,
                               Integer clusterConcurrentLeaderMovements,
                               Integer brokerConcurrentLeaderMovements,
                               Long executionProgressCheckIntervalMs,
                               ReplicaMovementStrategy replicaMovementStrategy,
                               Long replicationThrottle,
                               boolean isTriggeredByUserRequest,
                               String uuid,
                               boolean skipInterBrokerReplicaConcurrencyAdjustment) throws OngoingExecutionException {
    if (hasProposalsToExecute(proposals, uuid)) {
      _executor.executeProposals(proposals, unthrottledBrokers, null, _loadMonitor, concurrentInterBrokerPartitionMovements,
                                 maxInterBrokerPartitionMovements, concurrentIntraBrokerPartitionMovements, clusterConcurrentLeaderMovements,
                                 brokerConcurrentLeaderMovements, executionProgressCheckIntervalMs, replicaMovementStrategy, replicationThrottle,
                                 isTriggeredByUserRequest, uuid, isKafkaAssignerMode, skipInterBrokerReplicaConcurrencyAdjustment);
    } else {
      failGeneratingProposalsForExecution(uuid);
    }
  }

  /**
   * Execute the given balancing proposals for remove operations.
   * @param proposals the given balancing proposals
   * @param throttleDecommissionedBroker Whether throttle the brokers that are being decommissioned.
   * @param removedBrokers Brokers to be removed, null if no brokers have been removed.
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param maxInterBrokerPartitionMovements The upper bound of concurrent inter-broker partition movements in cluster
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param clusterLeaderMovementConcurrency The maximum number of concurrent leader movements in a cluster
   *                                          (if null, use num.concurrent.leader.movements).
   * @param brokerLeaderMovementConcurrency The maximum number of concurrent leader movements involved in a broker
   *                                         (if null, use num.concurrent.leader.movements.per.broker).
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing remove operations (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   */
  public void executeRemoval(Set<ExecutionProposal> proposals,
                             boolean throttleDecommissionedBroker,
                             Set<Integer> removedBrokers,
                             boolean isKafkaAssignerMode,
                             Integer concurrentInterBrokerPartitionMovements,
                             Integer maxInterBrokerPartitionMovements,
                             Integer clusterLeaderMovementConcurrency,
                             Integer brokerLeaderMovementConcurrency,
                             Long executionProgressCheckIntervalMs,
                             ReplicaMovementStrategy replicaMovementStrategy,
                             Long replicationThrottle,
                             boolean isTriggeredByUserRequest,
                             String uuid) throws OngoingExecutionException {
    if (hasProposalsToExecute(proposals, uuid)) {
      _executor.executeProposals(proposals, throttleDecommissionedBroker ? Collections.emptySet() : removedBrokers, removedBrokers,
                                 _loadMonitor, concurrentInterBrokerPartitionMovements, maxInterBrokerPartitionMovements, 0,
                                 clusterLeaderMovementConcurrency, brokerLeaderMovementConcurrency,
                                 executionProgressCheckIntervalMs, replicaMovementStrategy, replicationThrottle,
                                 isTriggeredByUserRequest, uuid, isKafkaAssignerMode, false);
    } else {
      failGeneratingProposalsForExecution(uuid);
    }
  }

  /**
   * Execute the given balancing proposals for demote operations.
   * @param proposals The given balancing proposals
   * @param demotedBrokers Brokers to be demoted.
   * @param clusterLeaderMovementConcurrency The maximum number of concurrent leader movements in a cluster
   *                                          (if null, use num.concurrent.leader.movements).
   * @param brokerLeaderMovementConcurrency The maximum number of concurrent leader movements involved in a broker
   *                                          (if null, use num.concurrent.leader.movements.per.broker).
   * @param brokerCount Number of brokers in the cluster.
   * @param executionProgressCheckIntervalMs The interval between checking and updating the progress of an initiated
   *                                         execution (if null, use execution.progress.check.interval.ms).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing demote operations (if null, no throttling is applied).
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   * @param uuid UUID of the execution.
   */
  public void executeDemotion(Set<ExecutionProposal> proposals,
                              Set<Integer> demotedBrokers,
                              Integer clusterLeaderMovementConcurrency,
                              Integer brokerLeaderMovementConcurrency,
                              int brokerCount,
                              Long executionProgressCheckIntervalMs,
                              ReplicaMovementStrategy replicaMovementStrategy,
                              Long replicationThrottle,
                              boolean isTriggeredByUserRequest,
                              String uuid) throws OngoingExecutionException {
    if (hasProposalsToExecute(proposals, uuid)) {
      // (1) Kafka Assigner mode is irrelevant for demoting.
      // (2) Ensure that replica swaps within partitions, which are prerequisites for broker demotion and does not trigger data move,
      //     are throttled by clusterLeaderMovementConcurrency and config max.num.cluster.movements.
      int concurrentSwaps = clusterLeaderMovementConcurrency != null
                            ? clusterLeaderMovementConcurrency
                            : _config.getInt(ExecutorConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG);
      concurrentSwaps = Math.min(_config.getInt(ExecutorConfig.MAX_NUM_CLUSTER_MOVEMENTS_CONFIG) / brokerCount, concurrentSwaps);

      _executor.executeDemoteProposals(proposals, demotedBrokers, _loadMonitor, concurrentSwaps, clusterLeaderMovementConcurrency,
                                       brokerLeaderMovementConcurrency, executionProgressCheckIntervalMs, replicaMovementStrategy,
                                       replicationThrottle, isTriggeredByUserRequest, uuid);
    } else {
      failGeneratingProposalsForExecution(uuid);
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * @param stopExternalAgent Whether to stop ongoing execution started by external agents.
   */
  public void userTriggeredStopExecution(boolean stopExternalAgent) {
    _executor.userTriggeredStopExecution(stopExternalAgent);
  }

  /**
   * See {@link Executor#setGeneratingProposalsForExecution(String, Supplier, boolean)}.
   * @param uuid UUID of the current execution.
   * @param reasonSupplier Reason supplier for the execution.
   * @param isTriggeredByUserRequest Whether the execution is triggered by a user request.
   */
  public void setGeneratingProposalsForExecution(String uuid, Supplier<String> reasonSupplier, boolean isTriggeredByUserRequest)
      throws OngoingExecutionException {
    _executor.setGeneratingProposalsForExecution(uuid, reasonSupplier, isTriggeredByUserRequest);
  }

  /**
   * See {@link Executor#failGeneratingProposalsForExecution(String)}.
   * @param uuid UUID of the failed proposal generation for execution.
   */
  public synchronized void failGeneratingProposalsForExecution(String uuid) {
    _executor.failGeneratingProposalsForExecution(uuid);
  }

  /**
   * @return The interval between checking and updating (if needed) the progress of an initiated execution.
   */
  public long executionProgressCheckIntervalMs() {
    return _executor.executionProgressCheckIntervalMs();
  }

  /**
   * @return The current state of the executor.
   */
  public ExecutorState.State executionState() {
    return executorState().state();
  }

  /**
   * @return Executor state.
   */
  public ExecutorState executorState() {
    return _executor.state();
  }

  /**
   * Get the state of the load monitor.
   *
   * @param cluster Kafka cluster.
   * @return The state of the load monitor.
   */
  public LoadMonitorState monitorState(Cluster cluster) {
    return _loadMonitor.state(cluster);
  }

  /**
   * Get the analyzer state from the goal optimizer.
   *
   * @param cluster Kafka cluster.
   * @return The analyzer state.
   */
  public AnalyzerState analyzerState(Cluster cluster) {
    return _goalOptimizer.state(cluster);
  }

  /**
   * @return Anomaly detector state.
   */
  public AnomalyDetectorState anomalyDetectorState() {
    return _anomalyDetectorManager.anomalyDetectorState();
  }

  /**
   * @return The cluster information from Kafka metadata.
   */
  public Cluster kafkaCluster() {
    return _loadMonitor.kafkaCluster();
  }

  /**
   * @return The topic config provider.
   */
  public TopicConfigProvider topicConfigProvider() {
    return _loadMonitor.topicConfigProvider();
  }

  /**
   * @return The broker set resolver
   */
  public BrokerSetResolver brokerSetResolver() {
    return _goalOptimizer.brokerSetResolver();
  }

  /**
   * @return The Kafka Cruise Control Version
   */
  public static String cruiseControlVersion() {
    return VERSION;
  }

  /**
   * @return The Kafka Cruise Control's current code's commit id
   */
  public static String cruiseControlCommitId() {
    return COMMIT_ID;
  }

  /**
   * If the goals is empty or null, return the default model completeness requirements, otherwise combine the load
   * requirement options for the given goals and return the resulting model completeness requirements.
   *
   * @param goals Goals to combine load requirement options.
   * @return Model completeness requirements.
   */
  public ModelCompletenessRequirements modelCompletenessRequirements(Collection<Goal> goals) {
    return goals == null || goals.isEmpty() ? _goalOptimizer.defaultModelCompletenessRequirements()
                                            : MonitorUtils.combineLoadRequirementOptions(goals);
  }

  /**
   * Check if the completeness requirements are met for the given goals.
   *
   * @param goals A list of goals to check completeness for.
   * @return {@code true} if completeness requirements are met for the given goals, {@code false} otherwise.
   */
  public boolean meetCompletenessRequirements(List<String> goals) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    return goalsByPriority(goals, _config).stream().allMatch(g -> _loadMonitor.meetCompletenessRequirements(
        clusterAndGeneration.cluster(), g.clusterModelCompletenessRequirements()));
  }

  /**
   * Sanity check whether the provided brokers exist in cluster or not.
   * @param brokerIds A set of broker ids.
   */
  public void sanityCheckBrokerPresence(Set<Integer> brokerIds) {
    Cluster cluster = _loadMonitor.refreshClusterAndGeneration().cluster();
    Set<Integer> invalidBrokerIds = brokerIds.stream().filter(id -> cluster.nodeById(id) == null).collect(Collectors.toSet());
    if (!invalidBrokerIds.isEmpty()) {
      throw new IllegalArgumentException(String.format("Broker %s does not exist.", invalidBrokerIds));
    }
  }
}
