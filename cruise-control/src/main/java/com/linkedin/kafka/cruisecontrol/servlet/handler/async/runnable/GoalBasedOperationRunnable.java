/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.GoalBasedOptimizationParameters;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckLoadMonitorReadiness;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_DRYRUN;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_SKIP_HARD_GOAL_CHECK;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_EXCLUDED_TOPICS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_IS_TRIGGERED_BY_USER_REQUEST;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.SELF_HEALING_FAST_MODE;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;


/**
 * An abstract class to extract the common logic of goal based operation runnables.
 */
public abstract class GoalBasedOperationRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(GoalBasedOperationRunnable.class);
  protected final List<String> _goals;
  protected final ModelCompletenessRequirements _modelCompletenessRequirements;
  protected final boolean _dryRun;
  protected final boolean _stopOngoingExecution;
  protected final boolean _skipHardGoalCheck;
  protected final Pattern _excludedTopics;
  protected final boolean _allowCapacityEstimation;
  protected final boolean _excludeRecentlyDemotedBrokers;
  protected final boolean _excludeRecentlyRemovedBrokers;
  protected final String _uuid;
  protected final Supplier<String> _reasonSupplier;
  protected final boolean _isTriggeredByUserRequest;
  protected final boolean _fastMode;
  protected OperationProgress _operationProgress;
  // Combined completeness requirements to be used after initialization.
  protected ModelCompletenessRequirements _combinedCompletenessRequirements;
  protected List<Goal> _goalsByPriority;

  /**
   * Constructor to be used for creating a runnable for a user request.
   */
  public GoalBasedOperationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    GoalBasedOptimizationParameters parameters,
                                    boolean dryRun,
                                    boolean stopOngoingExecution,
                                    boolean skipHardGoalCheck,
                                    String uuid,
                                    Supplier<String> reasonSupplier) {
    this(kafkaCruiseControl, future, dryRun, parameters.goals(), stopOngoingExecution,
         parameters.modelCompletenessRequirements(), skipHardGoalCheck, parameters.excludedTopics(),
         parameters.allowCapacityEstimation(), parameters.excludeRecentlyDemotedBrokers(),
         parameters.excludeRecentlyRemovedBrokers(), uuid, reasonSupplier, !SELF_HEALING_IS_TRIGGERED_BY_USER_REQUEST,
         parameters.fastMode());
  }

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public GoalBasedOperationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    List<String> goals,
                                    boolean allowCapacityEstimation,
                                    boolean excludeRecentlyDemotedBrokers,
                                    boolean excludeRecentlyRemovedBrokers,
                                    String uuid,
                                    Supplier<String> reasonSupplier,
                                    boolean stopOngoingExecution) {
    this(kafkaCruiseControl, future, SELF_HEALING_DRYRUN, goals, stopOngoingExecution,
         SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS, SELF_HEALING_SKIP_HARD_GOAL_CHECK, SELF_HEALING_EXCLUDED_TOPICS,
         allowCapacityEstimation, excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, uuid, reasonSupplier,
         SELF_HEALING_IS_TRIGGERED_BY_USER_REQUEST, SELF_HEALING_FAST_MODE);
  }

  /**
   * Constructor to be used for creating a runnable for self-healing.
   */
  public GoalBasedOperationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    List<String> goals,
                                    boolean allowCapacityEstimation,
                                    boolean excludeRecentlyDemotedBrokers,
                                    boolean excludeRecentlyRemovedBrokers,
                                    String uuid,
                                    Supplier<String> reasonSupplier,
                                    boolean stopOngoingExecution,
                                    boolean skipHardGoalCheck) {
    this(kafkaCruiseControl, future, SELF_HEALING_DRYRUN, goals, stopOngoingExecution,
            SELF_HEALING_MODEL_COMPLETENESS_REQUIREMENTS, skipHardGoalCheck, SELF_HEALING_EXCLUDED_TOPICS,
            allowCapacityEstimation, excludeRecentlyDemotedBrokers, excludeRecentlyRemovedBrokers, uuid, reasonSupplier,
            SELF_HEALING_IS_TRIGGERED_BY_USER_REQUEST, SELF_HEALING_FAST_MODE);
  }

  public GoalBasedOperationRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    boolean dryRun,
                                    List<String> goals,
                                    boolean stopOngoingExecution,
                                    ModelCompletenessRequirements modelCompletenessRequirements,
                                    boolean skipHardGoalCheck,
                                    Pattern excludedTopics,
                                    boolean allowCapacityEstimation,
                                    boolean excludeRecentlyDemotedBrokers,
                                    boolean excludeRecentlyRemovedBrokers,
                                    String uuid,
                                    Supplier<String> reasonSupplier,
                                    boolean isTriggeredByUserRequest,
                                    boolean fastMode) {
    super(kafkaCruiseControl, future);
    _goals = goals;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _dryRun = dryRun;
    _stopOngoingExecution = stopOngoingExecution;
    _skipHardGoalCheck = skipHardGoalCheck;
    _excludedTopics = excludedTopics;
    _allowCapacityEstimation = allowCapacityEstimation;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _uuid = uuid;
    _reasonSupplier = reasonSupplier;
    _isTriggeredByUserRequest = isTriggeredByUserRequest;
    _operationProgress = null;
    _combinedCompletenessRequirements = null;
    _goalsByPriority = null;
    _fastMode = fastMode;
  }

  /**
   * Perform the initializations before {@link #computeResult()}.
   */
  protected void init() {
    _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    sanityCheckGoals(_goals, _skipHardGoalCheck, config);
    _goalsByPriority = goalsByPriority(_goals, config);
    _operationProgress = _future.operationProgress();
    if (_stopOngoingExecution) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _operationProgress);
    }
    _combinedCompletenessRequirements =
        _kafkaCruiseControl.modelCompletenessRequirements(_goalsByPriority).weaker(_modelCompletenessRequirements);
    sanityCheckLoadMonitorReadiness(_combinedCompletenessRequirements, _kafkaCruiseControl.getLoadMonitorTaskRunnerState());
  }

  protected void handleFailGeneratingProposalsForExecution() {
    if (!_dryRun) {
      _kafkaCruiseControl.failGeneratingProposalsForExecution(_uuid);
    }
  }

  /**
   * Compute the underlying goal-based optimization and (if requested and applicable) start execution to reach this outcome.
   *
   * @return Optimizer result to indicate the outcome of underlying goal-based optimization.
   */
  public OptimizerResult computeResult() throws KafkaCruiseControlException {
    init();
    if (!_dryRun) {
      _kafkaCruiseControl.setGeneratingProposalsForExecution(_uuid, _reasonSupplier, _isTriggeredByUserRequest);
    }
    OptimizerResult result;
    if (shouldWorkWithClusterModel()) {
      try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(_operationProgress)) {
        result = workWithClusterModel();
      } catch (KafkaCruiseControlException kcce) {
        handleFailGeneratingProposalsForExecution();
        throw kcce;
      } catch (Exception e) {
        handleFailGeneratingProposalsForExecution();
        throw new KafkaCruiseControlException(e);
      } finally {
        finish();
      }
    } else {
      try {
        result = workWithoutClusterModel();
      } catch (KafkaCruiseControlException kcce) {
        handleFailGeneratingProposalsForExecution();
        throw kcce;
      } catch (Exception e) {
        handleFailGeneratingProposalsForExecution();
        throw new KafkaCruiseControlException(e);
      } finally {
        finish();
      }
    }
    return result;
  }

  /**
   * @return {@code true} to generate an optimizer result with {@link #workWithClusterModel()}, and {@code false} to generate an
   * optimizer result with {@link #workWithoutClusterModel()}.
   */
  protected abstract boolean shouldWorkWithClusterModel();

  /**
   * Work without the need for generating a cluster model, without acquiring the cluster model semaphore.
   * @return Optimizer result to indicate the outcome of underlying goal-based optimization.
   */
  protected abstract OptimizerResult workWithoutClusterModel() throws KafkaCruiseControlException;

  /**
   * Work while acquiring the cluster model semaphore to throttle simultaneous cluster model creation.
   * @return Optimizer result to indicate the outcome of underlying goal-based optimization.
   */
  protected abstract OptimizerResult workWithClusterModel()
      throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException;

  /**
   * Perform the memory clean up after {@link #computeResult()}.
   */
  protected void finish() {
    if (_operationProgress != null) {
      long totalTimeMs = _operationProgress.getCurrentTotalExecutionTimeMs();
      LOG.info("User task {}: {} finished with total time: {}ms; steps: {}",
          _uuid, _operationProgress.getOperation(), totalTimeMs, _operationProgress);
    }
    _operationProgress = null;
    _combinedCompletenessRequirements = null;
    if (_goalsByPriority != null) {
      _goalsByPriority.clear();
    }
  }
}
