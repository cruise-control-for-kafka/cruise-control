/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * An immutable snapshot of an execution, persisted by an {@link ExecutionStateStore} so that the execution can be recovered
 * after a Cruise Control restart. It contains
 * <ul>
 *   <li>the parameters that the execution was started with -- to resume it with the same parameters,</li>
 *   <li>the proposals of the execution -- once per partition, and</li>
 *   <li>the latest known state of each execution task -- to decide on restart whether a task was pending, in progress
 *   (i.e. submitted to Kafka), or finished, and</li>
 *   <li>fingerprints of the replicas that the execution does not move -- to tell on restart whether the cluster has changed
 *   since, in which case the execution is not resumed.</li>
 * </ul>
 */
public final class PersistedExecutionState {
  // Version 2 added the fingerprints and the cleanup-only marker. A version 1 state is recovered, but never resumed.
  public static final int CURRENT_VERSION = 2;

  /**
   * The operation that started the execution.
   */
  public enum Operation {
    EXECUTE_PROPOSALS, DEMOTE_BROKERS
  }

  private final int _version;
  private final String _uuid;
  private final String _reason;
  private final Operation _operation;
  private final boolean _triggeredByUserRequest;
  private final long _startTimeMs;
  private final long _lastUpdateTimeMs;
  private final int _resumeCount;
  private final ExecutorState.State _executorState;
  private final Set<Integer> _removedBrokers;
  private final Set<Integer> _demotedBrokers;
  private final Set<Integer> _brokersToSkipConcurrencyCheck;
  private final Integer _interBrokerPartitionMovementConcurrency;
  private final Integer _maxInterBrokerPartitionMovements;
  private final Integer _intraBrokerPartitionMovementConcurrency;
  private final Integer _clusterLeadershipMovementConcurrency;
  private final Integer _brokerLeadershipMovementConcurrency;
  private final Long _executionProgressCheckIntervalMs;
  private final String _replicaMovementStrategy;
  private final Long _replicationThrottle;
  private final boolean _kafkaAssignerMode;
  private final boolean _skipInterBrokerReplicaConcurrencyAdjustment;
  private final boolean _cleanupOnly;
  private final ReplicaPlacementFingerprint _outsideExecutionFingerprint;
  private final Map<Integer, Long> _logDirFingerprintByBroker;
  private final Map<TopicPartition, ExecutionProposal> _proposals;
  private final List<PersistedTask> _tasks;

  private PersistedExecutionState(Builder builder) {
    _version = builder._version;
    _uuid = validateNotNull(builder._uuid, "UUID of the execution cannot be null.");
    _reason = builder._reason == null ? "" : builder._reason;
    _operation = validateNotNull(builder._operation, "Operation of the execution cannot be null.");
    _triggeredByUserRequest = builder._triggeredByUserRequest;
    _startTimeMs = builder._startTimeMs;
    _lastUpdateTimeMs = builder._lastUpdateTimeMs;
    _resumeCount = builder._resumeCount;
    _executorState = builder._executorState;
    _removedBrokers = immutableSetOrNull(builder._removedBrokers);
    _demotedBrokers = immutableSetOrNull(builder._demotedBrokers);
    _brokersToSkipConcurrencyCheck = immutableSetOrNull(builder._brokersToSkipConcurrencyCheck);
    _interBrokerPartitionMovementConcurrency = builder._interBrokerPartitionMovementConcurrency;
    _maxInterBrokerPartitionMovements = builder._maxInterBrokerPartitionMovements;
    _intraBrokerPartitionMovementConcurrency = builder._intraBrokerPartitionMovementConcurrency;
    _clusterLeadershipMovementConcurrency = builder._clusterLeadershipMovementConcurrency;
    _brokerLeadershipMovementConcurrency = builder._brokerLeadershipMovementConcurrency;
    _executionProgressCheckIntervalMs = builder._executionProgressCheckIntervalMs;
    _replicaMovementStrategy = builder._replicaMovementStrategy;
    _replicationThrottle = builder._replicationThrottle;
    _kafkaAssignerMode = builder._kafkaAssignerMode;
    _skipInterBrokerReplicaConcurrencyAdjustment = builder._skipInterBrokerReplicaConcurrencyAdjustment;
    _cleanupOnly = builder._cleanupOnly;
    _outsideExecutionFingerprint = builder._outsideExecutionFingerprint;
    _logDirFingerprintByBroker = builder._logDirFingerprintByBroker == null
                                 ? null : Collections.unmodifiableMap(new TreeMap<>(builder._logDirFingerprintByBroker));
    _proposals = Collections.unmodifiableMap(new LinkedHashMap<>(builder._proposals));
    _tasks = Collections.unmodifiableList(new ArrayList<>(builder._tasks));
    for (PersistedTask task : _tasks) {
      if (!_proposals.containsKey(task.topicPartition())) {
        throw new IllegalArgumentException(String.format("Missing proposal for the persisted task %s.", task));
      }
    }
  }

  private static Set<Integer> immutableSetOrNull(Collection<Integer> brokers) {
    return brokers == null ? null : Collections.unmodifiableSet(new TreeSet<>(brokers));
  }

  /**
   * @return A builder initialized with the content of this state.
   */
  public Builder toBuilder() {
    return new Builder(_uuid, _operation)
        .version(_version)
        .reason(_reason)
        .triggeredByUserRequest(_triggeredByUserRequest)
        .startTimeMs(_startTimeMs)
        .lastUpdateTimeMs(_lastUpdateTimeMs)
        .resumeCount(_resumeCount)
        .executorState(_executorState)
        .removedBrokers(_removedBrokers)
        .demotedBrokers(_demotedBrokers)
        .brokersToSkipConcurrencyCheck(_brokersToSkipConcurrencyCheck)
        .interBrokerPartitionMovementConcurrency(_interBrokerPartitionMovementConcurrency)
        .maxInterBrokerPartitionMovements(_maxInterBrokerPartitionMovements)
        .intraBrokerPartitionMovementConcurrency(_intraBrokerPartitionMovementConcurrency)
        .clusterLeadershipMovementConcurrency(_clusterLeadershipMovementConcurrency)
        .brokerLeadershipMovementConcurrency(_brokerLeadershipMovementConcurrency)
        .executionProgressCheckIntervalMs(_executionProgressCheckIntervalMs)
        .replicaMovementStrategy(_replicaMovementStrategy)
        .replicationThrottle(_replicationThrottle)
        .kafkaAssignerMode(_kafkaAssignerMode)
        .skipInterBrokerReplicaConcurrencyAdjustment(_skipInterBrokerReplicaConcurrencyAdjustment)
        .cleanupOnly(_cleanupOnly)
        .outsideExecutionFingerprint(_outsideExecutionFingerprint)
        .logDirFingerprintByBroker(_logDirFingerprintByBroker)
        .proposals(_proposals.values())
        .tasks(_tasks);
  }

  public int version() {
    return _version;
  }

  public String uuid() {
    return _uuid;
  }

  public String reason() {
    return _reason;
  }

  public Operation operation() {
    return _operation;
  }

  public boolean triggeredByUserRequest() {
    return _triggeredByUserRequest;
  }

  public long startTimeMs() {
    return _startTimeMs;
  }

  public long lastUpdateTimeMs() {
    return _lastUpdateTimeMs;
  }

  /**
   * @return Number of times this execution has been resumed after a Cruise Control restart.
   */
  public int resumeCount() {
    return _resumeCount;
  }

  /**
   * @return The latest known executor state of the execution, or {@code null} if unknown.
   */
  public ExecutorState.State executorState() {
    return _executorState;
  }

  public Set<Integer> removedBrokers() {
    return _removedBrokers;
  }

  public Set<Integer> demotedBrokers() {
    return _demotedBrokers;
  }

  public Set<Integer> brokersToSkipConcurrencyCheck() {
    return _brokersToSkipConcurrencyCheck;
  }

  public Integer interBrokerPartitionMovementConcurrency() {
    return _interBrokerPartitionMovementConcurrency;
  }

  public Integer maxInterBrokerPartitionMovements() {
    return _maxInterBrokerPartitionMovements;
  }

  public Integer intraBrokerPartitionMovementConcurrency() {
    return _intraBrokerPartitionMovementConcurrency;
  }

  public Integer clusterLeadershipMovementConcurrency() {
    return _clusterLeadershipMovementConcurrency;
  }

  public Integer brokerLeadershipMovementConcurrency() {
    return _brokerLeadershipMovementConcurrency;
  }

  public Long executionProgressCheckIntervalMs() {
    return _executionProgressCheckIntervalMs;
  }

  /**
   * @return Name of the replica movement strategy (see {@link
   * com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy#name()}), or {@code null} to use the default.
   */
  public String replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }

  public boolean kafkaAssignerMode() {
    return _kafkaAssignerMode;
  }

  public boolean skipInterBrokerReplicaConcurrencyAdjustment() {
    return _skipInterBrokerReplicaConcurrencyAdjustment;
  }

  /**
   * @return {@code true} if a Cruise Control restart has already decided not to resume this execution -- i.e. later restarts
   * must only clean it up, {@code false} otherwise.
   */
  public boolean cleanupOnly() {
    return _cleanupOnly;
  }

  /**
   * @return The fingerprint of the replica placement of the partitions that the execution does not move, as of the latest
   * persisted state, or {@code null} if unknown.
   */
  public ReplicaPlacementFingerprint outsideExecutionFingerprint() {
    return _outsideExecutionFingerprint;
  }

  /**
   * @return For each broker with intra-broker replica movements, the fingerprint of the logdirs of its replicas that the
   * execution does not move, as of the start of the execution, or {@code null} if the execution has no intra-broker replica
   * movements or the fingerprint is unknown.
   */
  public Map<Integer, Long> logDirFingerprintByBroker() {
    return _logDirFingerprintByBroker;
  }

  /**
   * @return Proposals of the execution by partition.
   */
  public Map<TopicPartition, ExecutionProposal> proposals() {
    return _proposals;
  }

  /**
   * @return The latest known state of each task of the execution.
   */
  public List<PersistedTask> tasks() {
    return _tasks;
  }

  @Override
  public String toString() {
    return String.format("{uuid: %s, operation: %s, reason: %s, executorState: %s, resumeCount: %d, proposals: %d, tasks: %d}",
                         _uuid, _operation, _reason, _executorState, _resumeCount, _proposals.size(), _tasks.size());
  }

  /**
   * A builder of {@link PersistedExecutionState}.
   */
  public static final class Builder {
    private int _version = CURRENT_VERSION;
    private final String _uuid;
    private String _reason;
    private final Operation _operation;
    private boolean _triggeredByUserRequest;
    private long _startTimeMs = -1L;
    private long _lastUpdateTimeMs = -1L;
    private int _resumeCount;
    private ExecutorState.State _executorState;
    private Collection<Integer> _removedBrokers;
    private Collection<Integer> _demotedBrokers;
    private Collection<Integer> _brokersToSkipConcurrencyCheck;
    private Integer _interBrokerPartitionMovementConcurrency;
    private Integer _maxInterBrokerPartitionMovements;
    private Integer _intraBrokerPartitionMovementConcurrency;
    private Integer _clusterLeadershipMovementConcurrency;
    private Integer _brokerLeadershipMovementConcurrency;
    private Long _executionProgressCheckIntervalMs;
    private String _replicaMovementStrategy;
    private Long _replicationThrottle;
    private boolean _kafkaAssignerMode;
    private boolean _skipInterBrokerReplicaConcurrencyAdjustment;
    private boolean _cleanupOnly;
    private ReplicaPlacementFingerprint _outsideExecutionFingerprint;
    private Map<Integer, Long> _logDirFingerprintByBroker;
    private final Map<TopicPartition, ExecutionProposal> _proposals = new LinkedHashMap<>();
    private final List<PersistedTask> _tasks = new ArrayList<>();

    public Builder(String uuid, Operation operation) {
      _uuid = uuid;
      _operation = operation;
    }

    /**
     * @param version Version of the persisted format.
     * @return This builder.
     */
    public Builder version(int version) {
      _version = version;
      return this;
    }

    /**
     * @param reason Reason of the execution.
     * @return This builder.
     */
    public Builder reason(String reason) {
      _reason = reason;
      return this;
    }

    /**
     * @param triggeredByUserRequest Whether the execution was triggered by a user request.
     * @return This builder.
     */
    public Builder triggeredByUserRequest(boolean triggeredByUserRequest) {
      _triggeredByUserRequest = triggeredByUserRequest;
      return this;
    }

    /**
     * @param startTimeMs Time the execution started.
     * @return This builder.
     */
    public Builder startTimeMs(long startTimeMs) {
      _startTimeMs = startTimeMs;
      return this;
    }

    /**
     * @param lastUpdateTimeMs Time the state was last updated.
     * @return This builder.
     */
    public Builder lastUpdateTimeMs(long lastUpdateTimeMs) {
      _lastUpdateTimeMs = lastUpdateTimeMs;
      return this;
    }

    /**
     * @param resumeCount Number of times the execution has been resumed.
     * @return This builder.
     */
    public Builder resumeCount(int resumeCount) {
      _resumeCount = resumeCount;
      return this;
    }

    /**
     * @param executorState The latest known executor state of the execution.
     * @return This builder.
     */
    public Builder executorState(ExecutorState.State executorState) {
      _executorState = executorState;
      return this;
    }

    /**
     * @param removedBrokers Removed brokers, or {@code null} if none.
     * @return This builder.
     */
    public Builder removedBrokers(Collection<Integer> removedBrokers) {
      _removedBrokers = removedBrokers;
      return this;
    }

    /**
     * @param demotedBrokers Demoted brokers, or {@code null} if none.
     * @return This builder.
     */
    public Builder demotedBrokers(Collection<Integer> demotedBrokers) {
      _demotedBrokers = demotedBrokers;
      return this;
    }

    /**
     * @param brokersToSkipConcurrencyCheck Brokers to skip the concurrency check for, or {@code null} if none.
     * @return This builder.
     */
    public Builder brokersToSkipConcurrencyCheck(Collection<Integer> brokersToSkipConcurrencyCheck) {
      _brokersToSkipConcurrencyCheck = brokersToSkipConcurrencyCheck;
      return this;
    }

    /**
     * @param concurrency Requested inter-broker partition movement concurrency per broker, or {@code null} for the default.
     * @return This builder.
     */
    public Builder interBrokerPartitionMovementConcurrency(Integer concurrency) {
      _interBrokerPartitionMovementConcurrency = concurrency;
      return this;
    }

    /**
     * @param maxMovements Requested max inter-broker partition movements in the cluster, or {@code null} for the default.
     * @return This builder.
     */
    public Builder maxInterBrokerPartitionMovements(Integer maxMovements) {
      _maxInterBrokerPartitionMovements = maxMovements;
      return this;
    }

    /**
     * @param concurrency Requested intra-broker partition movement concurrency, or {@code null} for the default.
     * @return This builder.
     */
    public Builder intraBrokerPartitionMovementConcurrency(Integer concurrency) {
      _intraBrokerPartitionMovementConcurrency = concurrency;
      return this;
    }

    /**
     * @param concurrency Requested cluster leadership movement concurrency, or {@code null} for the default.
     * @return This builder.
     */
    public Builder clusterLeadershipMovementConcurrency(Integer concurrency) {
      _clusterLeadershipMovementConcurrency = concurrency;
      return this;
    }

    /**
     * @param concurrency Requested per broker leadership movement concurrency, or {@code null} for the default.
     * @return This builder.
     */
    public Builder brokerLeadershipMovementConcurrency(Integer concurrency) {
      _brokerLeadershipMovementConcurrency = concurrency;
      return this;
    }

    /**
     * @param intervalMs Requested execution progress check interval, or {@code null} for the default.
     * @return This builder.
     */
    public Builder executionProgressCheckIntervalMs(Long intervalMs) {
      _executionProgressCheckIntervalMs = intervalMs;
      return this;
    }

    /**
     * @param replicaMovementStrategy Name of the replica movement strategy, or {@code null} for the default.
     * @return This builder.
     */
    public Builder replicaMovementStrategy(String replicaMovementStrategy) {
      _replicaMovementStrategy = replicaMovementStrategy;
      return this;
    }

    /**
     * @param replicationThrottle Replication throttle in bytes/second, or {@code null} if not throttled.
     * @return This builder.
     */
    public Builder replicationThrottle(Long replicationThrottle) {
      _replicationThrottle = replicationThrottle;
      return this;
    }

    /**
     * @param kafkaAssignerMode Whether the execution is in Kafka assigner mode.
     * @return This builder.
     */
    public Builder kafkaAssignerMode(boolean kafkaAssignerMode) {
      _kafkaAssignerMode = kafkaAssignerMode;
      return this;
    }

    /**
     * @param skip Whether to skip auto adjusting the inter-broker replica movement concurrency.
     * @return This builder.
     */
    public Builder skipInterBrokerReplicaConcurrencyAdjustment(boolean skip) {
      _skipInterBrokerReplicaConcurrencyAdjustment = skip;
      return this;
    }

    /**
     * @param cleanupOnly {@code true} if the execution must not be resumed anymore, only cleaned up.
     * @return This builder.
     */
    public Builder cleanupOnly(boolean cleanupOnly) {
      _cleanupOnly = cleanupOnly;
      return this;
    }

    /**
     * @param fingerprint The fingerprint of the replica placement of the partitions that the execution does not move.
     * @return This builder.
     */
    public Builder outsideExecutionFingerprint(ReplicaPlacementFingerprint fingerprint) {
      _outsideExecutionFingerprint = fingerprint;
      return this;
    }

    /**
     * @param logDirFingerprintByBroker The fingerprint of the logdirs of the replicas that the execution does not move, of each
     *                                  broker with intra-broker replica movements.
     * @return This builder.
     */
    public Builder logDirFingerprintByBroker(Map<Integer, Long> logDirFingerprintByBroker) {
      _logDirFingerprintByBroker = logDirFingerprintByBroker;
      return this;
    }

    /**
     * Replace the proposals with the given proposals.
     *
     * @param proposals Proposals of the execution.
     * @return This builder.
     */
    public Builder proposals(Collection<ExecutionProposal> proposals) {
      _proposals.clear();
      proposals.forEach(p -> _proposals.put(p.topicPartition(), p));
      return this;
    }

    /**
     * Replace the tasks with the given tasks.
     *
     * @param tasks Tasks of the execution.
     * @return This builder.
     */
    public Builder tasks(Collection<PersistedTask> tasks) {
      _tasks.clear();
      _tasks.addAll(tasks);
      return this;
    }

    public PersistedExecutionState build() {
      return new PersistedExecutionState(this);
    }
  }
}
