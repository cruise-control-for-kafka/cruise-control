/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.model.Disk;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * Represents the load balancing operation over a replica for Kafka Load GoalOptimizer.
 * Note _destinationTp field is only relevant in replica swap operation, for other operations, it will be null.
 */
public class BalancingAction {
  private static final String TOPIC_PARTITION = "topicPartition";
  private static final String SOURCE_BROKER_ID = "sourceBrokerId";
  private static final String SOURCE_BROKER_LOGDIR = "sourceBrokerLogdir";
  private static final String DESTINATION_BROKER_ID = "destinationBrokerId";
  private static final String DESTINATION_BROKER_LOGDIR = "destinationBrokerLogdir";
  private static final String DESTINATION_TOPIC_PARTITION = "destinationTopicPartition";
  private static final String ACTION_TYPE = "actionType";
  private final TopicPartition _tp;
  private final Integer _sourceBrokerId;
  private final String _sourceBrokerLogdir;
  private final String _destinationBrokerLogdir;
  private final Integer _destinationBrokerId;
  private final ActionType _actionType;
  private final TopicPartition _destinationTp;

  /**
   * Constructor for creating a balancing proposal with given topic partition, source and destination broker id, and
   * balancing action type.
   *
   * @param tp                  Topic partition of the replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param actionType          Action type.
   */
  public BalancingAction(TopicPartition tp,
                         Integer sourceBrokerId,
                         Integer destinationBrokerId,
                         ActionType actionType) {
    this(tp, sourceBrokerId, destinationBrokerId, actionType, tp);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partitions, source and destination broker id, balancing
   * action type and the topic partition of replica to swap with.
   *
   * @param sourceTp            Topic partition of the source replica.
   * @param sourceBrokerId      Source broker id of the replica.
   * @param destinationBrokerId Destination broker id of the replica.
   * @param actionType          Action type.
   * @param destinationTp       Topic partition of the replica to swap with, null if there is no replica to swap with, i.e.
   *                            only move source replica to destination broker.
   */
  public BalancingAction(TopicPartition sourceTp,
                         Integer sourceBrokerId,
                         Integer destinationBrokerId,
                         ActionType actionType,
                         TopicPartition destinationTp) {
    this(sourceTp, sourceBrokerId, null, destinationBrokerId, null, actionType, destinationTp);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partition, source and destination disk (on the same broker),
   * and balancing action type.
   *
   * @param tp              Topic partition of the replica.
   * @param sourceDisk      Source disk of the replica.
   * @param destinationDisk Destination disk of the replica.
   * @param actionType      Action type.
   */
  public BalancingAction(TopicPartition tp,
                         Disk sourceDisk,
                         Disk destinationDisk,
                         ActionType actionType) {
    this(tp, sourceDisk.broker().id(), sourceDisk.logDir(), destinationDisk.broker().id(), destinationDisk.logDir(), actionType, tp);
  }

  /**
   * Constructor for creating a balancing proposal with given topic partitions, source and destination disk (on the same broker),
   * balancing action type and the topic partition of replica to swap with.
   *
   * @param sourceTp        Topic partition of the source replica.
   * @param sourceDisk      Source disk of the replica.
   * @param destinationDisk Destination disk of the replica.
   * @param actionType      Action type.
   * @param destinationTp   Topic partition of the replica to swap with, null if there is no replica to swap with, i.e.
   *                        only move source replica to destination disk.
   */
  public BalancingAction(TopicPartition sourceTp,
                         Disk sourceDisk,
                         Disk destinationDisk,
                         ActionType actionType,
                         TopicPartition destinationTp) {
    this(sourceTp,
         sourceDisk.broker().id(),
         sourceDisk.logDir(),
         destinationDisk.broker().id(),
         destinationDisk.logDir(),
         actionType,
         destinationTp);
  }

  private BalancingAction(TopicPartition sourceTp,
                         Integer sourceBrokerId,
                         String sourceBrokerLogdir,
                         Integer destinationBrokerId,
                         String destinationBrokerLogdir,
                         ActionType actionType,
                         TopicPartition destinationTp) {
    _tp = sourceTp;
    _sourceBrokerId = sourceBrokerId;
    _sourceBrokerLogdir = sourceBrokerLogdir;
    _destinationBrokerId = destinationBrokerId;
    _destinationBrokerLogdir = destinationBrokerLogdir;
    _actionType = actionType;
    _destinationTp = destinationTp;
    validate();
  }

  private void validate() {
    switch (_actionType) {
      case INTER_BROKER_REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
      case INTER_BROKER_REPLICA_SWAP:
        validateNotNull(_destinationBrokerId, () -> "The destination broker cannot be null for balancing action " + this);
        validateNotNull(_sourceBrokerId, () -> "The source broker cannot be null for balancing action " + this);
        break;
      case INTRA_BROKER_REPLICA_MOVEMENT:
      case INTRA_BROKER_REPLICA_SWAP:
        validateNotNull(_destinationBrokerId, () -> "The destination broker cannot be null for balancing action " + this);
        validateNotNull(_sourceBrokerId, () -> "The source broker cannot be null for balancing action " + this);
        validateNotNull(_sourceBrokerLogdir, () -> "The source disk cannot be null for balancing action " + this);
        validateNotNull(_destinationBrokerLogdir, () -> "The destination disk cannot be null for balancing action " + this);
        if (!_sourceBrokerId.equals(_destinationBrokerId)) {
          throw new IllegalArgumentException("Replica movement between disks across broker is not supported "
                                             + "for balancing action " + this);
        }
        break;
      default:
        throw new IllegalStateException("Should never be here");
    }
  }

  /**
   * @return The destination topic partition to swap with.
   */
  public TopicPartition destinationTopicPartition() {
    return _destinationTp;
  }

  /**
   * @return Topic name of the replica to swap with at the destination.
   */
  public String destinationTopic() {
    return _destinationTp.topic();
  }

  /**
   * @return The partition Id that is impacted by the balancing action.
   */
  public int partitionId() {
    return _tp.partition();
  }

  /**
   * @return Topic name of the impacted partition.
   */
  public String topic() {
    return _tp.topic();
  }

  /**
   * @return Topic partition.
   */
  public TopicPartition topicPartition() {
    return _tp;
  }

  /**
   * @return The source broker logdir.
   */
  public String sourceBrokerLogdir() {
    return _sourceBrokerLogdir;
  }

  /**
   * @return The destination broker logdir.
   */
  public String destinationBrokerLogdir() {
    return _destinationBrokerLogdir;
  }

  /**
   * @return The source broker id that is impacted by the balancing action.
   */
  public Integer sourceBrokerId() {
    return _sourceBrokerId;
  }

  /**
   * @return The destination broker Id.
   */
  public Integer destinationBrokerId() {
    return _destinationBrokerId;
  }

  /**
   * @return The type of action that provides balancing.
   */
  public ActionType balancingAction() {
    return _actionType;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> proposalMap = new HashMap<>();
    proposalMap.put(TOPIC_PARTITION, _tp);
    proposalMap.put(SOURCE_BROKER_ID, _sourceBrokerId);
    proposalMap.put(DESTINATION_BROKER_ID, _destinationBrokerId);
    proposalMap.put(DESTINATION_TOPIC_PARTITION, _destinationTp);
    proposalMap.put(ACTION_TYPE, _actionType);
    // _sourceBrokerLogdir and _destinationBrokerLogdir are guaranteed to both be null or neither be null.
    if (_sourceBrokerLogdir != null) {
      proposalMap.put(SOURCE_BROKER_LOGDIR, _sourceBrokerLogdir);
      proposalMap.put(DESTINATION_BROKER_LOGDIR, _destinationBrokerLogdir);
    }
    return proposalMap;
  }

  /**
   * Get string representation of this balancing proposal.
   */
  @Override
  public String toString() {
    String actSymbol = (_actionType.equals(ActionType.INTER_BROKER_REPLICA_SWAP)
                        || _actionType.equals(ActionType.INTRA_BROKER_REPLICA_SWAP)) ? "<->" : "->";
    return String.format("(%s%s%s, %d%s%s%d%s, %s)",
                         _tp, actSymbol, _destinationTp, _sourceBrokerId,
                         _sourceBrokerLogdir == null ? "" : String.format("(%s)", _sourceBrokerLogdir),
                         actSymbol, _destinationBrokerId,
                         _destinationBrokerLogdir == null ? "" : String.format("(%s)", _destinationBrokerLogdir),
                         _actionType);
  }

  /**
   * Compare the given object with this object.
   *
   * @param other Other object to be compared with this object.
   * @return {@code true} if other object equals this object, {@code false} otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BalancingAction otherAction)) {
      return false;
    }

    if (this == other) {
      return true;
    }

    return Objects.equals(_sourceBrokerId, otherAction._sourceBrokerId)
           && Objects.equals(_sourceBrokerLogdir, otherAction._sourceBrokerLogdir)
           && Objects.equals(_tp, otherAction._tp)
           && Objects.equals(_destinationBrokerId, otherAction._destinationBrokerId)
           && Objects.equals(_destinationBrokerLogdir, otherAction._destinationBrokerLogdir)
           && Objects.equals(_destinationTp, otherAction._destinationTp)
           && Objects.equals(_actionType, otherAction._actionType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_tp, _sourceBrokerId, _sourceBrokerLogdir, _destinationBrokerId, _destinationBrokerLogdir,
                        _actionType, _destinationTp);
  }
}
