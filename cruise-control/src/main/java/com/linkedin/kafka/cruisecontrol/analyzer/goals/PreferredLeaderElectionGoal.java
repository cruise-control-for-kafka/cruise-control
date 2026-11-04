/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.exception.PartitionNotExistsException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.isPartitionUnderReplicated;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * Soft goal to move the leaders to the first replica of each partition.
 */
public class PreferredLeaderElectionGoal implements Goal {
  private static final Logger LOG = LoggerFactory.getLogger(PreferredLeaderElectionGoal.class);
  private final ProvisionResponse _provisionResponse;
  private final boolean _skipUrpDemotion;
  private final boolean _excludeFollowerDemotion;
  private final boolean _honorExcludedTopics;
  private Cluster _kafkaCluster;

  public PreferredLeaderElectionGoal() {
    this(false, false, null);
  }

  public PreferredLeaderElectionGoal(boolean skipUrpDemotion,
                                     boolean excludeFollowerDemotion,
                                     Cluster kafkaCluster) {
    this(skipUrpDemotion, excludeFollowerDemotion, kafkaCluster, false);
  }

  /**
   * @param skipUrpDemotion {@code true} to skip the demotion of partitions that are currently under replicated.
   * @param excludeFollowerDemotion {@code true} to skip moving follower replicas on demoted brokers or disks to the end of
   *                                the replica list.
   * @param kafkaCluster Kafka cluster used to check whether a partition is under replicated (required if skipUrpDemotion is true),
   *                     and to move the leadership of a demoted partition only to an in-sync replica. If {@code null}, the
   *                     in-sync replicas are unknown and every replica is considered in sync.
   * @param honorExcludedTopics {@code true} to leave the replica order and leadership of partitions of
   *                            {@link OptimizationOptions#excludedTopics()} unchanged, {@code false} to ignore excluded topics.
   */
  public PreferredLeaderElectionGoal(boolean skipUrpDemotion,
                                     boolean excludeFollowerDemotion,
                                     Cluster kafkaCluster,
                                     boolean honorExcludedTopics) {
    if (skipUrpDemotion && kafkaCluster == null) {
      throw new IllegalArgumentException("Cluster information is not provided.");
    }
    _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
    _skipUrpDemotion = skipUrpDemotion;
    _excludeFollowerDemotion = excludeFollowerDemotion;
    _honorExcludedTopics = honorExcludedTopics;
    _kafkaCluster = kafkaCluster;
  }

  private void sanityCheckOptimizationOptions(OptimizationOptions optimizationOptions) {
    if (optimizationOptions.isTriggeredByGoalViolation()) {
      throw new IllegalArgumentException(String.format("%s goal does not support use by goal violation detector.", name()));
    }
  }

  private boolean shouldSkipOperationOnURP(TopicPartition tp, String operation) {
    // Return true if the partition is under replicated and the flag to skip URP demotion is set.
    // Return false otherwise.
    // If the partition doesn't exist, return true.
    // Operation string is solely for logging purpose in case of partition not found.
    try {
      return _skipUrpDemotion && isPartitionUnderReplicated(_kafkaCluster, tp);
    } catch (PartitionNotExistsException ex) {
      LOG.warn("Skip {} operation for partition {} due to exception: {}", operation, tp, ex);
      return true;
    }
  }

  /**
   * @param replica A replica.
   * @return {@code true} if the in-sync replicas are unknown (i.e. no Kafka cluster is provided) or the given replica is in
   * the ISR of its partition, {@code false} otherwise -- including when the partition does not exist in the Kafka cluster.
   */
  private boolean isInSync(Replica replica) {
    if (_kafkaCluster == null) {
      return true;
    }
    PartitionInfo partitionInfo = _kafkaCluster.partition(replica.topicPartition());
    int brokerId = replica.broker().id();
    return partitionInfo != null && Arrays.stream(partitionInfo.inSyncReplicas()).anyMatch(node -> node.id() == brokerId);
  }

  private static boolean isDemoted(Replica replica) {
    return replica.broker().isDemoted() || (replica.disk() != null && replica.disk().state() == Disk.State.DEMOTED);
  }

  /**
   * A demotion moves the leadership of a partition only to a replica that Kafka can elect -- i.e. a replica that is in sync,
   * online, not demoted, and on an alive broker that is not excluded for leadership.
   *
   * @param leader The leader replica of a partition to demote.
   * @param clusterModel The state of the cluster.
   * @param excludedBrokersForLeadership Brokers excluded for leadership transfer.
   * @return {@code true} if another replica of the partition can take over its leadership, {@code false} otherwise.
   */
  private boolean hasLeaderCandidate(Replica leader, ClusterModel clusterModel, Set<Integer> excludedBrokersForLeadership) {
    return clusterModel.partition(leader.topicPartition()).replicas().stream()
                       .anyMatch(r -> r != leader && r.broker().isAlive() && !r.isCurrentOffline() && !isDemoted(r)
                                      && !excludedBrokersForLeadership.contains(r.broker().id()) && isInSync(r));
  }

  private void maybeMoveReplicaToEndOfReplicaList(Replica replica,
                                                  ClusterModel clusterModel,
                                                  Set<String> excludedTopics,
                                                  Set<Integer> excludedBrokersForLeadership) {
    // There are five scenarios where replica swap operation is skipped:
    // 1.the replica is not leader replica and _excludeFollowerDemotion is true.
    // 2.the replica's partition is currently under replicated and _skipUrpDemotion is true.
    // 3.the replica doesn't exist.
    // 4.the replica's topic is excluded and _honorExcludedTopics is true.
    // 5.the replica is the leader replica and no other replica can take over the leadership (see hasLeaderCandidate).
    if (excludedTopics.contains(replica.topicPartition().topic())) {
      return;
    }
    boolean skipReplicaMove = shouldSkipOperationOnURP(replica.topicPartition(), "replica move");
    if (!skipReplicaMove
        && !(_excludeFollowerDemotion && !replica.isLeader())
        && !(replica.isLeader() && !hasLeaderCandidate(replica, clusterModel, excludedBrokersForLeadership))) {
      Partition p = clusterModel.partition(replica.topicPartition());
      p.moveReplicaToEnd(replica);
    }
  }

  private void maybeChangeLeadershipForPartition(Set<Replica> leaderReplicas,
                                                 Set<TopicPartition> partitionsToMove,
                                                 Set<String> excludedTopics,
                                                 ClusterModel clusterModel,
                                                 Set<Integer> excludedBrokersForLeadership) {
    // If the leader replica's topic is excluded, or its partition is currently under replicated and _skipUrpDemotion is
    // true, skip leadership change operation. If the partition is not found skip the operation as well. If no other
    // replica can take over the leadership (e.g. none of them is in sync), the leadership stays where it is.
    leaderReplicas.stream()
                  .filter(r -> !excludedTopics.contains(r.topicPartition().topic())
                               && !shouldSkipOperationOnURP(r.topicPartition(), "leadership change"))
                  .filter(r -> {
                    if (hasLeaderCandidate(r, clusterModel, excludedBrokersForLeadership)) {
                      return true;
                    }
                    LOG.warn("Skipped demotion of partition {} on broker {}, because none of its other replicas can take over "
                             + "the leadership (in sync, online, not demoted, and on an alive broker not excluded for "
                             + "leadership {}).", r.topicPartition(), r.broker().id(), excludedBrokersForLeadership);
                    return false;
                  })
                  .forEach(r -> partitionsToMove.add(r.topicPartition()));
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
    sanityCheckOptimizationOptions(optimizationOptions);
    // This goal does not move partitions, hence it ignores the excluded topics unless it is asked to honor them (e.g. when
    // a demote_broker request explicitly specifies the topics to exclude). Partitions of honored excluded topics retain
    // their replica order and leadership.
    Set<String> excludedTopics = _honorExcludedTopics ? optimizationOptions.excludedTopics() : Collections.emptySet();
    Set<Integer> excludedBrokersForLeadership = optimizationOptions.excludedBrokersForLeadership();
    // First move the replica on the demoted brokers to the end of the replica list.
    // If all the replicas are demoted, no change is made to the leader.
    boolean hasBrokerOrDiskToBeDemoted = false;
    Set<TopicPartition> partitionsToMove = new HashSet<>();
    for (Broker b : clusterModel.aliveBrokers()) {
      if (b.isDemoted()) {
        hasBrokerOrDiskToBeDemoted = true;
        for (Replica r : b.replicas()) {
          maybeMoveReplicaToEndOfReplicaList(r, clusterModel, excludedTopics, excludedBrokersForLeadership);
        }
        maybeChangeLeadershipForPartition(b.leaderReplicas(), partitionsToMove, excludedTopics, clusterModel,
                                          excludedBrokersForLeadership);
      } else {
        for (Disk d : b.disks()) {
          if (d.state() == Disk.State.DEMOTED) {
            hasBrokerOrDiskToBeDemoted = true;
            for (Replica r : d.replicas()) {
              maybeMoveReplicaToEndOfReplicaList(r, clusterModel, excludedTopics, excludedBrokersForLeadership);
            }
            maybeChangeLeadershipForPartition(d.leaderReplicas(), partitionsToMove, excludedTopics, clusterModel,
                                              excludedBrokersForLeadership);
          }
        }
      }
    }
    // Check whether this goal has relocated any leadership.
    boolean relocatedLeadership = false;
    for (List<Partition> partitions : clusterModel.getPartitionsByTopic().values()) {
      for (Partition p : partitions) {
        if (excludedTopics.contains(p.topicPartition().topic())
            || (hasBrokerOrDiskToBeDemoted && !partitionsToMove.contains(p.topicPartition()))) {
          continue;
        }
        for (int i = 0; i < p.replicas().size(); i++) {
          // If there is no broker or disk to be demoted, only try to transfer the leadership to the first replica of the partition.
          if (!hasBrokerOrDiskToBeDemoted && i > 0) {
            break;
          }
          Replica r = p.replicas().get(i);
          // Iterate over the replicas and ensure that (1) the leader is set to the first alive replica, (2) the leadership
          // is not transferred to a broker excluded for leadership transfer, and (3) the leadership is transferred only to
          // an in-sync replica, and on a demotion only to a replica that is not demoted.
          Broker leaderCandidate = r.broker();
          if (leaderCandidate.isAlive()) {
            if (r.isCurrentOffline()) {
              LOG.warn("The preferred replica of partition {} on broker {} is offline.", p.topicPartition(), leaderCandidate);
              continue;
            }
            if (!r.isLeader()) {
              if (excludedBrokersForLeadership.contains(leaderCandidate.id())) {
                LOG.warn("Skipped leadership transfer of partition {} to broker {} because it is among brokers excluded"
                         + " for leadership {}.", p.topicPartition(), leaderCandidate, excludedBrokersForLeadership);
                continue;
              }
              if (!isInSync(r)) {
                LOG.warn("Skipped leadership transfer of partition {} to broker {} because the replica is not in sync.",
                         p.topicPartition(), leaderCandidate);
                continue;
              }
              if (hasBrokerOrDiskToBeDemoted && isDemoted(r)) {
                // E.g. a follower on another demoted broker, which stays in place if _excludeFollowerDemotion is true. The
                // partition is demoted only if a replica that is not demoted can take over (see hasLeaderCandidate).
                continue;
              }
              clusterModel.relocateLeadership(r.topicPartition(), p.leader().broker().id(), leaderCandidate.id());
              relocatedLeadership = true;
            }
            if (clusterModel.demotedBrokers().contains(leaderCandidate)) {
              LOG.warn("The leader of partition {} has to be on a demoted broker {} because all the alive "
                       + "replicas are demoted.", p.topicPartition(), leaderCandidate.id());
            }
            if (r.disk() != null && r.disk().state() == Disk.State.DEMOTED) {
              LOG.warn("The leader of partition {} has to be on a demoted disk {} of broker {} because all the alive "
                       + "replicas are demoted.", p.topicPartition(), r.disk().logDir(), leaderCandidate.id());
            }
            break;
          }
        }
      }
    }
    // This goal is optimized in one pass.
    finish();
    // Return true if at least one leadership has been relocated.
    return relocatedLeadership;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    return ACCEPT;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ClusterModelStatsComparator() {
      @Override
      public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
        return 0;
      }

      @Override
      public String explainLastComparison() {
        return String.format("Comparison for the %s is irrelevant.", name());
      }
    };
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0, true);
  }

  @Override
  public String name() {
    return PreferredLeaderElectionGoal.class.getSimpleName();
  }

  @Override
  public void finish() {
    _kafkaCluster = null;
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  @Override
  public ProvisionStatus provisionStatus() {
    // Provision status computation is not relevant to PLE goal.
    return provisionResponse().status();
  }

  @Override
  public ProvisionResponse provisionResponse() {
    return _provisionResponse;
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
