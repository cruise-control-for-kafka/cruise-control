/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.model.Disk.State.DEAD;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;


/**
 * A util class for Monitor.
 */
public final class MonitorUtils {
    // A utility variable for conversion of unit interval to percentage -- i.e. [0, 1.0] -> [0, 100.0].
    public static final double UNIT_INTERVAL_TO_PERCENTAGE = 100.0;
    private static final Logger LOG = LoggerFactory.getLogger(MonitorUtils.class);
    public static final Map<Resource, Double> EMPTY_BROKER_CAPACITY;
    public static final long BROKER_CAPACITY_FETCH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

    static {
        Map<Resource, Double> emptyBrokerCapacity = new HashMap<>();
        Resource.cachedValues().forEach(r -> emptyBrokerCapacity.put(r, 0.0));
        EMPTY_BROKER_CAPACITY = Collections.unmodifiableMap(emptyBrokerCapacity);
    }

    private MonitorUtils() {

    }

    /**
     * Derive follower load from the leader load.
     * <p>
     * If linear regression model is used, the way we derive the follower metrics is the following:
     * <ul>
     * <li>FOLLOWER_CPU = LEADER_NETWORK_IN * coefficient + residue </li>
     * <li>FOLLOWER_DISK = LEADER_DISK </li>
     * <li>FOLLOWER_BYTES_IN = LEADER_BYTES_IN </li>
     * <li>FOLLOWER_BYTES_OUT = 0 </li>
     * </ul>
     * <p>
     * If linear regression model is not used, CPU utilization of the follower will be fixed to be 0.2;
     *
     * @param aggregatedMetricValues the leader aggregated metric values to convert.
     * @return The aggregated metric values for the corresponding follower.
     */
    private static AggregatedMetricValues toFollowerMetricValues(AggregatedMetricValues aggregatedMetricValues) {
        AggregatedMetricValues followerLoad = new AggregatedMetricValues();
        for (short metricId : aggregatedMetricValues.metricIds()) {
            String metricGroup = KafkaMetricDef.commonMetricDef().metricInfo(metricId).group();
            if (!Resource.CPU.name().equals(metricGroup) && !Resource.NW_OUT.name().equals(metricGroup)) {
                followerLoad.add(metricId, aggregatedMetricValues.valuesFor(metricId));
            }
        }
        MetricValues followerCpu = new MetricValues(aggregatedMetricValues.length());
        MetricValues leaderBytesInRate =
                aggregatedMetricValues.valuesForGroup(Resource.NW_IN.name(), KafkaMetricDef.commonMetricDef(), false);
        MetricValues leaderBytesOutRate =
                aggregatedMetricValues.valuesForGroup(Resource.NW_OUT.name(), KafkaMetricDef.commonMetricDef(), false);
        MetricValues leaderCpuUtilization = aggregatedMetricValues.valuesFor(KafkaMetricDef.commonMetricDefId(CPU_USAGE));
        for (int i = 0; i < aggregatedMetricValues.length(); i++) {
            double followerCpuUtil = ModelUtils.getFollowerCpuUtilFromLeaderLoad(leaderBytesInRate.get(i),
                    leaderBytesOutRate.get(i),
                    leaderCpuUtilization.get(i));
            followerCpu.set(i, followerCpuUtil);
        }
        for (short nwOutMetricId : KafkaMetricDef.resourceToMetricIds(Resource.NW_OUT)) {
            followerLoad.add(nwOutMetricId, new MetricValues(aggregatedMetricValues.length()));
        }
        followerLoad.add(KafkaMetricDef.commonMetricDefId(CPU_USAGE), followerCpu);
        return followerLoad;
    }

    /**
     * @param previous Previous cluster state.
     * @param current  Current cluster state.
     * @return {@code true} if the metadata has changed, {@code false} otherwise.
     */
    public static boolean metadataChanged(Cluster previous, Cluster current) {
        // Broker has changed.
        Set<Node> prevNodeSet = new HashSet<>(previous.nodes());
        if (prevNodeSet.size() != current.nodes().size()) {
            return true;
        }
        current.nodes().forEach(prevNodeSet::remove);
        if (!prevNodeSet.isEmpty()) {
            return true;
        }
        // Topic has changed
        if (!previous.topics().equals(current.topics())) {
            return true;
        }

        // partition has changed.
        for (String topic : previous.topics()) {
            if (!previous.partitionCountForTopic(topic).equals(current.partitionCountForTopic(topic))) {
                return true;
            }
            for (PartitionInfo prevPartInfo : previous.partitionsForTopic(topic)) {
                PartitionInfo currPartInfo = current.partition(new TopicPartition(prevPartInfo.topic(), prevPartInfo.partition()));
                if (leaderChanged(prevPartInfo, currPartInfo) || replicaListChanged(prevPartInfo, currPartInfo)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean leaderChanged(PartitionInfo prevPartInfo, PartitionInfo currPartInfo) {
        Node prevLeader = prevPartInfo.leader();
        Node currLeader = currPartInfo.leader();
        return !(prevLeader == null && currLeader == null) && !(prevLeader != null && currLeader != null
                && prevLeader.id() == currLeader.id());
    }

    private static boolean replicaListChanged(PartitionInfo prevPartInfo, PartitionInfo currPartInfo) {
        if (prevPartInfo.replicas().length != currPartInfo.replicas().length) {
            return true;
        }
        for (int i = 0; i < prevPartInfo.replicas().length; i++) {
            if (prevPartInfo.replicas()[i].id() != currPartInfo.replicas()[i].id()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Combine load requirement options.
     *
     * @param goals Goals for which the load requirement options will be combined.
     * @return Combined load requirement options.
     */
    public static ModelCompletenessRequirements combineLoadRequirementOptions(Collection<Goal> goals) {
        ModelCompletenessRequirements requirements = null;
        for (Goal goal : goals) {
            requirements = goal.clusterModelCompletenessRequirements().stronger(requirements);
        }
        return requirements;
    }

    /**
     * Get the total number of partitions in the cluster.
     *
     * @param cluster Kafka cluster.
     * @return The total number of partitions in the cluster.
     */
    public static int totalNumPartitions(Cluster cluster) {
        int totalNumPartitions = 0;
        for (String topic : cluster.topics()) {
            totalNumPartitions += cluster.partitionCountForTopic(topic);
        }
        return totalNumPartitions;
    }

    /**
     * Convert replica's cpu usage metric from absolute value to percentage value since the cpu capacity reported by
     * {@link BrokerCapacityConfigResolver} is percentage value.
     *
     * @param aggregatedMetricValues the {@link AggregatedMetricValues} for the replica.
     */
    private static void adjustCpuUsage(AggregatedMetricValues aggregatedMetricValues) {
        short cpuUsageId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.CPU_USAGE);
        MetricValues cpuUsage = aggregatedMetricValues.valuesFor(cpuUsageId);
        for (int i = 0; i < cpuUsage.length(); i++) {
            cpuUsage.set(i, cpuUsage.get(i) * UNIT_INTERVAL_TO_PERCENTAGE);
        }
    }

    /**
     * Get the {@link AggregatedMetricValues} based on the replica role (leader/follower) and the replication factor.
     *
     * @param valuesAndExtrapolations the values and extrapolations of the leader replica.
     * @param partitionInfo           the partition info.
     * @param isLeader                whether the value is created for leader replica or follower replica.
     * @param needToAdjustCpuUsage    whether need to cast cpu usage metric for replica from absolute value to percentage.
     * @return The {@link AggregatedMetricValues} to use for the given replica.
     */
    private static AggregatedMetricValues getAggregatedMetricValues(ValuesAndExtrapolations valuesAndExtrapolations,
                                                                    PartitionInfo partitionInfo,
                                                                    boolean isLeader,
                                                                    boolean needToAdjustCpuUsage) {
        AggregatedMetricValues aggregatedMetricValues = valuesAndExtrapolations.metricValues();
        if (needToAdjustCpuUsage) {
            adjustCpuUsage(aggregatedMetricValues);
        }

        return isLeader ? fillInReplicationBytesOut(aggregatedMetricValues, partitionInfo)
                : toFollowerMetricValues(aggregatedMetricValues);
    }

    /**
     * When the replica is a leader replica, we need to fill in the replication bytes out if it has not been filled in
     * yet. This is because currently Kafka does not report this metric. We simply use the leader bytes in rate multiplied
     * by the number of followers as the replication bytes out rate. The assumption is that all the followers will
     * eventually keep up with the leader.
     * <p>
     * We only fill in the replication bytes out rate when creating the cluster model because the replication factor
     * may have changed since the time the PartitionMetricSample was created.
     *
     * @param aggregatedMetricValues the {@link AggregatedMetricValues} for the leader replica.
     * @param info                   the partition info for the partition.
     * @return The {@link AggregatedMetricValues} with the replication bytes out rate filled in.
     */
    private static AggregatedMetricValues fillInReplicationBytesOut(AggregatedMetricValues aggregatedMetricValues,
                                                                    PartitionInfo info) {
        int numFollowers = info.replicas().length - 1;
        short leaderBytesInRateId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.LEADER_BYTES_IN);
        short replicationBytesOutRateId = KafkaMetricDef.commonMetricDefId(KafkaMetricDef.REPLICATION_BYTES_OUT_RATE);

        MetricValues leaderBytesInRate = aggregatedMetricValues.valuesFor(leaderBytesInRateId);
        MetricValues replicationBytesOutRate = aggregatedMetricValues.valuesFor(replicationBytesOutRateId);
        // If the replication bytes out rate is already reported, update it. Otherwise add a new MetricValues.
        if (replicationBytesOutRate == null) {
            replicationBytesOutRate = new MetricValues(leaderBytesInRate.length());
            aggregatedMetricValues.add(replicationBytesOutRateId, replicationBytesOutRate);
        }
        for (int i = 0; i < leaderBytesInRate.length(); i++) {
            replicationBytesOutRate.set(i, leaderBytesInRate.get(i) * numFollowers);
        }

        return aggregatedMetricValues;
    }

    /**
     * @param node The node whose rack is requested.
     * @return Rack of the given node if the corresponding value is not null and not empty, the host of the node otherwise.
     */
    public static String getRackHandleNull(Node node) {
        return node.rack() == null || node.rack().isEmpty() ? node.host() : node.rack();
    }

    /**
     * @param cluster Kafka cluster.
     * @return All the brokers in the cluster that host at least one replica.
     */
    static Set<Integer> brokersWithReplicas(Cluster cluster) {
        Set<Integer> allBrokers = new HashSet<>();
        for (String topic : cluster.topics()) {
            for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
                Arrays.stream(partition.replicas()).map(Node::id).forEach(allBrokers::add);
            }
        }
        return allBrokers;
    }

    /**
     * @param cluster Kafka cluster.
     * @return Number of replicas in the cluster.
     */
    static int numReplicas(Cluster cluster) {
        int numReplicas = 0;
        for (String topic : cluster.topics()) {
            for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
                numReplicas += (partition.replicas() == null ? 0 : partition.replicas().length);
            }
        }
        return numReplicas;
    }

    /**
     * @param cluster Kafka cluster.
     * @return {@code true} if the cluster has partitions with ISR greater than replicas, {@code false} otherwise.
     */
    static boolean hasPartitionsWithIsrGreaterThanReplicas(Cluster cluster) {
        for (String topic : cluster.topics()) {
            for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
                int numISR = partitionInfo.inSyncReplicas().length;
                if (numISR > partitionInfo.replicas().length) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param cluster Kafka cluster.
     * @return All the dead brokers in the cluster that host at least one replica.
     */
    static Set<Integer> deadBrokersWithReplicas(Cluster cluster) {
        Set<Integer> brokersWithReplicas = brokersWithReplicas(cluster);
        cluster.nodes().forEach(node -> brokersWithReplicas.remove(node.id()));
        return brokersWithReplicas;
    }

    /**
     * @param cluster Kafka cluster.
     * @return All the brokers that host at least one offline replica.
     */
    static Set<Integer> brokersWithOfflineReplicas(Cluster cluster) {
        Set<Integer> brokersWithOfflineReplicas = new HashSet<>();
        for (String topic : cluster.topics()) {
            for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
                if (partition.leader() != null) {
                    brokersWithOfflineReplicas.addAll(Arrays.stream(partition.offlineReplicas()).map(Node::id).collect(Collectors.toSet()));
                }
            }
        }
        return brokersWithOfflineReplicas;
    }

    /**
     * Set the state of bad brokers in clusterModel based on the given Kafka cluster state:
     * <ul>
     *   <li>Get the dead brokers and mark them as dead.</li>
     *   <li>Get the alive brokers with bad disks and mark them accordingly.</li>
     * </ul>
     *
     * @param clusterModel The cluster model to set the broker states.
     * @param cluster      Kafka cluster.
     */
    static void setBadBrokerState(ClusterModel clusterModel, Cluster cluster) {
        MonitorUtils.deadBrokersWithReplicas(cluster).forEach(brokerId -> clusterModel.setBrokerState(brokerId, Broker.State.DEAD));
        for (Integer brokerId : MonitorUtils.brokersWithOfflineReplicas(cluster)) {
            if (clusterModel.broker(brokerId).isAlive()) {
                clusterModel.setBrokerState(brokerId, Broker.State.BAD_DISKS);
            }
        }
    }

    /**
     * Get replica placement information, i.e. each replica resides on which logdir of the broker.
     *
     * @param clusterModel The cluster model to populate replica placement information.
     * @param cluster      Kafka cluster.
     * @param adminClient  Admin client to send request to kafka cluster
     * @param config       Kafka Cruise Control relate config object
     * @return A map from topic partition to replica placement information.
     *
     */
    static Map<TopicPartition, Map<Integer, String>> getReplicaPlacementInfo(ClusterModel clusterModel,
                                                                             Cluster cluster,
                                                                             AdminClient adminClient,
                                                                             KafkaCruiseControlConfig config) {
        Map<TopicPartition, Map<Integer, String>> replicaPlacementInfo = new HashMap<>();
        Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> logDirsByBrokerId =
                adminClient.describeLogDirs(cluster.nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toList())).descriptions();
        for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : logDirsByBrokerId.entrySet()) {
            Integer brokerId = entry.getKey();
            try {
                entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
                    if (info.error() == null) {
                        for (Map.Entry<TopicPartition, ReplicaInfo> e : info.replicaInfos().entrySet()) {
                            if (!e.getValue().isFuture()) {
                                replicaPlacementInfo.putIfAbsent(e.getKey(), new HashMap<>());
                                replicaPlacementInfo.get(e.getKey()).put(brokerId, logdir);
                            } else {
                                LOG.trace("Topic partition {}'s replica is moving to {} on broker {}.", e.getKey(), logdir, brokerId);
                            }
                        }
                    } else {
                        clusterModel.broker(brokerId).disk(logdir).setState(DEAD);
                    }
                });
            } catch (TimeoutException te) {
                throw new RuntimeException(String.format("Getting logdir information for broker %d encountered TimeoutException %s.",
                        entry.getKey(), te));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(String.format("Populating logdir information for broker %d encountered Exception %s.",
                        entry.getKey(), e));
            }
        }
        return replicaPlacementInfo;
    }

    /**
     * Create replicas of the partition with the given (1) identifier and (2) load information to populate the given cluster model.
     * If partition with the given identifier does not exist in the given cluster, do nothing.
     *
     * @param cluster                      Kafka cluster.
     * @param clusterModel                 The cluster model to populate load information.
     * @param tp                           Topic partition that identifies the partition to populate the load for.
     * @param valuesAndExtrapolations      The values and extrapolations of the leader replica.
     * @param replicaPlacementInfo         The distribution of replicas over broker logdirs if available, {@code null} otherwise.
     * @param brokerCapacityConfigResolver The resolver for retrieving broker capacities.
     * @param allowCapacityEstimation      whether allow capacity estimation in cluster model if the underlying live broker capacity is unavailable.
     */
    static void populatePartitionLoad(Cluster cluster,
                                      ClusterModel clusterModel,
                                      TopicPartition tp,
                                      ValuesAndExtrapolations valuesAndExtrapolations,
                                      Map<TopicPartition, Map<Integer, String>> replicaPlacementInfo,
                                      BrokerCapacityConfigResolver brokerCapacityConfigResolver,
                                      boolean allowCapacityEstimation)
            throws TimeoutException {
        PartitionInfo partitionInfo = cluster.partition(tp);
        // If partition info does not exist, the topic may have been deleted.
        if (partitionInfo != null) {
            Set<Integer> aliveBrokers = cluster.nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
            boolean needToAdjustCpuUsage = true;
            Set<Integer> deadBrokersWithUnknownCapacity = new HashSet<>();
            for (int index = 0; index < partitionInfo.replicas().length; index++) {
                Node replica = partitionInfo.replicas()[index];
                String rack = getRackHandleNull(replica);
                BrokerCapacityInfo brokerCapacity;
                try {
                    // Do not allow capacity estimation for dead brokers.
                    brokerCapacity = brokerCapacityConfigResolver.capacityForBroker(rack, replica.host(), replica.id(), BROKER_CAPACITY_FETCH_TIMEOUT_MS,
                            aliveBrokers.contains(replica.id()) && allowCapacityEstimation);
                } catch (TimeoutException | BrokerCapacityResolutionException e) {
                    // Capacity resolver may not be able to return the capacity information of dead brokers.
                    if (!aliveBrokers.contains(replica.id())) {
                        brokerCapacity = new BrokerCapacityInfo(EMPTY_BROKER_CAPACITY);
                        deadBrokersWithUnknownCapacity.add(replica.id());
                    } else {
                        String errorMessage = String.format("Unable to retrieve capacity for broker %d. This may be caused by churn in "
                                + "the cluster, please retry.", replica.id());
                        LOG.warn(errorMessage, e);
                        throw new TimeoutException(errorMessage);
                    }
                }
                clusterModel.handleDeadBroker(rack, replica.id(), brokerCapacity);
                boolean isLeader;
                if (partitionInfo.leader() == null) {
                    LOG.warn("Detected offline partition {}-{}, skipping", partitionInfo.topic(), partitionInfo.partition());
                    continue;
                } else {
                    isLeader = replica.id() == partitionInfo.leader().id();
                }
                boolean isOffline = Arrays.stream(partitionInfo.offlineReplicas())
                        .anyMatch(offlineReplica -> offlineReplica.id() == replica.id());

                String logdir = replicaPlacementInfo == null ? null : replicaPlacementInfo.get(tp).get(replica.id());
                // If the replica's logdir is null, it is either because replica placement information is not populated for the cluster
                // model or this replica is hosted on a dead disk and is not considered for intra-broker replica operations.
                clusterModel.createReplica(rack, replica.id(), tp, index, isLeader, isOffline, logdir, false);
                clusterModel.setReplicaLoad(rack,
                        replica.id(),
                        tp,
                        getAggregatedMetricValues(valuesAndExtrapolations,
                                cluster.partition(tp),
                                isLeader,
                                needToAdjustCpuUsage),
                        valuesAndExtrapolations.windows());
                needToAdjustCpuUsage = false;
            }
            if (!deadBrokersWithUnknownCapacity.isEmpty()) {
                LOG.info("Assign empty capacity to brokers {} because they are dead and capacity resolver is unable to fetch their capacity.",
                        deadBrokersWithUnknownCapacity);
            }
        }
    }
}
