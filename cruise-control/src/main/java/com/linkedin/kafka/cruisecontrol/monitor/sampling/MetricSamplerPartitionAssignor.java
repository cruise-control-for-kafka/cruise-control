/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;

/**
 * The interface to assign the partitions to the metric samplers.
 */
public interface MetricSamplerPartitionAssignor extends CruiseControlConfigurable {

    /**
     * @param cluster     The Kafka cluster.
     * @param numFetchers The number of metric fetchers.
     * @return A List of partition assignment for each of the fetchers.
     * @deprecated Please use {@link #assignPartitions(Cluster)}.
     * Assign the partitions in the cluster to the metric fetchers.
     */
    @Deprecated
    List<Set<TopicPartition>> assignPartitions(Cluster cluster, int numFetchers);

    /**
     * Assign the partitions in the cluster to the single metric fetcher.
     *
     * @param cluster The Kafka cluster
     * @return Set of topic partitions assigned to the fetcher.
     */
    Set<TopicPartition> assignPartitions(Cluster cluster);
}
