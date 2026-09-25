/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager.SUPPORTED_NUM_METRIC_FETCHER;


/**
 * The default implementation of metric sampler partition assignor.
 * <p>
 * The assignment tries to achieve the following goals:
 * 1. All the partitions of the same topic goes to one metric fetcher.
 * 2. The number of partitions assigned to each fetcher should be about the same.
 */
public class DefaultMetricSamplerPartitionAssignor implements MetricSamplerPartitionAssignor {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricSamplerPartitionAssignor.class);

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public List<Set<TopicPartition>> assignPartitions(Cluster cluster, int numMetricFetchers) {
        if (numMetricFetchers != SUPPORTED_NUM_METRIC_FETCHER) {
            throw new IllegalArgumentException("DefaultMetricSamplerPartitionAssignor supports only a single metric fetcher.");
        }
        // Create an array to host the assignment of all the metric fetchers.
        List<Set<TopicPartition>> assignments = new ArrayList<>();
        assignments.add(assignPartitions(cluster));
        return assignments;
    }

    @Override
    public Set<TopicPartition> assignPartitions(Cluster cluster) {
        // Create an array to host the assignment of the metric fetcher.
        Set<TopicPartition> assignment = new HashSet<>();
        for (String topic : cluster.topics()) {
            List<PartitionInfo> partitionsForTopic = cluster.partitionsForTopic(topic);
            for (PartitionInfo partitionInfo : partitionsForTopic) {
                assignment.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        LOG.trace("Partition assignment for metric fetcher: {}", assignment);
        return assignment;
    }
}
