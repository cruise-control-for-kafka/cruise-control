/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.PartitionLoadState;

import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG;


/**
 * The async runnable to get partition load in the cluster.
 */
public class PartitionLoadRunnable extends OperationRunnable {
    protected final PartitionLoadParameters _parameters;

    public PartitionLoadRunnable(KafkaCruiseControl kafkaCruiseControl,
                                 OperationFuture future,
                                 PartitionLoadParameters parameters) {
        super(kafkaCruiseControl, future);
        _parameters = parameters;
    }

    @Override
    protected PartitionLoadState getResult() throws Exception {
        _kafkaCruiseControl.sanityCheckBrokerPresence(_parameters.brokerIds());

        LoadRunnable loadRunnable = new LoadRunnable(_kafkaCruiseControl, _future, _parameters);
        Double minValidPartitionRatio = _parameters.minValidPartitionRatio();
        if (minValidPartitionRatio == null) {
            minValidPartitionRatio = _kafkaCruiseControl.config().getDouble(MIN_VALID_PARTITION_RATIO_CONFIG);
        }
        ClusterModel clusterModel = loadRunnable.clusterModel(minValidPartitionRatio);
        int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
        List<Partition> partitionList = clusterModel.replicasSortedByUtilization(_parameters.resource(),
                _parameters.wantMaxLoad(),
                _parameters.wantAvgLoad());
        if (!_parameters.brokerIds().isEmpty()) {
            partitionList = partitionList.stream()
                    .filter(partition -> partition.partitionBrokers().stream().anyMatch(
                            broker -> _parameters.brokerIds().contains(broker.id())))
                    .collect(Collectors.toList());
        }
        return new PartitionLoadState(partitionList,
                _parameters.wantMaxLoad(),
                _parameters.wantAvgLoad(),
                _parameters.entries(),
                _parameters.partitionUpperBoundary(),
                _parameters.partitionLowerBoundary(),
                _parameters.topic(),
                topicNameLength,
                _kafkaCruiseControl.config());
    }
}
