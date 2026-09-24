/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ClusterLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;

import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.POPULATE_DISK_INFO_PARAM;


/**
 * The async runnable to get the {@link BrokerStats} for the cluster model.
 */
public class LoadRunnable extends OperationRunnable {
    protected final long _start;
    protected final long _end;
    protected final ModelCompletenessRequirements _modelCompletenessRequirements;
    protected final boolean _allowCapacityEstimation;
    protected final boolean _populateDiskInfo;
    protected final boolean _capacityOnly;

    /**
     * Constructor to be used for creating a runnable for partition load.
     */
    public LoadRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture future, PartitionLoadParameters parameters) {
        super(kafkaCruiseControl, future);
        _start = parameters.startMs();
        _end = parameters.endMs();
        Double minValidPartitionRatio = parameters.minValidPartitionRatio();
        if (minValidPartitionRatio == null) {
            minValidPartitionRatio = kafkaCruiseControl.config().getDouble(MIN_VALID_PARTITION_RATIO_CONFIG);
        }
        _modelCompletenessRequirements = new ModelCompletenessRequirements(1, minValidPartitionRatio, true);
        _allowCapacityEstimation = parameters.allowCapacityEstimation();
        _populateDiskInfo = false;
        _capacityOnly = false;
    }

    public LoadRunnable(KafkaCruiseControl kafkaCruiseControl, OperationFuture future, ClusterLoadParameters parameters) {
        super(kafkaCruiseControl, future);
        _start = parameters.startMs();
        _end = parameters.endMs();
        _modelCompletenessRequirements = parameters.requirements();
        _allowCapacityEstimation = parameters.allowCapacityEstimation();
        _populateDiskInfo = parameters.populateDiskInfo();
        _capacityOnly = parameters.capacityOnly();
    }

    @Override
    protected BrokerStats getResult() throws Exception {
        if (!_populateDiskInfo) {
            // Check whether the cached broker stats is still valid.
            BrokerStats cachedBrokerStats = _kafkaCruiseControl.cachedBrokerLoadStats(_allowCapacityEstimation);
            if (cachedBrokerStats != null) {
                return cachedBrokerStats;
            }
        } else if (isClusterUsingJBOD()) {
            throw new UserRequestException(String.format("Cannot set %s=true for non-JBOD Kafka clusters.", POPULATE_DISK_INFO_PARAM));
        }

        if (_start != DEFAULT_START_TIME_FOR_CLUSTER_MODEL) {
            return clusterModel(_modelCompletenessRequirements.minMonitoredPartitionsPercentage()).brokerStats(_kafkaCruiseControl.config());
        } else {
            return clusterModelFromEarliest().brokerStats(_kafkaCruiseControl.config());
        }
    }

    private boolean isClusterUsingJBOD() throws Exception {
        ClusterModel clusterModel = _kafkaCruiseControl.loadMonitor().clusterCapacity();
        // If and only if all brokers in the cluster are using JBOD, the cluster is considered to be using JBOD
        return clusterModel.brokers().stream().allMatch(Broker::isUsingJBOD);
    }

    /**
     * Get the cluster model starting from earliest available timestamp to {@link #_end} timestamp.
     *
     * @return The cluster model.
     * @throws KafkaCruiseControlException When the cluster model generation encounter errors.
     */
    public ClusterModel clusterModelFromEarliest() throws KafkaCruiseControlException {
        return clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL, _modelCompletenessRequirements);
    }

    /**
     * Get the cluster model starting from {@link #_start} timestamp to {@link #_end} timestamp.
     *
     * @param minValidPartitionRatio Minimum valid partition ratio required as part of the model completeness.
     * @return The cluster model.
     * @throws KafkaCruiseControlException When the cluster model generation encounter errors.
     */
    public ClusterModel clusterModel(double minValidPartitionRatio) throws KafkaCruiseControlException {
        return clusterModel(_start, new ModelCompletenessRequirements(1, minValidPartitionRatio, false));
    }

    protected ClusterModel clusterModel(long start, ModelCompletenessRequirements requirements) throws KafkaCruiseControlException {
        OperationProgress operationProgress = _future.operationProgress();
        try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
            return _capacityOnly ? _kafkaCruiseControl.clusterCapacity()
                    : _kafkaCruiseControl.clusterModel(start,
                    _end,
                    requirements,
                    _populateDiskInfo,
                    _allowCapacityEstimation,
                    operationProgress);
        } catch (KafkaCruiseControlException kcce) {
            throw kcce;
        } catch (Exception e) {
            throw new KafkaCruiseControlException(e);
        }
    }
}
