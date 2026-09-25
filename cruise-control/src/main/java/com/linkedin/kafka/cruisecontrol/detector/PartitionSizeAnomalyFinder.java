/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigDef;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cruisecontrol.common.config.ConfigDef.Type.CLASS;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfigUtils.getConfiguredInstance;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;


/**
 * The class will check whether there are topics having partition(s) with gigantic size.
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #SELF_HEALING_PARTITION_SIZE_THRESHOLD_MB_CONFIG}: The config for the partition size threshold to alert,
 *   default value is set to {@link #DEFAULT_SELF_HEALING_PARTITION_SIZE_THRESHOLD_MB} mb.
 *   <li>{@link #TOPIC_EXCLUDED_FROM_PARTITION_SIZE_CHECK}: The config to specify topics excluded from the anomaly checking.
 *   The value is treated as a regular expression, default value is set to {@link #DEFAULT_TOPIC_EXCLUDED_FROM_PARTITION_SIZE_CHECK}.
 *   <li>{@link #TOPIC_PARTITION_SIZE_ANOMALY_CLASS_CONFIG}: The config for the topic anomaly class name,
 *   default value is set to {@link #DEFAULT_TOPIC_PARTITION_SIZE_ANOMALY_CLASS}.
 * </ul>
 */
public class PartitionSizeAnomalyFinder implements TopicAnomalyFinder {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionSizeAnomalyFinder.class);
    public static final String SELF_HEALING_PARTITION_SIZE_THRESHOLD_MB_CONFIG = "self.healing.partition.size.threshold.mb";
    public static final Integer DEFAULT_SELF_HEALING_PARTITION_SIZE_THRESHOLD_MB = 1024 * 1024;
    public static final String TOPIC_EXCLUDED_FROM_PARTITION_SIZE_CHECK = "topic.excluded.from.partition.size.check";
    public static final String DEFAULT_TOPIC_EXCLUDED_FROM_PARTITION_SIZE_CHECK = "";
    public static final String TOPIC_PARTITION_SIZE_ANOMALY_CLASS_CONFIG = "topic.partition.size.anomaly.class";
    public static final Class<?> DEFAULT_TOPIC_PARTITION_SIZE_ANOMALY_CLASS = TopicPartitionSizeAnomaly.class;
    public static final String PARTITIONS_WITH_LARGE_SIZE_CONFIG = "partitions.with.large.size";
    private KafkaCruiseControl _kafkaCruiseControl;
    private int _partitionSizeThresholdInMb;
    private Pattern _topicExcludedFromCheck;
    private Class<?> _topicPartitionSizeAnomalyClass;
    private boolean _allowCapacityEstimation;

    @Override
    public Set<TopicAnomaly> topicAnomalies() {
        Map<TopicPartition, Double> partitionsWithLargeSize = new HashMap<>();
        OperationProgress operationProgress = new OperationProgress();
        ClusterModel clusterModel;
        try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(operationProgress)) {
            clusterModel = _kafkaCruiseControl.clusterModel(new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true),
                    _allowCapacityEstimation,
                    new OperationProgress());
            for (Map.Entry<String, List<Partition>> entry : clusterModel.getPartitionsByTopic().entrySet()) {
                if (_topicExcludedFromCheck.matcher(entry.getKey()).matches()) {
                    continue;
                }
                for (Partition partition : entry.getValue()) {
                    double partitionSize = partition.leader().load().expectedUtilizationFor(Resource.DISK);
                    if (partitionSize > _partitionSizeThresholdInMb) {
                        partitionsWithLargeSize.put(partition.topicPartition(), partitionSize);
                    }
                }
            }
        } catch (NotEnoughValidWindowsException neve) {
            LOG.debug("Skipping topic partition size anomaly detection because there are not enough valid windows.", neve);
        } catch (KafkaCruiseControlException cache) {
            LOG.warn("Partition size anomaly finder received exception", cache);
        } catch (Exception ex) {
            LOG.error("Unexpected exception", ex);
        }
        if (!partitionsWithLargeSize.isEmpty()) {
            return Collections.singleton(createTopicPartitionSizeAnomaly(partitionsWithLargeSize));
        }
        return Collections.emptySet();
    }

    private TopicAnomaly createTopicPartitionSizeAnomaly(Map<TopicPartition, Double> partitionsWithLargeSize) {
        Map<String, Object> configs = Map.of(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl,
                PARTITIONS_WITH_LARGE_SIZE_CONFIG, partitionsWithLargeSize,
                ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
        return getConfiguredInstance(_topicPartitionSizeAnomalyClass, TopicAnomaly.class, configs);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        _kafkaCruiseControl = (KafkaCruiseControl) configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        if (_kafkaCruiseControl == null) {
            throw new IllegalArgumentException("Partition size anomaly finder is missing " + KAFKA_CRUISE_CONTROL_OBJECT_CONFIG);
        }
        _allowCapacityEstimation = _kafkaCruiseControl.config().getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
        String topicExcludedFromCheck = (String) configs.get(TOPIC_EXCLUDED_FROM_PARTITION_SIZE_CHECK);
        _topicExcludedFromCheck = Pattern.compile(topicExcludedFromCheck == null ? DEFAULT_TOPIC_EXCLUDED_FROM_PARTITION_SIZE_CHECK
                : topicExcludedFromCheck);
        String partitionSizeThreshold = (String) configs.get(SELF_HEALING_PARTITION_SIZE_THRESHOLD_MB_CONFIG);
        _partitionSizeThresholdInMb = partitionSizeThreshold == null ? DEFAULT_SELF_HEALING_PARTITION_SIZE_THRESHOLD_MB
                : Integer.parseInt(partitionSizeThreshold);
        String topicPartitionSizeAnomalyClass = (String) configs.get(TOPIC_PARTITION_SIZE_ANOMALY_CLASS_CONFIG);
        if (topicPartitionSizeAnomalyClass == null) {
            _topicPartitionSizeAnomalyClass = DEFAULT_TOPIC_PARTITION_SIZE_ANOMALY_CLASS;
        } else {
            _topicPartitionSizeAnomalyClass = (Class<?>) ConfigDef.parseType(TOPIC_PARTITION_SIZE_ANOMALY_CLASS_CONFIG,
                    topicPartitionSizeAnomalyClass,
                    CLASS);
            if (_topicPartitionSizeAnomalyClass == null || !TopicAnomaly.class.isAssignableFrom(_topicPartitionSizeAnomalyClass)) {
                throw new IllegalArgumentException(String.format("Invalid %s is provided to partition size anomaly finder, provided %s",
                        TOPIC_PARTITION_SIZE_ANOMALY_CLASS_CONFIG, _topicPartitionSizeAnomalyClass));
            }
        }
    }
}
