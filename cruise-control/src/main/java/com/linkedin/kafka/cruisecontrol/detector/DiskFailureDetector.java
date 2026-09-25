/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.DISK_FAILURES_CLASS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;

/**
 * This class detects disk failures.
 **/
public class DiskFailureDetector extends AbstractAnomalyDetector implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(DiskFailureDetector.class);
    public static final String FAILED_DISKS_OBJECT_CONFIG = "failed.disks.object";
    private final AdminClient _adminClient;
    private ModelGeneration _lastCheckedModelGeneration;
    private final KafkaCruiseControlConfig _config;

    public DiskFailureDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
        super(anomalies, kafkaCruiseControl);
        _adminClient = kafkaCruiseControl.adminClient();
        _lastCheckedModelGeneration = new ModelGeneration(0, -1L);
        _config = _kafkaCruiseControl.config();
    }

    /**
     * Retrieve the {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the disk failure detector
     * is ready to check for an anomaly.
     * <p>
     * Skip disk failure detection if any of the following is satisfied:
     * <ul>
     *   <li>Cluster model generation has not changed since the last disk failure check.</li>
     *   <li>There are dead brokers in the cluster,
     *   {@link KafkaBrokerFailureDetector} should take care of the anomaly.</li>
     *   <li>{@link AnomalyDetectorUtils#getAnomalyDetectionStatus(KafkaCruiseControl, boolean, boolean)} is not {@link AnomalyDetectionStatus#READY}.
     *   <li>See {@link AnomalyDetectionStatus} for details.</li>
     * </ul>
     *
     * @return The {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the anomaly detector is ready.
     */
    private AnomalyDetectionStatus getDiskFailureDetectionStatus() {
        ModelGeneration currentClusterModelGeneration = _kafkaCruiseControl.loadMonitor().clusterModelGeneration();
        if (currentClusterModelGeneration.equals(_lastCheckedModelGeneration)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skipping disk failure detection because the model generation hasn't changed. Current model generation {}",
                        _kafkaCruiseControl.loadMonitor().clusterModelGeneration());
            }
            return AnomalyDetectionStatus.SKIP_MODEL_GENERATION_NOT_CHANGED;
        }
        _lastCheckedModelGeneration = currentClusterModelGeneration;

        Set<Integer> deadBrokers = _kafkaCruiseControl.loadMonitor().deadBrokersWithReplicas(MAX_METADATA_WAIT_MS);
        if (!deadBrokers.isEmpty()) {
            LOG.debug("Skipping disk failure detection because there are dead broker in the cluster, dead broker: {}", deadBrokers);
            return AnomalyDetectionStatus.SKIP_HAS_DEAD_BROKERS;
        }

        return getAnomalyDetectionStatus(_kafkaCruiseControl, false, true);
    }

    @Override
    public void run() {
        try {
            if (getDiskFailureDetectionStatus() != AnomalyDetectionStatus.READY) {
                return;
            }
            Map<Integer, Map<String, Long>> failedDisksByBroker = new HashMap<>();
            Set<Integer> aliveBrokers = _kafkaCruiseControl.kafkaCluster().nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
            _adminClient.describeLogDirs(aliveBrokers).descriptions().forEach((broker, future) -> {
                try {
                    future.get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logDir, info) -> {
                        if (info.error() != null) {
                            failedDisksByBroker.putIfAbsent(broker, new HashMap<>());
                            failedDisksByBroker.get(broker).put(logDir, _kafkaCruiseControl.timeMs());
                        }
                    });
                } catch (TimeoutException | InterruptedException | ExecutionException e) {
                    LOG.warn("Retrieving logDir information for broker {} encountered exception {}.", broker, e);
                }
            });
            if (!failedDisksByBroker.isEmpty()) {
                Map<String, Object> parameterConfigOverrides = new HashMap<>();
                parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
                parameterConfigOverrides.put(FAILED_DISKS_OBJECT_CONFIG, failedDisksByBroker);
                parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
                DiskFailures diskFailures = _config.getConfiguredInstance(DISK_FAILURES_CLASS_CONFIG,
                        DiskFailures.class,
                        parameterConfigOverrides);
                _anomalies.add(diskFailures);
            }
        } catch (Exception e) {
            LOG.error("Unexpected exception", e);
        } finally {
            LOG.debug("Disk failure detection finished.");
        }
    }
}
