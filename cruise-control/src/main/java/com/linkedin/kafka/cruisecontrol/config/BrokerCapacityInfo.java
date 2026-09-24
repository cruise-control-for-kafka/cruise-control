/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * A class encapsulating the capacity information of a broker, which includes all the resources defined in {@link Resource}.
 * <p>
 * The units for each resource are:
 * DISK - MegaBytes
 * CPU - Percentage (0 - 100)
 * Network Inbound - KB/s
 * Network Outbounds - KB/s
 * <p>
 * The information also contains the number of CPU cores and may contain disk capacities by logDirs (i.e. for JBOD).
 */
public class BrokerCapacityInfo {
    public static final short DEFAULT_NUM_CPU_CORES = 1;
    private static final String DEFAULT_ESTIMATION_INFO = "";
    private static final Map<String, Double> DEFAULT_DISK_CAPACITY_BY_LOGDIR = null;
    private final Map<Resource, Double> _capacity;
    private final String _estimationInfo;
    private final Map<String, Double> _diskCapacityByLogDir;
    private final double _numCpuCores;

    /**
     * BrokerCapacityInfo with the given capacity, estimation, per absolute logDir disk capacity, and number of CPU cores.
     *
     * @param capacity             Capacity information for each resource.
     * @param estimationInfo       Description if there is any capacity estimation, {@code null} or {@link #DEFAULT_ESTIMATION_INFO} otherwise.
     * @param diskCapacityByLogDir Disk capacity by absolute logDir.
     * @param numCpuCores          Number of CPU cores.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity,
                              String estimationInfo,
                              Map<String, Double> diskCapacityByLogDir,
                              double numCpuCores) {
        sanityCheckCapacity(capacity);
        _capacity = capacity;
        _estimationInfo = estimationInfo == null ? DEFAULT_ESTIMATION_INFO : estimationInfo;
        _diskCapacityByLogDir = diskCapacityByLogDir;
        _numCpuCores = numCpuCores;
    }

    /**
     * BrokerCapacityInfo with the given capacity, per absolute logDir disk capacity, and number of CPU cores.
     *
     * @param capacity             Capacity information for each resource.
     * @param diskCapacityByLogDir Disk capacity by absolute logDir.
     * @param numCpuCores          Number of CPU cores.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity, Map<String, Double> diskCapacityByLogDir, double numCpuCores) {
        this(capacity, DEFAULT_ESTIMATION_INFO, diskCapacityByLogDir, numCpuCores);
    }

    /**
     * BrokerCapacityInfo with the given capacity and number of CPU cores.
     *
     * @param capacity    Capacity information for each resource.
     * @param numCpuCores Number of CPU cores.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity, double numCpuCores) {
        this(capacity, DEFAULT_ESTIMATION_INFO, DEFAULT_DISK_CAPACITY_BY_LOGDIR, numCpuCores);
    }

    /**
     * BrokerCapacityInfo with the given capacity, estimation, and per absolute logDir disk capacity.
     *
     * @param capacity             Capacity information for each resource.
     * @param estimationInfo       Description if there is any capacity estimation, {@code null} or {@link #DEFAULT_ESTIMATION_INFO} otherwise.
     * @param diskCapacityByLogDir Disk capacity by absolute logDir.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo, Map<String, Double> diskCapacityByLogDir) {
        this(capacity, estimationInfo, diskCapacityByLogDir, DEFAULT_NUM_CPU_CORES);
    }

    /**
     * BrokerCapacityInfo with no capacity information specified per absolute logDir.
     *
     * @param capacity       Capacity information for each resource.
     * @param estimationInfo Description if there is any capacity estimation, {@code null} or {@link #DEFAULT_ESTIMATION_INFO} otherwise.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity, String estimationInfo) {
        this(capacity, estimationInfo, DEFAULT_DISK_CAPACITY_BY_LOGDIR, DEFAULT_NUM_CPU_CORES);
    }

    /**
     * BrokerCapacityInfo with no estimation.
     *
     * @param capacity             Capacity information for each resource.
     * @param diskCapacityByLogDir Disk capacity by absolute logDir.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity, Map<String, Double> diskCapacityByLogDir) {
        this(capacity, DEFAULT_ESTIMATION_INFO, diskCapacityByLogDir, DEFAULT_NUM_CPU_CORES);
    }

    /**
     * BrokerCapacityInfo with no estimation, no capacity information specified per absolute logDir.
     *
     * @param capacity Capacity information for each resource.
     */
    public BrokerCapacityInfo(Map<Resource, Double> capacity) {
        this(capacity, DEFAULT_ESTIMATION_INFO, DEFAULT_DISK_CAPACITY_BY_LOGDIR, DEFAULT_NUM_CPU_CORES);
    }

    /**
     * @return The broker capacity for different resource types.
     */
    public Map<Resource, Double> capacity() {
        return Collections.unmodifiableMap(_capacity);
    }

    /**
     * @return {@code true} if the capacity of the broker for at least one resource is based on an estimation, {@code false} otherwise.
     */
    public boolean isEstimated() {
        return !_estimationInfo.isEmpty();
    }

    /**
     * @return {@link #DEFAULT_ESTIMATION_INFO} if no estimation, related estimation info otherwise.
     */
    public String estimationInfo() {
        return _estimationInfo;
    }

    /**
     * @return Disk capacity by absolute logDir if specified, {@link #DEFAULT_DISK_CAPACITY_BY_LOGDIR} otherwise.
     */
    public Map<String, Double> diskCapacityByLogDir() {
        return _diskCapacityByLogDir;
    }

    /**
     * @return Number of CPU cores (if provided), {@link #DEFAULT_NUM_CPU_CORES} otherwise.
     */
    public double numCpuCores() {
        return _numCpuCores;
    }

    /**
     * Sanity check to ensure the provided capacity information contains all the resource type.
     *
     * @param capacity The provided capacity map.
     */
    static void sanityCheckCapacity(Map<Resource, Double> capacity) {
        Set<Resource> providedResource = capacity.keySet();
        Set<Resource> missingResource = Resource.cachedValues().stream().filter(r -> !providedResource.contains(r))
                .collect(Collectors.toSet());
        if (!missingResource.isEmpty()) {
            throw new IllegalArgumentException(String.format("Provided capacity information missing value for resource %s.", missingResource));
        }
    }
}
