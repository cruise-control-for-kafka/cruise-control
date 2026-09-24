/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is created to describe the requirements of the model. In Cruise Control, the requirement
 * of the monitored performance metrics differs in different scenarios. For example, RackAwareness goal do not really
 * need any load information, but it need to include all the topics. On the other hand, resource distribution
 * would need more load information.
 * <p>
 * This class allows different goals to specify different requirements for the cluster model. Currently the following
 * requirements can be specified:
 * </p>
 * <ul>
 *   <li><b>Number of Windows</b> - specifies how many snapshots must be there for each partition in
 *   order for a partition to be considered as valid for the cluster model. The snapshots are counted backwards from
 *   the current snapshot window</li>
 *
 *   <li><b>Minimum Monitored Partitions Percentage</b> - specifies the minimum required percentage of
 *   valid partitions out of the all the partitions in the cluster.</li>
 *
 *   <li><b>Whether Include All Topics</b> - For some goals that do not care about the load information at all (e.g.
 *   Rack Awareness Goal), they can simply include all the topics. Notice include all the topics does not
 *   override the other requirements. So if include all topics is specified and a minimum monitored partitions
 *   percentage is not met, the entire requirement will still be considered not met.</li>
 * </ul>
 *
 */
@JsonResponseClass
public class ModelCompletenessRequirements {
    @JsonResponseField
    private static final String REQUIRED_NUM_SNAPSHOTS = "requiredNumSnapshots";
    @JsonResponseField
    private static final String MIN_MONITORED_PARTITION_PERCENTAGE = "minMonitoredPartitionsPercentage";
    @JsonResponseField
    private static final String INCLUDE_ALL_TOPICS = "includeAllTopics";
    private final int _minRequiredNumWindows;
    private final double _minMonitoredPartitionsPercentage;
    private final boolean _includeAllTopics;

    /**
     * Constructor for the requirements.
     *
     * @param minNumValidWindows      the minimum number of valid windows to generate the model. Tha value must positive.
     * @param minValidPartitionsRatio The minimum required percentage of monitored partitions.
     * @param includeAllTopics        whether all the topics should be included to the time window. When set to {@code true}, all the
     *                                topics will be included even when there is not enough snapshots. An empty snapshot will
     *                                be used if there is no sample for a partition.
     */
    public ModelCompletenessRequirements(int minNumValidWindows,
                                         double minValidPartitionsRatio,
                                         boolean includeAllTopics) {
        if (minNumValidWindows <= 0) {
            throw new IllegalArgumentException("Invalid minNumValidWindows " + minNumValidWindows + ". The minNumValidWindows must be positive.");
        }
        if (minValidPartitionsRatio < 0 || minValidPartitionsRatio > 1) {
            throw new IllegalArgumentException("Invalid minValidPartitionsRatio " + minValidPartitionsRatio + ". The value must be between 0 and 1"
                    + ", both inclusive.");
        }
        _minRequiredNumWindows = minNumValidWindows;
        _includeAllTopics = includeAllTopics;
        _minMonitoredPartitionsPercentage = minValidPartitionsRatio;
    }

    public int minRequiredNumWindows() {
        return _minRequiredNumWindows;
    }

    public double minMonitoredPartitionsPercentage() {
        return _minMonitoredPartitionsPercentage;
    }

    public boolean includeAllTopics() {
        return _includeAllTopics;
    }

    /**
     * Combine the requirements of this ModelCompletenessRequirements and another one. The result should meet both requirements.
     *
     * @param other the other ModelCompletenessRequirements
     * @return The combined stronger model completeness requirements.
     */
    public ModelCompletenessRequirements stronger(ModelCompletenessRequirements other) {
        if (other == null) {
            return this;
        }
        return new ModelCompletenessRequirements(Math.max(_minRequiredNumWindows, other.minRequiredNumWindows()),
                Math.max(_minMonitoredPartitionsPercentage, other.minMonitoredPartitionsPercentage()),
                _includeAllTopics || other.includeAllTopics());
    }

    /**
     * Combine the requirements of this ModelCompletenessRequirements and another one.
     * The result will be the weaker one for each requirement in the two specifications.
     *
     * @param other the other ModelCompletenessRequirements
     * @return The combined weaker model completeness requirements.
     */
    public ModelCompletenessRequirements weaker(ModelCompletenessRequirements other) {
        if (other == null) {
            return this;
        }
        return new ModelCompletenessRequirements(Math.min(_minRequiredNumWindows, other.minRequiredNumWindows()),
                Math.min(_minMonitoredPartitionsPercentage, other.minMonitoredPartitionsPercentage()),
                _includeAllTopics && other.includeAllTopics());
    }

    /**
     *
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
        Map<String, Object> requirements = new HashMap<>();
        requirements.put(REQUIRED_NUM_SNAPSHOTS, _minRequiredNumWindows);
        requirements.put(MIN_MONITORED_PARTITION_PERCENTAGE, _minMonitoredPartitionsPercentage);
        requirements.put(INCLUDE_ALL_TOPICS, _includeAllTopics);
        return requirements;
    }

    @Override
    public String toString() {
        return String.format("(requiredNumWindows=%d, minMonitoredPartitionPercentage=%.3f, includedAllTopics=%s)",
                _minRequiredNumWindows, _minMonitoredPartitionsPercentage, _includeAllTopics);
    }

}
