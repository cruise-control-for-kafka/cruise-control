/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;


/**
 * A class for representing load information for each resource. Each Load in a cluster must have the same number of
 * windows.
 */
public class Load implements Serializable {
    // load by their time.
    private List<Long> _windows;
    private final AggregatedMetricValues _metricValues;

    /**
     * Package constructor for load with given load properties.
     */
    public Load() {
        _windows = null;
        _metricValues = new AggregatedMetricValues();
    }

    /**
     * @return Aggregated metric values associated with the load.
     */
    public AggregatedMetricValues loadByWindows() {
        return _metricValues;
    }

    /**
     * @return The number of windows in the load.
     */
    public int numWindows() {
        return _metricValues.length();
    }

    /**
     * @return The windows list for the load.
     */
    public List<Long> windows() {
        return _windows;
    }

    /**
     * Get a single snapshot value that is representative for the given resource. The current algorithm uses
     * <ol>
     *   <li>If the max or avg load is not requested, then:
     *   <ol>
     *   <li>It is the mean of the recent resource load for inbound network load, outbound network load, and cpu load.</li>
     *   <li>It is the latest utilization for disk space usage.</li>
     *   </ol>
     *   </li>
     *   <li>If the max load is requested: the peak load.</li>
     *   <li>If the avg load is requested: the avg load.</li>
     * </ol>
     *
     * @param resource    Resource for which the expected utilization will be provided.
     * @param wantMaxLoad {@code true} if the requested utilization represents the peak load, {@code false} otherwise.
     * @param wantAvgLoad {@code true} if the requested utilization represents the avg load, {@code false} otherwise.
     * @return A single representative utilization value on a resource.
     */
    public double expectedUtilizationFor(Resource resource, boolean wantMaxLoad, boolean wantAvgLoad) {
        if (wantMaxLoad && wantAvgLoad) {
            throw new IllegalArgumentException("Attempt to request expected utilization with both max and avg load.");
        }
        if (_metricValues.isEmpty()) {
            return 0.0;
        }
        double result = 0;
        for (MetricInfo info : KafkaMetricDef.resourceToMetricInfo(resource)) {
            MetricValues valuesForId = _metricValues.valuesFor(info.id());
            result += wantMaxLoad ? valuesForId.max()
                    : (resource == Resource.DISK && !wantAvgLoad ? valuesForId.latest() : valuesForId.avg());
        }
        return max(result, 0.0);
    }

    public double expectedUtilizationFor(Resource resource) {
        return ModelUtils.expectedUtilizationFor(resource, _metricValues);
    }

    /**
     * Get a single snapshot value that is representative for the given KafkaMetric type. The current algorithm uses
     * <ol>
     *   <li>If the max or avg load is not requested, it is max/latest/mean load depending on the ValueComputingStrategy
     *   which the KafkaMetric type uses.</li>
     *   <li>If the max load is requested, it is the max load.</li>
     *   <li>If the avg load is requested, it is the avg load.</li>
     * </ol>
     *
     * @param metric      KafkaMetric type for which the expected utilization will be provided.
     * @param wantMaxLoad {@code true} if the requested utilization represents the peak load, {@code false} otherwise.
     * @param wantAvgLoad {@code true} if the requested utilization represents the avg load, {@code false} otherwise.
     * @return A single representative utilization value on a metric type.
     */
    public double expectedUtilizationFor(KafkaMetricDef metric, boolean wantMaxLoad, boolean wantAvgLoad) {
        if (wantMaxLoad && wantAvgLoad) {
            throw new IllegalArgumentException("Attempt to request expected utilization with both max and avg load.");
        }
        MetricInfo info = switch (metric.defScope()) {
            case COMMON -> KafkaMetricDef.commonMetricDef().metricInfo(metric.name());
            case BROKER_ONLY -> KafkaMetricDef.brokerMetricDef().metricInfo(metric.name());
            default ->
                    throw new IllegalArgumentException("Metric scope " + metric.defScope() + " for metric " + metric.name() + " is invalid.");
        };
        if (_metricValues.isEmpty()) {
            return 0.0;
        }
        MetricValues valuesForId = _metricValues.valuesFor(info.id());
        if (wantMaxLoad) {
            return max(valuesForId.max(), 0.0);
        } else if (wantAvgLoad) {
            return max(valuesForId.avg(), 0.0);
        }
        return switch (metric.valueComputingStrategy()) {
            case MAX -> max(valuesForId.max(), 0.0);
            case AVG -> max(valuesForId.avg(), 0.0);
            case LATEST -> max(valuesForId.latest(), 0.0);
            default ->
                    throw new IllegalArgumentException("Metric value computing strategy " + metric.valueComputingStrategy()
                            + " for metric " + metric.name() + " is invalid.");
        };
    }

    /**
     * @return {@code true} if this load is empty, {@code false} otherwise.
     */
    boolean isEmpty() {
        return _metricValues.isEmpty();
    }

    /**
     * Overwrite the load using the given AggregatedMetricValues
     *
     * @param loadToSet Load to set.
     */
    void setLoad(AggregatedMetricValues loadToSet) {
        if (loadToSet.length() != _metricValues.length()) {
            throw new IllegalArgumentException("Load to set and load for the resources must have exactly "
                    + _metricValues.length() + " entries.");
        }
        loadToSet.metricIds().forEach(id -> {
            MetricValues valuesToSet = loadToSet.valuesFor(id);
            MetricValues values = _metricValues.valuesFor(id);
            for (int i = 0; i < values.length(); i++) {
                values.set(i, (float) valuesToSet.get(i));
            }
        });
    }

    /**
     * Overwrite the load for given metric with the given load.
     *
     * @param metricId  the metric id to set.
     * @param loadToSet Load for the given metric id to overwrite the original load by snapshot time.
     */
    void setLoad(short metricId, MetricValues loadToSet) {
        if (loadToSet.length() != _metricValues.length()) {
            throw new IllegalArgumentException("Load to set and load for the resources must have exactly "
                    + _metricValues.length() + " entries.");
        }
        MetricValues values = _metricValues.valuesFor(metricId);
        for (int i = 0; i < loadToSet.length(); i++) {
            values.set(i, (float) loadToSet.get(i));
        }
    }

    /**
     * Clear the utilization for given resource.
     *
     * @param resource Resource for which the utilization will be cleared.
     */
    void clearLoadFor(Resource resource) {
        KafkaMetricDef.resourceToMetricIds(resource).forEach(id -> _metricValues.valuesFor(id).clear());
    }

    /**
     * Initialize the metric values for this load. This method should only be called once for initialization.
     * This method is used for the entity load, which should be immutable for most cases.
     *
     * @param aggregatedMetricValues the metric values to set as initialization.
     * @param windows                the list of windows corresponding to the metric values.
     */
    void initializeMetricValues(AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
        if (!_metricValues.isEmpty()) {
            throw new IllegalStateException("Metric values already exists, cannot set it again.");
        }
        _windows = windows;
        _metricValues.add(aggregatedMetricValues);
    }

    /**
     * Add the metric values to the existing metric values.
     *
     * @param aggregatedMetricValues the metric values to add.
     * @param windows                the windows list of the aggregated metric values.
     */
    void addMetricValues(AggregatedMetricValues aggregatedMetricValues, List<Long> windows) {
        if (_windows == null) {
            _windows = windows;
        }
        _metricValues.add(aggregatedMetricValues);
    }

    /**
     * Add the given load to this load.
     *
     * @param loadToAdd Load to add to this load.
     */
    void addLoad(Load loadToAdd) {
        _metricValues.add(loadToAdd.loadByWindows());
    }

    /**
     * Add the given load for the given resource to this load.
     *
     * @param loadToAdd Load to add to this load for the given resource.
     */
    void addLoad(AggregatedMetricValues loadToAdd) {
        if (!_metricValues.isEmpty()) {
            _metricValues.add(loadToAdd);
        }
    }

    /**
     * Subtract the given load from this load.
     *
     * @param loadToSubtract Load to subtract from this load.
     */
    void subtractLoad(Load loadToSubtract) {
        _metricValues.subtract(loadToSubtract.loadByWindows());
    }

    /**
     * Subtract the given load for the given resource from this load.
     *
     * @param loadToSubtract Load to subtract from this load for the given resource.
     */
    void subtractLoad(AggregatedMetricValues loadToSubtract) {
        if (!_metricValues.isEmpty()) {
            _metricValues.subtract(loadToSubtract);
        }
    }

    /**
     * Clear the content of the circular list for each resource.
     */
    void clearLoad() {
        _metricValues.clear();
    }

    /**
     * Get the load for the requested resource across all the windows. The returned value may include multiple
     * metrics that are associated with the requested resource.
     *
     * @param resource        Resource for which the load will be provided.
     * @param shareValueArray Whether the returned result should share the value array with this class or not. When this
     *                        value is set to true, the returned result share the same value array with this object.
     *                        Otherwise, data copy will be made and a dedicated result will be returned.
     * @return Load of the requested resource as a mapping from snapshot time to utilization for the given resource.
     */
    AggregatedMetricValues loadFor(Resource resource, boolean shareValueArray) {
        return _metricValues.valuesFor(KafkaMetricDef.resourceToMetricIds(resource), shareValueArray);
    }

    /**
     * @return An object that can be further used to encode into JSON
     */
    public Map<String, Object> getJsonStructure() {
        MetricDef metricDef = KafkaMetricDef.commonMetricDef();
        Map<String, Object> loadMap = new HashMap<>();
        List<Object> metricValueList = new ArrayList<>();
        for (MetricInfo metricInfo : metricDef.all()) {
            MetricValues metricValues = _metricValues.valuesFor(metricInfo.id());
            if (metricValues != null) {
                Map<Long, Double> metricValuesMap = new HashMap<>();
                for (int i = 0; i < _windows.size(); i++) {
                    metricValuesMap.put(_windows.get(i), metricValues.get(i));
                }
                metricValueList.add(metricValuesMap);
            }
        }
        loadMap.put(ModelUtils.METRIC_VALUES, metricValueList);
        return loadMap;
    }

    /**
     * Output writing string representation of this class to the stream.
     *
     * @param out the output stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write("<Load>".getBytes(StandardCharsets.UTF_8));
        _metricValues.writeTo(out);
        out.write("</Load>%n".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Get string representation of {@link Load}.
     */
    @Override
    public String toString() {
        return String.format("Load[metricValues=%s]", _metricValues);
    }
}
