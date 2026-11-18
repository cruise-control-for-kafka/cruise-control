/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.*;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.SamplingUtils.replaceDotsWithUnderscores;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.allowMissingBrokerMetric;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.sanityCheckMetricScope;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.convertUnit;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.METRIC_TYPES_TO_SUM;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.MISSING_BROKER_METRIC_VALUE;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * A helper class to hold broker load.
 */
public class BrokerLoad {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerLoad.class);
  private static final double MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT = 0.01;
  private static final double MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT = 0.01;
  private final RawMetricsHolder _brokerMetrics;
  private final Map<String, RawMetricsHolder> _dotHandledTopicMetrics;
  private final Map<TopicPartition, RawMetricsHolder> _dotHandledPartitionMetrics;
  // Remember which topic has partition size reported. Because the topic level IO metrics are only created when
  // there is IO, the topic level IO metrics may be missing if there was no traffic to the topic on the broker.
  // However, because the partition size will always be reported, when we see partition size was reported for
  // a topic but the topic level IO metrics are not reported, we assume there was no traffic to the topic.
  private final Set<String> _dotHandledTopicsWithPartitionSizeReported = new HashSet<>();
  private final Set<RawMetricType> _missingBrokerMetricsInMinSupportedVersion = new HashSet<>();
  private boolean _minRequiredBrokerMetricsAvailable = false;
  // Set to the latest possible deserialization version based on the sampled data.
  private byte _brokerSampleDeserializationVersion = -1;

  public BrokerLoad() {
    _brokerMetrics = new RawMetricsHolder();
    _dotHandledTopicMetrics = new HashMap<>();
    _dotHandledPartitionMetrics = new HashMap<>();
  }

  /**
   * Record the given Cruise Control metric.
   *
   * @param ccm Cruise Control metric.
   */
  public void recordMetric(CruiseControlMetric ccm) {
    RawMetricType rawMetricType = ccm.rawMetricType();
    switch (rawMetricType.metricScope()) {
      case BROKER:
        _brokerMetrics.recordCruiseControlMetric(ccm);
        break;
      case TOPIC:
        TopicMetric tm = (TopicMetric) ccm;
        _dotHandledTopicMetrics.computeIfAbsent(replaceDotsWithUnderscores(tm.topic()), t -> new RawMetricsHolder())
                               .recordCruiseControlMetric(ccm);
        break;
      case PARTITION:
        PartitionMetric pm = (PartitionMetric) ccm;
        _dotHandledPartitionMetrics.computeIfAbsent(new TopicPartition(replaceDotsWithUnderscores(pm.topic()), pm.partition()), tp -> new RawMetricsHolder())
                                   .recordCruiseControlMetric(ccm);
        _dotHandledTopicsWithPartitionSizeReported.add(replaceDotsWithUnderscores(pm.topic()));
        break;
      default:
        throw new IllegalStateException(String.format("Should never be here. Unrecognized metric scope %s",
                                                      rawMetricType.metricScope()));
    }
  }

  /**
   * Check whether all dot handled topic metrics are available for the given dot-handled topic name.
   *
   * @param dotHandledTopic Dot-handled topic name.
   * @return {@code true} if all dot handled topic metrics are available for the given dot-handled topic name, {@code false} otherwise.
   */
  public boolean allDotHandledTopicMetricsAvailable(String dotHandledTopic) {
    // We rely on the partition size metric to determine whether a topic metric is available or not.
    // The topic names in this set are dot handled -- i.e. dots (".") in topic name is replaced with underscores ("_").
    // Note that metrics reporter implicitly does this conversion, but the metadata topic names keep the original name.
    return _dotHandledTopicsWithPartitionSizeReported.contains(dotHandledTopic);
  }

  public boolean minRequiredBrokerMetricsAvailable() {
    return _minRequiredBrokerMetricsAvailable;
  }

  public boolean brokerMetricAvailable(RawMetricType rawMetricType) {
    return _brokerMetrics.metricValue(rawMetricType) != null;
  }

  /**
   * Check whether partition metrics are available for the given dot-handled partition name and raw metric type.
   *
   * @param tpWithDotHandled Topic partition with dot-handled topic name.
   * @param rawMetricType Raw metric type.
   * @return {@code true} if partition metrics are available for the given dot-handled partition name and raw metric type, false
   * otherwise.
   */
  public boolean partitionMetricAvailable(TopicPartition tpWithDotHandled, RawMetricType rawMetricType) {
    RawMetricsHolder rawMetricsHolder = _dotHandledPartitionMetrics.get(tpWithDotHandled);
    return rawMetricsHolder != null && rawMetricsHolder.metricValue(rawMetricType) != null;
  }

  public Set<RawMetricType> missingBrokerMetricsInMinSupportedVersion() {
    return Collections.unmodifiableSet(_missingBrokerMetricsInMinSupportedVersion);
  }

  /**
   * Get the broker metric for the given raw metric type.
   *
   * @param rawMetricType Raw metric type.
   * @return The broker metric for the given raw metric type.
   */
  public double brokerMetric(RawMetricType rawMetricType) {
    sanityCheckMetricScope(rawMetricType, BROKER);
    ValueHolder valueHolder = validateNotNull(_brokerMetrics.metricValue(rawMetricType),
            () -> String.format("Broker metric %s does not exist.", rawMetricType));
    return convertUnit(valueHolder.value(), rawMetricType);
  }

  public double topicMetrics(String dotHandledTopic, RawMetricType rawMetricType) {
    return topicMetrics(dotHandledTopic, rawMetricType, true);
  }

  private double topicMetrics(String dotHandledTopic, RawMetricType rawMetricType, boolean convertUnit) {
    sanityCheckMetricScope(rawMetricType, TOPIC);
    if (!allDotHandledTopicMetricsAvailable(dotHandledTopic)) {
      throw new IllegalArgumentException(String.format("Topic metric %s does not exist for dot handled topic name %s.",
                                                       rawMetricType, dotHandledTopic));
    }
    RawMetricsHolder rawMetricsHolder = _dotHandledTopicMetrics.get(dotHandledTopic);
    if (rawMetricsHolder == null || rawMetricsHolder.metricValue(rawMetricType) == null) {
      return 0.0;
    }
    double rawMetricValue = rawMetricsHolder.metricValue(rawMetricType).value();
    return convertUnit ? convertUnit(rawMetricValue, rawMetricType) : rawMetricValue;
  }

  /**
   * Get partition metric with the given name for the partition from given topic and partition number. If the partition
   * metric does not exist for given dot-handled topic name and partition, return {@code null}.
   *
   * @param dotHandledTopic Dot-handled topic name.
   * @param partition Partition number.
   * @param rawMetricType Raw metric type.
   * @return Partition metric with the given name for the partition from given topic and partition number.
   */
  public Double partitionMetric(String dotHandledTopic, int partition, RawMetricType rawMetricType) {
    sanityCheckMetricScope(rawMetricType, PARTITION);
    RawMetricsHolder metricsHolder = _dotHandledPartitionMetrics.get(new TopicPartition(dotHandledTopic, partition));
    if (metricsHolder == null || metricsHolder.metricValue(rawMetricType) == null) {
      LOG.error("Partition metric {} does not exist for dot handled topic {} and partition {}.",
                rawMetricType, dotHandledTopic, partition);
      return null;
    }
    return convertUnit(metricsHolder.metricValue(rawMetricType).value(), rawMetricType);
  }

  /**
   * Due to the yammer metric exponential decaying mechanism, the broker metric and the sum of the partition metrics
   * on the same broker may differ by a lot. Our experience shows that in that case, the sum of the topic/partition
   * level metrics are more accurate. So we will just replace the following metrics with the sum of topic/partition
   * level metrics:
   * <ul>
   *   <li>BrokerProduceRate</li>
   *   <li>BrokerFetchRate</li>
   *   <li>BrokerLeaderBytesInRate</li>
   *   <li>BrokerLeaderBytesOutRate</li>
   *   <li>BrokerReplicationBytesInRate</li>
   *   <li>BrokerReplicationBytesOutRate</li>
   *   <li>BrokerMessagesInRate</li>
   * </ul>
   *
   * We use the cluster metadata to check if the reported topic level metrics are complete. If the reported topic
   * level metrics are not complete, we ignore the broker metric sample by setting the _minRequiredBrokerMetricsAvailable
   * flag to false.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The broker id to prepare metrics for.
   * @param time The last sample time.
   */
  public void prepareBrokerMetrics(Cluster cluster, int brokerId, long time) {
    boolean enoughTopicPartitionMetrics = enoughTopicPartitionMetrics(cluster, brokerId);
    // Ensure there are enough topic level metrics.
    if (enoughTopicPartitionMetrics) {
      Map<RawMetricType, Double> sumOfTopicMetrics = new HashMap<>();
      for (String dotHandledTopic : _dotHandledTopicsWithPartitionSizeReported) {
        METRIC_TYPES_TO_SUM.keySet().forEach(type -> {
          double value = topicMetrics(dotHandledTopic, type, false);
          sumOfTopicMetrics.compute(type, (t, v) -> (v == null ? 0 : v) + value);
        });
      }
      for (Map.Entry<RawMetricType, Double> entry : sumOfTopicMetrics.entrySet()) {
        RawMetricType rawTopicMetricType = entry.getKey();
        double value = entry.getValue();
        _brokerMetrics.setRawMetricValue(METRIC_TYPES_TO_SUM.get(rawTopicMetricType), value, time);
      }
    }
    // Check if all the broker raw metrics are available.
    maybeSetBrokerRawMetrics(cluster, brokerId, time);

    // A broker metric is only available if it has enough valid topic metrics and it has reported
    // replication bytes in/out metrics.
    _minRequiredBrokerMetricsAvailable = enoughTopicPartitionMetrics && _missingBrokerMetricsInMinSupportedVersion.isEmpty();
  }

  /**
   * Use the latest supported version for which the raw broker metrics are available.
   *
   * 1) If broker metrics are incomplete for the {@link BrokerMetricSample#MIN_SUPPORTED_VERSION}, then broker metrics
   * are insufficient to create a broker sample.
   * 2) If broker metrics are complete in a version-X but not in (version-X + 1), then use version-X.
   *
   * {@link #_missingBrokerMetricsInMinSupportedVersion} is relevant only for {@link BrokerMetricSample#MIN_SUPPORTED_VERSION}.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The broker id to prepare metrics for.
   * @param time The last sample time.
   */
  private void maybeSetBrokerRawMetrics(Cluster cluster, int brokerId, long time) {
    for (byte v = BrokerMetricSample.MIN_SUPPORTED_VERSION; v <= BrokerMetricSample.LATEST_SUPPORTED_VERSION; v++) {
      Set<RawMetricType> missingBrokerMetrics = new HashSet<>();
      for (RawMetricType rawBrokerMetricType : RawMetricType.brokerMetricTypesDiffForVersion(v)) {
        if (_brokerMetrics.metricValue(rawBrokerMetricType) == null) {
          if (allowMissingBrokerMetric(cluster, brokerId, rawBrokerMetricType)) {
            // If the metric is allowed to be missing, we simply use MISSING_BROKER_METRIC_VALUE as the value.
            _brokerMetrics.setRawMetricValue(rawBrokerMetricType, MISSING_BROKER_METRIC_VALUE, time);
          } else {
            missingBrokerMetrics.add(rawBrokerMetricType);
          }
        }
      }

      if (!missingBrokerMetrics.isEmpty()) {
        if (_brokerSampleDeserializationVersion == -1) {
          _missingBrokerMetricsInMinSupportedVersion.addAll(missingBrokerMetrics);
        }
        break;
      } else {
        // Set supported deserialization version so far.
        _brokerSampleDeserializationVersion = v;
      }
    }
    // Set nullable broker metrics for missing metrics if a valid deserialization exists with an old version.
    setNullableBrokerMetrics();
  }

  public byte brokerSampleDeserializationVersion() {
    return _brokerSampleDeserializationVersion;
  }

  private void setNullableBrokerMetrics() {
    if (_brokerSampleDeserializationVersion != -1) {
      Set<RawMetricType> nullableBrokerMetrics = new HashSet<>();
      for (byte v = (byte) (_brokerSampleDeserializationVersion + 1); v <= BrokerMetricSample.LATEST_SUPPORTED_VERSION; v++) {
        Set<RawMetricType> nullableMetrics = new HashSet<>(RawMetricType.brokerMetricTypesDiffForVersion(v));
        nullableBrokerMetrics.addAll(nullableMetrics);
      }
      nullableBrokerMetrics.forEach(nullableMetric -> _brokerMetrics.setRawMetricValue(nullableMetric, 0.0, 0L));
    }
  }

  /**
   * Verify whether we have collected enough metrics to generate the broker metric samples. The broker must have
   * missed less than {@link #MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT} of the topic level
   * and {@link #MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT} partition level metrics in the
   * broker to generate broker level metrics.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The broker id to check.
   * @return {@code true} if there are enough topic level metrics, {@code false} otherwise.
   */
  private boolean enoughTopicPartitionMetrics(Cluster cluster, int brokerId) {
    Set<String> missingTopics = new HashSet<>();
    Set<String> topicsInBroker = new HashSet<>();
    AtomicInteger missingPartitions = new AtomicInteger(0);
    List<PartitionInfo> leaderPartitionsInNode = cluster.partitionsForNode(brokerId);
    if (leaderPartitionsInNode.isEmpty()) {
      // If the broker does not have any leader partition, return true immediately.
      return true;
    }
    leaderPartitionsInNode.forEach(info -> {
      String topicWithDotHandled = replaceDotsWithUnderscores(info.topic());
      topicsInBroker.add(topicWithDotHandled);
      if (!_dotHandledTopicsWithPartitionSizeReported.contains(topicWithDotHandled)) {
        missingPartitions.incrementAndGet();
        missingTopics.add(topicWithDotHandled);
      }
    });
    boolean result = ((double) missingTopics.size() / topicsInBroker.size()) <= MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT
                     && (double) missingPartitions.get() / cluster.partitionsForNode(brokerId).size()
                        <= MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT;
    if (!result) {
      LOG.warn("Broker {} is missing {}/{} topics metrics and {}/{} leader partition metrics. Missing leader topics: {}.", brokerId,
               missingTopics.size(), topicsInBroker.size(), missingPartitions.get(), cluster.partitionsForNode(brokerId).size(), missingTopics);
    }
    return result;
  }

  /**
   * @return Disk usage of the broker.
   */
  public double diskUsage() {
    double result = 0.0;
    for (RawMetricsHolder rawMetricsHolder : _dotHandledPartitionMetrics.values()) {
      result += rawMetricsHolder.metricValue(RawMetricType.PARTITION_SIZE).value();
    }
    return convertUnit(result, RawMetricType.PARTITION_SIZE);
  }
}
