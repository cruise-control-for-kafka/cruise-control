/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;


final class HolderUtils {
  static final double MISSING_BROKER_METRIC_VALUE = 0.0;
  static final Map<RawMetricType, RawMetricType> METRIC_TYPES_TO_SUM = new HashMap<>();
  static {
    METRIC_TYPES_TO_SUM.put(TOPIC_PRODUCE_REQUEST_RATE, ALL_TOPIC_PRODUCE_REQUEST_RATE);
    METRIC_TYPES_TO_SUM.put(TOPIC_FETCH_REQUEST_RATE, ALL_TOPIC_FETCH_REQUEST_RATE);
    METRIC_TYPES_TO_SUM.put(TOPIC_BYTES_IN, ALL_TOPIC_BYTES_IN);
    METRIC_TYPES_TO_SUM.put(TOPIC_BYTES_OUT, ALL_TOPIC_BYTES_OUT);
    METRIC_TYPES_TO_SUM.put(TOPIC_REPLICATION_BYTES_IN, ALL_TOPIC_REPLICATION_BYTES_IN);
    METRIC_TYPES_TO_SUM.put(TOPIC_REPLICATION_BYTES_OUT, ALL_TOPIC_REPLICATION_BYTES_OUT);
    METRIC_TYPES_TO_SUM.put(TOPIC_MESSAGES_IN_PER_SEC, ALL_TOPIC_MESSAGES_IN_PER_SEC);
  }
  private static final int BYTES_IN_KB = 1024;
  private static final int BYTES_IN_MB = 1024 * 1024;

  private HolderUtils() {

  }

  static void sanityCheckMetricScope(RawMetricType rawMetricType, RawMetricType.MetricScope expectedMetricScope) {
    if (rawMetricType.metricScope() != expectedMetricScope) {
      throw new IllegalArgumentException(
          String.format("Metric scope %s of raw metric type with id %d does not match the expected metric scope %s.",
                        rawMetricType.metricScope(), rawMetricType.id(), expectedMetricScope));
    }
  }

  /**
   * Check if a broker raw metric is reasonable to be missing. As of now, it looks that only the following metrics
   * might be missing:
   * <ul>
   *   <li>BROKER_FOLLOWER_FETCH_REQUEST_RATE (with additional constraints)</li>
   *   <li>BROKER_LOG_FLUSH_RATE</li>
   *   <li>BROKER_LOG_FLUSH_TIME_MS_MEAN</li>
   *   <li>BROKER_LOG_FLUSH_TIME_MS_MAX</li>
   *   <li>BROKER_LOG_FLUSH_TIME_MS_50TH</li>
   *   <li>BROKER_LOG_FLUSH_TIME_MS_999TH</li>
   *   <li>BROKER_PRODUCE_REQUEST_RATE</li>
   *   <li>BROKER_CONSUMER_FETCH_REQUEST_RATE</li>
   * </ul>
   * When these raw metrics are missing, broker load is expected to use {@link #MISSING_BROKER_METRIC_VALUE} as the value.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The id of the broker whose raw metric is missing
   * @param rawMetricType The raw metric type that is missing.
   * @return {@code true} if the missing is allowed, {@code false} otherwise.
   */
  static boolean allowMissingBrokerMetric(Cluster cluster, int brokerId, RawMetricType rawMetricType) {
    return switch (rawMetricType) {
      case BROKER_FOLLOWER_FETCH_REQUEST_RATE -> {
        for (PartitionInfo partitionInfo : cluster.partitionsForNode(brokerId)) {
          // If there is at least one leader partition on the broker that meets the following condition:
          // 1. replication factor is greater than 1,
          // 2. there are more than one alive replicas.
          // Then the broker must report BrokerFollowerFetchRequestRate.
          if (partitionInfo.replicas().length > 1
              && partitionInfo.leader() != null
              && partitionInfo.leader().id() == brokerId) {
            yield false;
          }
        }
        yield true;
      }
      case BROKER_LOG_FLUSH_RATE, BROKER_LOG_FLUSH_TIME_MS_MEAN, BROKER_LOG_FLUSH_TIME_MS_MAX,
           BROKER_LOG_FLUSH_TIME_MS_50TH, BROKER_LOG_FLUSH_TIME_MS_999TH, BROKER_PRODUCE_REQUEST_RATE,
           BROKER_CONSUMER_FETCH_REQUEST_RATE -> true;
      default -> false;
    };
  }

  static double convertUnit(double value, RawMetricType rawMetricType) {
    return switch (rawMetricType) {
      case ALL_TOPIC_BYTES_IN, ALL_TOPIC_BYTES_OUT, ALL_TOPIC_REPLICATION_BYTES_IN, ALL_TOPIC_REPLICATION_BYTES_OUT,
           TOPIC_BYTES_IN, TOPIC_BYTES_OUT, TOPIC_REPLICATION_BYTES_IN, TOPIC_REPLICATION_BYTES_OUT ->
              value / BYTES_IN_KB;
      case PARTITION_SIZE -> value / BYTES_IN_MB;
      default -> value;
    };
  }
}
