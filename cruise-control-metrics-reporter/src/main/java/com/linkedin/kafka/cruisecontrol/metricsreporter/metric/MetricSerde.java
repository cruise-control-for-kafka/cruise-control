/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


public class MetricSerde implements Serializer<CruiseControlMetric>, Deserializer<CruiseControlMetric> {

  // The overhead of the type bytes
  private static final int METRIC_TYPE_OFFSET = 0;
  private static final int HEADER_LENGTH = 1;

  /**
   * Serialize the Cruise Control metric to a byte array.
   *
   * @param metric Metric to be serialized.
   * @return Serialized Cruise Control metric as a byte array.
   */
  public static byte[] toBytes(CruiseControlMetric metric) {
    ByteBuffer byteBuffer = metric.toBuffer(HEADER_LENGTH);
    byteBuffer.put(METRIC_TYPE_OFFSET, metric.metricClassId().id());
    return byteBuffer.array();
  }

  /**
   * Deserialize from byte array to Cruise Control metric
   * @param bytes Bytes array corresponding to Cruise Control metric.
   * @return Deserialized byte array as Cruise Control metric.
   */
  public static CruiseControlMetric fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return switch (CruiseControlMetric.MetricClassId.forId(buffer.get())) {
      case BROKER_METRIC -> BrokerMetric.fromBuffer(buffer);
      case TOPIC_METRIC -> TopicMetric.fromBuffer(buffer);
      case PARTITION_METRIC -> PartitionMetric.fromBuffer(buffer);
    };
  }

  @Override
  public CruiseControlMetric deserialize(String topic, byte[] bytes) {
    try {
      return fromBytes(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Error occurred when deserialize Cruise Control metrics.", e);
    }
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String s, CruiseControlMetric cruiseControlMetric) {
    return toBytes(cruiseControlMetric);
  }

  @Override
  public void close() {

  }
}
