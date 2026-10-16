/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import java.util.Map;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.CPU_USAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.DISK_USAGE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.FETCH_RATE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.LEADER_BYTES_IN;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.LEADER_BYTES_OUT;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.MESSAGE_IN_RATE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.PRODUCE_RATE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.REPLICATION_BYTES_IN_RATE;
import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.REPLICATION_BYTES_OUT_RATE;
import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * The class that hosts all the metric samples for a partition.
 */
public class PartitionMetricSample extends MetricSample<String, PartitionEntity> {
  static final byte MIN_SUPPORTED_VERSION = 0;
  static final byte LATEST_SUPPORTED_VERSION = 1;

  private final int _brokerId;

  public PartitionMetricSample(int brokerId, TopicPartition tp) {
    super(new PartitionEntity(tp));
    _brokerId = brokerId;
  }

  /**
   * @return The id of the broker from which the metrics are from.
   */
  public int brokerId() {
    return _brokerId;
  }

  /**
   * This method serialize the metric sample using a simple protocol.
   * 1 byte  - version
   * 4 bytes - brokerId
   * 8 bytes - CPU Utilization
   * 8 bytes - DISK Utilization
   * 8 bytes - Network Inbound Utilization
   * 8 bytes - Network Outbound Utilization.
   * 8 bytes - Produce Request Rate
   * 8 bytes - Fetch Request Rate
   * 8 bytes - Messages In Per Sec
   * 8 bytes - Replication Bytes In Per Sec
   * 8 bytes - Replication Bytes Out Per Sec
   * 8 bytes - Sample time
   * 4 bytes - partition id
   * N bytes - topic string bytes
   * @return Serialized bytes.
   */
  public byte[] toBytes() {
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();
    byte[] topicStringBytes = entity().group().getBytes(UTF_8);
    // Allocate memory:
    ByteBuffer buffer = ByteBuffer.allocate(89 + topicStringBytes.length);
    buffer.put(LATEST_SUPPORTED_VERSION);
    buffer.putInt(_brokerId);
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(CPU_USAGE.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(DISK_USAGE.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(LEADER_BYTES_IN.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(LEADER_BYTES_OUT.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(PRODUCE_RATE.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(FETCH_RATE.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(MESSAGE_IN_RATE.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()).id()));
    buffer.putDouble(_valuesByMetricId.get(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()).id()));
    buffer.putLong(_sampleTimeMs);
    buffer.putInt(entity().tp().partition());
    buffer.put(topicStringBytes);
    return buffer.array();
  }

  /**
   * Deserialize given byte array into a partition metric sample.
   *
   * @param bytes Byte array for a partition metric sample.
   * @return Partition metric sample.
   */
  public static PartitionMetricSample fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    // Not used at this point.
    byte version = buffer.get();
    return switch (version) {
      case 0 -> readV0(buffer);
      case 1 -> readV1(buffer);
      default -> throw new UnknownVersionException("Unsupported deserialization version: " + version + " (Latest: "
              + LATEST_SUPPORTED_VERSION + ", Minimum: " + MIN_SUPPORTED_VERSION + ")");
    };
  }

  @Override
  public String toString() {
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();
    StringBuilder builder = new StringBuilder().append("{");
    for (Map.Entry<Short, Double> entry : _valuesByMetricId.entrySet()) {
      builder.append(metricDef.metricInfo(entry.getKey()).name())
          .append("=")
          .append(entry.getValue().toString())
          .append(", ");
    }
    builder.delete(builder.length() - 2, builder.length()).append("}");
    return String.format("[brokerId: %d, Partition: %s, time: %s, metrics: %s]", _brokerId, entity().tp(), utcDateFor(_sampleTimeMs), builder);
  }

  private static PartitionMetricSample readV0(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();
    int brokerId = buffer.getInt();
    int partition = buffer.getInt(45);
    String topic = new String(buffer.array(), 49, buffer.array().length - 49, UTF_8);
    PartitionMetricSample sample = new PartitionMetricSample(brokerId, new TopicPartition(topic, partition));
    sample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    sample.close(buffer.getLong());
    return sample;
  }

  private static PartitionMetricSample readV1(ByteBuffer buffer) {
    MetricDef metricDef = KafkaMetricDef.commonMetricDef();
    int brokerId = buffer.getInt();
    int partition = buffer.getInt(85);
    String topic = new String(buffer.array(), 89, buffer.array().length - 89, UTF_8);
    PartitionMetricSample sample = new PartitionMetricSample(brokerId, new TopicPartition(topic, partition));
    sample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(PRODUCE_RATE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(FETCH_RATE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), buffer.getDouble());
    sample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    sample.close(buffer.getLong());
    return sample;
  }
}
