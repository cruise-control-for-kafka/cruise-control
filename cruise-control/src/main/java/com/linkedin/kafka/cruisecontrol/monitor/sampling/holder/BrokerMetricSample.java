/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.nio.ByteBuffer;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.charset.StandardCharsets;

import static com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef.*;


/**
 * The class hosting all the broker level metrics in {@link KafkaMetricDef}.
 */
public class BrokerMetricSample extends MetricSample<String, BrokerEntity> {
  public static final byte MIN_SUPPORTED_VERSION = 4;
  public static final byte LATEST_SUPPORTED_VERSION = 5;
  private final byte _deserializationVersion;

  /**
   * Create a broker metric sample with the given host name, broker id, and version to be used in deserialization.
   *
   * @param host Host name.
   * @param brokerId Broker Id.
   * @param deserializationVersion Version used in serialization that shows the latest version that a deserializer should use.
   */
  public BrokerMetricSample(String host, Integer brokerId, byte deserializationVersion) throws UnknownVersionException {
    super(new BrokerEntity(host, brokerId));
    if (host != null && host.length() >= Short.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("The length of host name %s is %d, which is longer than "
                                                           + "the max allowed length of %d", host, host.length(),
                                                       Short.MAX_VALUE));
    }

    if (deserializationVersion < MIN_SUPPORTED_VERSION || deserializationVersion > LATEST_SUPPORTED_VERSION) {
      throw new UnknownVersionException("Unsupported serialization version: " + deserializationVersion + " (Latest: "
                                        + LATEST_SUPPORTED_VERSION + ", Minimum: " + MIN_SUPPORTED_VERSION + ")");
    }

    _deserializationVersion = deserializationVersion;
  }

  public byte deserializationVersion() {
    return _deserializationVersion;
  }

  public int brokerId() {
    return entity().brokerId();
  }

  /**
   * Serialize the broker metric sample using the {@link #LATEST_SUPPORTED_VERSION} protocol. The version field
   * indicates the version that a deserializer should use.
   *
   * 1 byte - version
   * 4 bytes - broker ID
   * 2 bytes - host name length
   * N bytes - host name
   * 8 bytes - broker cpu utilization.
   * 8 bytes - broker leader bytes in rate
   * 8 bytes - broker leader bytes out rate
   * 8 bytes - broker replication bytes in rate
   * 8 bytes - broker replication bytes out rate
   * 8 bytes - broker messages in rate
   * 8 bytes - broker produce request rate
   * 8 bytes - broker consumer fetch request rate
   * 8 bytes - broker replication fetch request rate
   * 8 bytes - broker request handler average idle percent
   * 8 bytes - broker disk utilization
   * 8 bytes - all topics produce request rate
   * 8 bytes - all topics fetch request rate
   * 8 bytes - sample time
   * 4 bytes - broker request queue size
   * 4 bytes - broker response queue size
   * 8 bytes - broker produce request queue time ms (max)
   * 8 bytes - broker produce request queue time ms (mean)
   * 8 bytes - broker consumer fetch request queue time ms (max)
   * 8 bytes - broker consumer fetch request queue time ms (mean)
   * 8 bytes - broker follower fetch request queue time ms (max)
   * 8 bytes - broker follower fetch request queue time ms (mean)
   * 8 bytes - broker produce total time ms (max)
   * 8 bytes - broker produce total time ms (mean)
   * 8 bytes - broker consumer fetch total time ms (max)
   * 8 bytes - broker consumer fetch total time ms (mean)
   * 8 bytes - broker follower fetch total time ms (max)
   * 8 bytes - broker follower fetch total time ms (mean)
   * 8 bytes - broker produce local time ms (max)
   * 8 bytes - broker produce local time ms (mean)
   * 8 bytes - broker consumer fetch local time ms (max)
   * 8 bytes - broker consumer fetch local time ms (mean)
   * 8 bytes - broker follower fetch local time ms (max)
   * 8 bytes - broker follower fetch local time ms (mean)
   * 8 bytes - broker log flush rate
   * 8 bytes - broker log flush time ms (max)
   * 8 bytes - broker log flush time ms (mean)
   * 8 bytes - broker produce request queue time ms (50TH percentile)
   * 8 bytes - broker produce request queue time ms (999TH percentile)
   * 8 bytes - broker consumer fetch request queue time ms (50TH percentile)
   * 8 bytes - broker consumer fetch request queue time ms (999TH percentile)
   * 8 bytes - broker follower fetch request queue time ms (50TH percentile)
   * 8 bytes - broker follower fetch request queue time ms (999TH percentile)
   * 8 bytes - broker produce total time ms (50TH percentile)
   * 8 bytes - broker produce total time ms (999TH percentile)
   * 8 bytes - broker consumer fetch total time ms (50TH percentile)
   * 8 bytes - broker consumer fetch total time ms (999TH percentile)
   * 8 bytes - broker follower fetch total time ms (50TH percentile)
   * 8 bytes - broker follower fetch total time ms (999TH percentile)
   * 8 bytes - broker produce local time ms (50TH percentile)
   * 8 bytes - broker produce local time ms (999TH percentile)
   * 8 bytes - broker consumer fetch local time ms (50TH percentile)
   * 8 bytes - broker consumer fetch local time ms (999TH percentile)
   * 8 bytes - broker follower fetch local time ms (50TH percentile)
   * 8 bytes - broker follower fetch local time ms (999TH percentile)
   * 8 bytes - broker log flush time ms (50TH percentile)
   * 8 bytes - broker log flush time ms (999TH percentile)
   * @return The serialized bytes.
   */
  public byte[] toBytes() {
    byte[] hostBytes = (entity().group() != null ? entity().group() : "UNKNOWN").getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(457 + hostBytes.length);
    buffer.put(_deserializationVersion);
    buffer.putInt(entity().brokerId());
    buffer.putShort((short) hostBytes.length);
    buffer.put(hostBytes);
    buffer.putDouble(metricValue(CPU_USAGE));
    buffer.putDouble(metricValue(LEADER_BYTES_IN));
    buffer.putDouble(metricValue(LEADER_BYTES_OUT));
    buffer.putDouble(metricValue(REPLICATION_BYTES_IN_RATE));
    buffer.putDouble(metricValue(REPLICATION_BYTES_OUT_RATE));
    buffer.putDouble(metricValue(MESSAGE_IN_RATE));
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_RATE));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_RATE));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_RATE));
    buffer.putDouble(metricValue(BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT));
    buffer.putDouble(metricValue(DISK_USAGE));
    buffer.putDouble(metricValue(PRODUCE_RATE));
    buffer.putDouble(metricValue(FETCH_RATE));
    buffer.putLong(_sampleTimeMs);
    buffer.putInt(metricValue(BROKER_REQUEST_QUEUE_SIZE).intValue());
    buffer.putInt(metricValue(BROKER_RESPONSE_QUEUE_SIZE).intValue());
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_PRODUCE_TOTAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_PRODUCE_LOCAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_RATE));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_TIME_MS_MAX));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_TIME_MS_MEAN));
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_PRODUCE_TOTAL_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_PRODUCE_TOTAL_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_PRODUCE_LOCAL_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_PRODUCE_LOCAL_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_TIME_MS_50TH));
    buffer.putDouble(metricValue(BROKER_LOG_FLUSH_TIME_MS_999TH));
    return buffer.array();
  }

  /**
   * Deserialize the bytes to get a broker metric data.
   * @param bytes the bytes to deserialize.
   * @return The deserialized broker metric sample.
   * @throws UnknownVersionException
   */
  public static BrokerMetricSample fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte version = buffer.get();

    return switch (version) {
      case 4 -> readV4(buffer);
      case 5 -> readV5(buffer);
      default -> throw new UnknownVersionException("Unsupported deserialization version: " + version + " (Latest: "
              + LATEST_SUPPORTED_VERSION + ", Minimum: " + MIN_SUPPORTED_VERSION + ")");
    };
  }

  @Override
  protected MetricDef metricDefForToString() {
    return KafkaMetricDef.brokerMetricDef();
  }

  /**
   * Get the metric for the given resource.
   *
   * @param resource The resource type.
   * @return The metric for the given resource.
   */
  public Double metricFor(Resource resource) {
    return switch (resource) {
      case CPU -> metricValue(CPU_USAGE);
      case DISK -> metricValue(DISK_USAGE);
      case NW_IN -> metricValue(LEADER_BYTES_IN) + metricValue(REPLICATION_BYTES_IN_RATE);
      case NW_OUT -> metricValue(LEADER_BYTES_OUT) + metricValue(REPLICATION_BYTES_OUT_RATE);
    };
  }

  public Double metricValue(KafkaMetricDef kafkaMetricDef) {
    return _valuesByMetricId.get(KafkaMetricDef.brokerMetricDef().metricInfo(kafkaMetricDef.name()).id());
  }

  /**
   * Populate the given broker metric sample with v4 buffer deserialization and return the sample time.
   *
   * @param buffer Buffer to deserialize.
   * @param brokerMetricSample Broker metric sample to populate.
   * @return Sample time.
   */
  private static long populateV4BrokerMetricSample(ByteBuffer buffer, BrokerMetricSample brokerMetricSample) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();

    brokerMetricSample.record(metricDef.metricInfo(CPU_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_IN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(LEADER_BYTES_OUT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(REPLICATION_BYTES_OUT_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(MESSAGE_IN_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_HANDLER_POOL_IDLE_PERCENT.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(DISK_USAGE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(PRODUCE_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(FETCH_RATE.name()), buffer.getDouble());
    long sampleTime = buffer.getLong();
    brokerMetricSample.record(metricDef.metricInfo(BROKER_REQUEST_QUEUE_SIZE.name()), buffer.getInt());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_RESPONSE_QUEUE_SIZE.name()), buffer.getInt());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_RATE.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_MAX.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_MEAN.name()), buffer.getDouble());

    return sampleTime;
  }

  private static BrokerMetricSample readV4(ByteBuffer buffer) throws UnknownVersionException {
    int brokerId = buffer.getInt();
    int hostLength = buffer.getShort();
    byte[] hostBytes = new byte[hostLength];
    buffer.get(hostBytes);
    String host = new String(hostBytes, StandardCharsets.UTF_8);
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample(host, brokerId, (byte) 4);

    long sampleTime = populateV4BrokerMetricSample(buffer, brokerMetricSample);
    if (sampleTime >= 0) {
      brokerMetricSample.close(sampleTime);
    }
    return brokerMetricSample;
  }

  /**
   * Populate the given broker metric sample with v5 buffer deserialization and return the sample time.
   *
   * @param buffer Buffer to deserialize.
   * @param brokerMetricSample Broker metric sample to populate.
   * @return Sample time.
   */
  private static long populateV5BrokerMetricSample(ByteBuffer buffer, BrokerMetricSample brokerMetricSample) {
    MetricDef metricDef = KafkaMetricDef.brokerMetricDef();
    long sampleTime = populateV4BrokerMetricSample(buffer, brokerMetricSample);

    // Metrics added from v4 -> v5.
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_TOTAL_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_PRODUCE_LOCAL_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_50TH.name()), buffer.getDouble());
    brokerMetricSample.record(metricDef.metricInfo(BROKER_LOG_FLUSH_TIME_MS_999TH.name()), buffer.getDouble());

    return sampleTime;
  }

  private static BrokerMetricSample readV5(ByteBuffer buffer) throws UnknownVersionException {
    int brokerId = buffer.getInt();
    int hostLength = buffer.getShort();
    byte[] hostBytes = new byte[hostLength];
    buffer.get(hostBytes);
    String host = new String(hostBytes, StandardCharsets.UTF_8);
    BrokerMetricSample brokerMetricSample = new BrokerMetricSample(host, brokerId, (byte) 5);

    long sampleTime = populateV5BrokerMetricSample(buffer, brokerMetricSample);

    if (sampleTime >= 0) {
      brokerMetricSample.close(sampleTime);
    }
    return brokerMetricSample;
  }
}
