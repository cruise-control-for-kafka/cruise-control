/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.DiskStats;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@JsonResponseClass
public class SingleBrokerStats extends BasicStats {
  @JsonResponseField
  protected static final String HOST = "Host";
  @JsonResponseField
  protected static final String BROKER = "Broker";
  @JsonResponseField
  protected static final String RACK = "Rack";
  @JsonResponseField
  protected static final String BROKER_STATE = "BrokerState";
  @JsonResponseField(required = false)
  protected static final String DISK_STATE = "DiskState";
  protected final String _host;
  protected final int _id;
  protected final Broker.State _state;
  protected final boolean _isEstimated;
  protected final Map<String, DiskStats> _diskStatsByLogdir;
  protected final String _rack;

  SingleBrokerStats(Broker broker, double potentialBytesOutRate, boolean isEstimated) {
    super(broker, potentialBytesOutRate);
    _host = broker.host().name();
    _id = broker.id();
    _state = broker.state();
    _isEstimated = isEstimated;
    _diskStatsByLogdir = broker.diskStats();
    _rack = broker.rack().id();
  }

  public String host() {
    return _host;
  }

  public Broker.State state() {
    return _state;
  }

  public int id() {
    return _id;
  }

  public String rack() {
    return _rack;
  }

  /**
   * Get per-logdir disk statistics of the broker.
   *
   * @return The per-logdir disk statistics. This method is relevant only when the
   *         {@link com.linkedin.kafka.cruisecontrol.model.ClusterModel} has been created with a request to populate
   *         replica placement info, otherwise returns an empty map.
   */
  public Map<String, DiskStats> diskStatsByLogdir() {
    return Collections.unmodifiableMap(_diskStatsByLogdir);
  }

  /**
   * @return {@code true} if the broker capacity has been estimated, {@code false} otherwise.
   */
  public boolean isEstimated() {
    return _isEstimated;
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> entry = super.getJsonStructure();
    entry.put(HOST, _host);
    entry.put(BROKER, _id);
    entry.put(BROKER_STATE, _state);
    if (!_diskStatsByLogdir.isEmpty()) {
      Map<String, Object> diskStates = new HashMap<>();
      _diskStatsByLogdir.forEach((k, v) -> diskStates.put(k, v.getJsonStructure()));
      entry.put(DISK_STATE, diskStates);
    }
    entry.put(RACK, _rack);
    return entry;
  }
}
