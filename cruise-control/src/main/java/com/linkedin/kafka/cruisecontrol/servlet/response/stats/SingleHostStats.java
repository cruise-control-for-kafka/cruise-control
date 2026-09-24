/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response.stats;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;

import java.util.Map;

@JsonResponseClass
public class SingleHostStats extends BasicStats {
  @JsonResponseField
  protected static final String HOST = "Host";
  @JsonResponseField
  protected static final String RACK = "Rack";
  private final String _host;
  private final String _rack;

  SingleHostStats(String host, String rack) {
    super();
    _host = host;
    _rack = rack;
  }

  /**
   * Return an object that can be further used to encode into JSON.
   *
   * @return The map describing host statistics.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> hostEntry = super.getJsonStructure();
    hostEntry.put(HOST, _host);
    hostEntry.put(RACK, _rack);
    return hostEntry;
  }
}
