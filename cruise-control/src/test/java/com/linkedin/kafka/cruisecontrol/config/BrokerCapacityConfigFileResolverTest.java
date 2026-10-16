/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.BROKER_CAPACITY_FETCH_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit test for {@link BrokerCapacityConfigFileResolver}
 */
public class BrokerCapacityConfigFileResolverTest {
  private static BrokerCapacityConfigResolver getBrokerCapacityConfigResolver(String configFileName, Class<?> klass) {
    BrokerCapacityConfigResolver configResolver = new BrokerCapacityConfigFileResolver();
    String fileName = Objects.requireNonNull(klass.getClassLoader().getResource(configFileName)).getFile();
    Map<String, String> configs = Collections.singletonMap(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, fileName);
    configResolver.configure(configs);

    return configResolver;
  }

  @Test(expected = BrokerCapacityResolutionException.class)
  public void testParseConfigFile() throws TimeoutException, BrokerCapacityResolutionException {
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver("testCapacityConfig.json", this.getClass());

    assertEquals(200000.0, configResolver.capacityForBroker("", "", 0, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false)
                                         .capacity().get(Resource.NW_IN), 0.01);
    assertEquals(100000.0, configResolver.capacityForBroker("", "", 2, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true)
                                         .capacity().get(Resource.NW_IN), 0.01);
    try {
      configResolver.capacityForBroker("", "", BrokerCapacityConfigFileResolver.DEFAULT_CAPACITY_BROKER_ID, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false);
      fail("Should have thrown exception for negative broker id");
    } catch (IllegalArgumentException e) {
      // let it go
    }

    assertTrue(configResolver.capacityForBroker("", "", 2, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).isEstimated());
    assertFalse(configResolver.capacityForBroker("", "", 2, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).estimationInfo().isEmpty());

    // If resolver is unable to get broker capacity and not allowed to estimate, a BrokerCapacityResolutionException will be thrown.
    configResolver.capacityForBroker("", "", 2, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false);
  }

  @Test
  public void testParseConfigJbodFile() throws TimeoutException, BrokerCapacityResolutionException {
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver("testCapacityConfigJBOD.json", this.getClass());

    assertEquals(2000000.0, configResolver.capacityForBroker("", "", 0, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false)
                                         .capacity().get(Resource.DISK), 0.01);
    assertEquals(2200000.0, configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true)
                                         .capacity().get(Resource.DISK), 0.01);
    assertEquals(200000.0, configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true)
                                         .diskCapacityByLogDir().get("/tmp/kafka-logs-4"), 0.01);

    assertFalse(configResolver.capacityForBroker("", "", 2, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).isEstimated());
    assertFalse(configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).estimationInfo().isEmpty());
  }

  @Test
  public void testParseConfigCoresFile() throws TimeoutException, BrokerCapacityResolutionException {
    BrokerCapacityConfigResolver configResolver = getBrokerCapacityConfigResolver("testCapacityConfigCores.json", this.getClass());

    assertEquals(BrokerCapacityConfigFileResolver.DEFAULT_CPU_CAPACITY_WITH_CORES, configResolver
        .capacityForBroker("", "", 0, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false).capacity().get(Resource.CPU), 0.01);
    assertEquals(BrokerCapacityConfigFileResolver.DEFAULT_CPU_CAPACITY_WITH_CORES, configResolver
        .capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).capacity().get(Resource.CPU), 0.01);

    assertEquals(8, configResolver.capacityForBroker("", "", 0, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false).numCpuCores(), 0.01);
    assertEquals(64, configResolver.capacityForBroker("", "", 1, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false).numCpuCores(), 0.01);
    assertEquals(2.5, configResolver.capacityForBroker("", "", 2, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).numCpuCores(), 0.01);
    assertEquals(16, configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).numCpuCores(), 0.01);

    assertFalse(configResolver.capacityForBroker("", "", 1, BROKER_CAPACITY_FETCH_TIMEOUT_MS, false).isEstimated());
    assertTrue(configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).isEstimated());
    assertFalse(configResolver.capacityForBroker("", "", 3, BROKER_CAPACITY_FETCH_TIMEOUT_MS, true).estimationInfo().isEmpty());
  }
}
