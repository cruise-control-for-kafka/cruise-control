/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Unit test for {@link NoOpBrokerSetAssignmentPolicy}
 */
public class NoOpBrokerSetAssignmentPolicyTest {
  private static final NoOpBrokerSetAssignmentPolicy NO_OP_BROKER_SET_ASSIGNMENT_POLICY = new NoOpBrokerSetAssignmentPolicy();

  /**
   * Tests if no op policy does not make any assignment to missing brokers
   */
  @Test
  public void testBrokerSetAssignment() {
    Map<Integer, String> brokers = Map.of(0, "", 1, "", 2, "", 3, "", 4, "", 5, "");
    Map<String, Set<Integer>> mappedBrokers = Map.of("bs1", Set.of(0, 1, 2, 3, 4), "bs2", Set.of(5));
    Map<String, Set<Integer>> mappedBrokersAfterAssignment =
        NO_OP_BROKER_SET_ASSIGNMENT_POLICY.assignBrokerSetsForUnresolvedBrokers(brokers, mappedBrokers);

    assertNotNull(mappedBrokersAfterAssignment);

    assertEquals(mappedBrokersAfterAssignment, mappedBrokers);
  }

  /**
   * Tests if no op policy throws resolution exception when unassigned
   */
  @Test
  public void testBrokerSetAssignmentForUnmappedBrokers() {
    Map<Integer, String> brokers = Map.of(0, "", 1, "", 2, "", 3, "", 4, "", 5, "");
    Map<String, Set<Integer>> mappedBrokers = new HashMap<>();
    mappedBrokers.put("bs1", Set.of(0, 1, 2));
    mappedBrokers.put("bs2", Set.of(5));
    NO_OP_BROKER_SET_ASSIGNMENT_POLICY.assignBrokerSetsForUnresolvedBrokers(brokers, mappedBrokers);
    assertEquals(2, mappedBrokers.get(NoOpBrokerSetAssignmentPolicy.UNMAPPED_BROKER_SET_ID).size());
  }
}
