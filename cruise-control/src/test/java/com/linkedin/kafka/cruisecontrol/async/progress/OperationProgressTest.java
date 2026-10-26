/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.async.progress;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class OperationProgressTest {

  @Test
  public void testRefer() {
    OperationProgress progress1 = new OperationProgress();
    progress1.addStep(new Pending());
    OperationProgress progress2 = new OperationProgress();
    progress2.addStep(new WaitingForClusterModel());

    assertEquals(Pending.class, progress1.progress().get(0).getClass());
    progress1.refer(progress2);
    assertEquals(WaitingForClusterModel.class, progress1.progress().get(0).getClass());
    assertEquals(progress1.progress(), progress2.progress());
  }

  @Test
  public void testImmutableAfterRefer() {
    OperationProgress progress1 = new OperationProgress();
    OperationProgress progress2 = new OperationProgress();
    progress1.refer(progress2);

    try {
      progress1.addStep(new Pending());
      fail("Should have thrown IllegalStateException.");
    } catch (IllegalStateException ise) {
      // let it go.
    }

    try {
      progress1.refer(progress2);
      fail("Should have thrown IllegalStateException.");
    } catch (IllegalStateException ise) {
      // let it go.
    }

    try {
      progress1.clear();
      // After calling clear(), OperationProgress should be able to refer to another OperationProgress.
      progress1.refer(progress2);
    } catch (Exception e) {
      fail("Should not throw any Exception.");
    }
  }
}
