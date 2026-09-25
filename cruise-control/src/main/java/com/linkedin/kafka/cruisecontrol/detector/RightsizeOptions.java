/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import java.util.Map;

import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * A class to indicate options intended to be used during application of a {@link Provisioner#rightsize(Map, RightsizeOptions)}.
 */
@InterfaceStability.Evolving
public final class RightsizeOptions {
    public RightsizeOptions() {}
}
