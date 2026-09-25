/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.exception.SamplingException;

import java.time.Duration;
import java.util.Set;

import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * This interface is for users to implement a reader for retrieving {@link MaintenanceEvent maintenance events} from the
 * user-defined store.
 *
 * <p>Maintenance events will be used by {@link MaintenanceEventDetector}.</p>
 */
@InterfaceStability.Evolving
public interface MaintenanceEventReader extends CruiseControlConfigurable, AutoCloseable {

    /**
     * Retrieve {@link MaintenanceEvent maintenance events} from the user-defined store. On each read, the event reader
     * is expected to continue retrieving events (if any) from where it left off.
     * <p>
     * This method returns immediately when maintenance events are available. Otherwise, it will wait as long as the
     * timeout, and return an empty set.
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     * @return Set of maintenance events, or empty set if none is available after the given timeout expires.
     */
    Set<MaintenanceEvent> readEvents(Duration timeout) throws SamplingException;
}
