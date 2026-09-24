/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.purgatory;

import java.util.Collections;
import java.util.List;


/**
 * Possible status of requests in {@link Purgatory}.
 */
public enum ReviewStatus {
    PENDING_REVIEW, APPROVED, SUBMITTED, DISCARDED;

    private static final List<ReviewStatus> CACHED_VALUES = List.of(values());

    /**
     * Use this instead of values() because values() creates a new array each time.
     *
     * @return enumerated values in the same order as values()
     */
    public static List<ReviewStatus> cachedValues() {
        return CACHED_VALUES;
    }
}
