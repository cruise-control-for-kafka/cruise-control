/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.Resource;


/**
 * These are the resources derived from the base resources used by the various goals to perform balancing. This is a
 * super set of the Resources enumeration.
 */
public enum RawAndDerivedResource {
    DISK(Resource.DISK),
    CPU(Resource.CPU),
    LEADER_NW_IN(Resource.NW_IN),
    FOLLOWER_NW_IN(Resource.NW_IN),
    NW_OUT(Resource.NW_OUT),
    PWN_NW_OUT(Resource.NW_OUT),
    REPLICAS(null);

    private final Resource _derivedFrom;

    RawAndDerivedResource(Resource derivedFrom) {
        _derivedFrom = derivedFrom;
    }

    public Resource derivedFrom() {
        return _derivedFrom;
    }
}
