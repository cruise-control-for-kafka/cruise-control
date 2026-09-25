/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;

import java.util.Set;

import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * The finder which checks violation of user-defined topic properties.
 */
@InterfaceStability.Evolving
public interface TopicAnomalyFinder extends CruiseControlConfigurable {

    /**
     * Get a collection of topic anomalies for topics which are misconfigured or violate user-defined topic properties.
     *
     * @return Set of topic anomalies.
     */
    Set<TopicAnomaly> topicAnomalies();
}
