/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;

import java.util.Map;


/**
 * A no-op provisioner, which ignores expansion / shrinking requests.
 */
public class NoopProvisioner implements Provisioner {
    @Override
    public ProvisionerState rightsize(Map<String, ProvisionRecommendation> recommendationByRecommender, RightsizeOptions rightsizeOptions) {
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
