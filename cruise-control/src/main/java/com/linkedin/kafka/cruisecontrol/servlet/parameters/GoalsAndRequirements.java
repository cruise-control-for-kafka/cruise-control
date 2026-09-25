/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;

import java.util.Collections;
import java.util.List;


/**
 * A helper class for goals and requirements.
 */
public class GoalsAndRequirements {
    protected final List<String> _goals;
    protected final ModelCompletenessRequirements _requirements;

    public GoalsAndRequirements(List<String> goals, ModelCompletenessRequirements requirements) {
        // An empty list indicates the default goals.
        _goals = goals;
        _requirements = requirements;
    }

    public List<String> goals() {
        return Collections.unmodifiableList(_goals);
    }

    public ModelCompletenessRequirements requirements() {
        return _requirements;
    }
}
