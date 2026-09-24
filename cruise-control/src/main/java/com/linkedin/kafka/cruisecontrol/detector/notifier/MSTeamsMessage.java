/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

public final class MSTeamsMessage implements Serializable {
    private final Map<String, String> _facts;

    public MSTeamsMessage(Map<String, String> facts) {
        _facts = facts;
    }

    public Map<String, String> getFacts() {
        return _facts;
    }

    @Override
    public String toString() {
        return "{\"@type\": \"MessageCard\","
                + "\"@context\": \"http://schema.org/extensions\","
                + "\"themeColor\": \"0076D7\","
                + "\"summary\": \"Cruise-Control Alert\","
                + "\"sections\": [{"
                + "\"facts\": ["
                + _facts.entrySet().stream()
                .map(e -> "{\"name\": \"" + e.getKey() + "\", \"value\": \"" + e.getValue() + "\"}")
                .collect(Collectors.joining(","))
                + "]}]}";
    }
}
