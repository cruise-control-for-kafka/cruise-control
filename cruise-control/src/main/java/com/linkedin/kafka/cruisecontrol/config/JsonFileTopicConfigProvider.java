/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * Abstract implementation of {@link TopicConfigProvider} which provides a method for loading cluster configurations
 * from a JSON file.
 */
public abstract class JsonFileTopicConfigProvider implements TopicConfigProvider {

    public static final String CLUSTER_CONFIGS_FILE = "cluster.configs.file";

    /**
     * Method which will find the file path from the supplied config map using the config key {@link #CLUSTER_CONFIGS_FILE} and
     * load the configs contained in that JSON file into a {@link java.util.Properties} instance.
     * <p>
     * The format of the file is JSON, with properties listed as top level key/value pairs:
     *
     * <pre>
     *   {
     *     "min.insync.replicas": 1,
     *     "an.example.cluster.config": false
     *   }
     * </pre>
     *
     * @param configs The map of config key/value pairs
     * @return A {@link java.util.Properties} instance containing the contents of the specified JSON config file.
     */
    protected static Properties loadClusterConfigs(Map<String, ?> configs) {
        String configFile = KafkaCruiseControlUtils.getRequiredConfig(configs, CLUSTER_CONFIGS_FILE);
        try {
            try (JsonReader reader = new JsonReader(new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8))) {
                Gson gson = new Gson();
                return gson.fromJson(reader, Properties.class);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
