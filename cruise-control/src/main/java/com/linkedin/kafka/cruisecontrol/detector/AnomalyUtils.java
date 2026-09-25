/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * A util class for anomalies.
 */
public final class AnomalyUtils {

  private AnomalyUtils() {

  }

  /**
   * Extract {@link KafkaCruiseControl} instance from configs.
   *
   * @param configs The configs used to configure anomaly instance.
   * @param anomalyType The type of anomaly.
   * @return The extracted {@link KafkaCruiseControl} instance.
   */
  public static KafkaCruiseControl extractKafkaCruiseControlObjectFromConfig(Map<String, ?> configs,
                                                                            AnomalyType anomalyType) {
    return (KafkaCruiseControl) validateNotNull(configs.get(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG),
            () -> String.format("Missing %s when creating anomaly of type %s.", KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, anomalyType));
  }

  /**
   * Check whether the load monitor state is ready -- i.e. not in loading or bootstrapping state.
   *
   * @param loadMonitorTaskRunnerState Load monitor task runner state.
   * @return {@code true} if the load monitor is ready, {@code false} otherwise.
   */
  public static boolean isLoadMonitorReady(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorTaskRunnerState) {
    return loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING
            || loadMonitorTaskRunnerState == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.BOOTSTRAPPING;
  }

  /**
   * Build a regular expression which can match and only match given set of strings.
   * @param stringsToMatch The set of strings to be matched.
   * @return The matching regular expression.
   */
  public static Pattern buildTopicRegex(Set<String> stringsToMatch) {
    StringJoiner sj = new StringJoiner("|");
    stringsToMatch.forEach(sj::add);
    return Pattern.compile(sj.toString());
  }

  /**
   * Parse and validate the integer value for specific config in provided config map.
   * @param config The provided config map.
   * @param key The name of config to check.
   * @param defaultValue The default value of the config to check.
   * @param illegalValueCheck The check of config value.
   * @return The value of the config.
   */
  public static int parseAndGetConfig(Map<String, Object> config,
                                      String key,
                                      int defaultValue,
                                      Predicate<Integer> illegalValueCheck) {
    String valueString = (String) config.get(key);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      Integer value = Integer.parseInt(valueString);
      if (illegalValueCheck.test(value)) {
        throw new ConfigException(key, value);
      }
      return value;
    } catch (NumberFormatException e) {
      throw new ConfigException(key, valueString, e.getMessage());
    }
  }

  /**
   * Parse and validate the double value for specific config in provided config map.
   * @param config The provided config map.
   * @param key The name of config to check.
   * @param defaultValue The default value of the config to check.
   * @param illegalValueCheck The check of config value.
   * @return The value of the config.
   */
  public static double parseAndGetConfig(Map<String, Object> config,
                                         String key,
                                         double defaultValue,
                                         Predicate<Double> illegalValueCheck) {
    String valueString = (String) config.get(key);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      Double value = Double.parseDouble(valueString);
      if (illegalValueCheck.test(value)) {
        throw new ConfigException(key, value);
      }
      return value;
    } catch (NumberFormatException e) {
      throw new ConfigException(key, valueString, e.getMessage());
    }
  }
}
