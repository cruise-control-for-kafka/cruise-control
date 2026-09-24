/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * A class to keep Cruise Control User Task Manager Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class UserTaskManagerConfig {

    /**
     * <code>max.cached.completed.kafka.monitor.user.tasks</code>
     */
    public static final String MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS_CONFIG = "max.cached.completed.kafka.monitor.user.tasks";
    public static final Integer DEFAULT_MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS = null;
    public static final String MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS_DOC = "The maximum number of completed kafka monitoring "
            + "user tasks for which the response and access details will be cached. If this config is missing, the value set in config "
            + "max.cached.completed.user.tasks will be used.";

    /**
     * <code>max.cached.completed.cruise.control.monitor.user.tasks</code>
     */
    public static final String MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS_CONFIG =
            "max.cached.completed.cruise.control.monitor.user.tasks";
    public static final Integer DEFAULT_MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS = null;
    public static final String MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS_DOC = "The maximum number of completed "
            + "Cruise Control monitoring user tasks for which the response and access details will be cached. If this config is "
            + "missing, the value set in config max.cached.completed.user.tasks will be used.";

    /**
     * <code>max.cached.completed.kafka.admin.user.tasks</code>
     */
    public static final String MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS_CONFIG = "max.cached.completed.kafka.admin.user.tasks";
    public static final Integer DEFAULT_MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS = null;
    public static final String MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS_DOC = "The maximum number of completed kafka administration "
            + "user tasks for which the response and access details will be cached. If this config is missing, the value set in config "
            + "max.cached.completed.user.tasks will be used.";

    /**
     * <code>max.cached.completed.cruise.control.admin.user.tasks</code>
     */
    public static final String MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS_CONFIG =
            "max.cached.completed.cruise.control.admin.user.tasks";
    public static final Integer DEFAULT_MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS = null;
    public static final String MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS_DOC = "The maximum number of completed "
            + "cruise control administration user tasks for which the response and access details will be cached. If this config is "
            + "missing, the value set in config max.cached.completed.user.tasks will be used.";

    /**
     * <code>max.cached.completed.user.tasks</code>
     */
    public static final String MAX_CACHED_COMPLETED_USER_TASKS_CONFIG = "max.cached.completed.user.tasks";
    public static final int DEFAULT_MAX_CACHED_COMPLETED_USER_TASKS = 25;
    public static final String MAX_CACHED_COMPLETED_USER_TASKS_DOC = "The fallback maximum number of completed user tasks of"
            + "certain type for which the response and access details will be cached. This config will be used if more specific "
            + "config for certain user task type is not set (e.g. MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS_CONFIG).";

    public static final String COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG =
            "completed.kafka.monitor.user.task.retention.time.ms";
    public static final Long DEFAULT_COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS = null;
    public static final String COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds "
            + "to store the response and access details of a completed kafka monitoring user task. If this config is missing, "
            + "the value set in config completed.user.task.retention.time.ms will be used.";

    /**
     * <code>completed.cruise.control.monitor.user.task.retention.time.ms</code>
     */
    public static final String COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG =
            "completed.cruise.control.monitor.user.task.retention.time.ms";
    public static final Long DEFAULT_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS = null;
    public static final String COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds "
            + "to store the response and access details of a completed cruise control monitoring user task. If this config is missing, "
            + "the value set in config completed.user.task.retention.time.ms will be used.";

    /**
     * <code>completed.kafka.admin.user.task.retention.time.ms</code>
     */
    public static final String COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS_CONFIG =
            "completed.kafka.admin.user.task.retention.time.ms";
    public static final Long DEFAULT_COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS = null;
    public static final String COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds "
            + "to store the response and access details of a completed kafka administration user task. If this config is missing, "
            + "the value set in config completed.user.task.retention.time.ms will be used.";

    /**
     * <code>completed.cruise.control.admin.user.task.retention.time.ms</code>
     */
    public static final String COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS_CONFIG =
            "completed.cruise.control.admin.user.task.retention.time.ms";
    public static final Long DEFAULT_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS = null;
    public static final String COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds "
            + "to store the response and access details of a completed cruise control administration user task. If this config is "
            + "missing, the value set in config completed.user.task.retention.time.ms will be used.";

    /**
     * <code>completed.user.task.retention.time.ms</code>
     */
    public static final String COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG = "completed.user.task.retention.time.ms";
    public static final long DEFAULT_COMPLETED_USER_TASK_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(24);
    public static final String COMPLETED_USER_TASK_RETENTION_TIME_MS_DOC = "The fallback maximum time in milliseconds to store "
            + "the response and access details of a completed user task if more specific config for certain user task type is not set"
            + " (e.g. COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG).";

    private UserTaskManagerConfig() {
    }

    /**
     * Define configs for User Task Manager.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the configs for User Task Manager.
     */
    public static ConfigDef define(ConfigDef configDef) {
        return configDef.define(MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS,
                        ConfigDef.Importance.MEDIUM,
                        MAX_CACHED_COMPLETED_KAFKA_MONITOR_USER_TASKS_DOC)
                .define(MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS,
                        ConfigDef.Importance.MEDIUM,
                        MAX_CACHED_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASKS_DOC)
                .define(MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS,
                        ConfigDef.Importance.MEDIUM,
                        MAX_CACHED_COMPLETED_KAFKA_ADMIN_USER_TASKS_DOC)
                .define(MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS,
                        ConfigDef.Importance.MEDIUM,
                        MAX_CACHED_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASKS_DOC)
                .define(MAX_CACHED_COMPLETED_USER_TASKS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_CACHED_COMPLETED_USER_TASKS,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        MAX_CACHED_COMPLETED_USER_TASKS_DOC)
                .define(COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS,
                        ConfigDef.Importance.MEDIUM,
                        COMPLETED_KAFKA_MONITOR_USER_TASK_RETENTION_TIME_MS_DOC)
                .define(COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS,
                        ConfigDef.Importance.MEDIUM,
                        COMPLETED_CRUISE_CONTROL_MONITOR_USER_TASK_RETENTION_TIME_MS_DOC)
                .define(COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS,
                        ConfigDef.Importance.MEDIUM,
                        COMPLETED_KAFKA_ADMIN_USER_TASK_RETENTION_TIME_MS_DOC)
                .define(COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS,
                        ConfigDef.Importance.MEDIUM,
                        COMPLETED_CRUISE_CONTROL_ADMIN_USER_TASK_RETENTION_TIME_MS_DOC)
                .define(COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_COMPLETED_USER_TASK_RETENTION_TIME_MS,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        COMPLETED_USER_TASK_RETENTION_TIME_MS_DOC);
    }
}
