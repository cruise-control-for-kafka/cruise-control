/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The interface for {@link Executor} to use to send notifications.
 */
@InterfaceStability.Evolving
public interface ExecutorNotifier extends CruiseControlConfigurable {
    /**
     * Send out a notification when needed.
     *
     * @param message Information to be sent.
     */
    void sendNotification(String message);

    /**
     * Send out an alert when needed.
     *
     * @param alertMessage Information to be sent.
     */
    void sendAlert(String alertMessage);
}
