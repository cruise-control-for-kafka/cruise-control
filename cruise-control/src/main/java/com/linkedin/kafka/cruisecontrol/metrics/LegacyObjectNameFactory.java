/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metrics;

import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * {@link ObjectNameFactory} that uses the legacy method of generating metric names.
 * <p>
 * {@code io.dropwizard.metrics} updated the default behaviour in
 * {@link com.codahale.metrics.jmx.DefaultObjectNameFactory} in {@code v4.1.0-rc2} and the metric
 * type is also added to the metric name breaking existing metrics.
 * </p>
 * To keep backward compatibility we are maintaining the old name generator.
 *
 * @see <a href="https://github.com/dropwizard/metrics/pull/1310">dropwizard/metrics/pull/1310</a>
 * @see <a href="https://github.com/dropwizard/metrics/releases/tag/v4.1.0-rc2">v4.1.0-rc2</a>
 */
public final class LegacyObjectNameFactory implements ObjectNameFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxReporter.class);
    private static final ObjectNameFactory INSTANCE = new LegacyObjectNameFactory();

    private LegacyObjectNameFactory() {
    }

    public static ObjectNameFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public ObjectName createName(String type, String domain, String name) {
        try {
            ObjectName objectName = new ObjectName(domain, "name", name);
            if (objectName.isPattern()) {
                objectName = new ObjectName(domain, "name", ObjectName.quote(name));
            }
            return objectName;
        } catch (MalformedObjectNameException e) {
            try {
                return new ObjectName(domain, "name", ObjectName.quote(name));
            } catch (MalformedObjectNameException mone) {
                LOGGER.warn("Unable to register {} {}", type, name, mone);
                throw new RuntimeException(mone);
            }
        }
    }
}
