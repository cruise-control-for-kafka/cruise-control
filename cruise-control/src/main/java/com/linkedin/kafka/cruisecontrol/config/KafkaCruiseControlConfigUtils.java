/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;


public final class KafkaCruiseControlConfigUtils {

    private KafkaCruiseControlConfigUtils() {}

    /**
     * Get a configured instance of the given concrete class. If the object implements {@link CruiseControlConfigurable},
     * configure it using the configuration.
     *
     * @param c       The concrete class of the returned instance
     * @param t       The interface of the returned instance
     * @param configs Configuration to used to config the returned instance.
     * @param <T>     The type of the instance to be returned.
     * @return A configured instance of the class c
     */
    public static <T> T getConfiguredInstance(Class<?> c, Class<T> t, Map<String, Object> configs) {
        if (c == null) {
            throw new IllegalArgumentException("Attempt to get configured instance of null configuration " + t.getName());
        }

        Object instance;
        try {
            instance = c.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find a public no-argument constructor for " + c.getName(), e);
        }

        if (!t.isInstance(instance)) {
            throw new IllegalArgumentException(c.getName() + " is not an instance of " + t.getName());
        }
        T o = t.cast(instance);
        if (o instanceof CruiseControlConfigurable) {
            ((CruiseControlConfigurable) o).configure(configs);
        }
        return o;
    }
}
