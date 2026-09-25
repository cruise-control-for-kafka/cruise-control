/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.vertx;

import com.linkedin.cruisecontrol.http.CruiseControlHttpSession;
import io.vertx.ext.web.Session;

import java.util.Objects;

public class VertxSession implements CruiseControlHttpSession {

    private final Session _vertxSession;

    public VertxSession(Session vertxSession) {
        _vertxSession = vertxSession;
    }

    @Override
    public void invalidateSession() {
        _vertxSession.destroy();
    }

    @Override
    public long getLastAccessed() {
        return _vertxSession.lastAccessed();
    }

    @Override
    public String getSessionId() {
        return _vertxSession.id();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VertxSession that = (VertxSession) o;
        return _vertxSession.equals(that._vertxSession);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_vertxSession);
    }
}
