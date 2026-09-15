/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.cruisecontrol.http.CruiseControlHttpSession;
import jakarta.servlet.http.HttpSession;
import java.util.Objects;

public class ServletSession implements CruiseControlHttpSession {

    private final HttpSession _httpSession;

    public ServletSession(HttpSession httpSession) {
        _httpSession = httpSession;
    }

    @Override
    public void invalidateSession() {
        _httpSession.invalidate();
    }

    @Override
    public long getLastAccessed() {
        return _httpSession.getLastAccessedTime();
    }

    @Override
    public String getSessionId() {
        return _httpSession.getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServletSession that = (ServletSession) o;
        return _httpSession.equals(that._httpSession);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_httpSession);
    }
}
