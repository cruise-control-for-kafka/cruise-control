/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;


public class ReplicaPlacementInfo {
    private final int _brokerId;
    private final String _logdir;

    public ReplicaPlacementInfo(int brokerId, String logdir) {
        _brokerId = brokerId;
        _logdir = logdir;
    }

    public ReplicaPlacementInfo(Integer brokerId) {
        this(brokerId, null);
    }

    public Integer brokerId() {
        return _brokerId;
    }

    public String logdir() {
        return _logdir;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReplicaPlacementInfo)) {
            return false;
        }
        AtomicReference<ReplicaPlacementInfo> info = new AtomicReference<>((ReplicaPlacementInfo) o);
        return _brokerId == info.get()._brokerId && Objects.equals(_logdir, info.get()._logdir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_brokerId, _logdir);
    }

    @Override
    public String toString() {
        if (_logdir == null) {
            return String.format("{Broker: %d}", _brokerId);
        } else {
            return String.format("{Broker: %d, Logdir: %s}", _brokerId, _logdir);
        }
    }
}
