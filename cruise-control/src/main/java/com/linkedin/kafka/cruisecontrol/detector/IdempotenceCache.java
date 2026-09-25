/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A cache to facilitate idempotency support to handle duplicate maintenance events.
 */
public class IdempotenceCache {
    private static final Logger LOG = LoggerFactory.getLogger(IdempotenceCache.class);
    private final Duration _idempotenceRetention;
    private final ScheduledExecutorService _idempotenceCacheCleaner;
    private final Map<MaintenanceEvent, Long> _timeByMaintenanceEvent;
    private final KafkaCruiseControl _kafkaCruiseControl;

    public IdempotenceCache(KafkaCruiseControl kafkaCruiseControl, Duration cleanerPeriod, Duration cleanerInitialDelay) {
        _kafkaCruiseControl = kafkaCruiseControl;
        KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
        _idempotenceRetention = Duration.ofMillis(config.getLong(AnomalyDetectorConfig.MAINTENANCE_EVENT_IDEMPOTENCE_RETENTION_MS_CONFIG));
        int maxIdempotenceCacheSize = config.getInt(AnomalyDetectorConfig.MAINTENANCE_EVENT_MAX_IDEMPOTENCE_CACHE_SIZE_CONFIG);
        _timeByMaintenanceEvent = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<MaintenanceEvent, Long> eldest) {
                return this.size() > maxIdempotenceCacheSize;
            }
        };
        _idempotenceCacheCleaner = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("IdempotenceCacheCleaner"));
        _idempotenceCacheCleaner.scheduleAtFixedRate(new IdempotenceCacheCleaner(),
                cleanerInitialDelay.toMillis(),
                cleanerPeriod.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private synchronized void removeOldEventsFromIdempotenceCache() {
        LOG.debug("Checking idempotence cache ({}) for expired events.", _timeByMaintenanceEvent);
        if (_timeByMaintenanceEvent.entrySet().removeIf(entry -> (entry.getValue() + _idempotenceRetention.toMillis() < _kafkaCruiseControl.timeMs()))) {
            LOG.debug("Remaining events in idempotence cache: {}.", _timeByMaintenanceEvent);
        }
    }

    /**
     * A runnable class to remove old events from idempotence cache.
     */
    private class IdempotenceCacheCleaner implements Runnable {
        @Override
        public void run() {
            try {
                removeOldEventsFromIdempotenceCache();
            } catch (Throwable t) {
                LOG.warn("Received exception when trying to remove old events from idempotence cache.", t);
            }
        }
    }

    /**
     * Package private for unit test.
     *
     * @return Time by maintenance event.
     */
    Map<MaintenanceEvent, Long> timeByMaintenanceEvent() {
        return Collections.unmodifiableMap(_timeByMaintenanceEvent);
    }

    /**
     * Ensure idempotency upon reading given events from maintenance event reader.
     * <ul>
     *   <li>Drop duplicates from the given maintenance events, and</li>
     *   <li>Refresh the idempotence cache with new events if the event is not already in the cache.</li>
     * </ul>
     * <p>
     * A {@link MaintenanceEvent} is considered as a duplicate, if it requires the same actions as any event in the idempotence cache.
     *
     * @param newEvents New maintenance events to (1) drop the duplicates from and (2) refresh the idempotence cache with.
     */
    synchronized void ensureIdempotency(Set<MaintenanceEvent> newEvents) {
        // Drop duplicates from the given maintenance events
        newEvents.removeIf(_timeByMaintenanceEvent::containsKey);
        // Refresh the idempotence cache with new events
        newEvents.forEach(event -> _timeByMaintenanceEvent.put(event, event.detectionTimeMs()));
    }

    /**
     * Shutdown the idempotence cache.
     */
    void shutdown() {
        if (_idempotenceCacheCleaner != null) {
            _idempotenceCacheCleaner.shutdownNow();
        }
    }
}
