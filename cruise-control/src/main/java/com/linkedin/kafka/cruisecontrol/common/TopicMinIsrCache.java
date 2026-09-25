/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A cache to store the value of {@link TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} corresponding to topics.
 */
public class TopicMinIsrCache {
    private static final Logger LOG = LoggerFactory.getLogger(TopicMinIsrCache.class);
    private final Duration _cacheRetention;
    private final ScheduledExecutorService _cacheCleaner;
    private final Map<String, MinIsrWithTime> _minIsrWithTimeByTopic;
    private final Time _time;

    public TopicMinIsrCache(Duration cacheRetention, int maxCacheSize, Duration cleanerPeriod, Duration cleanerInitialDelay, Time time) {
        _cacheRetention = cacheRetention;
        _minIsrWithTimeByTopic = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, MinIsrWithTime> eldest) {
                return this.size() > maxCacheSize;
            }
        };
        _cacheCleaner = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("TopicMinIsrCacheCleaner"));
        _cacheCleaner.scheduleAtFixedRate(new CacheCleaner(), cleanerInitialDelay.toMillis(), cleanerPeriod.toMillis(), TimeUnit.MILLISECONDS);
        _time = time;
    }

    private synchronized void removeOldEntriesFromCache() {
        LOG.debug("Checking the cache ({}) for expired entries.", _minIsrWithTimeByTopic);
        if (_minIsrWithTimeByTopic.entrySet().removeIf(entry -> (entry.getValue().timeMs() + _cacheRetention.toMillis() < _time.milliseconds()))) {
            LOG.debug("Remaining entries in the cache: {}.", _minIsrWithTimeByTopic);
        }
    }

    /**
     * A runnable class to remove old entries from the cache.
     */
    private class CacheCleaner implements Runnable {
        @Override
        public void run() {
            try {
                removeOldEntriesFromCache();
            } catch (Throwable t) {
                LOG.warn("Received exception when trying to remove old entries from the cache.", t);
            }
        }
    }

    /**
     * @return An unordered shallow copy (i.e. a snapshot) of the cache, containing minIsr with time by topic name.
     */
    public synchronized Map<String, MinIsrWithTime> minIsrWithTimeByTopic() {
        return new HashMap<>(_minIsrWithTimeByTopic);
    }

    /**
     * Retrieve the topic names and the corresponding {@link TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} values from the given
     * {@link DescribeConfigsResult}, and put these entries to the cache.
     * <p>
     * Skip updating the cache
     * <ul>
     *   <li>for topics for which the config retrieval failed with an {@link ExecutionException} other than a {@code TimeoutException} such as
     *   {@code UnknownTopicOrPartitionException} or {@code InvalidTopicException}</li>
     *   <li>for topics for which the config retrieval failed with an {@link InterruptedException}, and</li>
     *   <li>for all topics if the config retrieval failed with a {@code TimeoutException}</li>
     *   <li>if the given describeConfigsResult is {@code null}</li>
     * </ul>
     *
     * @param describeConfigsResult DescribeConfigsResult containing the topic names and the corresponding
     *                              {@link TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} values to update the cache with.
     * @return Number of entries in the cache after the put operation.
     */
    public synchronized int putTopicMinIsr(DescribeConfigsResult describeConfigsResult) {
        if (describeConfigsResult == null) {
            return _minIsrWithTimeByTopic.size();
        }

        long updateTimeMs = _time.milliseconds();
        // Update the cache with the config result
        for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : describeConfigsResult.values().entrySet()) {
            try {
                short minIsr = Short.parseShort(entry.getValue().get().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value());
                _minIsrWithTimeByTopic.put(entry.getKey().name(), new MinIsrWithTime(minIsr, updateTimeMs));
            } catch (ExecutionException ee) {
                if (Errors.REQUEST_TIMED_OUT.exception().getClass() == ee.getCause().getClass()) {
                    LOG.warn("Failed to retrieve value of config {} for {} topics due to describeConfigs request time out. Check for Kafka-side issues"
                                    + " and consider increasing the configured timeout (see {}).", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                            describeConfigsResult.values().size(), ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG, ee.getCause());
                    return _minIsrWithTimeByTopic.size();
                }
                // e.g. could be UnknownTopicOrPartitionException due to topic deletion or InvalidTopicException
                LOG.debug("Cannot retrieve value of config {} for topic {}.", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, entry.getKey().name(), ee);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted while getting value of config {} for topic {}.", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, entry.getKey().name(), ie);
            }
        }
        return _minIsrWithTimeByTopic.size();
    }

    /**
     * Shutdown the cache.
     */
    public void shutdown() {
        if (_cacheCleaner != null) {
            _cacheCleaner.shutdownNow();
            _minIsrWithTimeByTopic.clear();
        }
    }

    /**
     * A class to encapsulate the value of {@link TopicConfig#MIN_IN_SYNC_REPLICAS_CONFIG} along with the time that the value was captured.
     */
    public static class MinIsrWithTime {
        private final short _minIsr;
        private final long _timeMs;

        public MinIsrWithTime(short minIsr, long timeMs) {
            _minIsr = minIsr;
            _timeMs = timeMs;
        }

        public short minISR() {
            return _minIsr;
        }

        public long timeMs() {
            return _timeMs;
        }
    }
}
