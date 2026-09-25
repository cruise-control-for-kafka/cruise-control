/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import java.util.Map;


public class NoopSampleStore implements SampleStore {
    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void storeSamples(MetricSampler.Samples samples) {

    }

    @Override
    public void loadSamples(SampleLoader sampleLoader) {

    }

    @Override
    public double sampleLoadingProgress() {
        return 0.0;
    }

    @Override
    public void evictSamplesBefore(long timestamp) {

    }

    @Override
    public void close() {

    }
}
