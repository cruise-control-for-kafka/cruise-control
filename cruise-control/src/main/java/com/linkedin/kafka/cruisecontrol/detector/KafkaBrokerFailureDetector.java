/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toSet;


/**
 * This class detects broker failures via the Kafka API
 */
public class KafkaBrokerFailureDetector extends AbstractBrokerFailureDetector {

    private static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private final AdminClient _adminClient;

    public KafkaBrokerFailureDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
        super(anomalies, kafkaCruiseControl);
        _adminClient = kafkaCruiseControl.adminClient();
        // Load the failed broker information.
        String failedBrokerListString = loadPersistedFailedBrokerList();
        parsePersistedFailedBrokers(failedBrokerListString);
    }

    @Override
    public void run() {
        detectBrokerFailures(false);
    }

    Set<Integer> aliveBrokers() throws ExecutionException, InterruptedException, TimeoutException {
        return _adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .stream().map(Node::id).collect(toSet());
    }

}
