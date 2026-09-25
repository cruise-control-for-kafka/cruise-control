/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.cruisecontrol.detector.AnomalyType;

public final class NotifierUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NotifierUtils.class);

    private NotifierUtils() {
    }

    /**
     * Send POST message to a specific URL (application/json)
     *
     * @param message          The message that will be posted
     * @param postUrl          The post URL
     * @param authorisationKey Optional API key that would be needed to authenticate to the API (put <code>null</code> if not used)
     * @throws IOException In case of issue with the API.
     */
    public static void sendMessage(String message, String postUrl, String authorisationKey) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(postUrl);
            StringEntity entity = new StringEntity(message);
            httpPost.setEntity(entity);
            if (authorisationKey != null) {
                httpPost.setHeader("Authorization", "Key " + authorisationKey);
            }
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            LOG.debug("Sending alert to: {}\nBody:\n{}", httpPost, message);
            CloseableHttpResponse httpResponse = client.execute(httpPost);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Response status: {}", httpResponse.getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * Transform {@link AnomalyType} to {@link AlertSeverity}
     *
     * @param anomalyType The alert anomaly type
     * @return the {@link AlertSeverity} which correspond to the provided anomaly type
     */
    protected static AlertSeverity getAlertSeverity(AnomalyType anomalyType) {
        switch (anomalyType.priority()) {
            case 0:
                return AlertSeverity.CRITICAL;
            case 1:
            case 2:
                return AlertSeverity.MAJOR;
            case 3:
            case 4:
                return AlertSeverity.MINOR;
            default:
                return AlertSeverity.WARNING;
        }
    }

}
