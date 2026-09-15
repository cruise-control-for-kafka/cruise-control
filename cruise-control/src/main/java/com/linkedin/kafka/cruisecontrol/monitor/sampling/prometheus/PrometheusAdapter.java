/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusResponse;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.SEC_TO_MS;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;

/**
 * This class provides an adapter to make queries to a Prometheus Server to fetch metric values.
 */
class PrometheusAdapter {
    private static final Gson GSON = new Gson();
    static final String QUERY_RANGE_API_PATH = "/api/v1/query_range";
    static final String SUCCESS = "success";
    private static final String QUERY = "query";
    private static final String START = "start";
    private static final String END = "end";
    private static final String STEP = "step";

    private final CloseableHttpClient _httpClient;
    protected final HttpHost _prometheusEndpoint;
    protected final int _samplingIntervalMs;

    PrometheusAdapter(CloseableHttpClient httpClient,
                      HttpHost prometheusEndpoint,
                      int samplingIntervalMs) {
        _httpClient = validateNotNull(httpClient, "httpClient cannot be null.");
        _prometheusEndpoint = validateNotNull(prometheusEndpoint, "prometheusEndpoint cannot be null.");
        _samplingIntervalMs = samplingIntervalMs;
    }

    public int samplingIntervalMs() {
        return _samplingIntervalMs;
    }

    public List<PrometheusQueryResult> queryMetric(String queryString,
                                                   long startTimeMs,
                                                   long endTimeMs) throws IOException {
        URI queryUri = URI.create(_prometheusEndpoint.toURI() + QUERY_RANGE_API_PATH);
        HttpPost httpPost = new HttpPost(queryUri);

        List<NameValuePair> data = new ArrayList<>();
        data.add(new BasicNameValuePair(QUERY, queryString));
        /* "start" and "end" are expected to be unix timestamp in seconds (number of seconds since the Unix epoch).
         They accept values with a decimal point (up to 64 bits). The samples returned are inclusive of the "end"
         timestamp provided.
         */
        data.add(new BasicNameValuePair(START, String.valueOf((double) startTimeMs / SEC_TO_MS)));
        data.add(new BasicNameValuePair(END, String.valueOf((double) endTimeMs / SEC_TO_MS)));
        // step is expected to be in seconds, and accept values with a decimal point (up to 64 bits).
        data.add(new BasicNameValuePair(STEP, String.valueOf((double) _samplingIntervalMs / SEC_TO_MS)));

        httpPost.setEntity(new UrlEncodedFormEntity(data));
        try (CloseableHttpResponse response = _httpClient.execute(httpPost)) {
            int responseCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            String responseString = IOUtils.toString(content, StandardCharsets.UTF_8);
            if (responseCode != HttpServletResponse.SC_OK) {
                throw new IOException(String.format("Received non-success response code on Prometheus API HTTP call,"
                                                    + " response code = %d, response body = %s",
                                                    responseCode, responseString));
            }
            PrometheusResponse prometheusResponse = GSON.fromJson(responseString, PrometheusResponse.class);
            if (prometheusResponse == null) {
                throw new IOException(String.format(
                    "No response received from Prometheus API query, response body = %s", responseString));
            }

            if (!SUCCESS.equals(prometheusResponse.status())) {
                throw new IOException(String.format(
                    "Prometheus API query was not successful, response body = %s", responseString));
            }
            if (prometheusResponse.data() == null
                || prometheusResponse.data().result() == null) {
                throw new IOException(String.format(
                    "Response from Prometheus HTTP API is malformed, response body = %s", responseString));
            }
            EntityUtils.consume(entity);
            return prometheusResponse.data().result();
        }
    }
}
