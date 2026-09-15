/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalServerTestBase;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusMetric;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusQueryResult;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus.model.PrometheusValue;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.SEC_TO_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class PrometheusAdapterTest extends LocalServerTestBase {
    private static final long START_TIME_SECS = 1603301400L;
    private static final long END_TIME_SECS = 1603301459L;
    private static final long START_TIME_MS = START_TIME_SECS * SEC_TO_MS;
    private static final long END_TIME_MS = END_TIME_SECS * SEC_TO_MS;
    private static final long PLAIN_TIMESTAMP_START_TIME_MS = 1784144612388L;
    private static final long PLAIN_TIMESTAMP_END_TIME_MS = 1784148212388L;
    private static final int SAMPLING_INTERVAL_MS = (int) TimeUnit.SECONDS.toMillis(30);
    private static final int ONE_KB = 1024;

    @Test
    public void testSuccessfulResponseDeserialized() throws Exception {
        this.serverBootstrap.registerHandler(PrometheusAdapter.QUERY_RANGE_API_PATH, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(buildSuccessResponseEntity());
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);
        final List<PrometheusQueryResult> prometheusQueryResults = prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);

        assertEquals(expectedResults().toString(), prometheusQueryResults.toString());
        assertEquals(expectedResults(), prometheusQueryResults);
    }

    @Test
    public void testQueryTimestampsUsePlainDecimalNotation() throws Exception {
        AtomicReference<List<NameValuePair>> requestParameters = new AtomicReference<>();
        this.serverBootstrap.registerHandler(PrometheusAdapter.QUERY_RANGE_API_PATH, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws IOException {
                HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
                String requestBody = EntityUtils.toString(requestEntity, StandardCharsets.UTF_8);
                requestParameters.set(URLEncodedUtils.parse(requestBody, StandardCharsets.UTF_8));
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(buildSuccessResponseEntity());
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);
        prometheusAdapter.queryMetric("up", PLAIN_TIMESTAMP_START_TIME_MS, PLAIN_TIMESTAMP_END_TIME_MS);

        List<NameValuePair> parameters = requestParameters.get();
        assertNotNull("Expected the local server to capture the request parameters.", parameters);
        String start = parameterValue(parameters, "start");
        String end = parameterValue(parameters, "end");
        assertEquals("1784144612.388", start);
        assertEquals("1784148212.388", end);
        assertFalse(start.matches(".*[Ee].*"));
        assertFalse(end.matches(".*[Ee].*"));
    }

    private static String parameterValue(List<NameValuePair> parameters, String name) {
        return parameters.stream()
            .filter(parameter -> name.equals(parameter.getName()))
            .map(NameValuePair::getValue)
            .findFirst()
            .orElse(null);
    }

    private static HttpEntity buildSuccessResponseEntity() {
        return new StringEntity("{\n"
            + "    \"status\": \"success\",\n"
            + "    \"data\": {\n"
            + "        \"resultType\": \"matrix\",\n"
            + "        \"result\": [\n"
            + "            {\n"
            + "                \"metric\": {\n"
            + "                    \"__name__\": \"kafka_server_BrokerTopicMetrics_OneMinuteRate\",\n"
            + "                    \"instance\": \"b-1.test-cluster.org:11001\",\n"
            + "                    \"job\": \"jmx\",\n"
            + "                    \"name\": \"BytesOutPerSec\"\n"
            + "                },\n"
            + "                \"values\": [\n"
            + "                    [\n"
            + "                        1603301400,\n"
            + "                        \"1024\"\n"
            + "                    ],\n"
            + "                    [\n"
            + "                        1603301430,\n"
            + "                        \"2048\"\n"
            + "                    ]\n"
            + "                ]\n"
            + "            },\n"
            + "            {\n"
            + "                \"metric\": {\n"
            + "                    \"__name__\": \"kafka_server_BrokerTopicMetrics_OneMinuteRate\",\n"
            + "                    \"instance\": \"b-2.test-cluster.org:11001\",\n"
            + "                    \"job\": \"jmx\",\n"
            + "                    \"name\": \"BytesOutPerSec\"\n"
            + "                },\n"
            + "                \"values\": [\n"
            + "                    [\n"
            + "                        1603301400,\n"
            + "                        \"4096\"\n"
            + "                    ],\n"
            + "                    [\n"
            + "                        1603301430,\n"
            + "                        \"4096\"\n"
            + "                    ]\n"
            + "                ]\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}", StandardCharsets.UTF_8);
    }

    private static List<PrometheusQueryResult> expectedResults() {
        return Arrays.asList(
            new PrometheusQueryResult(
                new PrometheusMetric(
                    "b-1.test-cluster.org:11001",
                    null, null),
                Arrays.asList(
                    new PrometheusValue(START_TIME_SECS, ONE_KB),
                    new PrometheusValue(START_TIME_SECS + 30, 2 * ONE_KB)
                )
            ),
            new PrometheusQueryResult(
                new PrometheusMetric(
                    "b-2.test-cluster.org:11001",
                    null, null),
                Arrays.asList(
                    new PrometheusValue(START_TIME_SECS, 4 * ONE_KB),
                    new PrometheusValue(START_TIME_SECS + 30, 4 * ONE_KB)
                )
            )
        );
    }

    @Test(expected = IOException.class)
    public void testFailureResponseWith200Code() throws Exception {
        this.serverBootstrap.registerHandler(PrometheusAdapter.QUERY_RANGE_API_PATH, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                    "{\"status\": \"failure\", \"data\": {\"result\": []}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testFailureResponseWith403Code() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_FORBIDDEN);
                response.setEntity(new StringEntity(
                    "{\"status\": \"failure\", \"data\": {\"result\": []}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyResponse() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                    "", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyStatus() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                    "{\"data\":{\"result\": []}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyData() throws Exception {
        this.serverBootstrap.registerHandler("/api/v1/query_range", new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                    "{\"status\":\"success\"}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }

    @Test(expected = IOException.class)
    public void testEmptyResult() throws Exception {
        this.serverBootstrap.registerHandler(PrometheusAdapter.QUERY_RANGE_API_PATH, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                response.setStatusCode(HttpServletResponse.SC_OK);
                response.setEntity(new StringEntity(
                    "{\"status\": \"success\", \"data\": {}}", StandardCharsets.UTF_8));
            }
        });

        HttpHost httpHost = this.start();
        PrometheusAdapter prometheusAdapter
            = new PrometheusAdapter(this.httpclient, httpHost, SAMPLING_INTERVAL_MS);

        prometheusAdapter.queryMetric(
            "kafka_server_BrokerTopicMetrics_OneMinuteRate{name=\"BytesOutPerSec\",topic=\"\"}",
            START_TIME_MS, END_TIME_MS);
    }
}
