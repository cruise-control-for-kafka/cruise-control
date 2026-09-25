/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class ParameterUtilsTest {

  private static final Long DEFAULT_START_TIME_MS = -1L;
  private static final Long DEFAULT_END_TIME_MS = -2L;
  private static final String START_TIME_STRING = "12345";
  private static final String END_TIME_STRING = "23456";
  private static final String REPLICATION_THROTTLE_STRING = "1000";
  private static final String DEFAULT_REPLICATION_THROTTLE_STRING = "2000";
  private static final String EXECUTION_PROGRESS_CHECK_INTERVAL_STRING = "1500";

  @Test
  public void testParseTimeRangeSet() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    Map<String, String[]> paramMap = new HashMap<>();
    paramMap.put(ParameterUtils.START_MS_PARAM, new String[]{ParameterUtils.START_MS_PARAM});
    paramMap.put(ParameterUtils.END_MS_PARAM, new String[]{ParameterUtils.END_MS_PARAM});

    EasyMock.expect(mockRequest.getParameterMap()).andReturn(paramMap).times(4);
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.START_MS_PARAM)).andReturn(START_TIME_STRING).times(2);
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.END_MS_PARAM)).andReturn(END_TIME_STRING).times(2);
    EasyMock.replay(mockRequest);

    Long startMs = ParameterUtils.startMsOrDefault(mockRequest, null);
    Long endMs = ParameterUtils.endMsOrDefault(mockRequest, null);
    Assert.assertNotNull(startMs);
    Assert.assertNotNull(endMs);
    Assert.assertEquals(Long.valueOf(START_TIME_STRING), startMs);
    Assert.assertEquals(Long.valueOf(END_TIME_STRING), endMs);
    ParameterUtils.validateTimeRange(startMs, endMs);

    startMs = ParameterUtils.startMsOrDefault(mockRequest, DEFAULT_START_TIME_MS);
    endMs = ParameterUtils.endMsOrDefault(mockRequest, DEFAULT_END_TIME_MS);

    Assert.assertEquals(Long.valueOf(START_TIME_STRING), startMs);
    Assert.assertEquals(Long.valueOf(END_TIME_STRING), endMs);
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testMinIsrBasedConcurrencyAdjustment() {
    String firstResponse = Boolean.TRUE.toString();
    String secondResponse = Boolean.FALSE.toString();

    // Mock for (1) default response (2) response for valid input with true/false.
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getParameterMap()).andReturn(Collections.emptyMap())
            .andReturn(Collections.singletonMap(ParameterUtils.MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM, new String[]{firstResponse}))
            .andReturn(Collections.singletonMap(ParameterUtils.MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM, new String[]{secondResponse}));
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.MIN_ISR_BASED_CONCURRENCY_ADJUSTMENT_PARAM))
            .andReturn(firstResponse).andReturn(secondResponse);

    // Verify default response.
    EasyMock.replay(mockRequest);
    Boolean minIsrBasedConcurrencyAdjustment = ParameterUtils.minIsrBasedConcurrencyAdjustment(mockRequest);
    Assert.assertNull(minIsrBasedConcurrencyAdjustment);

    // Verify response for valid input.
    minIsrBasedConcurrencyAdjustment = ParameterUtils.minIsrBasedConcurrencyAdjustment(mockRequest);
    Assert.assertNotNull(minIsrBasedConcurrencyAdjustment);
    Assert.assertTrue(minIsrBasedConcurrencyAdjustment);
    minIsrBasedConcurrencyAdjustment = ParameterUtils.minIsrBasedConcurrencyAdjustment(mockRequest);
    Assert.assertNotNull(minIsrBasedConcurrencyAdjustment);
    Assert.assertFalse(minIsrBasedConcurrencyAdjustment);

    EasyMock.verify(mockRequest);
  }

  @Test
  public void testParseTimeRangeNotSet() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    // Mock the request so that it does not contain start/end time
    EasyMock.expect(mockRequest.getParameterMap()).andReturn(Collections.emptyMap()).times(4);
    EasyMock.replay(mockRequest);

    Long startMs = ParameterUtils.startMsOrDefault(mockRequest, null);
    Long endMs = ParameterUtils.endMsOrDefault(mockRequest, null);
    Assert.assertNull(startMs);
    Assert.assertNull(endMs);

    startMs = ParameterUtils.startMsOrDefault(mockRequest, DEFAULT_START_TIME_MS);
    endMs = ParameterUtils.endMsOrDefault(mockRequest, DEFAULT_END_TIME_MS);

    Assert.assertEquals(DEFAULT_START_TIME_MS, startMs);
    Assert.assertEquals(DEFAULT_END_TIME_MS, endMs);
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testParseReplicationThrottleWithNoDefault() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    KafkaCruiseControlConfig controlConfig = EasyMock.mock(KafkaCruiseControlConfig.class);

    Map<String, String[]> paramMap = Collections.singletonMap(
        ParameterUtils.REPLICATION_THROTTLE_PARAM,
        new String[]{ParameterUtils.REPLICATION_THROTTLE_PARAM});

    EasyMock.expect(mockRequest.getParameterMap()).andReturn(paramMap).once();
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.REPLICATION_THROTTLE_PARAM)).andReturn(REPLICATION_THROTTLE_STRING).once();
    // No default
    EasyMock.expect(controlConfig.getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG)).andReturn(null);

    EasyMock.replay(mockRequest, controlConfig);

    Long replicationThrottle = ParameterUtils.replicationThrottle(mockRequest, controlConfig);
    Assert.assertEquals(Long.valueOf(REPLICATION_THROTTLE_STRING), replicationThrottle);
    EasyMock.verify(mockRequest, controlConfig);
  }

  @Test
  public void testParseReplicationThrottleWithDefault() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    KafkaCruiseControlConfig controlConfig = EasyMock.mock(KafkaCruiseControlConfig.class);
    // No parameter string value in the parameter map
    EasyMock.expect(mockRequest.getParameterMap()).andReturn(Collections.emptyMap()).once();
    EasyMock.expect(controlConfig.getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG))
            .andReturn(Long.valueOf(DEFAULT_REPLICATION_THROTTLE_STRING));

    EasyMock.replay(mockRequest, controlConfig);

    Long replicationThrottle = ParameterUtils.replicationThrottle(mockRequest, controlConfig);
    Assert.assertEquals(Long.valueOf(DEFAULT_REPLICATION_THROTTLE_STRING), replicationThrottle);
    EasyMock.verify(mockRequest, controlConfig);
  }

  @Test
  public void testParseExecutionProgressCheckIntervalMsNoValue() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getParameterMap()).andReturn(Collections.emptyMap()).once();
    EasyMock.replay(mockRequest);
    Assert.assertNull(ParameterUtils.executionProgressCheckIntervalMs(mockRequest));
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testParseExecutionProgressCheckIntervalMsWithValue() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);

    Map<String, String[]> paramMap = Collections.singletonMap(
        ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM,
        new String[]{ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM});

    EasyMock.expect(mockRequest.getParameterMap()).andReturn(paramMap).once();
    EasyMock.expect(mockRequest.getParameter(ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM))
            .andReturn(EXECUTION_PROGRESS_CHECK_INTERVAL_STRING).once();

    EasyMock.replay(mockRequest);

    Long executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(mockRequest);

    EasyMock.verify(mockRequest);
    Assert.assertEquals(Long.valueOf(EXECUTION_PROGRESS_CHECK_INTERVAL_STRING), executionProgressCheckIntervalMs);
  }

  @Test
  public void testEndPointValidPath() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("GET");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn("/state");
    EasyMock.replay(mockRequest);

    CruiseControlEndPoint result = ParameterUtils.endPoint(mockRequest);
    Assert.assertEquals(CruiseControlEndPoint.STATE, result);
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testEndPointNullPathInfo() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("GET");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn(null);
    EasyMock.replay(mockRequest);

    Assert.assertNull(ParameterUtils.endPoint(mockRequest));
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testEndPointUnrecognized() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("GET");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn("/nonexistent");
    EasyMock.replay(mockRequest);

    Assert.assertNull(ParameterUtils.endPoint(mockRequest));
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testEndPointCaseInsensitive() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("POST");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn("/ADMIN");
    EasyMock.replay(mockRequest);

    Assert.assertEquals(CruiseControlEndPoint.ADMIN, ParameterUtils.endPoint(mockRequest));
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testEndPointRejectsExtraPathSegments() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("POST");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn("/x/admin");
    EasyMock.replay(mockRequest);

    Assert.assertNull(ParameterUtils.endPoint(mockRequest));
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testEndPointRejectsDeepNestedPath() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("POST");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn("/a/b/c/rebalance");
    EasyMock.replay(mockRequest);

    Assert.assertNull(ParameterUtils.endPoint(mockRequest));
    EasyMock.verify(mockRequest);
  }

  @Test
  public void testEndPointTrailingSlash() {
    CruiseControlRequestContext mockRequest = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(mockRequest.getMethod()).andReturn("GET");
    EasyMock.expect(mockRequest.getPathInfo()).andReturn("/state/");
    EasyMock.replay(mockRequest);

    Assert.assertEquals(CruiseControlEndPoint.STATE, ParameterUtils.endPoint(mockRequest));
    EasyMock.verify(mockRequest);
  }
}
