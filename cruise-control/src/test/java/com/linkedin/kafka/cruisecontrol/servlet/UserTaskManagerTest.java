/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;

public class UserTaskManagerTest {

  private static HttpSession getMockHttpSession(long lastAccessedTimeReturn) {
    HttpSession mockHttpSession = EasyMock.mock(HttpSession.class);
    EasyMock.expect(mockHttpSession.getLastAccessedTime()).andReturn(lastAccessedTimeReturn).anyTimes();
    mockHttpSession.invalidate();
    EasyMock.expectLastCall().andVoid().anyTimes();
    return mockHttpSession;
  }

  @Test
  public void testCreateUserTask() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = getMockHttpSession(100L);

    HttpServletRequest mockHttpServletRequest1 = prepareServletRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext requestContext = new ServletRequestContext(mockHttpServletRequest1, mockHttpServletResponse, cruiseControlConfigMock);
    requestContext.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
                                                          100, new MockTime(), mockUuidGenerator);
    // test-case: create user-task based on request and get future
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(requestContext, uuid -> future, 0, true, null).get(0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future1);
    EasyMock.verify(mockUuidGenerator, mockHttpServletResponse, mockHttpSession);
    EasyMock.reset(mockHttpServletResponse);
    // test-case: get same future back using sessions
    OperationFuture future2 =
            userTaskManager.getOrCreateUserTask(requestContext, uuid -> future, 0, true, null).get(0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future2);

    HttpServletRequest mockHttpServletRequest2 = prepareServletRequest(mockHttpSession, testUserTaskId.toString());
    EasyMock.reset(mockHttpServletResponse);
    // test-case: get future back using user-task-id
    ServletRequestContext requestContext1 = new ServletRequestContext(mockHttpServletRequest2, mockHttpServletResponse, cruiseControlConfigMock);
    OperationFuture future3 =
            userTaskManager.getOrCreateUserTask(requestContext1, uuid -> future, 0, true, null).get(0);

    Assert.assertEquals(userTaskHeader.getValue(), UserTaskManager.USER_TASK_HEADER_NAME);
    Assert.assertEquals(userTaskHeaderValue.getValue(), testUserTaskId.toString());
    Assert.assertEquals(future, future3);

    EasyMock.reset(mockHttpServletResponse);

    // test-case: for sync task, UserTaskManager does not create mapping between request URL and UUID.
    HttpServletRequest mockHttpServletRequest3 = prepareServletRequest(null, null, "test_sync_request", Collections.emptyMap());
    ServletRequestContext requestContext2 = new ServletRequestContext(mockHttpServletRequest3, mockHttpServletResponse, cruiseControlConfigMock);
    OperationFuture future4 =
            userTaskManager.getOrCreateUserTask(requestContext2, uuid -> future, 0, false, null).get(0);

    // New async request have session mapping where a session key is mapped to the UUID associated with the request. Such a mapping is
    // not created for sync request. So in the 2 asserts below , we expect that given a http request, we are able to find request associated UUID
    // for async request but not for sync request.
    UUID savedUUID = userTaskManager.getUserTaskId(requestContext2);
    Assert.assertNull(savedUUID);
    Assert.assertEquals(future4, future);

    EasyMock.reset(mockHttpServletResponse);
    OperationFuture future5 =
            userTaskManager.getOrCreateUserTask(requestContext2, uuid -> future, 0, true, null).get(0);
    savedUUID = userTaskManager.getUserTaskId(requestContext2);
    Assert.assertNotEquals(savedUUID, null);
    Assert.assertEquals(future5, future);

    userTaskManager.close();
  }

  @Test
  public void testSessionsShareUserTask() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = getMockHttpSession(System.currentTimeMillis());

    Map<String, String []> requestParams1 = new HashMap<>();
    requestParams1.put("param", new String[]{"true"});
    HttpServletRequest mockHttpServletRequest1 = prepareServletRequest(mockHttpSession, null, "test", requestParams1);
    HttpServletResponse mockHttpServletResponse1 = EasyMock.mock(HttpServletResponse.class);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext requestContext = new ServletRequestContext(mockHttpServletRequest1, mockHttpServletResponse1, cruiseControlConfigMock);
    requestContext.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    Map<String, String []> requestParams2 = new HashMap<>();
    requestParams2.put("param", new String[]{"true"});
    HttpServletRequest mockHttpServletRequest2 = prepareServletRequest(mockHttpSession, null, "test", requestParams2);
    HttpServletResponse mockHttpServletResponse2 = EasyMock.mock(HttpServletResponse.class);
    ServletRequestContext requestContext1 = new ServletRequestContext(mockHttpServletRequest2, mockHttpServletResponse2, cruiseControlConfigMock);
    requestContext1.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    Map<String, String []> requestParams3 = new HashMap<>();
    requestParams3.put("param", new String[]{"true"});
    HttpServletRequest mockHttpServletRequest3 = prepareServletRequest(mockHttpSession, testUserTaskId.toString(), "test", requestParams3);
    HttpServletResponse mockHttpServletResponse3 = EasyMock.mock(HttpServletResponse.class);
    ServletRequestContext requestContext2 = new ServletRequestContext(mockHttpServletRequest3, mockHttpServletResponse3, cruiseControlConfigMock);
    requestContext2.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse1, mockHttpServletResponse2, mockHttpServletResponse3);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 5, TimeUnit.HOURS.toMillis(6),
                                                          100, new MockTime(), mockUuidGenerator);
    userTaskManager.getOrCreateUserTask(requestContext, uuid -> future, 0, true, null);
    userTaskManager.getOrCreateUserTask(requestContext1, uuid -> future, 0, true, null);
    // Test UserTaskManger can recognize the previous created task by taskId.
    userTaskManager.getOrCreateUserTask(requestContext2, uuid -> future, 0, true, null);

    // The 2nd request should reuse the UserTask created for the 1st request since they use the same session and send the same request.
    Assert.assertEquals(1, userTaskManager.numActiveSessionKeys());
  }

  @Test
  public void testAddStepsFutures() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    HttpSession mockHttpSession = getMockHttpSession(System.currentTimeMillis());

    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext requestContext = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    requestContext.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);

    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
                                                          100, new MockTime(), mockUuidGenerator);

    OperationFuture testFuture1 = new OperationFuture("testFuture1");
    OperationFuture testFuture2 = new OperationFuture("testFuture2");

    OperationFuture insertedFuture1 =
            userTaskManager.getOrCreateUserTask(requestContext, uuid -> testFuture1, 0,
            true, null).get(0);
    Assert.assertEquals(testFuture1, insertedFuture1);
    EasyMock.verify(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    EasyMock.reset(mockHttpServletResponse);
    OperationFuture insertedFuture2 =
            userTaskManager.getOrCreateUserTask(requestContext, uuid -> testFuture2, 1,
            true, null).get(1);
    Assert.assertEquals(testFuture2, insertedFuture2);

    Assert.assertEquals(userTaskManager.getUserTaskByUserTaskId(testUserTaskId, requestContext).futures().size(), 2);
    userTaskManager.close();
  }

  @Test
  public void testCompletedTasks() throws Exception {
    HttpSession mockHttpSession = getMockHttpSession(100L);

    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);
    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(UUID.randomUUID()).anyTimes();

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
                                                          100, new MockTime(), mockUuidGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext requestContext = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    Capture<String> userTaskHeader = Capture.newInstance();
    Capture<String> userTaskHeaderValue = Capture.newInstance();
    requestContext.setHeader(EasyMock.capture(userTaskHeader), EasyMock.capture(userTaskHeaderValue));

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: verify if the background cleaner task removes tasks that are completed
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(requestContext, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, future1);

    future1.cancel(true);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS * 4));

    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCancelled());
    EasyMock.verify(mockHttpSession, mockUuidGenerator, mockHttpServletResponse);
    userTaskManager.close();
  }

  @Test
  public void testExpireSession() throws Exception {
    UUID testUserTaskId = UUID.randomUUID();

    UserTaskManager.UuidGenerator mockUuidGenerator = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(testUserTaskId).anyTimes();

    Time mockTime = new MockTime();
    HttpSession mockHttpSession = getMockHttpSession(100L);
    HttpServletRequest mockHttpServletRequest = prepareServletRequest(mockHttpSession, null);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
                                                          100, mockTime, mockUuidGenerator);

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext servletHandler = new ServletRequestContext(mockHttpServletRequest, mockHttpServletResponse, cruiseControlConfigMock);
    servletHandler.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockUuidGenerator, mockHttpSession, mockHttpServletResponse);
    // test-case: test if the sessions are removed on expiration
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(servletHandler, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, future1);

    mockTime.sleep(1001);
    Thread.sleep(TimeUnit.SECONDS.toMillis(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS + 1));

    OperationFuture servletFuture2 = userTaskManager.getFuture(servletHandler);
    Assert.assertNull(servletFuture2);

    userTaskManager.close();
  }

  @Test
  public void testMaximumActiveTasks() throws Exception {
    HttpSession mockHttpSession1 = getMockHttpSession(100L);

    HttpServletRequest mockHttpServletRequest1 = prepareServletRequest(mockHttpSession1, null);

    OperationFuture future = new OperationFuture("future");
    UserTaskManager userTaskManager = new UserTaskManager(1000, 1, TimeUnit.HOURS.toMillis(6),
                                                          100, new MockTime());

    HttpServletResponse mockHttpServletResponse = EasyMock.mock(HttpServletResponse.class);
    KafkaCruiseControlConfig cruiseControlConfigMock = EasyMock.niceMock(KafkaCruiseControlConfig.class);
    ServletRequestContext requestContext = new ServletRequestContext(mockHttpServletRequest1, mockHttpServletResponse, cruiseControlConfigMock);
    requestContext.setHeader(EasyMock.anyString(), EasyMock.anyString());

    EasyMock.replay(mockHttpSession1, mockHttpServletResponse);
    // test-case: test max limitation active tasks
    OperationFuture future1 =
            userTaskManager.getOrCreateUserTask(requestContext, uuid -> future, 0, true, null).get(0);
    Assert.assertEquals(future, future1);
    EasyMock.verify(mockHttpSession1, mockHttpServletRequest1, mockHttpServletResponse);
    HttpSession mockHttpSession2 = getMockHttpSession(100L);
    EasyMock.reset(mockHttpServletResponse);

    HttpServletRequest mockHttpServletRequest2 = prepareServletRequest(mockHttpSession2, null, "/test2", Collections.emptyMap());
    ServletRequestContext requestContext1 = new ServletRequestContext(mockHttpServletRequest2, mockHttpServletResponse, cruiseControlConfigMock);
    try {
      OperationFuture future2 =
              userTaskManager.getOrCreateUserTask(requestContext1, uuid -> future, 0, true, null).get(0);
      Assert.assertEquals(future, future2);
      EasyMock.verify(mockHttpSession2, mockHttpServletRequest2, mockHttpServletResponse);
    } catch (RuntimeException e) {
      userTaskManager.close();
      return;
    }
    Assert.fail("Don't expect to be here!");
  }

  private HttpServletRequest prepareServletRequest(HttpSession session, String userTaskId) {
    return prepareServletRequest(session, userTaskId, "/test", Collections.emptyMap());
  }

  private HttpServletRequest prepareServletRequest(HttpSession session, String userTaskId, String resource, Map<String, String []> params) {
    HttpServletRequest request = EasyMock.mock(HttpServletRequest.class);
    EasyMock.expect(request.getSession()).andReturn(session).anyTimes();
    EasyMock.expect(request.getSession(false)).andReturn(session).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn(GET_METHOD).anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn(resource).anyTimes();
    EasyMock.expect(request.getPathInfo()).andReturn(resource).anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(params).anyTimes();
    EasyMock.expect(request.getHeader(UserTaskManager.USER_TASK_HEADER_NAME)).andReturn(userTaskId).anyTimes();
    EasyMock.expect(request.getRemoteHost()).andReturn("test-host").anyTimes();
    EasyMock.expect(request.getHeaderNames()).andReturn(Collections.enumeration(Collections.emptyList())).anyTimes();
    for (String headerName : KafkaCruiseControlServletUtils.HEADERS_TO_TRY) {
      EasyMock.expect(request.getHeader(headerName)).andReturn("localhost").anyTimes();
    }
    EasyMock.replay(request);
    return request;
  }
}
