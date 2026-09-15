/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.UserTasksParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.response.UserTaskState;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import jakarta.servlet.http.HttpSession;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.LOAD;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.PROPOSALS;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REBALANCE;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.REMOVE_BROKER;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.USER_TASKS;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.GET_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;


public class KafkaCruiseControlServletEndpointTest {
  private static final Function<String, OperationFuture> FUTURE_CREATOR = uuid -> new OperationFuture("future");

  // A hack to make 2 requests to same endpoint 'look' different to UserTaskManager
  private static final Map<String, String[]> DIFF_PARAM = new HashMap<>();
  private static final Map<String, String[]> EMPTY_PARAM = Collections.emptyMap();

  private static final UUID repeatUUID = UUID.randomUUID();

  private static final Collection<Object[]> INITIALIZE_SERVLET_REQUESTS_OUTPUT = new ArrayList<>();
  private static final Collection<Object[]> POPULATE_USER_TASK_MANAGER_OUTPUT = new ArrayList<>();
  private static final UserTaskManager.UuidGenerator MOCK_UUID_GENERATOR;
  private static final HttpSession MOCK_HTTP_SESSION;
  private static final CruiseControlRequestContext MOCK_HTTP_SERVLET_RESPONSE;
  private static final UserTaskManager USER_TASK_MANAGER;

  private static final String[] PARAMS_TO_GET = {
      ParameterUtils.CLIENT_IDS_PARAM,
      ParameterUtils.ENDPOINTS_PARAM,
      ParameterUtils.TYPES_PARAM,
      ParameterUtils.USER_TASK_IDS_PARAM,
      ParameterUtils.ENTRIES_PARAM
  };

  static {
    DIFF_PARAM.put("param", new String[]{"true"});

    Time mockTime = new MockTime();
    MOCK_UUID_GENERATOR = EasyMock.mock(UserTaskManager.UuidGenerator.class);
    MOCK_HTTP_SESSION = EasyMock.mock(HttpSession.class);
    MOCK_HTTP_SERVLET_RESPONSE = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(MOCK_HTTP_SESSION.getLastAccessedTime()).andReturn(mockTime.milliseconds()).anyTimes();
    MOCK_HTTP_SESSION.invalidate();
    MOCK_HTTP_SERVLET_RESPONSE.setHeader(EasyMock.anyString(), EasyMock.anyString());
    EasyMock.expectLastCall().anyTimes();
    USER_TASK_MANAGER = new UserTaskManager(1000, 10, TimeUnit.HOURS.toMillis(6),
                                            100, mockTime, MOCK_UUID_GENERATOR);
  }

  private static class MockResult implements CruiseControlResponse {

    @Override
    public void writeSuccessResponse(CruiseControlParameters parameters, CruiseControlRequestContext requestContext) {

    }

    public void discardIrrelevantResponse(CruiseControlParameters parameters) { }
    public String cachedResponse() {
      return "";
    }
  }

  private static Object[] inputRequestParams(UUID userTaskId,
                                             String clientId,
                                             String endPoint,
                                             Map<String, String[]> params,
                                             boolean addToRequest,
                                             String methodType) {
    return new Object[]{userTaskId, clientId, endPoint, params, addToRequest, methodType};
  }

  private static Object[] outputRequestInfo(CruiseControlRequestContext mockHttpServletRequest) {
    return new Object[]{mockHttpServletRequest};
  }

  private static Object[] inputCreateTaskParams(CruiseControlRequestContext request, Integer taskIndex, Integer futureIndex) {
    return new Object[]{request, taskIndex, futureIndex};
  }

  private static Object[] outputCreateTaskInfo(OperationFuture future) {
    return new Object[]{future};
  }

  private static OperationFuture getFuture(int idx) {
    return (OperationFuture) ((ArrayList<Object[]>) POPULATE_USER_TASK_MANAGER_OUTPUT).get(idx)[0];
  }

  // Creates 6 requests below. These will create 5 entries of UserTaskInfo (request 5 and 6 have same id, so will be 1 UserTaskInfo)
  private void initializeServletRequests(HttpSession mockHttpSession, UserTaskManager.UuidGenerator mockUuidGenerator) {
    Collection<Object[]> allParams = new ArrayList<>();

    allParams.add(inputRequestParams(null, "0.0.0.1", LOAD.toString(), EMPTY_PARAM, false, GET_METHOD));
    allParams.add(inputRequestParams(null, "0.0.0.1", PROPOSALS.toString(), EMPTY_PARAM, false, GET_METHOD));
    allParams.add(inputRequestParams(null, "0.0.0.2", REBALANCE.toString(), EMPTY_PARAM, false, POST_METHOD));
    allParams.add(inputRequestParams(null, "0.0.0.3", REBALANCE.toString(), DIFF_PARAM, false, POST_METHOD));
    allParams.add(inputRequestParams(repeatUUID, "0.0.0.1", REMOVE_BROKER.toString(), EMPTY_PARAM, false, POST_METHOD));
    allParams.add(inputRequestParams(repeatUUID, "0.0.0.4", REMOVE_BROKER.toString(), EMPTY_PARAM, true, POST_METHOD));

    for (Object[] params : allParams) {
      CruiseControlRequestContext mockHttpServletRequest = prepareTestRequest(mockHttpSession, params[0], params[1], params[2],
                                                                     params[3], mockUuidGenerator, params[4], params[5]);
      INITIALIZE_SERVLET_REQUESTS_OUTPUT.add(outputRequestInfo(mockHttpServletRequest));
    }
  }

  // Create 5 User Tasks. Note the 5th and 6th one have same user task id, thus count as 1.
  private void populateUserTaskManager(CruiseControlRequestContext mockHttpFrameworkResponse, UserTaskManager userTaskManager) throws Exception {
    List<Object[]> allParams = new ArrayList<>();
    for (Object[] initInfo : INITIALIZE_SERVLET_REQUESTS_OUTPUT) {
      allParams.add(inputCreateTaskParams((CruiseControlRequestContext) initInfo[0], 0, 0));
    }
    // for the 6th getOrCreateUserTask() call, we set step to 1 and get the 2nd future
    allParams.get(5)[1] = 1;
    allParams.get(5)[2] = 1;

    for (Object[] params : allParams) {
      OperationFuture future = userTaskManager.getOrCreateUserTask(mockHttpFrameworkResponse, FUTURE_CREATOR,
                                                                   (int) params[1], true, null).get((int) params[2]);
      POPULATE_USER_TASK_MANAGER_OUTPUT.add(outputCreateTaskInfo(future));
    }
  }

  @Ignore
  @Test
  public void testUserTaskParameters() throws Exception {

    // Set up all mocked requests,  UserTaskManager, and start mocked objects.
    initializeServletRequests(MOCK_HTTP_SESSION, MOCK_UUID_GENERATOR);
    EasyMock.replay(MOCK_UUID_GENERATOR, MOCK_HTTP_SESSION, MOCK_HTTP_SERVLET_RESPONSE);
    populateUserTaskManager(MOCK_HTTP_SERVLET_RESPONSE, USER_TASK_MANAGER);

    UserTaskState userTaskState = new UserTaskState(USER_TASK_MANAGER.getAllUserTasks(), null);

    // Test Case 1: Get all PROPOSAL or REBALANCE tasks
    Map<String, String []> answerQueryParam1 = new HashMap<>();
    answerQueryParam1.put("param", new String[]{"true"});
    answerQueryParam1.put("endpoints", new String[]{PROPOSALS + "," + REBALANCE});
    CruiseControlRequestContext answerQueryRequest1 = prepareRequest(MOCK_HTTP_SESSION, null, "",
            USER_TASKS.toString(), answerQueryParam1, GET_METHOD);
    UserTasksParameters parameters1 = mockUserTasksParameters(answerQueryRequest1);
    List<UserTaskManager.UserTaskInfo> result1 = userTaskState.prepareResultList(parameters1);
    // Test Case 1 result
    Assert.assertEquals(3, result1.size());
    EasyMock.reset(MOCK_UUID_GENERATOR, MOCK_HTTP_SESSION, MOCK_HTTP_SERVLET_RESPONSE);

    // Test Case 2: Get all tasks from client 0.0.0.1
    Map<String, String []> answerQueryParam2 = new HashMap<>();
    answerQueryParam2.put("param", new String[]{"true"});
    answerQueryParam2.put("client_ids", new String[]{"0.0.0.1"});
    CruiseControlRequestContext answerQueryRequest2 = prepareRequest(MOCK_HTTP_SESSION, null, "",
            USER_TASKS.toString(), answerQueryParam2, GET_METHOD);
    UserTasksParameters parameters2 = mockUserTasksParameters(answerQueryRequest2);
    List<UserTaskManager.UserTaskInfo> result2 = userTaskState.prepareResultList(parameters2);
    // Test Case 2 result
    Assert.assertEquals(3, result2.size());
    EasyMock.reset(MOCK_UUID_GENERATOR, MOCK_HTTP_SESSION, MOCK_HTTP_SERVLET_RESPONSE);

    // Test Case 3: Get all PROPOSALS and REMOVE_BROKERS from client 0.0.0.1
    Map<String, String []> answerQueryParam3 = new HashMap<>();
    answerQueryParam3.put("param", new String[]{"true"});
    answerQueryParam3.put("client_ids", new String[]{"0.0.0.1"});
    answerQueryParam3.put("endpoints", new String[]{PROPOSALS + "," + REMOVE_BROKER});
    CruiseControlRequestContext answerQueryRequest3 = prepareRequest(MOCK_HTTP_SESSION, null, "",
            USER_TASKS.toString(), answerQueryParam3, GET_METHOD);
    UserTasksParameters parameters3 = mockUserTasksParameters(answerQueryRequest3);
    List<UserTaskManager.UserTaskInfo> result3 = userTaskState.prepareResultList(parameters3);
    // Test Case 3 result
    Assert.assertEquals(2, result3.size());
    EasyMock.reset(MOCK_UUID_GENERATOR, MOCK_HTTP_SESSION, MOCK_HTTP_SERVLET_RESPONSE);

    // Test Case 4: Get all tasks limit to 4 entries
    Map<String, String []> answerQueryParam4 = new HashMap<>();
    answerQueryParam4.put("param", new String[]{"true"});
    answerQueryParam4.put("entries", new String[]{"4"});
    CruiseControlRequestContext answerQueryRequest4 = prepareRequest(MOCK_HTTP_SESSION, null, "",
            USER_TASKS.toString(), answerQueryParam4, GET_METHOD);
    UserTasksParameters parameters4 = mockUserTasksParameters(answerQueryRequest4);
    List<UserTaskManager.UserTaskInfo> result4 = userTaskState.prepareResultList(parameters4);
    // Test Case 4 result
    Assert.assertEquals(4, result4.size());
    EasyMock.reset(MOCK_UUID_GENERATOR, MOCK_HTTP_SESSION, MOCK_HTTP_SERVLET_RESPONSE);

    // Transition UserTaskManager state: some tasks will move from ACTIVE to COMPLETED
    // Resolve futures. Allow those tasks to be moved into completed state
    // Complete 1st request
    getFuture(0).complete(new MockResult());
    // Complete 5th request
    getFuture(4).complete(new MockResult());
    // Complete 6th request
    getFuture(5).complete(new MockResult());
    // Update task manager active vs completed state
    USER_TASK_MANAGER.checkActiveUserTasks();
    // Now the UserTaskManager state has changed, so we reload the states
    UserTaskState userTaskState2 = new UserTaskState(USER_TASK_MANAGER.getAllUserTasks(), null);

    // Test Case 5: Get all LOAD or REMOVE_BROKER tasks that's completed and with user task id repeatUUID
    Map<String, String []> answerQueryParam5 = new HashMap<>();
    answerQueryParam5.put("param", new String[]{"true"});
    answerQueryParam5.put("endpoints", new String[]{LOAD + "," + REMOVE_BROKER});
    answerQueryParam5.put("user_task_ids", new String[]{repeatUUID.toString()});
    answerQueryParam5.put("types", new String[]{UserTaskManager.TaskState.COMPLETED.toString()});
    CruiseControlRequestContext answerQueryRequest5 = prepareRequest(MOCK_HTTP_SESSION, null, "",
            USER_TASKS.toString(), answerQueryParam5, GET_METHOD);
    UserTasksParameters parameters5 = mockUserTasksParameters(answerQueryRequest5);
    List<UserTaskManager.UserTaskInfo> result5 = userTaskState2.prepareResultList(parameters5);
    // Test Case 5 result
    Assert.assertEquals(1, result5.size());
    EasyMock.reset(MOCK_UUID_GENERATOR, MOCK_HTTP_SESSION, MOCK_HTTP_SERVLET_RESPONSE);

  }

  // Some how we cannot instantiate UserTasksParameters (fail at instantiating LOGGER object), so we mock it.
  private static UserTasksParameters mockUserTasksParameters(CruiseControlRequestContext answerQueryRequest) throws UnsupportedEncodingException {
    UserTasksParameters parameters = EasyMock.mock(UserTasksParameters.class);
    EasyMock.expect(parameters.userTaskIds()).andReturn(ParameterUtils.userTaskIds(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.clientIds()).andReturn(ParameterUtils.clientIds(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.endPoints()).andReturn(ParameterUtils.endPoints(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.endPoint()).andReturn(ParameterUtils.endPoint(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.types()).andReturn(ParameterUtils.types(answerQueryRequest)).anyTimes();
    EasyMock.expect(parameters.entries()).andReturn(ParameterUtils.entries(answerQueryRequest)).anyTimes();

    EasyMock.replay(parameters);
    return parameters;
  }

  @SuppressWarnings("unchecked")
  private CruiseControlRequestContext prepareTestRequest(HttpSession session, Object userTaskId, Object clientId, Object resource,
                                                         Object params, UserTaskManager.UuidGenerator mockUuidGenerator,
                                                         Object addToRequest, Object method) {

    UUID uuidForGenerator = (userTaskId == null ? UUID.randomUUID() : (UUID) userTaskId);
    String uuidForRequest = (((Boolean) addToRequest && userTaskId != null) ? userTaskId.toString() : null);

    EasyMock.expect(mockUuidGenerator.randomUUID()).andReturn(uuidForGenerator).once();
    return prepareRequest(session, uuidForRequest, (String) clientId, (String) resource, (Map<String, String []>) params, (String) method);
  }

  private CruiseControlRequestContext prepareRequest(HttpSession session, String userTaskId, String clientId, String resource,
                                            Map<String, String []> params, String method) {
    CruiseControlRequestContext request = EasyMock.mock(CruiseControlRequestContext.class);

    EasyMock.expect(request.getSession()).andReturn(new ServletSession(session)).anyTimes();
    EasyMock.expect(request.getMethod()).andReturn(method).anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn(KafkaCruiseControlServletTestUtils.getDefaultWebServerApiUrlPrefix() + resource).anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(params).anyTimes();
    EasyMock.expect(request.getPathInfo()).andReturn("/" + resource).anyTimes();
    EasyMock.expect(request.getHeader(UserTaskManager.USER_TASK_HEADER_NAME)).andReturn(userTaskId).anyTimes();
    for (String headerName : KafkaCruiseControlServletUtils.HEADERS_TO_TRY) {
      EasyMock.expect(request.getHeader(headerName)).andReturn(clientId).anyTimes();
    }

    for (String param : PARAMS_TO_GET) {
      String result = null;
      // Assume all parameters stored in first array entry
      if (params.get(param) != null) {
        result = params.get(param)[0];
      }
      EasyMock.expect(request.getParameter(param)).andReturn(result).anyTimes();
    }
    EasyMock.replay(request);
    return request;
  }
}
