/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.servlet.KafkaCruiseControlServletUtils.POST_METHOD;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.GET_RESPONSE_SCHEMA;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.JSON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.VERBOSE_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class RemoveDisksParametersTest {

  @Test
  public void testAcceptsVerboseAndCommonParameters() throws Exception {
    Map<String, String[]> parameterMap = new HashMap<>();
    parameterMap.put(BROKER_ID_AND_LOGDIRS_PARAM, new String[]{"0-/tmp/kafka-logs-1"});
    parameterMap.put(DRY_RUN_PARAM, new String[]{"true"});
    parameterMap.put(VERBOSE_PARAM, new String[]{"true"});
    parameterMap.put(ALLOW_CAPACITY_ESTIMATION_PARAM, new String[]{"false"});
    parameterMap.put(JSON_PARAM, new String[]{"true"});
    parameterMap.put(GET_RESPONSE_SCHEMA, new String[]{"true"});
    parameterMap.put(DO_AS, new String[]{"user"});

    CruiseControlRequestContext request = EasyMock.mock(CruiseControlRequestContext.class);
    EasyMock.expect(request.getMethod()).andReturn(POST_METHOD).anyTimes();
    EasyMock.expect(request.getPathInfo()).andReturn("/remove_disks").anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(parameterMap).anyTimes();
    EasyMock.replay(request);

    assertTrue(ParameterUtils.hasValidParameterNames(request, new RemoveDisksParameters()));
    EasyMock.verify(request);
  }

  @Test
  public void testRequiresBrokerIdAndLogdirs() {
    for (Map<String, String[]> parameterMap : List.of(Map.of(DRY_RUN_PARAM, new String[]{"true"}),
                                                      Map.of(BROKER_ID_AND_LOGDIRS_PARAM, new String[]{""}),
                                                      Map.of(BROKER_ID_AND_LOGDIRS_PARAM, new String[]{","}))) {
      UserRequestException e = assertThrows(UserRequestException.class, () -> parameters(parameterMap).initParameters());
      assertTrue(e.getMessage(), e.getMessage().startsWith(BROKER_ID_AND_LOGDIRS_PARAM + " is required"));
    }
  }

  @Test
  public void testRejectsMalformedBrokerIdAndLogdirs() {
    UserRequestException e = assertThrows(UserRequestException.class,
                                          () -> parameters(Map.of(BROKER_ID_AND_LOGDIRS_PARAM, new String[]{"101"})).initParameters());
    assertTrue(e.getMessage(), e.getMessage().contains("'101'"));
  }

  @Test
  public void testParsesBrokerIdAndLogdirs() throws Exception {
    RemoveDisksParameters parameters = parameters(Map.of(BROKER_ID_AND_LOGDIRS_PARAM,
                                                         new String[]{"101-/tmp/kafka-logs-1,101-/tmp/kafka-logs-2"}));
    parameters.initParameters();
    assertEquals(Map.of(101, Set.of("/tmp/kafka-logs-1", "/tmp/kafka-logs-2")), parameters.brokerIdAndLogdirs());
    assertTrue(parameters.dryRun());
  }

  /**
   * @param parameterMap The parameters of a remove_disks request.
   * @return The parameters of the request, ready to be initialized.
   */
  private static RemoveDisksParameters parameters(Map<String, String[]> parameterMap) {
    CruiseControlRequestContext request = EasyMock.niceMock(CruiseControlRequestContext.class);
    EasyMock.expect(request.getMethod()).andReturn(POST_METHOD).anyTimes();
    EasyMock.expect(request.getPathInfo()).andReturn("/remove_disks").anyTimes();
    EasyMock.expect(request.getParameterMap()).andReturn(parameterMap).anyTimes();
    EasyMock.expect(request.getParameter(EasyMock.anyString())).andAnswer(() -> {
      String[] values = parameterMap.get((String) EasyMock.getCurrentArgument(0));
      return values == null ? null : values[0];
    }).anyTimes();
    EasyMock.replay(request);
    RemoveDisksParameters parameters = new RemoveDisksParameters();
    parameters._requestContext = request;
    parameters._config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    return parameters;
  }
}
