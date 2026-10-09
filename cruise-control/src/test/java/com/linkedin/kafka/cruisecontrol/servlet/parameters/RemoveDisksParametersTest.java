/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import java.util.HashMap;
import java.util.Map;
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
}
