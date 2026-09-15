/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoLoginService;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.RoleDelegateUserIdentity;
import org.eclipse.jetty.security.SPNEGOUserPrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.Context;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.junit.Test;
import org.powermock.reflect.Whitebox;
import javax.security.auth.Subject;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TrustedProxyLoginServiceTest {

  public static final String TEST_SERVICE_USER = "testServiceUser";
  public static final String ENCODED_TOKEN = "encoded_token";
  public static final String TEST_USER = "testUser";
  private static final String IP_FILTER = "192\\.168\\.\\d{1,3}\\.\\d{1,3}";
  private final UserStore _adminUserStore = new UserStore();
  private final UserStore _serviceUserStore = new UserStore();

  private final SpnegoLoginService _mockSpnegoLoginService = mock(SpnegoLoginService.class);
  private final SpnegoLoginService _mockFallbackLoginService = mock(SpnegoLoginService.class);

  @Test
  public void testSuccessfulAuthentication() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath?" + DO_AS + "=" + TEST_USER);
    expect(mockRequest.getHttpURI()).andReturn(uri);
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockConfig.getUriCompliance()).andReturn(UriCompliance.DEFAULT).anyTimes();
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    IdentityService mockIdentityService = mock(IdentityService.class);
    expect(mockIdentityService.newUserIdentity(anyObject(), anyObject(), anyObject())).andReturn(serviceDelegate);

    replay(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig, mockIdentityService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
        _adminUserStore, _serviceUserStore, false);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertEquals(TEST_USER, doAsIdentity.getUserPrincipal().getName());
    assertEquals(((TrustedProxyPrincipal) doAsIdentity.getUserPrincipal()).servicePrincipal(), servicePrincipal);
    verify(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);
  }

  @Test
  public void testNoDoAsUser() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath");
    expect(mockRequest.getHttpURI()).andReturn(uri);
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    replay(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            _adminUserStore, _serviceUserStore, false);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertNull(doAsIdentity.getUserPrincipal().getName());
    assertFalse(((RoleDelegateUserIdentity) doAsIdentity).isEstablished());
    verify(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);
  }

  @Test
  public void testInvalidAuthServiceUser() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, null);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath?" + DO_AS + "=" + TEST_USER);
    expect(mockRequest.getHttpURI()).andReturn(uri);
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockConfig.getUriCompliance()).andReturn(UriCompliance.DEFAULT).anyTimes();
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    IdentityService mockIdentityService = mock(IdentityService.class);
    expect(_mockSpnegoLoginService.getIdentityService()).andReturn(mockIdentityService);
    expect(mockIdentityService.newUserIdentity(anyObject(), anyObject(), anyObject())).andReturn(null);
    replay(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig, mockIdentityService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            _adminUserStore, _serviceUserStore, false);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    assertFalse(((RoleDelegateUserIdentity) doAsIdentity).isEstablished());
  }

  @Test
  public void testFallbackToSpnego() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockFallbackLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath");
    expect(mockRequest.getHttpURI()).andReturn(uri);
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    replay(_mockSpnegoLoginService, _mockFallbackLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            _adminUserStore, _serviceUserStore, true);
    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertEquals(servicePrincipal, doAsIdentity.getUserPrincipal());
    verify(_mockSpnegoLoginService, _mockFallbackLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);
  }

  @Test
  public void testTrustedProxyWithKerberosRules() {
    String username = "user1";
    String proxy = "proxy2@realm";
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(proxy, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(username);
    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath?" + DO_AS + "=" + username);
    expect(mockRequest.getHttpURI()).andReturn(uri);
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockConfig.getUriCompliance()).andReturn(UriCompliance.DEFAULT).anyTimes();
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    IdentityService mockIdentityService = mock(IdentityService.class);
    replay(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig, mockIdentityService);
    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            _adminUserStore, _serviceUserStore, false);

    UserIdentity doAsIdentity = trustedProxyLoginService.login(proxy, ENCODED_TOKEN, mockRequest, null);

    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    assertEquals(username, doAsIdentity.getUserPrincipal().getName());
    assertEquals(((TrustedProxyPrincipal) doAsIdentity.getUserPrincipal()).servicePrincipal(), servicePrincipal);
    verify(_mockSpnegoLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig, mockIdentityService);
  }

  @Test
  public void testFallbackToSpnegoWithKerberosRules() {
    String username = "user1";
    String principal = "user1@realm";
    String usernameReplaced = username + "foo";
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(usernameReplaced, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockFallbackLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(username);
    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath");
    expect(mockRequest.getHttpURI()).andReturn(uri);
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    replay(_mockSpnegoLoginService, _mockFallbackLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);
    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
            _adminUserStore, _serviceUserStore, true);

    UserIdentity doAsIdentity = trustedProxyLoginService.login(principal, ENCODED_TOKEN, mockRequest, null);

    assertNotNull(doAsIdentity);
    assertNotNull(doAsIdentity.getUserPrincipal());
    SPNEGOUserPrincipal doAsPrincipal = (SPNEGOUserPrincipal) doAsIdentity.getUserPrincipal();
    assertEquals(servicePrincipal.getName(), doAsPrincipal.getName());
    assertTrue(((RoleDelegateUserIdentity) doAsIdentity).isEstablished());
    verify(_mockSpnegoLoginService, _mockFallbackLoginService, mockRequest, mockContext, mockConnectionMetaData, mockConfig);
  }

  private void addTestUser(String testUser) {
    _adminUserStore.addUser(testUser, SecurityUtils.NO_CREDENTIAL, new String[] { DefaultRoleSecurityProvider.ADMIN });
  }

  @Test
  public void testSuccessfulLoginWithIpFiltering() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mockRequestWithDoAs(TEST_USER, "192.168.0.1");
    IdentityService mockIdentityService = mock(IdentityService.class);
    expect(mockIdentityService.newUserIdentity(anyObject(), anyObject(), anyObject())).andReturn(serviceDelegate);
    replay(_mockSpnegoLoginService, mockIdentityService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
        _adminUserStore, _serviceUserStore, false);
    Whitebox.setInternalState(trustedProxyLoginService, "_identityService", mockIdentityService);
    Whitebox.setInternalState(trustedProxyLoginService, "_trustedProxyIpPattern", Pattern.compile(IP_FILTER));

    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    assertTrue(((RoleDelegateUserIdentity) doAsIdentity).isEstablished());
    assertEquals(TEST_USER, doAsIdentity.getUserPrincipal().getName());
  }

  @Test
  public void testUnsuccessfulLoginWithIpFiltering() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mockRequestWithDoAs(TEST_USER, "192.167.0.1");
    IdentityService mockIdentityService = mock(IdentityService.class);
    expect(mockIdentityService.newUserIdentity(anyObject(), anyObject(), anyObject())).andReturn(serviceDelegate);
    replay(_mockSpnegoLoginService, mockIdentityService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
        _adminUserStore, _serviceUserStore, false);
    Whitebox.setInternalState(trustedProxyLoginService, "_identityService", mockIdentityService);
    Whitebox.setInternalState(trustedProxyLoginService, "_trustedProxyIpPattern", Pattern.compile(IP_FILTER));

    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    // doAs lookup rejected by IP filter -> no delegate -> identity not established
    assertFalse(((RoleDelegateUserIdentity) doAsIdentity).isEstablished());
  }

  @Test
  public void testSuccessfulLoginWithoutIpFiltering() {
    SPNEGOUserPrincipal servicePrincipal = new SPNEGOUserPrincipal(TEST_SERVICE_USER, ENCODED_TOKEN);
    UserIdentity serviceDelegate = mock(UserIdentity.class);
    Subject subject = new Subject(true, Collections.singleton(servicePrincipal), Collections.emptySet(), Collections.emptySet());
    RoleDelegateUserIdentity result = new RoleDelegateUserIdentity(subject, servicePrincipal, serviceDelegate);
    expect(_mockSpnegoLoginService.login(anyString(), anyObject(), anyObject(), anyObject())).andReturn(result);

    addTestUser(TEST_USER);

    Request mockRequest = mockRequestWithDoAs(TEST_USER, "10.0.0.1");
    IdentityService mockIdentityService = mock(IdentityService.class);
    expect(mockIdentityService.newUserIdentity(anyObject(), anyObject(), anyObject())).andReturn(serviceDelegate);
    replay(_mockSpnegoLoginService, mockIdentityService);

    TrustedProxyLoginService trustedProxyLoginService = new TrustedProxyLoginService(_mockSpnegoLoginService, _mockFallbackLoginService,
        _adminUserStore, _serviceUserStore, false);
    Whitebox.setInternalState(trustedProxyLoginService, "_identityService", mockIdentityService);
    // no _trustedProxyIpPattern set -> null -> IP is not checked

    UserIdentity doAsIdentity = trustedProxyLoginService.login(null, ENCODED_TOKEN, mockRequest, null);
    assertNotNull(doAsIdentity);
    assertTrue(((RoleDelegateUserIdentity) doAsIdentity).isEstablished());
    assertEquals(TEST_USER, doAsIdentity.getUserPrincipal().getName());
  }

  private Request mockRequestWithDoAs(String doAsUser, String remoteAddr) {
    Request mockRequest = mock(Request.class);
    Context mockContext = mock(Context.class);
    HttpURI uri = HttpURI.from("http://cruisecontrol.mycompany.com/somePath?" + DO_AS + "=" + doAsUser);
    expect(mockRequest.getHttpURI()).andReturn(uri).anyTimes();
    expect(mockRequest.getContext()).andReturn(mockContext).anyTimes();
    expect(mockContext.getAttribute(anyString())).andReturn(null).anyTimes();
    ConnectionMetaData mockConnectionMetaData = mock(ConnectionMetaData.class);
    HttpConfiguration mockConfig = mock(HttpConfiguration.class);
    expect(mockRequest.getConnectionMetaData()).andReturn(mockConnectionMetaData).anyTimes();
    expect(mockConnectionMetaData.getHttpConfiguration()).andReturn(mockConfig).anyTimes();
    expect(mockConfig.getFormEncodedMethods()).andReturn(Set.of(""));
    expect(mockConfig.getUriCompliance()).andReturn(UriCompliance.DEFAULT).anyTimes();
    expect(mockRequest.getMethod()).andReturn("GET").anyTimes();
    expect(mockRequest.getAttribute(anyString())).andReturn(null).anyTimes();
    expect(mockConnectionMetaData.getRemoteSocketAddress())
        .andReturn(new java.net.InetSocketAddress(remoteAddr, 12345)).anyTimes();
    replay(mockRequest, mockContext, mockConnectionMetaData, mockConfig);
    return mockRequest;
  }

}
