/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import org.easymock.EasyMock;
import org.eclipse.jetty.http.HttpCookie;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.authentication.LoginAuthenticator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import java.io.IOException;
import java.util.List;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.security.*")
@PrepareForTest(Response.class)
public class JwtAuthenticatorTest {

  private static final String TEST_USER = "testUser";
  private static final String TEST_USER_2 = "testUser2";
  private static final String JWT_TOKEN = "jwt_token";
  private static final String EXPECTED_TOKEN = "token";
  private static final String RANDOM_COOKIE_NAME = "random_cookie_name";
  private static final String TOKEN_PROVIDER = "http://mytokenprovider.com?origin=" + JwtAuthenticator.REDIRECT_URL;
  private static final String CRUISE_CONTROL_ENDPOINT = "http://cruisecontrol.mycompany.com/state";
  private static final String USER_ROLE = "USER";
  private static final String BASIC_SCHEME = "Basic";

  @Test
  public void testParseTokenFromAuthHeader() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    Request request = mock(Request.class);
    HttpFields headers = mock(HttpFields.class);
    expect(request.getHeaders()).andReturn(headers).anyTimes();
    expect(headers.get(HttpHeader.AUTHORIZATION.asString())).andReturn(JwtAuthenticator.BEARER + " " + EXPECTED_TOKEN);
    replay(request, headers);
    String actualToken = authenticator.getJwtFromBearerAuthorization(request);
    verify(request, headers);
    assertEquals(EXPECTED_TOKEN, actualToken);
  }

  @Test
  public void testParseTokenFromAuthHeaderNoBearer() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    Request request = mock(Request.class);
    HttpFields headers = mock(HttpFields.class);
    expect(request.getHeaders()).andReturn(headers).anyTimes();
    expect(headers.get(HttpHeader.AUTHORIZATION.asString())).andReturn(BASIC_SCHEME + " " + EXPECTED_TOKEN);
    replay(request, headers);
    String actualToken = authenticator.getJwtFromBearerAuthorization(request);
    verify(request, headers);
    assertNull(actualToken);
  }

  @Test
  public void testParseTokenFromCookie() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    Request request = mock(Request.class);
    HttpCookie jwtCookie = HttpCookie.build(JWT_TOKEN, EXPECTED_TOKEN).build();
    expect(request.getAttribute(Request.COOKIE_ATTRIBUTE)).andReturn(List.of(jwtCookie));
    replay(request);
    String actualToken = authenticator.getJwtFromCookie(request);
    verify(request);
    assertEquals(EXPECTED_TOKEN, actualToken);
  }

  @Test
  public void testParseTokenFromCookieNoJwtCookie() {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    Request request = mock(Request.class);
    HttpCookie jwtCookie = HttpCookie.build(RANDOM_COOKIE_NAME, "").build();
    expect(request.getAttribute(Request.COOKIE_ATTRIBUTE)).andReturn(List.of(jwtCookie));
    replay(request);
    String actualToken = authenticator.getJwtFromCookie(request);
    verify(request);
    assertNull(actualToken);
  }

  @Test
  public void testRedirect() throws IOException, ServerAuthException {
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);

    Request request = mock(Request.class);
    HttpFields headers = mock(HttpFields.class);
    expect(request.getHeaders()).andReturn(headers).anyTimes();
    expect(headers.get(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    expect(request.getAttribute(Request.COOKIE_ATTRIBUTE)).andReturn(List.of());
    HttpURI uri = HttpURI.build().scheme("http").host("cruisecontrol.mycompany.com").port(80).path("/state").asImmutable();
    expect(request.getHttpURI()).andReturn(uri).anyTimes();

    Response response = mock(Response.class);
    PowerMock.mockStatic(Response.class);
    Response.sendRedirect(request, response, null, TOKEN_PROVIDER.replace(JwtAuthenticator.REDIRECT_URL, CRUISE_CONTROL_ENDPOINT));
    PowerMock.expectLastCall().andVoid();

    EasyMock.replay(request, headers, response);
    PowerMock.replay(Response.class);
    AuthenticationState actualAuthentication = authenticator.validateRequest(request, response, null);
    EasyMock.verify(request, headers, response);
    PowerMock.verify(Response.class);
    assertEquals(AuthenticationState.CHALLENGE, actualAuthentication);
  }

  @Test
  public void testSuccessfulLogin() throws Exception {
    PropertyUserStore testUserStore = new PropertyUserStore();
    testUserStore.addUser(TEST_USER, SecurityUtils.NO_CREDENTIAL, new String[]{USER_ROLE});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(testUserStore, tokenAndKeys.publicKey(), null);

    Authenticator.Configuration configuration = mock(Authenticator.Configuration.class);
    expect(configuration.getLoginService()).andReturn(loginService);
    expect(configuration.getIdentityService()).andReturn(new DefaultIdentityService());
    expect(configuration.isSessionRenewedOnAuthentication()).andReturn(true);
    expect(configuration.getSessionMaxInactiveIntervalOnAuthentication()).andReturn(0);
    expect(configuration.isPersistAuthenticationCredentials()).andReturn(false).anyTimes();

    Request request = niceMock(Request.class);
    HttpFields headers = mock(HttpFields.class);
    expect(request.getHeaders()).andReturn(headers);
    expect(headers.get(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    expect(request.setAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE, tokenAndKeys.token())).andReturn(null);
    HttpCookie jwtCookie = HttpCookie.build(JWT_TOKEN, tokenAndKeys.token()).build();
    expect(request.getAttribute(Request.COOKIE_ATTRIBUTE)).andReturn(List.of(jwtCookie)).anyTimes();
    expect(request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE)).andReturn(tokenAndKeys.token());

    Response response = mock(Response.class);

    replay(configuration, request, headers, response);
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    authenticator.setConfiguration(configuration);
    LoginAuthenticator.UserAuthenticationSucceeded authentication = (LoginAuthenticator.UserAuthenticationSucceeded)
        authenticator.validateRequest(request, response, null);
    verify(configuration, request, headers, response);

    assertNotNull(authentication);
    assertThat(authentication.getUserIdentity().getUserPrincipal(), instanceOf(JwtUserPrincipal.class));
    JwtUserPrincipal userPrincipal = (JwtUserPrincipal) authentication.getUserIdentity().getUserPrincipal();
    assertEquals(TEST_USER, userPrincipal.getName());
    assertEquals(tokenAndKeys.token(), userPrincipal.getSerializedToken());
  }

  @Test
  public void testFailedLoginWithUserNotFound() throws Exception {
    PropertyUserStore testUserStore = new PropertyUserStore();
    testUserStore.addUser(TEST_USER_2, SecurityUtils.NO_CREDENTIAL, new String[] {USER_ROLE});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(testUserStore, tokenAndKeys.publicKey(), null);

    Authenticator.Configuration configuration = mock(Authenticator.Configuration.class);
    expect(configuration.getLoginService()).andReturn(loginService);
    expect(configuration.getIdentityService()).andReturn(new DefaultIdentityService());
    expect(configuration.isSessionRenewedOnAuthentication()).andReturn(true);
    expect(configuration.getSessionMaxInactiveIntervalOnAuthentication()).andReturn(0);
    expect(configuration.isPersistAuthenticationCredentials()).andReturn(false).anyTimes();

    Request request = niceMock(Request.class);
    HttpFields headers = mock(HttpFields.class);
    expect(request.getHeaders()).andReturn(headers);
    expect(headers.get(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    expect(request.setAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE, tokenAndKeys.token())).andReturn(null);
    HttpCookie jwtCookie = HttpCookie.build(JWT_TOKEN, tokenAndKeys.token()).build();
    expect(request.getAttribute(Request.COOKIE_ATTRIBUTE)).andReturn(List.of(jwtCookie)).anyTimes();

    Response response = mock(Response.class);
    PowerMock.mockStatic(Response.class);
    Response.writeError(request, response, null, HttpStatus.UNAUTHORIZED_401);
    PowerMock.expectLastCall().andVoid();

    replay(configuration, request, headers, response);
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    authenticator.setConfiguration(configuration);
    AuthenticationState authentication = authenticator.validateRequest(request, response, null);
    verify(configuration, request, headers, response);

    assertNotNull(authentication);
    assertEquals(AuthenticationState.SEND_FAILURE, authentication);
  }

  @Test
  public void testFailedLoginWithInvalidToken() throws Exception {
    PropertyUserStore testUserStore = new PropertyUserStore();
    testUserStore.addUser(TEST_USER_2, SecurityUtils.NO_CREDENTIAL, new String[] {USER_ROLE});
    TokenGenerator.TokenAndKeys tokenAndKeys = TokenGenerator.generateToken(TEST_USER);
    TokenGenerator.TokenAndKeys tokenAndKeys2 = TokenGenerator.generateToken(TEST_USER);
    JwtLoginService loginService = new JwtLoginService(testUserStore, tokenAndKeys.publicKey(), null);

    Authenticator.Configuration configuration = mock(Authenticator.Configuration.class);
    expect(configuration.getLoginService()).andReturn(loginService);
    expect(configuration.getIdentityService()).andReturn(new DefaultIdentityService());
    expect(configuration.isSessionRenewedOnAuthentication()).andReturn(true);
    expect(configuration.getSessionMaxInactiveIntervalOnAuthentication()).andReturn(0);
    expect(configuration.isPersistAuthenticationCredentials()).andReturn(false).anyTimes();

    Request request = niceMock(Request.class);
    HttpFields headers = mock(HttpFields.class);
    expect(request.getHeaders()).andReturn(headers);
    expect(headers.get(HttpHeader.AUTHORIZATION.asString())).andReturn(null);
    expect(request.setAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE, tokenAndKeys2.token())).andReturn(null);
    HttpCookie jwtCookie = HttpCookie.build(JWT_TOKEN, tokenAndKeys2.token()).build();
    expect(request.getAttribute(Request.COOKIE_ATTRIBUTE)).andReturn(List.of(jwtCookie)).anyTimes();

    Response response = mock(Response.class);
    PowerMock.mockStatic(Response.class);
    Response.writeError(request, response, null, HttpStatus.UNAUTHORIZED_401);
    PowerMock.expectLastCall().andVoid();

    replay(configuration, request, headers, response);
    JwtAuthenticator authenticator = new JwtAuthenticator(TOKEN_PROVIDER, JWT_TOKEN);
    authenticator.setConfiguration(configuration);
    AuthenticationState authentication = authenticator.validateRequest(request, response, null);
    verify(configuration, request, headers, response);

    assertNotNull(authentication);
    assertEquals(AuthenticationState.SEND_FAILURE, authentication);
  }
}
