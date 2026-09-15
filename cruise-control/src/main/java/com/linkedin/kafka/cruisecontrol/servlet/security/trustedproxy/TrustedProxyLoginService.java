/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.servlet.security.DefaultRoleSecurityProvider;
import com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoLoginService;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.RoleDelegateUserIdentity;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.SPNEGOUserPrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.UserPrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Session;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.resource.PathResourceFactory;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.Subject;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;

/**
 * {@code TrustedProxyLoginService} is a special SPNEGO login service where we only allow a list of trusted services
 * to act on behalf of clients. The login service authenticates the trusted party but creates credentials for the client
 */
public class TrustedProxyLoginService extends ContainerLifeCycle implements LoginService {

  private static final Logger LOG = LoggerFactory.getLogger(TrustedProxyLoginService.class);
  public static final boolean READ_ONLY_SUBJECT = true;
  // authorizes the end user that is passed in via the doAs header
  private final UserStore _serviceUserStore;
  private final UserStore _userStore;
  // use encapsulation instead of inheritance as it's easier to test
  private final SpnegoLoginService _delegateSpnegoLoginService;
  private final SpnegoLoginService _fallbackSpnegoLoginService;
  // we can fall back to spnego and authenticate the service user only if no doAs user provided
  private final boolean _fallbackToSpnegoAllowed;
  private Pattern _trustedProxyIpPattern;
  private IdentityService _identityService;

  /**
   * Creates a new instance based on the kerberos realm, the list of trusted proxies, and their allowed IP pattern.
   * @param realm is the kerberos realm of the spnego service principal that is used by Cruise Control
   * @param privilegesFilePath creates user store to authorizes the user that is passed in via the doAs header
   * @param trustedProxies is a list of kerberos service shortnames that identifies the trusted proxies
   * @param trustedProxyIpPattern is a Java regex pattern that defines which IP addresses can be accepted by
   *                              Cruise Control as trusted proxies
   */
  public TrustedProxyLoginService(String realm, String privilegesFilePath, List<String> trustedProxies,
                                  String trustedProxyIpPattern, boolean fallbackToSpnegoAllowed, List<String> principalToLocalRules) {
    _serviceUserStore = new UserStore();
    _userStore = createUserStore(privilegesFilePath);
    _delegateSpnegoLoginService = new SpnegoLoginService(realm, _serviceUserStore, principalToLocalRules);
    _fallbackSpnegoLoginService = new SpnegoLoginService(realm, _userStore, principalToLocalRules);
    _fallbackToSpnegoAllowed = fallbackToSpnegoAllowed;
    _identityService = new DefaultIdentityService();
    setTrustedProxyIpPattern(trustedProxies, trustedProxyIpPattern);
  }

  // visible for testing
  TrustedProxyLoginService(SpnegoLoginService delegateSpnegoLoginService,
                           SpnegoLoginService fallbackSpnegoLoginService,
                           UserStore userStore, UserStore serviceUserStore, boolean fallbackToSpnegoAllowed) {
    _delegateSpnegoLoginService = delegateSpnegoLoginService;
    _fallbackSpnegoLoginService = fallbackSpnegoLoginService;
    _userStore = userStore;
    _serviceUserStore = serviceUserStore;
    _fallbackToSpnegoAllowed = fallbackToSpnegoAllowed;
    _identityService = new DefaultIdentityService();
  }

  // ------- ConfigurableSpnegoLoginService methods -------

  /**
   * Sets the service name for spnego login.
   * @param serviceName the service name for spnego login
   */
  public void setServiceName(String serviceName) {
    _delegateSpnegoLoginService.setServiceName(serviceName);
    _fallbackSpnegoLoginService.setServiceName(serviceName);
  }

  /**
   * Sets the hostname for spnego login.
   * @param hostName hostname for spnego login.
   */
  public void setHostName(String hostName) {
    _delegateSpnegoLoginService.setHostName(hostName);
    _fallbackSpnegoLoginService.setHostName(hostName);
  }

  /**
   * Sets the keytab path for spnego login.
   * @param path keytab path for spnego login
   */
  public void setKeyTabPath(Path path) {
    _delegateSpnegoLoginService.setKeyTabPath(path);
    _fallbackSpnegoLoginService.setKeyTabPath(path);
  }

  // ------- LoginService methods -------

  @Override
  public String getName() {
    return _delegateSpnegoLoginService.getName();
  }

  @Override
  public UserIdentity login(String username, Object credentials, Request request, Function<Boolean, Session> getOrCreateSession) {
    Fields reqParameters;
    try {
      reqParameters = Request.getParameters(request);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    String doAsUser = reqParameters.getValue(DO_AS);
    if (doAsUser == null && _fallbackToSpnegoAllowed) {
      RoleDelegateUserIdentity fallbackIdentity = (RoleDelegateUserIdentity) _fallbackSpnegoLoginService.login(username,
          credentials, request, getOrCreateSession);
      if (!fallbackIdentity.isEstablished()) {
        SPNEGOUserPrincipal fallbackPrincipal = (SPNEGOUserPrincipal) fallbackIdentity.getUserPrincipal();
        LOG.info("Service user {} isn't authorized as spnego fallback principal", fallbackPrincipal.getName());
      }
      return fallbackIdentity;
    } else {
      RoleDelegateUserIdentity serviceIdentity = (RoleDelegateUserIdentity) _delegateSpnegoLoginService.login(username,
          credentials, request, getOrCreateSession);
      SPNEGOUserPrincipal servicePrincipal = (SPNEGOUserPrincipal) serviceIdentity.getUserPrincipal();
      LOG.info("Authorizing proxy user {} from {} service", doAsUser, servicePrincipal.getName());
      UserIdentity doAsIdentity = null;
      if (doAsUser != null && !doAsUser.isEmpty()) {
        doAsIdentity = getUserIdentity(request, doAsUser);
      }

      Principal principal = new TrustedProxyPrincipal(doAsUser, servicePrincipal);
      Subject subject = new Subject(READ_ONLY_SUBJECT, Collections.singleton(principal), Collections.emptySet(), Collections.emptySet());

      if (!serviceIdentity.isEstablished()) {
        LOG.info("Service user {} isn't authorized as a trusted proxy", servicePrincipal.getName());
        return new RoleDelegateUserIdentity(subject, principal, null);
      } else {
        if (doAsIdentity == null) {
          LOG.info("Couldn't authorize user {}", doAsUser);
        }
        return new RoleDelegateUserIdentity(subject, principal, doAsIdentity);
      }
    }
  }

  @Override
  public boolean validate(UserIdentity user) {
    return _delegateSpnegoLoginService.validate(user);
  }

  @Override
  public IdentityService getIdentityService() {
    return _delegateSpnegoLoginService.getIdentityService();
  }

  @Override
  public void setIdentityService(IdentityService service) {
    _delegateSpnegoLoginService.setIdentityService(service);
  }

  @Override
  public void logout(UserIdentity user) {
    _delegateSpnegoLoginService.logout(user);
  }

  // ------- ContainerLifeCycle methods -------

  @Override
  protected void doStart() throws Exception {
    _userStore.start();
    _serviceUserStore.start();
    _delegateSpnegoLoginService.start();
    _fallbackSpnegoLoginService.start();
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
    _fallbackSpnegoLoginService.stop();
    _delegateSpnegoLoginService.stop();
    _userStore.stop();
    _serviceUserStore.stop();
  }

  private UserIdentity getUserIdentity(Request request, String name) {
    // SpnegoLoginService may pass names in servicename/host format but we only store the servicename
    int nameHostSeparatorIndex = name.indexOf('/');
    String serviceName = nameHostSeparatorIndex > 0 ? name.substring(0, nameHostSeparatorIndex) : name;
    UserPrincipal user = _userStore.getUserPrincipal(serviceName);
    List<RolePrincipal> roles = _userStore.getRolePrincipals(serviceName);
    if (user == null) {
      return null;
    }
    UserIdentity serviceIdentity = _identityService.newUserIdentity(
        createSubject(user, roles), 
        _userStore.getUserPrincipal(serviceName),
        _userStore.getRolePrincipals(serviceName).stream()
            .map(RolePrincipal::getName)
            .toArray(String[]::new)
    );
    if (_trustedProxyIpPattern != null) {
      return _trustedProxyIpPattern.matcher(Request.getRemoteAddr(request)).matches() ? serviceIdentity : null;
    } else {
      return serviceIdentity;
    }
  }

  private Subject createSubject(UserPrincipal user, List<RolePrincipal> rolePrincipals) {
    Subject subject = new Subject();
    subject.getPrincipals().add(user);
    subject.getPrincipals().addAll(rolePrincipals);
    return subject;
  }

  private void setTrustedProxyIpPattern(List<String> userNames, String trustedProxyIpPattern) {
    userNames.forEach(u -> _serviceUserStore.addUser(u, SecurityUtils.NO_CREDENTIAL, new String[] { DefaultRoleSecurityProvider.ADMIN }));
    if (trustedProxyIpPattern != null) {
      _trustedProxyIpPattern = Pattern.compile(trustedProxyIpPattern);
    } else {
      _trustedProxyIpPattern = null;
    }
  }

  private PropertyUserStore createUserStore(String privilegesFilePath) {
      PropertyUserStore userStore = new PropertyUserStore();
      Resource res = new PathResourceFactory().newResource(Path.of(privilegesFilePath));
      userStore.setConfig(res);
      return userStore;
  }
}
