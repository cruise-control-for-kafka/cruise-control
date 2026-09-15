/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.security.RolePrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.security.UserPrincipal;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Session;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.resource.PathResourceFactory;
import org.eclipse.jetty.util.resource.Resource;
import javax.security.auth.Subject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.time.Clock;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.servlet.security.jwt.JwtAuthenticator.JWT_LOGGER;

/**
 * <p>This class validates a JWT token. The token must be cryptographically encrypted and it uses an RSA public key for
 * validation that is expected to be stored in a PEM formatted file.</p>
 * <p>This class implements {@link AbstractLifeCycle} which means it is a managed bean, its lifecycle will be managed
 * by Jetty.</p>
 * <p>The login service also validates expiration time of the token and it expects the token to contain the expiration
 * in Unix epoch time format in UTC.</p>
 */
public class JwtLoginService extends AbstractLifeCycle implements LoginService {

  public static final String X_509_CERT_TYPE = "X.509";
  private final UserStore _userStore;
  private IdentityService _identityService;
  private final RSAPublicKey _publicKey;
  private final List<String> _audiences;
  private Clock _clock;

  public JwtLoginService(String privilegesFilePath, String publicKeyLocation, List<String> audiences)
      throws IOException, CertificateException {
    this(createUserStore(privilegesFilePath), readPublicKey(publicKeyLocation), audiences);
  }

  public JwtLoginService(UserStore userStore, RSAPublicKey publicKey, List<String> audiences) {
    this(userStore, publicKey, audiences, Clock.systemUTC());
  }

  public JwtLoginService(UserStore userStore, RSAPublicKey publicKey, List<String> audiences, Clock clock) {
    _userStore = userStore;
    _identityService = new DefaultIdentityService();
    _publicKey = publicKey;
    _audiences = audiences;
    _clock = clock;
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
    _userStore.start();
  }

  @Override
  protected void doStop() throws Exception {
    _userStore.stop();
    super.doStop();
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public UserIdentity login(String username, Object credentials, Request request, Function<Boolean, Session> getOrCreateSession) {
    if (!(credentials instanceof SignedJWT)) {
      return null;
    }

    SignedJWT jwtToken = (SignedJWT) credentials;
    JWTClaimsSet claimsSet;
    boolean valid;
    try {
      claimsSet = jwtToken.getJWTClaimsSet();
      valid = validateToken(jwtToken, claimsSet, username);
    } catch (ParseException e) {
      JWT_LOGGER.warn(String.format("%s: Couldn't parse a JWT token", username), e);
      return null;
    }
    if (valid) {
      String serializedToken = (String) request.getAttribute(JwtAuthenticator.JWT_TOKEN_REQUEST_ATTRIBUTE);
      UserIdentity rolesDelegate = getUserIdentityFromIdentityService(username);
      if (rolesDelegate == null) {
        return null;
      } else {
        return getUserIdentity(jwtToken, claimsSet, serializedToken, username, rolesDelegate);
      }
    } else {
      return null;
    }
  }

  @Override
  public boolean validate(UserIdentity user) {
    Set<JWTClaimsSet> claims = user.getSubject().getPrivateCredentials(JWTClaimsSet.class);
    return !claims.isEmpty() && claims.stream().allMatch(this::validateExpiration);
  }

  @Override
  public IdentityService getIdentityService() {
    return _identityService;
  }

  @Override
  public void setIdentityService(IdentityService service) {
    _identityService = service;
  }

  @Override
  public void logout(UserIdentity user) {

  }

  // visible for testing
  void setClock(Clock newClock) {
    _clock = newClock;
  }

  private boolean validateToken(SignedJWT jwtToken, JWTClaimsSet claimsSet, String username) {
    boolean sigValid = validateSignature(jwtToken);
    if (!sigValid) {
      JWT_LOGGER.warn(String.format("%s: Signature could not be verified", username));
    }
    boolean audValid = validateAudiences(claimsSet);
    if (!audValid) {
      JWT_LOGGER.warn(String.format("%s: Audience validation failed", username));
    }
    boolean expValid = validateExpiration(claimsSet);
    if (!expValid) {
      JWT_LOGGER.warn(String.format("%s: Expiration validation failed", username));
    }

    return sigValid && audValid && expValid;
  }

  private boolean validateSignature(SignedJWT jwtToken) {
    if (JWSObject.State.SIGNED != jwtToken.getState() || jwtToken.getSignature() == null) {
      return false;
    }
    JWSVerifier verifier = new RSASSAVerifier(_publicKey);
    try {
      return jwtToken.verify(verifier);
    } catch (JOSEException e) {
      JWT_LOGGER.warn("Couldn't verify the signature of a token", e);
      return false;
    }
  }

  private boolean validateAudiences(JWTClaimsSet claimsSet) {
    if (_audiences == null) {
      return true;
    }
    List<String> tokenAudienceList = claimsSet.getAudience();
    for (String aud : tokenAudienceList) {
      if (_audiences.contains(aud)) {
        JWT_LOGGER.trace("JWT token audience has been successfully validated");
        return true;
      }
    }
    JWT_LOGGER.trace("Couldn't find a valid audience");
    return false;
  }

  private boolean validateExpiration(JWTClaimsSet claimsSet) {
    Date expires = claimsSet.getExpirationTime();
    return expires == null || _clock.instant().isBefore(expires.toInstant());
  }

  private static RSAPublicKey readPublicKey(String location) throws CertificateException, IOException {
    byte[] publicKeyBytes = Files.readAllBytes(Paths.get(location));
    CertificateFactory fact = CertificateFactory.getInstance(X_509_CERT_TYPE);
    X509Certificate cer = (X509Certificate) fact.generateCertificate(new ByteArrayInputStream(publicKeyBytes));
    return (RSAPublicKey) cer.getPublicKey();
  }

  private static UserIdentity getUserIdentity(SignedJWT jwtToken, JWTClaimsSet claimsSet, String serializedToken,
                                              String username, UserIdentity rolesDelegate) {
    JwtUserPrincipal principal = new JwtUserPrincipal(username, serializedToken);
    Set<Object> privCreds = new HashSet<>();
    privCreds.add(jwtToken);
    privCreds.add(claimsSet);
    Subject subject = new Subject(true, Collections.singleton(principal), Collections.emptySet(), privCreds);
    return new JwtUserIdentity(subject, principal, rolesDelegate);
  }

    private UserIdentity getUserIdentityFromIdentityService(String userName) {
        UserPrincipal user = _userStore.getUserPrincipal(userName);
        List<RolePrincipal> roles = _userStore.getRolePrincipals(userName);
        if (user == null) {
            return null;
        }
        return _identityService.newUserIdentity(
            createSubject(user, roles),
            user,
            roles.stream().map(RolePrincipal::getName).toArray(String[]::new)
        );
    }

    private static PropertyUserStore createUserStore(String privilegesFilePath) {
        PropertyUserStore userStore = new PropertyUserStore();
        Resource res = new PathResourceFactory().newResource(Path.of(privilegesFilePath));
        userStore.setConfig(res);
        return userStore;
    }

    private Subject createSubject(UserPrincipal user, List<RolePrincipal> rolePrincipals) {
        Subject subject = new Subject();
        subject.getPrincipals().add(user);
        subject.getPrincipals().addAll(rolePrincipals);
        return subject;
    }
}
