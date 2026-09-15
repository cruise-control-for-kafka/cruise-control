/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SPNEGOUserPrincipal;
import org.eclipse.jetty.security.UserIdentity;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Session;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityUtils.NO_CREDENTIAL;

public class DummyLoginService implements LoginService {
    private final IdentityService _identityService = new DefaultIdentityService();

    @Override
    public String getName() {
        return "spnego-realm";
    }

    @Override
    public IdentityService getIdentityService() {
        return _identityService;
    }

    @Override
    public void setIdentityService(IdentityService svc) {
        // dummy implementation
    }

    @Override
    public boolean validate(UserIdentity user) {
        return true;
    }

    @Override
    public void logout(UserIdentity user) {
        // dummy implementation
    }

    @Override
    public UserIdentity login(String u, Object c, Request req, Function<Boolean, Session> getOrCreateSession) {
        return null;
    }

    @Override
    public UserIdentity getUserIdentity(Subject subject, Principal userPrincipal, boolean create) {
        return createUserIdentity(userPrincipal.getName());
    }

    private UserIdentity createUserIdentity(String username) {
        Principal userPrincipal = new SPNEGOUserPrincipal(username, "");
        Subject subject = new Subject();
        subject.getPrincipals().add(userPrincipal);
        subject.getPrivateCredentials().add(NO_CREDENTIAL);
        subject.setReadOnly();

        return _identityService.newUserIdentity(subject, userPrincipal, new String[0]);
    }
}
