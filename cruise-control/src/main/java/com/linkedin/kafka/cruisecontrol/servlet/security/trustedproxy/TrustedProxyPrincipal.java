/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import org.eclipse.jetty.security.SPNEGOUserPrincipal;

public class TrustedProxyPrincipal extends SPNEGOUserPrincipal {

  private final SPNEGOUserPrincipal _serviceUserPrincipal;
  private final String _doAsPrincipal;

  public TrustedProxyPrincipal(String doAsPrincipal, SPNEGOUserPrincipal serviceUserPrincipal) {
    super(doAsPrincipal, serviceUserPrincipal.getEncodedToken());
    _doAsPrincipal = doAsPrincipal;
    _serviceUserPrincipal = serviceUserPrincipal;
  }

  @Override
  public String getName() {
    return _doAsPrincipal;
  }

  public SPNEGOUserPrincipal servicePrincipal() {
    return _serviceUserPrincipal;
  }
}
