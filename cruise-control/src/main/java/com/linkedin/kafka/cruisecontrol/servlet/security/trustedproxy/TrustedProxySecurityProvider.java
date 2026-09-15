/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.security.spnego.SpnegoSecurityProvider;
import org.eclipse.jetty.security.LoginService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Paths;
import java.util.List;

/**
 * In trusted proxy authentication Cruise Control has a fronting proxy which authenticates clients and from which
 * Cruise Control accepts requests as authenticated ones. the authenticated user's ID is forwarded in the "doAs" HTTP
 * GET query parameter.
 */
public class TrustedProxySecurityProvider extends SpnegoSecurityProvider {

  private List<String> _trustedProxyServices;
  private String _trustedProxyServicesIpRegex;
  private boolean _fallbackToSpnegoAllowed;
  private List<String> _spnegoPrincipalToLocalRules;

  private static final Logger LOG = LoggerFactory.getLogger(TrustedProxySecurityProvider.class);

  @Override
  public void init(KafkaCruiseControlConfig config) {
    super.init(config);
    _trustedProxyServices = config.getList(WebServerConfig.TRUSTED_PROXY_SERVICES_CONFIG);
    _fallbackToSpnegoAllowed = config.getBoolean(WebServerConfig.TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED_CONFIG);
    _spnegoPrincipalToLocalRules = config.getList(WebServerConfig.SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG);
    String ipWhitelistRegex = config.getString(WebServerConfig.TRUSTED_PROXY_SERVICES_IP_REGEX_CONFIG);
    if (ipWhitelistRegex != null) {
      _trustedProxyServicesIpRegex = ipWhitelistRegex;
    }
    LOG.info("Setting up authentication for trusted proxy list [{}] with keytab {} and spnego principal {} with IP whitelist regex {}",
        String.join(",", _trustedProxyServices), _keyTabPath, _spnegoPrincipal, _trustedProxyServicesIpRegex);
  }

  @Override
  public LoginService loginService() {
    TrustedProxyLoginService loginService = new TrustedProxyLoginService(_spnegoPrincipal.realm(), _privilegesFilePath,
            _trustedProxyServices, _trustedProxyServicesIpRegex, _fallbackToSpnegoAllowed, _spnegoPrincipalToLocalRules);
    loginService.setServiceName(_spnegoPrincipal.serviceName());
    loginService.setHostName(_spnegoPrincipal.hostName());
    loginService.setKeyTabPath(Paths.get(_keyTabPath));
    return loginService;
  }
}
