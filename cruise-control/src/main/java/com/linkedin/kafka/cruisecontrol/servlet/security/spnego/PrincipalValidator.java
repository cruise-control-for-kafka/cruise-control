/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrincipalValidator implements Validator {
    private static final Pattern PRINCIPAL_REGEX =
            Pattern.compile("(?<primary>[^/\\s@]+)(/(?<instance>[\\w.-]+))?(@(?<realm>(\\S+)))?");

    private final boolean _instanceRequired;
    private final boolean _realmRequired;

    public PrincipalValidator(boolean instanceRequired, boolean realmRequired) {
        _instanceRequired = instanceRequired;
        _realmRequired = realmRequired;
    }

    /**
     * Creates a PrincipalName object.
     * @param configName The name of the configuration
     * @param principal The principal which will be the base of the PrincipalName object
     * @return PrincipalName object
     */
    public static PrincipalName parsePrincipal(String configName, String principal) {
        Matcher matcher = PRINCIPAL_REGEX.matcher(principal);
        if (!matcher.matches()) {
            throw new ConfigException(configName, principal, "Invalid principal");
        }
        String primary = matcher.group("primary");
        String instance = matcher.group("instance");
        String realm = matcher.group("realm");
        return new PrincipalName(primary, instance, realm);
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            return;
        }

        if (!(value instanceof String strVal)) {
            throw new ConfigException(name, value, "Value must be string");
        }

        PrincipalName principalName = parsePrincipal(name, strVal);
        if (_instanceRequired && principalName.getInstance() == null) {
            throw new ConfigException(name, strVal, "Principal must contain the instance section");
        }
        if (_realmRequired && principalName.getRealm() == null) {
            throw new ConfigException(name, strVal, "Principal must contain the realm section");
        }
    }
}
