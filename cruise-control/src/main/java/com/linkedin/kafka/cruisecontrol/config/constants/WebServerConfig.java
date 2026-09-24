/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import java.util.concurrent.TimeUnit;

import com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


/**
 * A class to keep Cruise Control Web Server Configs and defaults.
 * DO NOT CHANGE EXISTING CONFIG NAMES AS CHANGES WOULD BREAK USER CODE.
 */
public final class WebServerConfig {

    /**
     * <code>webserver.http.port</code>
     */
    public static final String WEBSERVER_HTTP_PORT_CONFIG = "webserver.http.port";
    public static final int DEFAULT_WEBSERVER_HTTP_PORT = 9090;
    public static final String WEBSERVER_HTTP_PORT_DOC = "Cruise Control Webserver bind port.";

    /**
     * <code>webserver.http.max.header.size</code>
     */
    public static final String WEBSERVER_HTTP_MAX_HEADER_SIZE = "webserver.http.header.size";
    public static final int DEFAULT_WEBSERVER_HTTP_MAX_HEADER_SIZE = 8 * 1024;
    public static final String WEBSERVER_HTTP_MAX_HEADER_SIZE_DOC = "Cruise Control Webserver max request header size.";

    /**
     * <code>webserver.http.address</code>
     */
    public static final String WEBSERVER_HTTP_ADDRESS_CONFIG = "webserver.http.address";
    public static final String DEFAULT_WEBSERVER_HTTP_ADDRESS = "127.0.0.1";
    public static final String WEBSERVER_HTTP_ADDRESS_DOC = "Cruise Control Webserver bind ip address.";

    /**
     * <code>webserver.http.cors.enabled</code>
     */
    public static final String WEBSERVER_HTTP_CORS_ENABLED_CONFIG = "webserver.http.cors.enabled";
    public static final boolean DEFAULT_WEBSERVER_HTTP_CORS_ENABLED = false;
    public static final String WEBSERVER_HTTP_CORS_ENABLED_DOC = "CORS enablement flag. true if enabled, false otherwise";

    /**
     * <code>two.step.verification.enabled</code>
     */
    public static final String TWO_STEP_VERIFICATION_ENABLED_CONFIG = "two.step.verification.enabled";
    public static final boolean DEFAULT_TWO_STEP_VERIFICATION_ENABLED = false;
    public static final String TWO_STEP_VERIFICATION_ENABLED_DOC = "Enable two-step verification for processing POST requests.";

    /**
     * <code>webserver.http.cors.origin</code>
     */
    public static final String WEBSERVER_HTTP_CORS_ORIGIN_CONFIG = "webserver.http.cors.origin";
    public static final String DEFAULT_WEBSERVER_HTTP_CORS_ORIGIN = "*";
    public static final String WEBSERVER_HTTP_CORS_ORIGIN_DOC = "Value for the Access-Control-Allow-Origin header.";

    /**
     * <code>webserver.http.cors.allowmethods</code>
     */
    public static final String WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG = "webserver.http.cors.allowmethods";
    public static final String DEFAULT_WEBSERVER_HTTP_CORS_ALLOWMETHODS = "OPTIONS, GET, POST";
    public static final String WEBSERVER_HTTP_CORS_ALLOWMETHODS_DOC = "Value for the Access-Control-Request-Method header.";

    /**
     * <code>webserver.http.cors.exposeheaders</code>
     */
    public static final String WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG = "webserver.http.cors.exposeheaders";
    public static final String DEFAULT_WEBSERVER_HTTP_CORS_EXPOSEHEADERS = "User-Task-ID";
    public static final String WEBSERVER_HTTP_CORS_EXPOSEHEADERS_DOC = "Value for the Access-Control-Expose-Headers header.";

    /**
     * <code>webserver.api.urlprefix</code>
     */
    public static final String WEBSERVER_API_URLPREFIX_CONFIG = "webserver.api.urlprefix";
    public static final String DEFAULT_WEBSERVER_API_URLPREFIX = "/kafkacruisecontrol/*";
    public static final String WEBSERVER_API_URLPREFIX_DOC = "REST API default url prefix";

    /**
     * <code>webserver.ui.diskpath</code>
     */
    public static final String WEBSERVER_UI_DISKPATH_CONFIG = "webserver.ui.diskpath";
    public static final String DEFAULT_WEBSERVER_UI_DISKPATH = "./cruise-control-ui/dist/";
    public static final String WEBSERVER_UI_DISKPATH_DOC = "Location where the Cruise Control frontend is deployed";

    /**
     * <code>webserver.ui.urlprefix</code>
     */
    public static final String WEBSERVER_UI_URLPREFIX_CONFIG = "webserver.ui.urlprefix";
    public static final String DEFAULT_WEBSERVER_UI_URLPREFIX = "/*";
    public static final String WEBSERVER_UI_URLPREFIX_DOC = "URL Path where UI is served from";

    /**
     * <code>webserver.request.maxBlockTimeMs</code>
     */
    public static final String WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_CONFIG = "webserver.request.maxBlockTimeMs";
    public static final long DEFAULT_WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS = TimeUnit.SECONDS.toMillis(10);
    public static final String WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_DOC = "Time after which request is converted to Async";

    /**
     * <code>webserver.session.maxExpiryTimeMs</code>
     */
    public static final String WEBSERVER_SESSION_EXPIRY_MS_CONFIG = "webserver.session.maxExpiryTimeMs";
    public static final long DEFAULT_WEBSERVER_SESSION_EXPIRY_MS = TimeUnit.MINUTES.toMillis(1);
    public static final String WEBSERVER_SESSION_EXPIRY_MS_DOC = "Default Session Expiry Period";

    /**
     * <code>webserver.session.path</code>
     */
    public static final String WEBSERVER_SESSION_PATH_CONFIG = "webserver.session.path";
    public static final String DEFAULT_WEBSERVER_SESSION_PATH = "/";
    public static final String WEBSERVER_SESSION_PATH_DOC = "Default Session Path (for cookies)";

    /**
     * <code>webserver.accesslog.enabled</code>
     */
    public static final String WEBSERVER_ACCESSLOG_ENABLED_CONFIG = "webserver.accesslog.enabled";
    public static final boolean DEFAULT_WEBSERVER_ACCESSLOG_ENABLED = true;
    public static final String WEBSERVER_ACCESSLOG_ENABLED_DOC = "true if access log is enabled";
    /**
     * <code>two.step.purgatory.retention.time.ms</code>
     */
    public static final String TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG = "two.step.purgatory.retention.time.ms";
    public static final long DEFAULT_TWO_STEP_PURGATORY_RETENTION_TIME_MS = TimeUnit.HOURS.toMillis(336);
    public static final String TWO_STEP_PURGATORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to "
            + "retain the requests in two-step (verification) purgatory.";

    /**
     * <code>two.step.purgatory.max.requests</code>
     */
    public static final String TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG = "two.step.purgatory.max.requests";
    public static final int DEFAULT_TWO_STEP_PURGATORY_MAX_REQUESTS = 25;
    public static final String TWO_STEP_PURGATORY_MAX_REQUESTS_DOC = "The maximum number of requests in two-step "
            + "(verification) purgatory.";

    /**
     * <code>max.active.user.tasks</code>
     */
    public static final String MAX_ACTIVE_USER_TASKS_CONFIG = "max.active.user.tasks";
    public static final int DEFAULT_MAX_ACTIVE_USER_TASKS = 5;
    public static final String MAX_ACTIVE_USER_TASKS_DOC = "The maximum number of user tasks for concurrently running in "
            + "async endpoints across all users.";

    /**
     * <code>webserver.security.provider</code>
     */
    public static final String WEBSERVER_SECURITY_PROVIDER_CONFIG = "webserver.security.provider";
    private static final String DEFAULT_WEBSERVER_SECURITY_PROVIDER = BasicSecurityProvider.class.getName();
    private static final String WEBSERVER_SECURITY_PROVIDER_DOC = "SecurityProvider implementation for defining authentication "
            + "and authorization rules for accessing the web API.";

    /**
     * <code>webserver.security.enable</code>
     */
    public static final String WEBSERVER_SECURITY_ENABLE_CONFIG = "webserver.security.enable";
    public static final boolean DEFAULT_WEBSERVER_SECURITY_ENABLE = false;
    private static final String WEBSERVER_SECURITY_ENABLE_DOC = "Enables the use of authentication and authorization features.";

    /**
     * <code>webserver.auth.credentials.file</code>
     */
    public static final String WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG = "webserver.auth.credentials.file";
    public static final String DEFAULT_WEBSERVER_AUTH_CREDENTIALS_FILE = "/etc/cruisecontrol-basic-auth.credentials";
    private static final String WEBSERVER_AUTH_CREDENTIALS_FILE_DOC = "A file that contains credentials for authentication "
            + "and roles for authorization. The format of the file is the following: username: password [,rolename ...] which "
            + "corresponds to Jetty's HashLoginService's credentials file format.";

    /**
     * <code>webserver.ssl.enable</code>
     */
    public static final String WEBSERVER_SSL_ENABLE_CONFIG = "webserver.ssl.enable";
    public static final boolean DEFAULT_WEBSERVER_SSL_ENABLE = false;
    private static final String WEBSERVER_SSL_ENABLE_DOC = "Enables SSL on the webserver.";

    /**
     * <code>webserver.ssl.keystore.location</code>
     */
    public static final String WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG = "webserver.ssl.keystore.location";
    public static final String DEFAULT_WEBSERVER_SSL_KEYSTORE_LOCATION = null;
    private static final String WEBSERVER_SSL_KEYSTORE_LOCATION_DOC = "The location of the SSL keystore file";

    /**
     * <code>webserver.ssl.keystore.password</code>
     */
    public static final String WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG = "webserver.ssl.keystore.password";
    public static final String DEFAULT_WEBSERVER_SSL_KEYSTORE_PASSWORD = null;
    private static final String WEBSERVER_SSL_KEYSTORE_PASSWORD_DOC = "The store password for the key store file. If this "
            + "isn't set we fall back to Jetty's default behavior.";

    /**
     * <code>webserver.ssl.keystore.type</code>
     */
    public static final String WEBSERVER_SSL_KEYSTORE_TYPE_CONFIG = "webserver.ssl.keystore.type";
    public static final String DEFAULT_WEBSERVER_SSL_KEYSTORE_TYPE = null;
    private static final String WEBSERVER_SSL_KEYSTORE_TYPE_DOC = "The file format of the key store file. This is an "
            + "optional config. If this isn't set we fall back to Jetty's default behavior.";

    /**
     * <code>webserver.ssl.key.password</code>
     */
    public static final String WEBSERVER_SSL_KEY_PASSWORD_CONFIG = "webserver.ssl.key.password";
    public static final String DEFAULT_WEBSERVER_SSL_KEY_PASSWORD = null;
    private static final String WEBSERVER_SSL_KEY_PASSWORD_DOC = "The password of the private key in the key store file.";

    /**
     * <code>webserver.ssl.protocol</code>
     */
    public static final String WEBSERVER_SSL_PROTOCOL_CONFIG = "webserver.ssl.protocol";
    public static final String DEFAULT_WEBSERVER_SSL_PROTOCOL = "TLS";
    private static final String WEBSERVER_SSL_PROTOCOL_DOC = "Sets the SSL protocol to use. By default it's TLS.";

    /**
     * <code>webserver.ssl.include.ciphers</code>
     */
    public static final String WEBSERVER_SSL_INCLUDE_CIPHERS_CONFIG = "webserver.ssl.include.ciphers";
    public static final String DEFAULT_WEBSERVER_SSL_INCLUDE_CIPHERS = null;
    private static final String WEBSERVER_SSL_INCLUDE_CIPHERS_DOC = "Sets the included ciphers (list) for the webserver. "
            + "If not set, it does not change the system defaults.";

    /**
     * <code>webserver.ssl.exclude.ciphers</code>
     */
    public static final String WEBSERVER_SSL_EXCLUDE_CIPHERS_CONFIG = "webserver.ssl.exclude.ciphers";
    public static final String DEFAULT_WEBSERVER_SSL_EXCLUDE_CIPHERS = null;
    private static final String WEBSERVER_SSL_EXCLUDE_CIPHERS_DOC = "Sets the excluded ciphers (list of patterns) for the webserver. "
            + "If not set, it does not change the system defaults.";

    /**
     * <code>webserver.ssl.include.protocols</code>
     */
    public static final String WEBSERVER_SSL_INCLUDE_PROTOCOLS_CONFIG = "webserver.ssl.include.protocols";
    public static final String DEFAULT_WEBSERVER_SSL_INCLUDE_PROTOCOLS = null;
    private static final String WEBSERVER_SSL_INCLUDE_PROTOCOLS_DOC = "Sets the included TLS protocols for the webserver. "
            + "If not set, it does not change the system defaults.";

    /**
     * <code>webserver.ssl.exclude.protocols</code>
     */
    public static final String WEBSERVER_SSL_EXCLUDE_PROTOCOLS_CONFIG = "webserver.ssl.exclude.protocols";
    public static final String DEFAULT_WEBSERVER_SSL_EXCLUDE_PROTOCOLS = null;
    private static final String WEBSERVER_SSL_EXCLUDE_PROTOCOLS_DOC = "Sets the excluded protocols (list of patterns) for the webserver. "
            + "If not set, it does not change the system defaults.";

    /**
     * <code>webserver.ssl.sts.enabled</code>
     */
    public static final String WEBSERVER_SSL_STS_ENABLED = "webserver.ssl.sts.enabled";
    public static final boolean DEFAULT_WEBSERVER_SSL_STS_ENABLED = false;
    public static final String WEBSERVER_SSL_STS_ENABLED_DOC = "Enables the Strict Transport Security header in the web server responses.";

    /**
     * <code>webserver.ssl.sts.include.subdomains</code>
     */
    public static final String WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS = "webserver.ssl.sts.include.subdomains";
    public static final boolean DEFAULT_WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS = true;
    public static final String WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS_DOC = "Sets the includeSubDomains directive of the STS header. "
            + "Only effective when webserver.ssl.sts.enabled is true.";

    /**
     * <code>webserver.ssl.sts.max.age</code>
     */
    public static final String WEBSERVER_SSL_STS_MAX_AGE = "webserver.ssl.sts.max.age";
    public static final long DEFAULT_WEBSERVER_SSL_STS_MAX_AGE = 31536000L;
    public static final String WEBSERVER_SSL_STS_MAX_AGE_DOC = "Sets the value of the max-age (in seconds) directive of the STS header. "
            + "Only effective when webserver.ssl.sts.enabled is true.";

    /**
     * <code>jwt.authentication.provider.url</code>
     */
    public static final String JWT_AUTHENTICATION_PROVIDER_URL_CONFIG = "jwt.authentication.provider.url";
    public static final String DEFAULT_JWT_AUTHENTICATION_PROVIDER_URL = null;
    private static final String JWT_AUTHENTICATION_PROVIDER_URL_DOC = "This is an endpoint of the token issuer. "
            + "Requests without tokens will be redirected to this endpoint for authentication. The given url can contain "
            + "the {redirectUrl} string which is an instruction to the authentication service to redirect to the original "
            + "Cruise Control URL after a successful login. For instance www.my-auth.service.com/websso?origin={redirectUrl}.";

    /**
     * <code>jwt.cookie.name</code>
     */
    public static final String JWT_COOKIE_NAME_CONFIG = "jwt.cookie.name";
    public static final String DEFAULT_JWT_COOKIE_NAME = null;
    private static final String JWT_COOKIE_NAME_DOC = "Cruise Control expects issued tokens to be forwarded in a cookie. "
            + "This config specifies which one will contain the token.";

    /**
     * <code>jwt.auth.certificate.location</code>
     */
    public static final String JWT_AUTH_CERTIFICATE_LOCATION_CONFIG = "jwt.auth.certificate.location";
    public static final String DEFAULT_JWT_AUTH_CERTIFICATE_LOCATION = null;
    private static final String JWT_AUTH_CERTIFICATE_LOCATION_DOC = "A private key is used to sign the JWT token by the "
            + "authentication service and its public key pair is used to validate the signature in the token. This config points "
            + "to the location of the file containing that public key.";

    /**
     * <code>jwt.expected.audiences</code>
     */
    public static final String JWT_EXPECTED_AUDIENCES_CONFIG = "jwt.expected.audiences";
    public static final String DEFAULT_JWT_EXPECTED_AUDIENCES = null;
    private static final String JWT_EXPECTED_AUDIENCES_DOC = "A comma separated list of audiences that Cruise Control accepts. "
            + "Audience is a way for the issuer to indicate what entities the token is intended for. The default value is null, "
            + "which means all audiences are accepted.";

    /**
     * <code>spnego.keytab.file</code>
     */
    public static final String SPNEGO_KEYTAB_FILE_CONFIG = "spnego.keytab.file";
    public static final String DEFAULT_SPNEGO_KEYTAB_FILE = null;
    private static final String SPNEGO_KEYTAB_FILE_DOC = "Specifies the path to the keytab which contains the spnego "
            + "principal that is used for SPNEGO based authentication methods.";

    /**
     * <code>spnego.principal</code>
     */
    public static final String SPNEGO_PRINCIPAL_CONFIG = "spnego.principal";
    public static final String DEFAULT_SPNEGO_PRINCIPAL = null;
    private static final String SPNEGO_PRINCIPAL_DOC = "Specifies the spnego service principal that is used by Cruise Control "
            + "to authenticate clients. This principal is stored in spnego.keytab.file. This must be a fully qualified principal "
            + "in the service/host@REALM format (service is usually HTTP).";

    /**
     * <code>spnego.principal.to.local.rules</code>
     */
    public static final String SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG =
            "spnego.principal.to.local.rules";
    public static final String DEFAULT_SPNEGO_PRINCIPAL_TO_LOCAL_RULES = null;
    public static final String SPNEGO_PRINCIPAL_TO_LOCAL_RULES_DOC = "A list of rules for mapping from principal "
            + "names to short names (typically operating system usernames). The rules are evaluated in order and the "
            + "first rule that matches a principal name is used to map it to a short name. Any later rules in the list are "
            + "ignored. By default, principal names of the form <code>{username}/{hostname}@{REALM}</code> are mapped "
            + "to <code>{username}</code>. When not specified, the short name will be used.";

    /**
     * <code>trusted.proxy.services</code>
     */
    public static final String TRUSTED_PROXY_SERVICES_CONFIG = "trusted.proxy.services";
    public static final String DEFAULT_TRUSTED_PROXY_SERVICES = null;
    private static final String TRUSTED_PROXY_SERVICES_DOC = "A list of trusted proxies who can delegate user commands "
            + "with the doAs query parameter.";

    /**
     * <code>trusted.proxy.services.ip.regex</code>
     */
    public static final String TRUSTED_PROXY_SERVICES_IP_REGEX_CONFIG = "trusted.proxy.services.ip.regex";
    public static final String DEFAULT_TRUSTED_PROXY_SERVICES_IP_REGEX = null;
    private static final String TRUSTED_PROXY_SERVICES_IP_REGEX_DOC = "A Java regular expression that defines the whitelist of "
            + "IP addresses of the trusted proxy services. If a request arrives from these addresses authenticated as one of the specified "
            + "trusted.proxy.services then the operation will be delegated as the user in the doAs parameter. This is an optional "
            + "parameter. Not specifying this means that the IP of the trusted proxy won't be validated.";

    /**
     * <code>trusted.proxy.spnego.fallback.enabled</code>
     */
    public static final String TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED_CONFIG = "trusted.proxy.spnego.fallback.enabled";
    public static final boolean DEFAULT_TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED = false;
    private static final String TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED_DOC = "In some cases it is favourable to use "
            + "the service user if no doAs user is provided. When this flag is enabled and if no doAs user is found in the "
            + "request, then the service user will be used as the authenticated principal (so there will be no delegation "
            + "or impersonation but rather a simple fallback to SPNEGO).";

    /**
     * <code>vertx.enabled</code>
     */
    public static final String VERTX_ENABLED_CONFIG = "vertx.enabled";
    public static final boolean DEFAULT_VERTX_ENABLED = false;
    private static final String VERTX_ENABLED_DOC = "Enables the Vertx.io based API and Swagger UI for Cruise Control.";

    private WebServerConfig() {
    }

    /**
     * Define configs for Web Server.
     *
     * @param configDef Config definition.
     * @return The given ConfigDef after defining the configs for Web Server.
     */
    public static ConfigDef define(ConfigDef configDef) {
        return configDef.define(WEBSERVER_HTTP_PORT_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_WEBSERVER_HTTP_PORT,
                        atLeast(0),
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_HTTP_PORT_DOC)
                .define(WEBSERVER_HTTP_MAX_HEADER_SIZE,
                        ConfigDef.Type.INT,
                        DEFAULT_WEBSERVER_HTTP_MAX_HEADER_SIZE,
                        atLeast(8 * 1024),
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_HTTP_MAX_HEADER_SIZE_DOC)
                .define(WEBSERVER_HTTP_ADDRESS_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_HTTP_ADDRESS,
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_HTTP_ADDRESS_DOC)
                .define(WEBSERVER_HTTP_CORS_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_WEBSERVER_HTTP_CORS_ENABLED,
                        ConfigDef.Importance.LOW,
                        WEBSERVER_HTTP_CORS_ENABLED_DOC)
                .define(TWO_STEP_VERIFICATION_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_TWO_STEP_VERIFICATION_ENABLED,
                        ConfigDef.Importance.MEDIUM,
                        TWO_STEP_VERIFICATION_ENABLED_DOC)
                .define(WEBSERVER_HTTP_CORS_ORIGIN_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_HTTP_CORS_ORIGIN,
                        ConfigDef.Importance.LOW,
                        WEBSERVER_HTTP_CORS_ORIGIN_DOC)
                .define(WEBSERVER_HTTP_CORS_ALLOWMETHODS_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_HTTP_CORS_ALLOWMETHODS,
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_HTTP_CORS_ALLOWMETHODS_DOC)
                .define(WEBSERVER_HTTP_CORS_EXPOSEHEADERS_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_HTTP_CORS_EXPOSEHEADERS,
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_HTTP_CORS_EXPOSEHEADERS_DOC)
                .define(WEBSERVER_API_URLPREFIX_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_API_URLPREFIX,
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_API_URLPREFIX_DOC)
                .define(WEBSERVER_UI_DISKPATH_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_UI_DISKPATH,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_UI_DISKPATH_DOC)
                .define(WEBSERVER_UI_URLPREFIX_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_UI_URLPREFIX,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_UI_URLPREFIX_DOC)
                .define(WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS,
                        atLeast(0L),
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_DOC)
                .define(WEBSERVER_SESSION_EXPIRY_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_WEBSERVER_SESSION_EXPIRY_MS,
                        atLeast(0L),
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_SESSION_EXPIRY_MS_DOC)
                .define(WEBSERVER_SESSION_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_SESSION_PATH,
                        ConfigDef.Importance.HIGH,
                        WEBSERVER_SESSION_PATH_DOC)
                .define(WEBSERVER_ACCESSLOG_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_WEBSERVER_ACCESSLOG_ENABLED,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_ACCESSLOG_ENABLED_DOC)
                .define(TWO_STEP_PURGATORY_RETENTION_TIME_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_TWO_STEP_PURGATORY_RETENTION_TIME_MS,
                        atLeast(TimeUnit.HOURS.toMillis(1)),
                        ConfigDef.Importance.MEDIUM,
                        TWO_STEP_PURGATORY_RETENTION_TIME_MS_DOC)
                .define(TWO_STEP_PURGATORY_MAX_REQUESTS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_TWO_STEP_PURGATORY_MAX_REQUESTS,
                        atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        TWO_STEP_PURGATORY_MAX_REQUESTS_DOC)
                .define(MAX_ACTIVE_USER_TASKS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_MAX_ACTIVE_USER_TASKS,
                        atLeast(1),
                        ConfigDef.Importance.HIGH,
                        MAX_ACTIVE_USER_TASKS_DOC)
                .define(WEBSERVER_SECURITY_ENABLE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_WEBSERVER_SECURITY_ENABLE,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SECURITY_ENABLE_DOC)
                .define(WEBSERVER_SECURITY_PROVIDER_CONFIG,
                        ConfigDef.Type.CLASS,
                        DEFAULT_WEBSERVER_SECURITY_PROVIDER,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SECURITY_PROVIDER_DOC)
                .define(WEBSERVER_AUTH_CREDENTIALS_FILE_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_AUTH_CREDENTIALS_FILE,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_AUTH_CREDENTIALS_FILE_DOC)
                .define(WEBSERVER_SSL_ENABLE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_WEBSERVER_SSL_ENABLE,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_ENABLE_DOC)
                .define(WEBSERVER_SSL_KEYSTORE_LOCATION_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_SSL_KEYSTORE_LOCATION,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_KEYSTORE_LOCATION_DOC)
                .define(WEBSERVER_SSL_KEYSTORE_PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        DEFAULT_WEBSERVER_SSL_KEYSTORE_PASSWORD,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_KEYSTORE_PASSWORD_DOC)
                .define(WEBSERVER_SSL_KEYSTORE_TYPE_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_SSL_KEYSTORE_TYPE,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_KEYSTORE_TYPE_DOC)
                .define(WEBSERVER_SSL_KEY_PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        DEFAULT_WEBSERVER_SSL_KEY_PASSWORD,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_KEY_PASSWORD_DOC)
                .define(WEBSERVER_SSL_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WEBSERVER_SSL_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_PROTOCOL_DOC)
                .define(WEBSERVER_SSL_INCLUDE_CIPHERS_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_WEBSERVER_SSL_INCLUDE_CIPHERS,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_INCLUDE_CIPHERS_DOC)
                .define(WEBSERVER_SSL_EXCLUDE_CIPHERS_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_WEBSERVER_SSL_EXCLUDE_CIPHERS,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_EXCLUDE_CIPHERS_DOC)
                .define(WEBSERVER_SSL_INCLUDE_PROTOCOLS_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_WEBSERVER_SSL_INCLUDE_PROTOCOLS,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_INCLUDE_PROTOCOLS_DOC)
                .define(WEBSERVER_SSL_EXCLUDE_PROTOCOLS_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_WEBSERVER_SSL_EXCLUDE_PROTOCOLS,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_EXCLUDE_PROTOCOLS_DOC)
                .define(WEBSERVER_SSL_STS_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_WEBSERVER_SSL_STS_ENABLED,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_STS_ENABLED_DOC)
                .define(WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_STS_INCLUDE_SUBDOMAINS_DOC)
                .define(WEBSERVER_SSL_STS_MAX_AGE,
                        ConfigDef.Type.LONG,
                        DEFAULT_WEBSERVER_SSL_STS_MAX_AGE,
                        ConfigDef.Importance.MEDIUM,
                        WEBSERVER_SSL_STS_MAX_AGE_DOC)
                .define(JWT_AUTHENTICATION_PROVIDER_URL_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_JWT_AUTHENTICATION_PROVIDER_URL,
                        ConfigDef.Importance.MEDIUM,
                        JWT_AUTHENTICATION_PROVIDER_URL_DOC)
                .define(JWT_COOKIE_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_JWT_COOKIE_NAME,
                        ConfigDef.Importance.MEDIUM,
                        JWT_COOKIE_NAME_DOC)
                .define(JWT_AUTH_CERTIFICATE_LOCATION_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_JWT_AUTH_CERTIFICATE_LOCATION,
                        ConfigDef.Importance.MEDIUM,
                        JWT_AUTH_CERTIFICATE_LOCATION_DOC)
                .define(JWT_EXPECTED_AUDIENCES_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_JWT_EXPECTED_AUDIENCES,
                        ConfigDef.Importance.MEDIUM,
                        JWT_EXPECTED_AUDIENCES_DOC)
                .define(SPNEGO_KEYTAB_FILE_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_SPNEGO_KEYTAB_FILE,
                        ConfigDef.Importance.MEDIUM,
                        SPNEGO_KEYTAB_FILE_DOC)
                .define(SPNEGO_PRINCIPAL_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_SPNEGO_PRINCIPAL,
                        ConfigDef.Importance.MEDIUM,
                        SPNEGO_PRINCIPAL_DOC)
                .define(SPNEGO_PRINCIPAL_TO_LOCAL_RULES_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_SPNEGO_PRINCIPAL_TO_LOCAL_RULES,
                        ConfigDef.Importance.MEDIUM,
                        SPNEGO_PRINCIPAL_TO_LOCAL_RULES_DOC)
                .define(TRUSTED_PROXY_SERVICES_CONFIG,
                        ConfigDef.Type.LIST,
                        DEFAULT_TRUSTED_PROXY_SERVICES,
                        ConfigDef.Importance.MEDIUM,
                        TRUSTED_PROXY_SERVICES_DOC)
                .define(TRUSTED_PROXY_SERVICES_IP_REGEX_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_TRUSTED_PROXY_SERVICES_IP_REGEX,
                        ConfigDef.Importance.MEDIUM,
                        TRUSTED_PROXY_SERVICES_IP_REGEX_DOC)
                .define(TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED,
                        ConfigDef.Importance.MEDIUM,
                        TRUSTED_PROXY_SPNEGO_FALLBACK_ENABLED_DOC)
                .define(VERTX_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_VERTX_ENABLED,
                        ConfigDef.Importance.MEDIUM,
                        VERTX_ENABLED_DOC);
    }
}
