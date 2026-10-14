/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils.ConnectionMode;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.bouncycastle.asn1.x500.X500Name;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils.createKeyStore;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils.createTrustStore;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils.generate;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils.generateKeyPair;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCSslTestUtils.tempFile;

public class CCSslConfigsBuilder {
    private final ConnectionMode _connectionMode;
    private final String _tlsProtocol;
    private final Password _trustStorePassword;
    private final Password _keyStorePassword;
    private final Password _keyPassword;
    private final String _algorithm;
    private boolean _useClientCert;
    private boolean _createTrustStore;
    private File _trustStoreFile;
    private String _certAlias;
    private String _cn;

    public CCSslConfigsBuilder(ConnectionMode connectionMode) {
        this._connectionMode = connectionMode;
        this._tlsProtocol = SslConfigs.DEFAULT_SSL_PROTOCOL;
        this._trustStorePassword = new Password("TrustStorePassword");
        this._keyStorePassword = connectionMode == CCSslTestUtils.ConnectionMode.SERVER ? new Password("ServerPassword")
            : new Password("ClientPassword");
        this._keyPassword = this._keyStorePassword;
        this._cn = "localhost";
        this._certAlias = connectionMode.name().toLowerCase(Locale.ROOT);
        this._algorithm = "RSA";
        this._createTrustStore = true;
    }

    /**
     * Set trust store file and create a new trust store to true.
     *
     * @param trustStoreFile Trust store file to set.
     * @return This.
     */
    public CCSslConfigsBuilder createNewTrustStore(File trustStoreFile) {
        this._trustStoreFile = trustStoreFile;
        this._createTrustStore = true;
        return this;
    }

    /**
     * Set trust store file and create a new trust store to false.
     * 
     * @param trustStoreFile The existing trust store file to use.
     * @return This.
     */
    public CCSslConfigsBuilder useExistingTrustStore(File trustStoreFile) {
        this._trustStoreFile = trustStoreFile;
        this._createTrustStore = false;
        return this;
    }

    /**
     * Set use client cert.
     *
     * @param useClientCert Whether to use a client certificate.
     * @return This.
     */
    public CCSslConfigsBuilder useClientCert(boolean useClientCert) {
        this._useClientCert = useClientCert;
        return this;
    }

    /**
     * Set use cert alias.
     *
     * @param certAlias The alias for the certificate in the trust store.
     * @return This.
     */
    public CCSslConfigsBuilder certAlias(String certAlias) {
        this._certAlias = certAlias;
        return this;
    }

    /**
     * Set cn.
     *
     * @param cn The common name (CN) for the certificate.
     * @return This.
     */
    public CCSslConfigsBuilder cn(String cn) {
        this._cn = cn;
        return this;
    }

    /**
     * Builds the Java KeyStore (JKS)-based SSL configuration.
     * Depending on the connection mode (CLIENT or SERVER), this method generates the required
     * key pairs and self-signed certificates, creates keystores, and optionally creates a trust store.
     *
     * @return A map of SSL configuration properties suitable for use with Kafka or other TLS-based systems.
     * @throws IOException If file operations fail during keystore or trust store creation.
     * @throws GeneralSecurityException If a cryptographic error occurs while generating keys or certificates.
     */
    public Map<String, Object> buildJks() throws IOException, GeneralSecurityException {
        Map<String, X509Certificate> certs = new HashMap<>();
        File keyStoreFile = null;
        if (this._connectionMode == CCSslTestUtils.ConnectionMode.CLIENT && this._useClientCert) {
            keyStoreFile = tempFile("clientKS", ".jks");
            KeyPair cKP = generateKeyPair(this._algorithm);
            X509Certificate cCert = generate(new X500Name("CN=" + _cn + ", O=A client"), cKP);
            createKeyStore(keyStoreFile.getPath(), this._keyStorePassword, this._keyPassword, "client",
                cKP.getPrivate(), cCert);
            certs.put(this._certAlias, cCert);
        } else if (this._connectionMode == CCSslTestUtils.ConnectionMode.SERVER) {
            keyStoreFile = tempFile("serverKS", ".jks");
            KeyPair sKP = generateKeyPair(this._algorithm);
            X509Certificate sCert = generate(new X500Name("CN=" + _cn + ", O=A server"), sKP);
            createKeyStore(keyStoreFile.getPath(), this._keyStorePassword, this._keyPassword, "server",
                sKP.getPrivate(), sCert);
            certs.put(this._certAlias, sCert);
            keyStoreFile.deleteOnExit();
        }

        if (this._createTrustStore) {
            createTrustStore(this._trustStoreFile.getPath(), this._trustStorePassword, certs);
            this._trustStoreFile.deleteOnExit();
        }

        Map<String, Object> sslConfigs = new HashMap<>();
        sslConfigs.put("ssl.protocol", this._tlsProtocol);
        if (this._connectionMode == CCSslTestUtils.ConnectionMode.SERVER || this._connectionMode == CCSslTestUtils.ConnectionMode.CLIENT
            && keyStoreFile != null) {
            sslConfigs.put("ssl.keystore.location", keyStoreFile.getPath());
            sslConfigs.put("ssl.keystore.type", "JKS");
            sslConfigs.put("ssl.keymanager.algorithm", TrustManagerFactory.getDefaultAlgorithm());
            sslConfigs.put("ssl.keystore.password", this._keyStorePassword);
            sslConfigs.put("ssl.key.password", this._keyPassword);
        }

        sslConfigs.put("ssl.truststore.location", this._trustStoreFile.getPath());
        sslConfigs.put("ssl.truststore.password", this._trustStorePassword);
        sslConfigs.put("ssl.truststore.type", "JKS");
        sslConfigs.put("ssl.trustmanager.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        List<String> enabledProtocols = new ArrayList<>();
        enabledProtocols.add(this._tlsProtocol);
        sslConfigs.put("ssl.enabled.protocols", enabledProtocols);
        return sslConfigs;
    }
}
