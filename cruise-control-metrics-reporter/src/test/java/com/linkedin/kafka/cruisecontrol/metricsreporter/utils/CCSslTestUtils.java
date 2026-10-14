/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import org.apache.kafka.common.config.types.Password;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcContentSignerBuilder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;

public final class CCSslTestUtils {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public enum ConnectionMode {
        CLIENT,
        SERVER
    }

    private CCSslTestUtils() {
        //utility class
    }

    /**
     * Creates a map of SSL configurations for the specified connection mode, trust store, and client certificate usage.
     *
     * @param useClientCert Whether to use a client certificate.
     * @param connectionMode The connection mode (CLIENT or SERVER).
     * @param trustStore Whether to create a trust store.
     * @param trustStoreFile The file for the trust store.
     * @param certAlias The alias for the certificate in the trust store.
     * @return A map containing SSL configurations.
     */
    public static Map<String, Object> createSslConfig(boolean useClientCert, boolean trustStore,
                                                      ConnectionMode connectionMode, File trustStoreFile,
                                                      String certAlias)
        throws Exception {
        CCSslConfigsBuilder builder = (new CCSslConfigsBuilder(connectionMode)).useClientCert(useClientCert)
            .certAlias(certAlias);

        if (trustStore) {
            builder = builder.createNewTrustStore(trustStoreFile);
        } else {
            builder = builder.useExistingTrustStore(trustStoreFile);
        }

        return builder.buildJks();
    }
    
    /**
     * Creates a trust store file with the specified certificates.
     *
     * @param dn The distinguished name for the self-signed certificate.
     * @param keyPair The key pair to use for signing the certificate.
     * @return A self-signed X509 certificate.
     */
    public static X509Certificate generate(X500Name dn, KeyPair keyPair) throws CertificateException {
        try {
            final int days = 30;
            final String algorithm = "SHA1withRSA";
            Security.addProvider(new BouncyCastleProvider());
            AlgorithmIdentifier sigAlgId = (new DefaultSignatureAlgorithmIdentifierFinder()).find(algorithm);
            AlgorithmIdentifier digAlgId = (new DefaultDigestAlgorithmIdentifierFinder()).find(sigAlgId);
            AsymmetricKeyParameter privateKeyAsymKeyParam =
                PrivateKeyFactory.createKey(keyPair.getPrivate().getEncoded());
            SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
            BcContentSignerBuilder signerBuilder;
            signerBuilder = new BcRSAContentSignerBuilder(sigAlgId, digAlgId);
            ContentSigner sigGen = signerBuilder.build(privateKeyAsymKeyParam);
            Date now = new Date();
            Date to = new Date(now.getTime() + (long) days * 86400000L);
            BigInteger sn = new BigInteger(64, new SecureRandom());
            X509v3CertificateBuilder v3CertGen = new X509v3CertificateBuilder(dn, sn, now, to, dn, subPubKeyInfo);

            X509CertificateHolder certificateHolder = v3CertGen.build(sigGen);
            return (new JcaX509CertificateConverter()).setProvider("BC").getCertificate(certificateHolder);
        } catch (CertificateException ce) {
            throw ce;
        } catch (Exception e) {
            throw new CertificateException(e);
        }
    }

    /**
     * Creates a trust store file with the specified certificates.
     *
     * @param filename The name of the trust store file to create.
     * @param password The password for the trust store.
     * @param certs A map of certificate aliases to certificates to be added to the trust store.
     * @param <T> The type of certificate (should extend Certificate).
     * @throws GeneralSecurityException If there is an error creating or saving the key store.
     * @throws IOException If there is an error reading or writing the file.
     */
    public static <T extends Certificate> void createTrustStore(String filename, Password password,
                                                                Map<String, T> certs)
        throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");

        try (InputStream in = Files.newInputStream(Paths.get(filename))) {
            ks.load(in, password.value().toCharArray());
        } catch (EOFException eofe) {
            ks = createEmptyKeyStore();
        }

        for (Map.Entry<String, T> cert : certs.entrySet()) {
            ks.setCertificateEntry(cert.getKey(), cert.getValue());
        }

        saveKeyStore(ks, filename, password);
    }

    /**
     * Creates an empty KeyStore instance.
     *
     * @return An empty KeyStore instance.
     * @throws GeneralSecurityException If there is an error creating the KeyStore.
     * @throws IOException If there is an error initializing the KeyStore.
     */
    private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        return ks;
    }

    /**
     * Saves the KeyStore to a file.
     *
     * @param ks The KeyStore to save.
     * @param filename The name of the file to save the KeyStore to.
     * @param password The password for the KeyStore.
     * @throws GeneralSecurityException If there is an error saving the KeyStore.
     * @throws IOException If there is an error writing to the file.
     */
    private static void saveKeyStore(KeyStore ks, String filename, Password password)
        throws GeneralSecurityException, IOException {
        try (OutputStream out = Files.newOutputStream(Paths.get(filename))) {
            ks.store(out, password.value().toCharArray());
        }
    }

    /**
     * Creates a KeyStore file with a private key and certificate.
     *
     * @param filename The name of the KeyStore file to create.
     * @param password The password for the KeyStore.
     * @param keyPassword The password for the private key.
     * @param alias The alias for the private key in the KeyStore.
     * @param privateKey The private key to be added to the KeyStore.
     * @param cert The certificate to be added to the KeyStore.
     * @throws GeneralSecurityException If there is an error creating or saving the KeyStore.
     * @throws IOException If there is an error reading or writing the file.
     */
    public static void createKeyStore(String filename, Password password, Password keyPassword, String alias,
                                      Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        ks.setKeyEntry(alias, privateKey, keyPassword.value().toCharArray(), new Certificate[]{cert});
        saveKeyStore(ks, filename, password);
    }

    /**
     * Generates a KeyPair for the specified algorithm.
     *
     * @param algorithm The algorithm to use for key generation (e.g., "RSA", "EC").
     * @return A KeyPair generated using the specified algorithm.
     * @throws NoSuchAlgorithmException If the specified algorithm is not available.
     */
    public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
        keyGen.initialize("EC".equals(algorithm) ? 256 : 2048);
        return keyGen.genKeyPair();
    }

    /**
     * Creates a temporary file with the specified prefix and suffix.
     *
     * @param prefix The prefix for the temporary file name.
     * @param suffix The suffix for the temporary file name.
     * @return A temporary file created with the specified prefix and suffix.
     * @throws IOException If an I/O error occurs while creating the file.
     */
    public static File tempFile(final String prefix, final String suffix) throws IOException {
        File file = Files.createTempFile(prefix, suffix).toFile();
        file.deleteOnExit();
        return file;
    }
}
