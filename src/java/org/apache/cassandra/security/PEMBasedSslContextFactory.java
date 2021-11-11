/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SslContextFactory for the <a href="">PEM standard</a> encoded private keys and certificates/public-keys.
 * It parses the key material based on the standard defined in the <a href="https://datatracker.ietf.org/doc/html/rfc7468">RFC 7468</a>.
 * It creates <a href="https://datatracker.ietf.org/doc/html/rfc5208">PKCS# 8</a> based RSA private key and X509 certificate(s)
 * for the public key to build the required keystore and the truststore managers that are used for the SSL context creation.
 * Internally it builds Java {@link KeyStore} with <a href="https://datatracker.ietf.org/doc/html/rfc7292">PKCS# 12</a> <a href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#keystore-types">store type</a>
 * to be used for keystore and the truststore managers.
 *
 * This factory also supports 'hot reloading' of the key material, the same way as defined by {@link FileBasedSslContextFactory},
 * if it is file based. This factory ignores the existing 'store_type' configuration used for other file based store
 * types like JKS.
 *
 * You can configure this factory with either inline PEM data or with the files having the required PEM data as shown
 * below,
 *
 * <b>Configuration: PEM keys/certs defined inline (mind the spaces in the YAML!)</b>
 * <pre>
 *     client/server_encryption_options:
 *      ssl_context_factory:
 *         class_name: org.apache.cassandra.security.PEMBasedSslContextFactory
 *         parameters:
 *             private_key: |
 *              -----BEGIN ENCRYPTED PRIVATE KEY----- OR -----BEGIN PRIVATE KEY-----
 *              <your base64 encoded private key>
 *              -----END ENCRYPTED PRIVATE KEY----- OR -----END PRIVATE KEY-----
 *              -----BEGIN CERTIFICATE-----
 *              <your base64 encoded certificate chain>
 *              -----END CERTIFICATE-----
 *
 *             private_key_password: "<your password if the private key is encrypted with a password>"
 *
 *             trusted_certificates: |
 *               -----BEGIN CERTIFICATE-----
 *               <your base64 encoded certificate>
 *               -----END CERTIFICATE-----
 * </pre>
 *
 * <b>Configuration: PEM keys/certs defined in files</b>
 * <pre>
 *     client/server_encryption_options:
 *      ssl_context_factory:
 *         class_name: org.apache.cassandra.security.PEMBasedSslContextFactory
 *      keystore: <file path to the keystore file in the PEM format with the private key and the certificate chain>
 *      keystore_password: "<your password if the private key is encrypted with a password>"
 *      truststore: <file path to the truststore file in the PEM format>
 * </pre>
 *
 */
public final class PEMBasedSslContextFactory extends FileBasedSslContextFactory
{
    private static final Logger logger = LoggerFactory.getLogger(PEMBasedSslContextFactory.class);

    private String targetStoreType;
    private String pemEncodedKey;
    private String keyPassword;
    private String pemEncodedCertificates;
    private boolean maybeFileBasedPrivateKey;
    private boolean maybeFileBasedTrustedCertificates;

    public static final String DEFAULT_TARGET_STORETYPE = "PKCS12";

    public enum ConfigKey
    {
        ENCODED_KEY("private_key"),
        KEY_PASSWORD("private_key_password"),
        ENCODED_CERTIFICATES("trusted_certificates");

        final String keyName;

        ConfigKey(String keyName)
        {
            this.keyName=keyName;
        }

        String getKeyName()
        {
            return keyName;
        }

        static Set<String> asSet()
        {
            Set<String> valueSet = new HashSet<>();
            ConfigKey[] values = values();
            for(ConfigKey key: values) {
                valueSet.add(key.getKeyName().toLowerCase());
            }
            return valueSet;
        }
    }

    public PEMBasedSslContextFactory()
    {
        targetStoreType = DEFAULT_TARGET_STORETYPE;
    }

    public PEMBasedSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);
        targetStoreType = DEFAULT_TARGET_STORETYPE;
        pemEncodedKey = getString(ConfigKey.ENCODED_KEY.getKeyName());
        keyPassword = getString(ConfigKey.KEY_PASSWORD.getKeyName());
        if (StringUtils.isEmpty(keyPassword))
        {
            keyPassword = keystore_password;
        }
        else if (!StringUtils.isEmpty(keystore_password) && !keyPassword.equals(keystore_password))
        {
            throw new IllegalArgumentException("'keystore_password' and 'key_password' both configurations are given and the " +
                                   "values don't match");
        }
        else
        {
            logger.warn("'keystore_password' and 'key_password' both are configured but since the values match it's " +
                        "okay. Ideally you should only specify one of them.");
        }

        if (!StringUtils.isEmpty(truststore_password))
        {
            logger.warn("PEM based truststore should not be using password. Ignoring the given value in " +
                        "'truststore_password' configuration.");
        }

        pemEncodedCertificates = getString(ConfigKey.ENCODED_CERTIFICATES.getKeyName());

        maybeFileBasedPrivateKey = StringUtils.isEmpty(pemEncodedKey);
        maybeFileBasedTrustedCertificates = StringUtils.isEmpty(pemEncodedCertificates);
    }

    /**
     * Decides if this factory has a keystore defined - key material specified in files or inline to the configuration.
     * @return {@code true} if there is a keystore defined; {@code false} otherwise
     */
    @Override
    public boolean hasKeystore()
    {
        return maybeFileBasedPrivateKey ? (keystore != null && new File(keystore).exists()) :
               !StringUtils.isEmpty(pemEncodedKey);
    }

    /**
     * Decides if this factory has a truststore defined - key material specified in files or inline to the
     * configuration.
     * @return {@code true} if there is a truststore defined; {@code false} otherwise
     */
    private boolean hasTruststore()
    {
        return maybeFileBasedTrustedCertificates ? (truststore != null && new File(truststore).exists()) :
               !StringUtils.isEmpty(pemEncodedCertificates);
    }

    /**
     * This enables 'hot' reloading of the key/trust stores based on the last updated timestamps if they are file based.
     */
    @Override
    public synchronized void initHotReloading()
    {
        List<HotReloadableFile> fileList = new ArrayList<>();
        if (maybeFileBasedPrivateKey && hasKeystore())
        {
            fileList.add(new HotReloadableFile(keystore));
        }
        if (maybeFileBasedTrustedCertificates && hasTruststore())
        {
            fileList.add(new HotReloadableFile(truststore));
        }
        if (!fileList.isEmpty())
        {
            hotReloadableFiles = fileList;
        }
    }

    /**
     * Builds required KeyManagerFactory from the PEM based keystore. It also checks for the PrivateKey's certificate's
     * expiry and logs {@code warning} for each expired PrivateKey's certitificate.
     *
     * @return KeyManagerFactory built from the PEM based keystore.
     * @throws SSLException if any issues encountered during the build process
     */
    protected KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        try
        {
            if (hasKeystore())
            {
                if (maybeFileBasedPrivateKey)
                {
                    pemEncodedKey = readPEMFile(keystore); // read PEM from the file
                }

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : algorithm);
                KeyStore ks = buildKeyStore();
                if (!checkedExpiry)
                {
                    checkExpiredCerts(ks);
                    checkedExpiry = true;
                }
                kmf.init(ks, keyPassword != null ? keyPassword.toCharArray() : null);
                return kmf;
            }
            else
            {
                throw new SSLException("Can't build KeyManagerFactory in absence of the Private Key");
            }
        } catch(Exception e) {
            throw new SSLException("failed to build key manager store for secure connections", e);
        }
    }

    /**
     * Builds TrustManagerFactory from the PEM based truststore.
     *
     * @return TrustManagerFactory from the PEM based truststore
     * @throws SSLException if any issues encountered during the build process
     */
    protected TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        try
        {
            if (hasTruststore())
            {
                if (maybeFileBasedTrustedCertificates)
                {
                    pemEncodedCertificates = readPEMFile(truststore); // read PEM from the file
                }

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                algorithm == null ? TrustManagerFactory.getDefaultAlgorithm() : algorithm);
                KeyStore ts = buildTrustStore();
                tmf.init(ts);
                return tmf;
            }
            else
            {
                throw new SSLException("Can't build KeyManagerFactory in absence of the trusted certificates");
            }
        } catch(Exception e) {
            throw new SSLException("failed to build trust manager store for secure connections", e);
        }
    }

    private String readPEMFile(String file) throws IOException
    {
        StringBuilder fileData = new StringBuilder();
        try(BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            String line = null;
            while( (line = br.readLine()) != null)
            {
                fileData.append(line).append(System.lineSeparator());
            }
        }
        return fileData.toString();
    }

    /**
     * Builds KeyStore object given the {@link #targetStoreType} out of the PEM formatted private key material.
     * It uses {@code cassandra-ssl-keystore} as the alias for the created key-entry.
     */
    private KeyStore buildKeyStore() throws GeneralSecurityException, IOException
    {
        boolean encryptedKey = keyPassword != null;
        char[] keyPasswordArray = encryptedKey ? keyPassword.toCharArray() : null;
        PrivateKey privateKey = encryptedKey ? PEMReader.extractPrivateKey(pemEncodedKey, keyPassword) :
                                PEMReader.extractPrivateKey(pemEncodedKey);
        Certificate[] certChainArray = PEMReader.extractCertificates(pemEncodedKey);
        if (certChainArray == null || certChainArray.length == 0)
        {
            throw new SSLException("Could not read any certificates for the certChain for the private key");
        }

        KeyStore keyStore = KeyStore.getInstance(targetStoreType);
        keyStore.load(null, null);
        keyStore.setKeyEntry("cassandra-ssl-keystore", privateKey, keyPasswordArray, certChainArray);
        return keyStore;
    }

    /**
     * Builds KeyStore object given the {@link #targetStoreType} out of the PEM formatted certificates/public-key
     * material.
     *
     * It uses {@code cassandra-ssl-trusted-cert-<numeric-id>} as the alias for the created certificate-entry.
     */
    private KeyStore buildTrustStore() throws GeneralSecurityException, IOException
    {
        Certificate[] certChainArray = PEMReader.extractCertificates(pemEncodedCertificates);
        if (certChainArray == null || certChainArray.length == 0)
        {
            throw new SSLException("Could not read any certificates from the given PEM");
        }

        KeyStore keyStore = KeyStore.getInstance(targetStoreType);
        keyStore.load(null, null);
        for (int i=0; i < certChainArray.length; i++) {
            keyStore.setCertificateEntry("cassandra-ssl-trusted-cert-"+(i+1), certChainArray[i]);
        }
        return keyStore;
    }
}
