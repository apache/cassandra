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

package org.apache.cassandra.distributed.impl;

import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.File;

/**
 * Simplified and independent version of {@link org.apache.cassandra.security.FileBasedSslContextFactory} for
 * testing SSL based JMX clients that require configuring keystore and/or truststore.
 */
public class JmxTestClientSslContextFactory
{
    private final Map<String, Object> parameters;
    // keystore is not needed when the JMX server does not require client-auth
    private final String keystore;
    // keystore could be null in case JMX server does not require client-auth
    private final String keystore_password;
    private final String truststore;
    private final String truststore_password;
    private final String protocol;
    private final String algorithm;
    private final String store_type;

    public JmxTestClientSslContextFactory(Map<String, Object> parameters)
    {
        this.parameters = parameters;
        keystore = getString(EncryptionOptions.ConfigKey.KEYSTORE.toString());
        keystore_password = getString(EncryptionOptions.ConfigKey.KEYSTORE_PASSWORD.toString());
        truststore = getString(EncryptionOptions.ConfigKey.TRUSTSTORE.toString());
        truststore_password = getString(EncryptionOptions.ConfigKey.TRUSTSTORE_PASSWORD.toString());
        protocol = getString(EncryptionOptions.ConfigKey.PROTOCOL.toString(), "TLS");
        algorithm = getString(EncryptionOptions.ConfigKey.ALGORITHM.toString());
        store_type = getString(EncryptionOptions.ConfigKey.STORE_TYPE.toString(), "JKS");
    }

    private String getString(String key, String defaultValue)
    {
        return parameters.get(key) == null ? defaultValue : (String) parameters.get(key);
    }

    private String getString(String key)
    {
        return (String) parameters.get(key);
    }

    private TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        try (InputStream tsf = Files.newInputStream(File.getPath(truststore)))
        {
            final String algorithm = this.algorithm == null ? TrustManagerFactory.getDefaultAlgorithm() : this.algorithm;
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
            KeyStore ts = KeyStore.getInstance(store_type);

            final char[] truststorePassword = StringUtils.isEmpty(truststore_password) ? null : truststore_password.toCharArray();
            ts.load(tsf, truststorePassword);
            tmf.init(ts);
            return tmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build trust manager store for secure connections", e);
        }
    }

    private KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        final String algorithm = this.algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : this.algorithm;

        if (keystore != null)
        {
            try (InputStream ksf = Files.newInputStream(File.getPath(keystore)))
            {
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
                KeyStore ks = KeyStore.getInstance(store_type);
                final char[] password = keystore_password.toCharArray();
                ks.load(ksf, password);
                kmf.init(ks, password);
                return kmf;
            }
            catch (Exception e)
            {
                throw new SSLException("failed to build key manager store for secure connections", e);
            }
        }
        else
        {
            return null;
        }
    }

    public SSLContext createSSLContext() throws SSLException
    {
        TrustManager[] trustManagers = buildTrustManagerFactory().getTrustManagers();
        KeyManagerFactory kmf = buildKeyManagerFactory();
        try
        {
            SSLContext ctx = SSLContext.getInstance(protocol);
            ctx.init(kmf != null ? kmf.getKeyManagers() : null, trustManagers, null);
            return ctx;
        }
        catch (Exception e)
        {
            throw new SSLException("Error creating/initializing the SSL Context", e);
        }
    }
}
