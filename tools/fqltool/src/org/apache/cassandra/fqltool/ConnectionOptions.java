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

package org.apache.cassandra.fqltool;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;

/**
 * Holds the SSL and authentication settings used to connect to target hosts during fqltool replay.
 * Note that providing any SSL-related configuration option implicitly enables SSL.
 */
public class ConnectionOptions
{
    private final boolean ssl;
    private final SSLOptions sslOptions;
    private final String authProviderClass;

    private ConnectionOptions(boolean ssl, SSLOptions sslOptions, String authProviderClass)
    {
        this.ssl = ssl;
        this.sslOptions = sslOptions;
        this.authProviderClass = authProviderClass;
    }

    public boolean ssl()
    {
        return ssl;
    }

    public SSLOptions sslOptions()
    {
        return sslOptions;
    }

    public String authProviderClass()
    {
        return authProviderClass;
    }

    /**
     * Builds the configured AuthProvider: a (String,String) constructor when credentials are present, otherwise a no-arg constructor.
     */
    @SuppressWarnings("unchecked")
    public AuthProvider instantiateAuthProvider(String user, String password)
    {
        try
        {
            Class<? extends AuthProvider> clazz = (Class<? extends AuthProvider>) Class.forName(authProviderClass);

            if (user != null && password != null)
                return clazz.getConstructor(String.class, String.class).newInstance(user, password);

            return clazz.getDeclaredConstructor().newInstance();
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException("Auth provider " + authProviderClass + " does not support plain text credentials", e);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not instantiate auth provider: " + authProviderClass, e);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean ssl;
        private String truststorePath;
        private String truststorePassword;
        private String keystorePath;
        private String keystorePassword;
        private String authProviderClass;

        public Builder withSsl(boolean ssl)
        {
            this.ssl = ssl;
            return this;
        }

        public Builder withTruststore(String truststorePath)
        {
            this.truststorePath = truststorePath;
            return this;
        }

        public Builder withTruststorePassword(String truststorePassword)
        {
            this.truststorePassword = truststorePassword;
            return this;
        }

        public Builder withKeystore(String keystorePath)
        {
            this.keystorePath = keystorePath;
            return this;
        }

        public Builder withKeystorePassword(String keystorePassword)
        {
            this.keystorePassword = keystorePassword;
            return this;
        }

        public Builder withAuthProviderClass(String authProviderClass)
        {
            this.authProviderClass = authProviderClass;
            return this;
        }

        public ConnectionOptions build()
        {
            if (truststorePassword != null && truststorePath == null)
                throw new IllegalArgumentException("--ssl-truststore-password requires --ssl-truststore to be set");
            if (keystorePassword != null && keystorePath == null)
                throw new IllegalArgumentException("--ssl-keystore-password requires --ssl-keystore to be set");

            // any SSL-related option implicitly enables SSL
            boolean effectiveSsl = ssl || truststorePath != null || keystorePath != null;

            if (authProviderClass != null)
                validateAuthProviderClass();

            SSLOptions sslOptions = effectiveSsl ? buildSSLOptions() : null;

            return new ConnectionOptions(effectiveSsl, sslOptions, authProviderClass);
        }

        private void validateAuthProviderClass()
        {
            try
            {
                Class<?> clazz = Class.forName(authProviderClass);
                if (!AuthProvider.class.isAssignableFrom(clazz))
                    throw new IllegalArgumentException(authProviderClass + " does not implement " + AuthProvider.class.getName());
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Could not find auth provider class: " + authProviderClass, e);
            }
        }

        private SSLOptions buildSSLOptions()
        {
            try
            {
                TrustManagerFactory tmf = buildTrustManagerFactory();
                KeyManagerFactory kmf = keystorePath != null ? buildKeyManagerFactory() : null;

                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(kmf != null ? kmf.getKeyManagers() : null,
                                tmf.getTrustManagers(),
                                null);

                return RemoteEndpointAwareJdkSSLOptions.builder()
                                                        .withSSLContext(sslContext)
                                                        .build();
            }
            catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException | CertificateException | IOException | UnrecoverableKeyException e)
            {
                throw new RuntimeException("Could not configure SSL for fqltool replay", e);
            }
        }

        private TrustManagerFactory buildTrustManagerFactory() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            if (truststorePath != null)
            {
                KeyStore ts = KeyStore.getInstance("JKS");
                char[] password = truststorePassword != null ? truststorePassword.toCharArray() : null;
                try (FileInputStream fis = new FileInputStream(truststorePath))
                {
                    ts.load(fis, password);
                }
                tmf.init(ts);
            }
            else
            {
                tmf.init((KeyStore) null);
            }

            return tmf;
        }

        private KeyManagerFactory buildKeyManagerFactory() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, UnrecoverableKeyException
        {
            KeyStore ks = KeyStore.getInstance("JKS");
            char[] password = keystorePassword != null ? keystorePassword.toCharArray() : null;
            try (FileInputStream fis = new FileInputStream(keystorePath))
            {
                ks.load(fis, password);
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, password);

            return kmf;
        }
    }
}
