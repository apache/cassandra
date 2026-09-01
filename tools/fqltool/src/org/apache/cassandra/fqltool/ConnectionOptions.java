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

import javax.net.ssl.SSLContext;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.SSLFactory;

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
                EncryptionOptions.ClientEncryptionOptions.Builder encBuilder = new EncryptionOptions.ClientEncryptionOptions.Builder();
                encBuilder.withEnabled(true);

                if (truststorePath != null)
                    encBuilder.withTrustStore(truststorePath);
                if (truststorePassword != null)
                    encBuilder.withTrustStorePassword(truststorePassword);

                EncryptionOptions.ClientEncryptionOptions.ClientAuth clientAuth = EncryptionOptions.ClientEncryptionOptions.ClientAuth.NOT_REQUIRED;
                if (keystorePath != null)
                {
                    encBuilder.withKeyStore(keystorePath);
                    clientAuth = EncryptionOptions.ClientEncryptionOptions.ClientAuth.REQUIRED;
                }
                if (keystorePassword != null)
                    encBuilder.withKeyStorePassword(keystorePassword);

                EncryptionOptions.ClientEncryptionOptions clientEncOptions = encBuilder.build();
                SSLContext sslContext = SSLFactory.createSSLContext(clientEncOptions, clientAuth);

                return RemoteEndpointAwareJdkSSLOptions.builder()
                                                        .withSSLContext(sslContext)
                                                        .build();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Could not configure SSL for fqltool replay", e);
            }
        }
    }
}
