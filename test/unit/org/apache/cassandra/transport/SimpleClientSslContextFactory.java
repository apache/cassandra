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

package org.apache.cassandra.transport;

import java.util.Map;
import javax.net.ssl.SSLException;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.FileBasedSslContextFactory;

/**
 * A custom implementation of {@link FileBasedSslContextFactory} to be used by tests utilizing {@link SimpleClient}.
 * <p>
 * Provides a subtly different implementation of {@link #createNettySslContext(EncryptionOptions.ClientAuth, SocketType, CipherSuiteFilter)}
 * that only configures an {@link SslContext} for clients and most importantly only configures a key manager if an
 * outbound keystore is configured, where the existing implementation always does this.  This is useful for tests
 * that try to create a client that uses encryption but does not provide a certificate.
 */
public class SimpleClientSslContextFactory extends FileBasedSslContextFactory
{

    public SimpleClientSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);
    }

    @Override
    public SslContext createNettySslContext(EncryptionOptions.ClientAuth clientAuth, SocketType socketType,
                                            CipherSuiteFilter cipherFilter) throws SSLException
    {
        SslContextBuilder builder = SslContextBuilder.forClient();
        // only provide a client certificate if keystore is present.
        if (hasOutboundKeystore())
        {
            builder.keyManager(buildOutboundKeyManagerFactory());
        }

        builder.sslProvider(getSslProvider())
               .protocols(getAcceptedProtocols())
               .trustManager(buildTrustManagerFactory());

        // only set the cipher suites if the operator has explicity configured values for it; else, use the default
        // for each ssl implemention (jdk or openssl)
        if (cipher_suites != null && !cipher_suites.isEmpty())
            builder.ciphers(cipher_suites, cipherFilter);

        return builder.build();
    }
}