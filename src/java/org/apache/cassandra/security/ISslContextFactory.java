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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.SslContext;
import org.apache.cassandra.config.EncryptionOptions;

/**
 * The purpose of this interface is to provide pluggable mechanism for creating custom JSSE and Netty SSLContext
 * objects. Please use the Cassandra configuration key {@code ssl_context_factory_class_name} and provide a custom class-name
 * implementing this interface to plugin a your own way to load the SSLContext.
 *
 * Implementation of this interface must have a constructor with argument of type {@code Map<String,String>} to allow
 * custom parameters to be passed from the Cassandra yaml configuration.
 *
 * Example:
 * <pre>
 *
 * </pre>
 *
 * Since on top of Netty, Cassandra is internally using JSSE SSLContext also for certain use-cases- this interface
 * has methods for both.
 */
public interface ISslContextFactory
{
    /**
     * Creates JSSE SSLContext.
     *
     * @param options EncryptionOptions that could be used for the SSL context creation
     * @param buildTruststore {@code true} if the caller requires Truststore; {@code false} otherwise
     * @return
     * @throws SSLException in case the Ssl Context creation fails for some reason
     */
    SSLContext createJSSESslContext(EncryptionOptions options, boolean buildTruststore) throws SSLException;

    /**
     * Creates Netty's SslContext object.
     *
     * @param options EncryptionOptions that could be used for the SSL context creation
     * @param buildTruststore {@code true} if the caller requires Truststore; {@code false} otherwise
     * @param socketType {@link SocketType} for Netty's Inbound or Outbound channels
     * @param useOpenSsl {@code true} if openSsl is enabled;{@code false} otherwise
     * @param cipherFilter to allow Netty's cipher suite filtering, e.g.
     * {@link io.netty.handler.ssl.SslContextBuilder#ciphers(Iterable, CipherSuiteFilter)}
     * @return
     * @throws SSLException in case the Ssl Context creation fails for some reason
     */
    SslContext createNettySslContext(EncryptionOptions options, boolean buildTruststore, SocketType socketType,
                                     boolean useOpenSsl, CipherSuiteFilter cipherFilter) throws SSLException;

    /**
     * Initializes hot reloading of the security keys/certs. The implementation must guarantee this to be thread safe.
     * @param serverOpts Server encryption options (Internode)
     * @param clientOpts Client encryption options (Native Protocol)
     * @throws SSLException
     */
    void initHotReloading(EncryptionOptions.ServerEncryptionOptions serverOpts,
                                                     EncryptionOptions clientOpts) throws SSLException;
    /**
     * Returns if any changes require the reloading of the SSL context returned by this factory.
     * This will be called by Cassandra's periodic polling for any potential changes that will reload the SSL context
     * . However only newer connections established after the reload will use the reloaded SSL context.
     * @return
     */
    boolean shouldReload();

    /**
     * Indicates if the process holds the inbound/listening end of the socket ({@link SSLFactory.SocketType#SERVER})), or the
     * outbound side ({@link SSLFactory.SocketType#CLIENT}).
     */
    enum SocketType {
        SERVER, CLIENT;
    }
}