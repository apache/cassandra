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

import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.SslContext;

/**
 * The purpose of this interface is to provide pluggable mechanism for creating custom JSSE and Netty SSLContext
 * objects. Please use the Cassandra configuration key {@code ssl_context_factory} as part of {@code
 * client_encryption_options}/{@code server_encryption_options} and provide a custom class-name implementing this
 * interface with parameters to be used to plugin a your own way to load the SSLContext.
 * <p>
 * Implementation of this interface must have a constructor with argument of type {@code Map<String,Object>} to allow
 * custom parameters, needed by the implementation, to be passed from the yaml configuration. Common SSL
 * configurations like {@code protocol, algorithm, cipher_suites, accepted_protocols, require_client_auth,
 * require_endpoint_verification, enabled, optional} will also be passed to that map by Cassanddra.
 * <p>
 * Since on top of Netty, Cassandra is internally using JSSE SSLContext also for certain use-cases- this interface
 * has methods for both.
 * <p>
 * Below is an example of how to configure a custom implementation with parameters
 * <pre>
 * ssl_context_factory:
 *       class_name: org.apache.cassandra.security.YourSslContextFactoryImpl
 *       parameters:
 *         key1: "value1"
 *         key2: "value2"
 *         key3: "value3"
 * </pre>
 */
public interface ISslContextFactory
{
    /**
     * Creates JSSE SSLContext.
     *
     * @param verifyPeerCertificate {@code true} if SSL peer's certificate needs to be verified; {@code false} otherwise
     * @return JSSE's {@link SSLContext}
     * @throws SSLException in case the Ssl Context creation fails for some reason
     */
    SSLContext createJSSESslContext(boolean verifyPeerCertificate) throws SSLException;

    /**
     * Creates Netty's SslContext object.
     *
     * @param verifyPeerCertificate {@code true} if SSL peer's certificate needs to be verified; {@code false} otherwise
     * @param socketType            {@link SocketType} for Netty's Inbound or Outbound channels
     * @param cipherFilter          to allow Netty's cipher suite filtering, e.g.
     *                              {@link io.netty.handler.ssl.SslContextBuilder#ciphers(Iterable, CipherSuiteFilter)}
     * @return Netty's {@link SslContext}
     * @throws SSLException in case the Ssl Context creation fails for some reason
     */
    SslContext createNettySslContext(boolean verifyPeerCertificate, SocketType socketType,
                                     CipherSuiteFilter cipherFilter) throws SSLException;

    /**
     * Initializes hot reloading of the security keys/certs. The implementation must guarantee this to be thread safe.
     *
     * @throws SSLException
     */
    void initHotReloading() throws SSLException;

    /**
     * Returns if any changes require the reloading of the SSL context returned by this factory.
     * This will be called by Cassandra's periodic polling for any potential changes that will reload the SSL context.
     * However only newer connections established after the reload will use the reloaded SSL context.
     *
     * @return {@code true} if SSL Context needs to be reload; {@code false} otherwise
     */
    boolean shouldReload();

    /**
     * Returns if this factory uses private keystore.
     *
     * @return {@code true} by default unless the implementation overrides this
     */
    default boolean hasKeystore()
    {
        return true;
    }

    /**
     * Returns if this factory uses outbound keystore.
     *
     * @return {@code true} by default unless the implementation overrides this
     */
    default boolean hasOutboundKeystore()
    {
        return false;
    }

    /**
     * Returns the prepared list of accepted protocols.
     *
     * @return array of protocol names suitable for passing to Netty's SslContextBuilder.protocols, or null if the
     * default
     */
    List<String> getAcceptedProtocols();

    /**
     * Returns the list of cipher suites supported by the implementation.
     *
     * @return List of supported cipher suites
     */
    List<String> getCipherSuites();

    /**
     * Indicates if the process holds the inbound/listening (Server) end of the socket or the outbound side (Client).
     */
    enum SocketType
    {
        SERVER, CLIENT;
    }
}