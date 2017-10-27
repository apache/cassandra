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


import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.config.EncryptionOptions;

/**
 * A Factory for providing and setting up client {@link SSLSocket}s. Also provides
 * methods for creating both JSSE {@link SSLContext} instances as well as netty {@link SslContext} instances.
 * <p>
 * Netty {@link SslContext} instances are expensive to create (as well as to destroy) and consume a lof of resources
 * (especially direct memory), but instances can be reused across connections (assuming the SSL params are the same).
 * Hence we cache created instances in {@link #cachedSslContexts}.
 */
public final class SSLFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

    /**
     * Indicator if a connection is shared with a client application ({@link ConnectionType#NATIVE_TRANSPORT})
     * or another cassandra node  ({@link ConnectionType#INTERNODE_MESSAGING}).
     */
    public enum ConnectionType
    {
        NATIVE_TRANSPORT, INTERNODE_MESSAGING
    }

    /**
     * Indicates if the process holds the inbound/listening end of the socket ({@link SocketType#SERVER})), or the
     * outbound side ({@link SocketType#CLIENT}).
     */
    public enum SocketType
    {
        SERVER, CLIENT
    }

    /**
     * Cached references of SSL Contexts
     */
    private static final ConcurrentHashMap<CacheKey, SslContext> cachedSslContexts = new ConcurrentHashMap<>();

    /**
     * Create a JSSE {@link SSLContext}.
     */
    @SuppressWarnings("resource")
    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        TrustManager[] trustManagers = null;
        if (buildTruststore)
            trustManagers = buildTrustManagerFactory(options).getTrustManagers();

        KeyManagerFactory kmf = buildKeyManagerFactory(options, false);

        try
        {
            SSLContext ctx = SSLContext.getInstance(options.protocol);
            ctx.init(kmf.getKeyManagers(), trustManagers, null);
            return ctx;
        }
        catch (Exception e)
        {
            throw new IOException("Error creating/initializing the SSL Context", e);
        }
    }

    static TrustManagerFactory buildTrustManagerFactory(EncryptionOptions options) throws IOException
    {
        try (InputStream tsf = Files.newInputStream(Paths.get(options.truststore)))
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            options.algorithm == null ? TrustManagerFactory.getDefaultAlgorithm() : options.algorithm);
            KeyStore ts = KeyStore.getInstance(options.store_type);
            ts.load(tsf, options.truststore_password.toCharArray());
            tmf.init(ts);
            return tmf;
        }
        catch (Exception e)
        {
            throw new IOException("failed to build trust manager store for secure connections", e);
        }
    }

    static KeyManagerFactory buildKeyManagerFactory(EncryptionOptions options, boolean forServer) throws IOException
    {
        try
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
            options.algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : options.algorithm);
            KeyStore ks = forServer ? KeyStoreManager.internodeInstance.getKeystore()
                                    : KeyStoreManager.clientInstance.getKeystore();
            kmf.init(ks, options.keystore_password.toCharArray());
            return kmf;
        }
        catch (Exception e)
        {
            throw new IOException("failed to build trust manager store for secure connections", e);
        }
    }

    public static String[] filterCipherSuites(String[] supported, String[] desired)
    {
        if (Arrays.equals(supported, desired))
            return desired;
        List<String> ldesired = Arrays.asList(desired);
        ImmutableSet<String> ssupported = ImmutableSet.copyOf(supported);
        String[] ret = Iterables.toArray(Iterables.filter(ldesired, Predicates.in(ssupported)), String.class);
        if (desired.length > ret.length && logger.isWarnEnabled())
        {
            Iterable<String> missing = Iterables.filter(ldesired, Predicates.not(Predicates.in(Sets.newHashSet(ret))));
            logger.warn("Filtering out {} as it isn't supported by the socket", Iterables.toString(missing));
        }
        return ret;
    }

    /**
     * get a netty {@link SslContext} instance
     */
    public static SslContext getSslContext(EncryptionOptions options, boolean buildTruststore, ConnectionType connectionType,
                                           SocketType socketType) throws IOException
    {
        return getSslContext(options, buildTruststore, connectionType, socketType, OpenSsl.isAvailable());
    }

    /**
     * Get a netty {@link SslContext} instance.
     */
    @VisibleForTesting
    static SslContext getSslContext(EncryptionOptions options, boolean buildTruststore, ConnectionType connectionType,
                                    SocketType socketType, boolean useOpenSsl) throws IOException
    {
        CacheKey key = new CacheKey(options, connectionType, socketType);
        SslContext sslContext;

        sslContext = cachedSslContexts.get(key);
        if (sslContext != null)
            return sslContext;

        sslContext = createNettySslContext(options, buildTruststore, connectionType, socketType, useOpenSsl);
        SslContext previous = cachedSslContexts.putIfAbsent(key, sslContext);
        if (previous == null)
            return sslContext;

        ReferenceCountUtil.release(sslContext);
        return previous;
    }

    /**
     * Create a Netty {@link SslContext}
     */
    static SslContext createNettySslContext(EncryptionOptions options, boolean buildTruststore, ConnectionType connectionType,
                                            SocketType socketType, boolean useOpenSsl) throws IOException
    {
        KeyStoreManager ksm = connectionType == ConnectionType.INTERNODE_MESSAGING ?
                              KeyStoreManager.internodeInstance : KeyStoreManager.clientInstance;
        SslContextBuilder builder;
        if (socketType == SocketType.SERVER)
        {
            // KeyStoreManager.instantiate() must be called first to instantiate stores
            X509Credentials cred = ksm.getCredentials();
            builder = SslContextBuilder.forServer(cred.privateKey, cred.chain);
            builder.clientAuth(options.require_client_auth ? ClientAuth.REQUIRE : ClientAuth.NONE);
        }
        else
        {
            builder = SslContextBuilder.forClient();
            if (options.require_client_auth)
            {
                X509Credentials cred = ksm.getCredentials();
                builder.keyManager(cred.privateKey, cred.chain);
            }
        }

        builder.sslProvider(useOpenSsl ? SslProvider.OPENSSL : SslProvider.JDK);

        // only set the cipher suites if the operator has explicitly configured values for it; else, use the default
        // for each ssl implementation (jdk or openssl)
        if (options.cipher_suites != null && options.cipher_suites.length > 0)
            builder.ciphers(Arrays.asList(options.cipher_suites), SupportedCipherSuiteFilter.INSTANCE);

        if (buildTruststore)
            builder.trustManager(buildTrustManagerFactory(options));

        return builder.build();
    }

    static class CacheKey
    {
        private final EncryptionOptions encryptionOptions;
        private final ConnectionType connectionType;
        private final SocketType socketType;

        public CacheKey(EncryptionOptions encryptionOptions, ConnectionType connectionType, SocketType socketType)
        {
            this.encryptionOptions = encryptionOptions;
            this.connectionType = connectionType;
            this.socketType = socketType;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return (connectionType == cacheKey.connectionType &&
                    socketType == cacheKey.socketType &&
                    Objects.equals(encryptionOptions, cacheKey.encryptionOptions));
        }

        public int hashCode()
        {
            int result = 0;
            result += 31 * connectionType.hashCode();
            result += 31 * socketType.hashCode();
            result += 31 * encryptionOptions.hashCode();
            return result;
        }
    }

    static void clearInternodeSslContext()
    {
        logger.debug("Clearing internode SSL context");
        cachedSslContexts.keySet().removeIf(key -> key.connectionType == ConnectionType.INTERNODE_MESSAGING);
    }

    static void clearNativeTransportSslContext()
    {
        logger.debug("Clearing native transport SSL context");
        cachedSslContexts.keySet().removeIf(key -> key.connectionType == ConnectionType.NATIVE_TRANSPORT);
    }
}
