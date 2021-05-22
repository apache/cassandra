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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.utils.FBUtilities;

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
     * Indicates if the process holds the inbound/listening end of the socket ({@link SocketType#SERVER})), or the
     * outbound side ({@link SocketType#CLIENT}).
     */
    public enum SocketType
    {
        SERVER, CLIENT
    }

    // Isolate calls to OpenSsl.isAvailable to allow in-jvm dtests to disable tcnative openssl
    // support.  It creates a circular reference that prevents the instance class loader from being
    // garbage collected.
    static private final boolean openSslIsAvailable;
    static
    {
        if (Boolean.getBoolean(Config.PROPERTY_PREFIX + "disable_tcactive_openssl"))
        {
            openSslIsAvailable = false;
        }
        else
        {
            openSslIsAvailable = OpenSsl.isAvailable();
        }
    }
    public static boolean openSslIsAvailable()
    {
        return openSslIsAvailable;
    }

    /**
     * Cached references of SSL Contexts
     */
    private static final ConcurrentHashMap<CacheKey, SslContext> cachedSslContexts = new ConcurrentHashMap<>();

    /**
     * Default initial delay for hot reloading
     */
    public static final int DEFAULT_HOT_RELOAD_INITIAL_DELAY_SEC = 600;

    /**
     * Default periodic check delay for hot reloading
     */
    public static final int DEFAULT_HOT_RELOAD_PERIOD_SEC = 600;

    /**
     * State variable to maintain initialization invariant
     */
    private static boolean isHotReloadingInitialized = false;

    private static ISslContextFactory sslContextFactory;

    /** Provides the list of protocols that would have been supported if "TLS" was selected as the
     * protocol before the change for CASSANDRA-13325 that expects explicit protocol versions.
     * @return list of enabled protocol names
     */
    public static List<String> tlsInstanceProtocolSubstitution()
    {
        try
        {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, null, null);
            SSLParameters params = ctx.getDefaultSSLParameters();
            String[] protocols = params.getProtocols();
            return Arrays.asList(protocols);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error finding supported TLS Protocols", e);
        }
    }

    public static void setSslContextFactory(ISslContextFactory sslContextFactory) {
        SSLFactory.sslContextFactory = sslContextFactory;
    }

    /**
     * Create a JSSE {@link SSLContext}.
     */
    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        return sslContextFactory.createJSSESslContext(options, buildTruststore);
    }

    /**
     * get a netty {@link SslContext} instance
     */
    public static SslContext getOrCreateSslContext(EncryptionOptions options, boolean buildTruststore,
                                                   SocketType socketType) throws IOException
    {
        return getOrCreateSslContext(options, buildTruststore, socketType, openSslIsAvailable());
    }

    /**
     * Get a netty {@link SslContext} instance.
     */
    @VisibleForTesting
    static SslContext getOrCreateSslContext(EncryptionOptions options,
                                            boolean buildTruststore,
                                            SocketType socketType,
                                            boolean useOpenSsl) throws IOException
    {
        CacheKey key = new CacheKey(options, socketType, useOpenSsl);
        SslContext sslContext;

        sslContext = cachedSslContexts.get(key);
        if (sslContext != null)
            return sslContext;

        sslContext = createNettySslContext(options, buildTruststore, socketType, useOpenSsl);

        SslContext previous = cachedSslContexts.putIfAbsent(key, sslContext);
        if (previous == null)
            return sslContext;

        ReferenceCountUtil.release(sslContext);
        return previous;
    }

    /**
     * Create a Netty {@link SslContext}
     */
    static SslContext createNettySslContext(EncryptionOptions options, boolean buildTruststore,
                                            SocketType socketType, boolean useOpenSsl) throws IOException
    {
        return createNettySslContext(options, buildTruststore, socketType, useOpenSsl,
                                     LoggingCipherSuiteFilter.QUIET_FILTER);
    }

    /**
     * Create a Netty {@link SslContext} with a supplied cipherFilter
     */
    static SslContext createNettySslContext(EncryptionOptions options, boolean buildTruststore,
                                            SocketType socketType, boolean useOpenSsl, CipherSuiteFilter cipherFilter) throws IOException
    {
        ISslContextFactory.SocketType adaptedSocketType = socketType == SocketType.SERVER ?
                                                          ISslContextFactory.SocketType.SERVER :
                                                          ISslContextFactory.SocketType.CLIENT;
        return sslContextFactory.createNettySslContext(options, buildTruststore, adaptedSocketType, useOpenSsl,
                                                       cipherFilter);
    }

    /**
     * Performs a lightweight check whether the certificate files have been refreshed.
     *
     * @throws IllegalStateException if {@link #initHotReloading(EncryptionOptions.ServerEncryptionOptions, EncryptionOptions, boolean)}
     *                               is not called first
     */
    public static void checkCertFilesForHotReloading(EncryptionOptions.ServerEncryptionOptions serverOpts,
                                                     EncryptionOptions clientOpts)
    {
        if (!isHotReloadingInitialized)
            throw new IllegalStateException("Hot reloading functionality has not been initialized.");

        logger.debug("Checking whether certificates have been updated {}", sslContextFactory.getClass().getName());

        if (sslContextFactory.shouldReload())
        {
            logger.info("SSL certificates have been updated. Reseting the ssl contexts for new connections.");
            try
            {
                validateSslCerts(serverOpts, clientOpts);
                cachedSslContexts.clear();
            }
            catch(Exception e)
            {
                logger.error("Failed to hot reload the SSL Certificates! Please check the certificate files.", e);
            }
        }
    }

    /**
     * Determines whether to hot reload certificates and schedules a periodic task for it.
     *
     * @param serverOpts Server encryption options (Internode)
     * @param clientOpts Client encryption options (Native Protocol)
     */
    public static synchronized void initHotReloading(EncryptionOptions.ServerEncryptionOptions serverOpts,
                                                     EncryptionOptions clientOpts,
                                                     boolean force) throws IOException
    {
        if (isHotReloadingInitialized && !force)
            return;

        logger.debug("Initializing hot reloading SSLContext");

        sslContextFactory.initHotReloading(serverOpts, clientOpts);

        if (!isHotReloadingInitialized)
        {
            ScheduledExecutors.scheduledTasks
                .scheduleWithFixedDelay(() -> checkCertFilesForHotReloading(
                                                DatabaseDescriptor.getInternodeMessagingEncyptionOptions(),
                                                DatabaseDescriptor.getNativeProtocolEncryptionOptions()),
                                        DEFAULT_HOT_RELOAD_INITIAL_DELAY_SEC,
                                        DEFAULT_HOT_RELOAD_PERIOD_SEC, TimeUnit.SECONDS);
        }

        isHotReloadingInitialized = true;
    }

    // Non-logging
    /*
     * This class will filter all requested ciphers out that are not supported by the current {@link SSLEngine},
     * logging messages for all dropped ciphers, and throws an exception if no ciphers are supported
     */
    public static final class LoggingCipherSuiteFilter implements CipherSuiteFilter
    {
        // Version without logging the ciphers, make sure same filtering logic is used
        // all the time, regardless of user output.
        public static final CipherSuiteFilter QUIET_FILTER = new LoggingCipherSuiteFilter();
        final String settingDescription;

        private LoggingCipherSuiteFilter()
        {
            this.settingDescription = null;
        }

        public LoggingCipherSuiteFilter(String settingDescription)
        {
            this.settingDescription = settingDescription;
        }


        @Override
        public String[] filterCipherSuites(Iterable<String> ciphers, List<String> defaultCiphers,
                                           Set<String> supportedCiphers)
        {
            Objects.requireNonNull(defaultCiphers, "defaultCiphers");
            Objects.requireNonNull(supportedCiphers, "supportedCiphers");

            final List<String> newCiphers;
            if (ciphers == null)
            {
                newCiphers = new ArrayList<>(defaultCiphers.size());
                ciphers = defaultCiphers;
            }
            else
            {
                newCiphers = new ArrayList<>(supportedCiphers.size());
            }
            for (String c : ciphers)
            {
                if (c == null)
                {
                    break;
                }
                if (supportedCiphers.contains(c))
                {
                    newCiphers.add(c);
                }
                else
                {
                    if (settingDescription != null)
                    {
                        logger.warn("Dropping unsupported cipher_suite {} from {} configuration",
                                    c, settingDescription.toLowerCase());
                    }
                }
            }
            if (newCiphers.isEmpty())
            {
                throw new IllegalStateException("No ciphers left after filtering supported cipher suite");
            }

            return newCiphers.toArray(new String[0]);
        }
    }

    private static boolean filterOutSSLv2Hello(String string)
    {
        return !string.equals("SSLv2Hello");
    }

    public static void validateSslContext(String contextDescription, EncryptionOptions options, boolean buildTrustStore, boolean logProtocolAndCiphers) throws IOException
    {
        if (options != null && options.tlsEncryptionPolicy() != EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
        {
            try
            {
                CipherSuiteFilter loggingCipherSuiteFilter = logProtocolAndCiphers ? new LoggingCipherSuiteFilter(contextDescription)
                                                                                   : LoggingCipherSuiteFilter.QUIET_FILTER;
                SslContext serverSslContext = createNettySslContext(options, buildTrustStore, SocketType.SERVER, openSslIsAvailable(), loggingCipherSuiteFilter);
                try
                {
                    SSLEngine engine = serverSslContext.newEngine(ByteBufAllocator.DEFAULT);
                    try
                    {
                        if (logProtocolAndCiphers)
                        {
                            String[] supportedProtocols = engine.getSupportedProtocols();
                            String[] supportedCiphers = engine.getSupportedCipherSuites();
                            // Netty always adds the SSLv2Hello pseudo-protocol.  (Netty commit 7a39afd031accea9ee38653afbd58eb1c466deda)
                            // To avoid triggering any log scanners that are concerned about SSL2 references, filter
                            // it from the output.
                            String[] enabledProtocols = engine.getEnabledProtocols();
                            String filteredEnabledProtocols =
                                supportedProtocols == null ? "system default"
                                                           : Arrays.stream(engine.getEnabledProtocols())
                                                            .filter(SSLFactory::filterOutSSLv2Hello)
                                                            .collect(Collectors.joining(", "));
                            String[] enabledCiphers = engine.getEnabledCipherSuites();

                            logger.debug("{} supported TLS protocols: {}", contextDescription,
                                         supportedProtocols == null ? "system default" : String.join(", ", supportedProtocols));
                            logger.debug("{} unfiltered enabled TLS protocols: {}", contextDescription,
                                        enabledProtocols == null ? "system default" : String.join(", ", enabledProtocols));
                            logger.info("{} enabled TLS protocols: {}", contextDescription, filteredEnabledProtocols);
                            logger.debug("{} supported cipher suites: {}", contextDescription,
                                         supportedCiphers == null ? "system default" : String.join(", ", supportedCiphers));
                            logger.info("{} enabled cipher suites: {}", contextDescription,
                                        enabledCiphers == null ? "system default" : String.join(", ", enabledCiphers));
                        }
                    }
                    finally
                    {
                        engine.closeInbound();
                        engine.closeOutbound();
                        ReferenceCountUtil.release(engine);
                    }
                }
                finally
                {
                    ReferenceCountUtil.release(serverSslContext);
                }

                // Make sure it is possible to build the client context too
                SslContext clientSslContext = createNettySslContext(options, buildTrustStore, SocketType.CLIENT, openSslIsAvailable());
                ReferenceCountUtil.release(clientSslContext);
            }
            catch (Exception e)
            {
                throw new IOException("Failed to create SSL context using " + contextDescription, e);
            }
        }
    }

    /**
     * Sanity checks all certificates to ensure we can actually load them
     */
    public static void validateSslCerts(EncryptionOptions.ServerEncryptionOptions serverOpts, EncryptionOptions clientOpts) throws IOException
    {
        validateSslContext("server_encryption_options", serverOpts, true, false);
        validateSslContext("client_encryption_options", clientOpts, clientOpts.require_client_auth, false);
    }

    static class CacheKey
    {
        private final EncryptionOptions encryptionOptions;
        private final SocketType socketType;
        private final boolean useOpenSSL;

        public CacheKey(EncryptionOptions encryptionOptions, SocketType socketType, boolean useOpenSSL)
        {
            this.encryptionOptions = encryptionOptions;
            this.socketType = socketType;
            this.useOpenSSL = useOpenSSL;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return (socketType == cacheKey.socketType &&
                    useOpenSSL == cacheKey.useOpenSSL &&
                    Objects.equals(encryptionOptions, cacheKey.encryptionOptions));
        }

        public int hashCode()
        {
            int result = 0;
            result += 31 * socketType.hashCode();
            result += 31 * encryptionOptions.hashCode();
            result += 31 * Boolean.hashCode(useOpenSSL);
            return result;
        }
    }
}
