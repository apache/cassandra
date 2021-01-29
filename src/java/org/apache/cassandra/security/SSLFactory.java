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


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
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
     * Indicates if the process holds the inbound/listening end of the socket ({@link SocketType#SERVER})), or the
     * outbound side ({@link SocketType#CLIENT}).
     */
    public enum SocketType
    {
        SERVER, CLIENT
    }

    @VisibleForTesting
    static volatile boolean checkedExpiry = false;

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
     * List of files that trigger hot reloading of SSL certificates
     */
    private static volatile List<HotReloadableFile> hotReloadableFiles = ImmutableList.of();

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

    /**
     * Helper class for hot reloading SSL Contexts
     */
    private static class HotReloadableFile
    {
        private final File file;
        private volatile long lastModTime;

        HotReloadableFile(String path)
        {
            file = new File(path);
            lastModTime = file.lastModified();
        }

        boolean shouldReload()
        {
            long curModTime = file.lastModified();
            boolean result = curModTime != lastModTime;
            lastModTime = curModTime;
            return result;
        }

        @Override
        public String toString()
        {
            return "HotReloadableFile{" +
                       "file=" + file +
                       ", lastModTime=" + lastModTime +
                       '}';
        }
    }

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

    /**
     * Create a JSSE {@link SSLContext}.
     */
    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        TrustManager[] trustManagers = null;
        if (buildTruststore)
            trustManagers = buildTrustManagerFactory(options).getTrustManagers();

        KeyManagerFactory kmf = buildKeyManagerFactory(options);

        try
        {
            SSLContext ctx = SSLContext.getInstance("TLS");
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

    static KeyManagerFactory buildKeyManagerFactory(EncryptionOptions options) throws IOException
    {
        try (InputStream ksf = Files.newInputStream(Paths.get(options.keystore)))
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
            options.algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : options.algorithm);
            KeyStore ks = KeyStore.getInstance(options.store_type);
            ks.load(ksf, options.keystore_password.toCharArray());
            if (!checkedExpiry)
            {
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.getCertificate(alias).getType().equals("X.509"))
                    {
                        Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                        if (expires.before(new Date()))
                            logger.warn("Certificate for {} expired on {}", alias, expires);
                    }
                }
                checkedExpiry = true;
            }
            kmf.init(ks, options.keystore_password.toCharArray());
            return kmf;
        }
        catch (Exception e)
        {
            throw new IOException("failed to build key manager store for secure connections", e);
        }
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
        /*
            There is a case where the netty/openssl combo might not support using KeyManagerFactory. specifically,
            I've seen this with the netty-tcnative dynamic openssl implementation. using the netty-tcnative static-boringssl
            works fine with KeyManagerFactory. If we want to support all of the netty-tcnative options, we would need
            to fall back to passing in a file reference for both a x509 and PKCS#8 private key file in PEM format (see
            {@link SslContextBuilder#forServer(File, File, String)}). However, we are not supporting that now to keep
            the config/yaml API simple.
         */
        KeyManagerFactory kmf = buildKeyManagerFactory(options);
        SslContextBuilder builder;
        if (socketType == SocketType.SERVER)
        {
            builder = SslContextBuilder.forServer(kmf);
            builder.clientAuth(options.require_client_auth ? ClientAuth.REQUIRE : ClientAuth.NONE);
        }
        else
        {
            builder = SslContextBuilder.forClient().keyManager(kmf);
        }

        builder.sslProvider(useOpenSsl ? SslProvider.OPENSSL : SslProvider.JDK);

        builder.protocols(options.acceptedProtocols());

        // only set the cipher suites if the opertor has explicity configured values for it; else, use the default
        // for each ssl implemention (jdk or openssl)
        if (options.cipher_suites != null && !options.cipher_suites.isEmpty())
            builder.ciphers(options.cipher_suites, cipherFilter);

        if (buildTruststore)
            builder.trustManager(buildTrustManagerFactory(options));

        return builder.build();
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

        logger.debug("Checking whether certificates have been updated {}", hotReloadableFiles);

        if (hotReloadableFiles.stream().anyMatch(HotReloadableFile::shouldReload))
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

        List<HotReloadableFile> fileList = new ArrayList<>();

        if (serverOpts != null && serverOpts.tlsEncryptionPolicy() != EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
        {
            fileList.add(new HotReloadableFile(serverOpts.keystore));
            fileList.add(new HotReloadableFile(serverOpts.truststore));
        }

        if (clientOpts != null && clientOpts.tlsEncryptionPolicy() != EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
        {
            fileList.add(new HotReloadableFile(clientOpts.keystore));
            fileList.add(new HotReloadableFile(clientOpts.truststore));
        }

        hotReloadableFiles = ImmutableList.copyOf(fileList);

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
