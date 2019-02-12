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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
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
import org.apache.cassandra.concurrent.ScheduledExecutors;
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

    /**
     * Create a JSSE {@link SSLContext}.
     */
    @SuppressWarnings("resource")
    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        TrustManager[] trustManagers = null;
        if (buildTruststore)
            trustManagers = buildTrustManagerFactory(options).getTrustManagers();

        KeyManagerFactory kmf = buildKeyManagerFactory(options);

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
    public static SslContext getOrCreateSslContext(EncryptionOptions options, boolean buildTruststore,
                                                   SocketType socketType) throws IOException
    {
        return getOrCreateSslContext(options, buildTruststore, socketType, OpenSsl.isAvailable());
    }

    /**
     * Get a netty {@link SslContext} instance.
     */
    @VisibleForTesting
    static SslContext getOrCreateSslContext(EncryptionOptions options, boolean buildTruststore,
                                            SocketType socketType, boolean useOpenSsl) throws IOException
    {
        CacheKey key = new CacheKey(options, socketType);
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

        // only set the cipher suites if the opertor has explicity configured values for it; else, use the default
        // for each ssl implemention (jdk or openssl)
        if (options.cipher_suites != null && options.cipher_suites.length > 0)
            builder.ciphers(Arrays.asList(options.cipher_suites), SupportedCipherSuiteFilter.INSTANCE);

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

        validateSslCerts(serverOpts, clientOpts);

        List<HotReloadableFile> fileList = new ArrayList<>();

        if (serverOpts != null && serverOpts.enabled)
        {
            fileList.add(new HotReloadableFile(serverOpts.keystore));
            fileList.add(new HotReloadableFile(serverOpts.truststore));
        }

        if (clientOpts != null && clientOpts.enabled)
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


    /**
     * Sanity checks all certificates to ensure we can actually load them
     */
    public static void validateSslCerts(EncryptionOptions.ServerEncryptionOptions serverOpts, EncryptionOptions clientOpts) throws IOException
    {
        try
        {
            // Ensure we're able to create both server & client SslContexts
            if (serverOpts != null && serverOpts.enabled)
            {
                createNettySslContext(serverOpts, true, SocketType.SERVER, OpenSsl.isAvailable());
                createNettySslContext(serverOpts, true, SocketType.CLIENT, OpenSsl.isAvailable());
            }
        }
        catch (Exception e)
        {
            throw new IOException("Failed to create SSL context using server_encryption_options!", e);
        }

        try
        {
            // Ensure we're able to create both server & client SslContexts
            if (clientOpts != null && clientOpts.enabled)
            {
                createNettySslContext(clientOpts, clientOpts.require_client_auth, SocketType.SERVER, OpenSsl.isAvailable());
                createNettySslContext(clientOpts, clientOpts.require_client_auth, SocketType.CLIENT, OpenSsl.isAvailable());
            }
        }
        catch (Exception e)
        {
            throw new IOException("Failed to create SSL context using client_encryption_options!", e);
        }
    }

    static class CacheKey
    {
        private final EncryptionOptions encryptionOptions;
        private final SocketType socketType;

        public CacheKey(EncryptionOptions encryptionOptions, SocketType socketType)
        {
            this.encryptionOptions = encryptionOptions;
            this.socketType = socketType;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return (socketType == cacheKey.socketType &&
                    Objects.equals(encryptionOptions, cacheKey.encryptionOptions));
        }

        public int hashCode()
        {
            int result = 0;
            result += 31 * socketType.hashCode();
            result += 31 * encryptionOptions.hashCode();
            return result;
        }
    }
}
