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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.cassandra.config.EncryptionOptions;

/**
 * Cassandra's default implementation class for the configuration key {@code ssl_context_factory}. It uses
 * file based keystores.
 */
public final class DefaultSslContextFactoryImpl implements ISslContextFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSslContextFactoryImpl.class);

    @VisibleForTesting
    static volatile boolean checkedExpiry = false;

    /**
     * List of files that trigger hot reloading of SSL certificates
     */
    private static volatile List<HotReloadableFile> hotReloadableFiles = ImmutableList.of();

    private Map<String,String> parameters;

    /* For test only */
    DefaultSslContextFactoryImpl(){}

    public DefaultSslContextFactoryImpl(Map<String,String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public SSLContext createJSSESslContext(EncryptionOptions options, boolean buildTruststore) throws SSLException
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
            throw new SSLException("Error creating/initializing the SSL Context", e);
        }
    }

    @Override
    public SslContext createNettySslContext(EncryptionOptions options, boolean buildTruststore, SocketType socketType
    , boolean useOpenSsl, CipherSuiteFilter cipherFilter) throws SSLException
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

        SslContext sslContext;
        try
        {
            sslContext = builder.build();
        }
        catch (SSLException e)
        {
            throw new SSLException("failed to build the final SslContext object for secure connections", e);
        }
        return sslContext;
    }

    @Override
    public synchronized void initHotReloading(EncryptionOptions options) throws SSLException {

        List<HotReloadableFile> fileList = new ArrayList<>();

        if (options != null && options.tlsEncryptionPolicy() != EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
        {
            fileList.add(new HotReloadableFile(options.keystore));
            fileList.add(new HotReloadableFile(options.truststore));
        }

        hotReloadableFiles = ImmutableList.copyOf(fileList);
    }

    @Override
    public boolean shouldReload()
    {
        return hotReloadableFiles.stream().anyMatch(HotReloadableFile::shouldReload);
    }

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

    KeyManagerFactory buildKeyManagerFactory(EncryptionOptions options) throws SSLException
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
            throw new SSLException("failed to build key manager store for secure connections", e);
        }
    }

    TrustManagerFactory buildTrustManagerFactory(EncryptionOptions options) throws SSLException
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
            throw new SSLException("failed to build trust manager store for secure connections", e);
        }
    }
}
