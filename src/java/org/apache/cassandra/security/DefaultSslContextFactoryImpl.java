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
import java.util.HashMap;
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

    /* This list is substituted in configurations that have explicitly specified the original "TLS" default,
     * by extracting it from the default "TLS" SSL Context instance
     */
    static private final List<String> TLS_PROTOCOL_SUBSTITUTION = SSLFactory.tlsInstanceProtocolSubstitution();

    private final Map<String,Object> parameters;
    private final String keystore;
    private final String keystore_password;
    private final String truststore;
    private final String truststore_password;
    private final List<String> cipher_suites;
    private String protocol;
    private List<String> accepted_protocols;
    private final String algorithm;
    private final String store_type;
    private final boolean require_client_auth;
    private final boolean require_endpoint_verification;
    // ServerEncryptionOptions does not use the enabled flag at all instead using the existing
    // internode_encryption option. So we force this private and expose through isEnabled
    // so users of ServerEncryptionOptions can't accidentally use this when they should use isEnabled
    // Long term we need to refactor ClientEncryptionOptions and ServerEncyrptionOptions to be separate
    // classes so we can choose appropriate configuration for each.
    // See CASSANDRA-15262 and CASSANDRA-15146
    protected Boolean enabled;
    protected Boolean optional;

    /* For test only */
    DefaultSslContextFactoryImpl(){
        parameters = new HashMap<>();
        keystore = "conf/.keystore";
        keystore_password = "cassandra";
        truststore = "conf/.truststore";
        truststore_password = "cassandra";
        cipher_suites = null;
        protocol = null;
        accepted_protocols = null;
        algorithm = null;
        store_type = "JKS";
        require_client_auth = false;
        require_endpoint_verification = false;
        enabled = null;
        optional = null;
    }

    public DefaultSslContextFactoryImpl(Map<String,Object> parameters) {
        this.parameters = parameters;
        keystore = getString("keystore");
        keystore_password = getString("keystore_password");
        truststore = getString("truststore");
        truststore_password = getString("truststore_password");
        cipher_suites = getStringList("cipher_suites");
        protocol = getString("protocol");
        accepted_protocols = getStringList("accepted_protocols");
        algorithm = getString("algorithm");
        store_type = getString("store_type", "JKS");
        require_client_auth = getBoolean("require_client_auth", false);
        require_endpoint_verification = getBoolean("require_endpoint_verification", false);
        enabled = getBoolean("enabled");
        this.optional = getBoolean("optional");
    }

    private String getString(String key, String defaultValue) {
        return this.parameters.get(key) == null ? defaultValue : (String)this.parameters.get(key);
    }

    private String getString(String key) {
        return (String)this.parameters.get(key);
    }

    private List<String> getStringList(String key) {
        return (List<String>)this.parameters.get(key);
    }

    private Boolean getBoolean(String key, boolean defaultValue) {
        return this.parameters.get(key) == null ? defaultValue : (Boolean)this.parameters.get(key);
    }

    private Boolean getBoolean(String key) {
        return (Boolean)this.parameters.get(key);
    }

    @Override
    public SSLContext createJSSESslContext(boolean buildTruststore) throws SSLException
    {
        TrustManager[] trustManagers = null;
        if (buildTruststore)
            trustManagers = buildTrustManagerFactory().getTrustManagers();

        KeyManagerFactory kmf = buildKeyManagerFactory();

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
    public SslContext createNettySslContext(boolean buildTruststore, SocketType socketType, boolean useOpenSsl,
                                            CipherSuiteFilter cipherFilter) throws SSLException
    {
        /*
            There is a case where the netty/openssl combo might not support using KeyManagerFactory. Specifically,
            I've seen this with the netty-tcnative dynamic openssl implementation. Using the netty-tcnative
            static-boringssl works fine with KeyManagerFactory. If we want to support all of the netty-tcnative
            options, we would need to fall back to passing in a file reference for both a x509 and PKCS#8 private
            key file in PEM format (see {@link SslContextBuilder#forServer(File, File, String)}). However, we are
            not supporting that now to keep the config/yaml API simple.
         */
        KeyManagerFactory kmf = buildKeyManagerFactory();
        SslContextBuilder builder;
        if (socketType == SocketType.SERVER)
        {
            builder = SslContextBuilder.forServer(kmf).clientAuth(this.require_client_auth ? ClientAuth.REQUIRE :
                                                                  ClientAuth.NONE);
        }
        else
        {
            builder = SslContextBuilder.forClient().keyManager(kmf);
        }

        builder.sslProvider(useOpenSsl ? SslProvider.OPENSSL : SslProvider.JDK).protocols(getAcceptedProtocols());

        // only set the cipher suites if the opertor has explicity configured values for it; else, use the default
        // for each ssl implemention (jdk or openssl)
        if (cipher_suites != null && !cipher_suites.isEmpty())
            builder.ciphers(cipher_suites, cipherFilter);

        if (buildTruststore)
            builder.trustManager(buildTrustManagerFactory());

        try
        {
            return builder.build();
        }
        catch (SSLException e)
        {
            throw new SSLException("failed to build the final SslContext object for secure connections", e);
        }
    }

    @Override
    public synchronized void initHotReloading() throws SSLException {
        boolean hasKeystore = hasKeystore();
        boolean hasTruststore = hasTruststore();

        if ( hasKeystore || hasTruststore ) {
            List<HotReloadableFile> fileList = new ArrayList<>();
            if ( hasKeystore )
            {
                fileList.add(new HotReloadableFile(keystore));
            }
            if ( hasTruststore ) {
                fileList.add(new HotReloadableFile(truststore));
            }
            hotReloadableFiles = ImmutableList.copyOf(fileList);
        }
    }

    @Override
    public boolean shouldReload()
    {
        return hotReloadableFiles.stream().anyMatch(HotReloadableFile::shouldReload);
    }

    @Override
    public boolean hasKeystore() {
        return keystore == null ? false : new File(keystore).exists();
    }

    private boolean hasTruststore() {
        return truststore == null ? false : new File(truststore).exists();
    }

    /**
     * Combine the pre-4.0 protocol field with the accepted_protocols list, substituting a list of
     * explicit protocols for the previous catchall default of "TLS"
     * @return array of protocol names suitable for passing to SslContextBuilder.protocols, or null if the default
     */
    @Override
    public List<String> getAcceptedProtocols()
    {
        if (accepted_protocols == null)
        {
            if (protocol == null)
            {
                return null;
            }
            // TLS is accepted by SSLContext.getInstance as a shorthand for give me an engine that
            // can speak some of the TLS protocols.  It is not supported by SSLEngine.setAcceptedProtocols
            // so substitute if the user hasn't provided an accepted protocol configuration
            else if (protocol.equalsIgnoreCase("TLS"))
            {
                return TLS_PROTOCOL_SUBSTITUTION;
            }
            else // the user was trying to limit to a single specific protocol, so try that
            {
                return ImmutableList.of(protocol);
            }
        }

        if (protocol != null && !protocol.equalsIgnoreCase("TLS") &&
            accepted_protocols.stream().noneMatch(ap -> ap.equalsIgnoreCase(protocol)))
        {
            // If the user provided a non-generic default protocol, append it to accepted_protocols - they wanted
            // it after all.
            return ImmutableList.<String>builder().addAll(accepted_protocols).add(protocol).build();
        }
        else
        {
            return accepted_protocols;
        }
    }

    @Override
    public List<String> getCipherSuites() {
        return cipher_suites;
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

    KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        try (InputStream ksf = Files.newInputStream(Paths.get(keystore)))
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
            algorithm == null ? KeyManagerFactory.getDefaultAlgorithm() : algorithm);
            KeyStore ks = KeyStore.getInstance(store_type);
            ks.load(ksf, keystore_password.toCharArray());
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
            kmf.init(ks, keystore_password.toCharArray());
            return kmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build key manager store for secure connections", e);
        }
    }

    TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        try (InputStream tsf = Files.newInputStream(Paths.get(truststore)))
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            algorithm == null ? TrustManagerFactory.getDefaultAlgorithm() : algorithm);
            KeyStore ts = KeyStore.getInstance(store_type);
            ts.load(tsf, truststore_password.toCharArray());
            tmf.init(ts);
            return tmf;
        }
        catch (Exception e)
        {
            throw new SSLException("failed to build trust manager store for secure connections", e);
        }
    }
}
