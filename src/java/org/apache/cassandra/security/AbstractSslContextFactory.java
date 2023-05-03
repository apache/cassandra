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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.collect.ImmutableList;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;

/**
 * Abstract class implementing {@code ISslContextFacotry} to provide most of the functionality that any
 * implementation might need. This does not assume any file-based credentials for keys/certs hence provide a good base
 * for any implementation that only need to customize the loading of keys/certs in a custom way.
 * <p>
 * {@code CAUTION:} While this is extremely useful abstraction, please be careful if you need to modify this class
 * given possible custom implementations out there!
 *
 * @see DefaultSslContextFactory
 */
abstract public class AbstractSslContextFactory implements ISslContextFactory
{
    /*
    This list is substituted in configurations that have explicitly specified the original "TLS" default,
    by extracting it from the default "TLS" SSL Context instance
     */
    static protected final List<String> TLS_PROTOCOL_SUBSTITUTION = SSLFactory.tlsInstanceProtocolSubstitution();

    protected boolean openSslIsAvailable;

    protected final Map<String, Object> parameters;
    protected final List<String> cipher_suites;
    protected final String protocol;
    protected final List<String> accepted_protocols;
    protected final String algorithm;
    protected final String store_type;
    protected final boolean require_client_auth;
    protected final boolean require_endpoint_verification;
    /*
    ServerEncryptionOptions does not use the enabled flag at all instead using the existing
    internode_encryption option. So we force this protected and expose through isEnabled
    so users of ServerEncryptionOptions can't accidentally use this when they should use isEnabled
    Long term we need to refactor ClientEncryptionOptions and ServerEncryptionOptions to be separate
    classes so we can choose appropriate configuration for each.
    See CASSANDRA-15262 and CASSANDRA-15146
     */
    protected Boolean enabled;
    protected Boolean optional;

    /* For test only */
    protected AbstractSslContextFactory()
    {
        parameters = new HashMap<>();
        cipher_suites = null;
        protocol = null;
        accepted_protocols = null;
        algorithm = null;
        store_type = "JKS";
        require_client_auth = false;
        require_endpoint_verification = false;
        enabled = null;
        optional = null;
        deriveIfOpenSslAvailable();
    }

    protected AbstractSslContextFactory(Map<String, Object> parameters)
    {
        this.parameters = parameters;
        cipher_suites = getStringList("cipher_suites");
        protocol = getString("protocol");
        accepted_protocols = getStringList("accepted_protocols");
        algorithm = getString("algorithm");
        store_type = getString("store_type", "JKS");
        require_client_auth = getBoolean("require_client_auth", false);
        require_endpoint_verification = getBoolean("require_endpoint_verification", false);
        enabled = getBoolean("enabled");
        optional = getBoolean("optional");
        deriveIfOpenSslAvailable();
    }

    /**
     * Dervies if {@code OpenSSL} is available. It allows in-jvm dtests to disable tcnative openssl support by
     * setting {@link  org.apache.cassandra.config.CassandraRelevantProperties#DISABLE_TCACTIVE_OPENSSL}
     * system property as {@code true}. Otherwise, it creates a circular reference that prevents the instance
     * class loader from being garbage collected.
     */
    protected void deriveIfOpenSslAvailable()
    {
        if (DISABLE_TCACTIVE_OPENSSL.getBoolean())
            openSslIsAvailable = false;
        else
            openSslIsAvailable = OpenSsl.isAvailable();
    }

    protected String getString(String key, String defaultValue)
    {
        return parameters.get(key) == null ? defaultValue : (String) parameters.get(key);
    }

    protected String getString(String key)
    {
        return (String) parameters.get(key);
    }

    protected List<String> getStringList(String key)
    {
        return (List<String>) parameters.get(key);
    }

    protected Boolean getBoolean(String key, boolean defaultValue)
    {
        return parameters.get(key) == null ? defaultValue : (Boolean) parameters.get(key);
    }

    protected Boolean getBoolean(String key)
    {
        return (Boolean) this.parameters.get(key);
    }

    @Override
    public SSLContext createJSSESslContext(boolean verifyPeerCertificate) throws SSLException
    {
        TrustManager[] trustManagers = null;
        if (verifyPeerCertificate)
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
    public SslContext createNettySslContext(boolean verifyPeerCertificate, SocketType socketType,
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
        SslContextBuilder builder;
        if (socketType == SocketType.SERVER)
        {
            KeyManagerFactory kmf = buildKeyManagerFactory();
            builder = SslContextBuilder.forServer(kmf).clientAuth(this.require_client_auth ? ClientAuth.REQUIRE :
                                                                  ClientAuth.NONE);
        }
        else
        {
            KeyManagerFactory kmf = buildOutboundKeyManagerFactory();
            builder = SslContextBuilder.forClient().keyManager(kmf);
        }

        builder.sslProvider(getSslProvider()).protocols(getAcceptedProtocols());

        // only set the cipher suites if the operator has explicity configured values for it; else, use the default
        // for each ssl implemention (jdk or openssl)
        if (cipher_suites != null && !cipher_suites.isEmpty())
            builder.ciphers(cipher_suites, cipherFilter);

        if (verifyPeerCertificate)
            builder.trustManager(buildTrustManagerFactory());

        return builder.build();
    }

    /**
     * Combine the pre-4.0 protocol field with the accepted_protocols list, substituting a list of
     * explicit protocols for the previous catchall default of "TLS"
     *
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
            // can speak some TLS protocols.  It is not supported by SSLEngine.setAcceptedProtocols
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
    public List<String> getCipherSuites()
    {
        return cipher_suites;
    }

    /**
     * Returns {@link SslProvider} to be used to build Netty's SslContext.
     *
     * @return appropriate SslProvider
     */
    protected SslProvider getSslProvider()
    {
        return openSslIsAvailable ? SslProvider.OPENSSL : SslProvider.JDK;
    }

    abstract protected KeyManagerFactory buildKeyManagerFactory() throws SSLException;

    abstract protected TrustManagerFactory buildTrustManagerFactory() throws SSLException;

    /**
     * Create a {@code KeyManagerFactory} for outbound connections.
     * It provides a seperate keystore for internode mTLS outbound connections.
     * @return {@code KeyManagerFactory}
     * @throws SSLException
     */
    abstract protected KeyManagerFactory buildOutboundKeyManagerFactory() throws SSLException;
}
