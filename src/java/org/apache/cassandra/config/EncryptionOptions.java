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
package org.apache.cassandra.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.security.DisableSslContextFactory;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This holds various options used for enabling SSL/TLS encryption.
 * Examples of such options are: supported cipher-suites, ssl protocol with version, accepted protocols, end-point
 * verification, require client-auth/cert etc.
 */
public class EncryptionOptions
{
    Logger logger = LoggerFactory.getLogger(EncryptionOptions.class);

    public enum TlsEncryptionPolicy
    {
        UNENCRYPTED("unencrypted"),
        OPTIONAL("optionally encrypted"),
        ENCRYPTED("encrypted");

        private final String description;

        TlsEncryptionPolicy(String description)
        {
            this.description = description;
        }

        public String description()
        {
            return description;
        }
    }

    /*
     * If the ssl_context_factory is configured, most likely it won't use file based keystores and truststores and
     * can choose to completely customize SSL context's creation. Most likely it won't also use keystore_password and
     * truststore_passwords configurations as they are in plaintext format.
     */
    public final ParameterizedClass ssl_context_factory;
    public final String keystore;
    @Nullable
    public final String keystore_password;
    public final String truststore;
    @Nullable
    public final String truststore_password;
    public final List<String> cipher_suites;
    protected String protocol;
    protected List<String> accepted_protocols;
    public final String algorithm;
    public final String store_type;
    public final boolean require_client_auth;
    public final boolean require_endpoint_verification;
    // ServerEncryptionOptions does not use the enabled flag at all instead using the existing
    // internode_encryption option. So we force this private and expose through isEnabled
    // so users of ServerEncryptionOptions can't accidentally use this when they should use isEnabled
    // Long term we need to refactor ClientEncryptionOptions and ServerEncyrptionOptions to be separate
    // classes so we can choose appropriate configuration for each.
    // See CASSANDRA-15262 and CASSANDRA-15146
    protected Boolean enabled;
    protected Boolean optional;

    // Calculated by calling applyConfig() after populating/parsing
    protected Boolean isEnabled;
    protected Boolean isOptional;

    /*
     * We will wait to initialize this until applyConfig() call to make sure we do it only when the caller is ready
     * to use this option instance.
     */
    public transient ISslContextFactory sslContextFactoryInstance;

    public enum ConfigKey
    {
        KEYSTORE("keystore"),
        KEYSTORE_PASSWORD("keystore_password"),
        OUTBOUND_KEYSTORE("outbound_keystore"),
        OUTBOUND_KEYSTORE_PASSWORD("outbound_keystore_password"),
        TRUSTSTORE("truststore"),
        TRUSTSTORE_PASSWORD("truststore_password"),
        CIPHER_SUITES("cipher_suites"),
        PROTOCOL("protocol"),
        ACCEPTED_PROTOCOLS("accepted_protocols"),
        ALGORITHM("algorithm"),
        STORE_TYPE("store_type"),
        REQUIRE_CLIENT_AUTH("require_client_auth"),
        REQUIRE_ENDPOINT_VERIFICATION("require_endpoint_verification"),
        ENABLED("enabled"),
        OPTIONAL("optional");

        final String keyName;

        ConfigKey(String keyName)
        {
            this.keyName=keyName;
        }

        String getKeyName()
        {
            return keyName;
        }

        static Set<String> asSet()
        {
            Set<String> valueSet = new HashSet<>();
            ConfigKey[] values = values();
            for(ConfigKey key: values) {
                valueSet.add(key.getKeyName().toLowerCase());
            }
            return valueSet;
        }
    }

    public EncryptionOptions()
    {
        ssl_context_factory = new ParameterizedClass("org.apache.cassandra.security.DefaultSslContextFactory",
                                                     new HashMap<>());
        keystore = "conf/.keystore";
        keystore_password = null;
        truststore = "conf/.truststore";
        truststore_password = null;
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

    public EncryptionOptions(ParameterizedClass ssl_context_factory, String keystore, String keystore_password,
                             String truststore, String truststore_password, List<String> cipher_suites,
                             String protocol, List<String> accepted_protocols, String algorithm, String store_type,
                             boolean require_client_auth, boolean require_endpoint_verification, Boolean enabled,
                             Boolean optional)
    {
        this.ssl_context_factory = ssl_context_factory;
        this.keystore = keystore;
        this.keystore_password = keystore_password;
        this.truststore = truststore;
        this.truststore_password = truststore_password;
        this.cipher_suites = cipher_suites;
        this.protocol = protocol;
        this.accepted_protocols = accepted_protocols;
        this.algorithm = algorithm;
        this.store_type = store_type;
        this.require_client_auth = require_client_auth;
        this.require_endpoint_verification = require_endpoint_verification;
        this.enabled = enabled;
        this.optional = optional;
    }

    public EncryptionOptions(EncryptionOptions options)
    {
        ssl_context_factory = options.ssl_context_factory;
        keystore = options.keystore;
        keystore_password = options.keystore_password;
        truststore = options.truststore;
        truststore_password = options.truststore_password;
        cipher_suites = options.cipher_suites;
        protocol = options.protocol;
        accepted_protocols = options.accepted_protocols;
        algorithm = options.algorithm;
        store_type = options.store_type;
        require_client_auth = options.require_client_auth;
        require_endpoint_verification = options.require_endpoint_verification;
        enabled = options.enabled;
        this.optional = options.optional;
    }

    /* Computes enabled and optional before use. Because the configuration can be loaded
     * through pluggable mechanisms this is the only safe way to make sure that
     * enabled and optional are set correctly.
     *
     * It also initializes the ISslContextFactory's instance
     */
    public EncryptionOptions applyConfig()
    {
        ensureConfigNotApplied();

        initializeSslContextFactory();

        isEnabled = this.enabled != null && enabled;

        if (optional != null)
        {
            isOptional = optional;
        }
        // If someone is asking for an _insecure_ connection and not explicitly telling us to refuse
        // encrypted connections AND they have a keystore file, we assume they would like to be able
        // to transition to encrypted connections in the future.
        else if (sslContextFactoryInstance.hasKeystore())
        {
            isOptional = !isEnabled;
        }
        else
        {
            // Otherwise if there's no keystore, not possible to establish an optional secure connection
            isOptional = false;
        }
        return this;
    }

    /**
     * Prepares the parameterized keys provided in the configuration for {@link ISslContextFactory} to be passed in
     * as the constructor for its implementation.
     *
     * @throws IllegalArgumentException in case any pre-defined key, as per {@link ConfigKey}, for the encryption
     * options is duplicated in the parameterized keys.
     */
    private void prepareSslContextFactoryParameterizedKeys(Map<String,Object> sslContextFactoryParameters)
    {
        if (ssl_context_factory.parameters != null)
        {
            Set<String> configKeys = ConfigKey.asSet();
            for (Map.Entry<String, String> entry : ssl_context_factory.parameters.entrySet())
            {
                if(configKeys.contains(entry.getKey().toLowerCase()))
                {
                    throw new IllegalArgumentException("SslContextFactory "+ssl_context_factory.class_name+" should " +
                                                       "configure '"+entry.getKey()+"' as encryption_options instead of" +
                                                       " parameterized keys");
                }
                sslContextFactoryParameters.put(entry.getKey(),entry.getValue());
            }
        }
    }

    protected void fillSslContextParams(Map<String, Object> sslContextFactoryParameters)
    {
        /*
         * Copy all configs to the Map to pass it on to the ISslContextFactory's implementation
         */
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.KEYSTORE, this.keystore);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.KEYSTORE_PASSWORD, this.keystore_password);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.TRUSTSTORE, this.truststore);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.TRUSTSTORE_PASSWORD, this.truststore_password);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.CIPHER_SUITES, this.cipher_suites);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.PROTOCOL, this.protocol);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.ACCEPTED_PROTOCOLS, this.accepted_protocols);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.ALGORITHM, this.algorithm);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.STORE_TYPE, this.store_type);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.REQUIRE_CLIENT_AUTH, this.require_client_auth);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.REQUIRE_ENDPOINT_VERIFICATION, this.require_endpoint_verification);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.ENABLED, this.enabled);
        putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.OPTIONAL, this.optional);
    }

    private void initializeSslContextFactory()
    {
        Map<String, Object> sslContextFactoryParameters = new HashMap<>();
        prepareSslContextFactoryParameterizedKeys(sslContextFactoryParameters);
        fillSslContextParams(sslContextFactoryParameters);

        if (CassandraRelevantProperties.TEST_JVM_DTEST_DISABLE_SSL.getBoolean())
        {
            sslContextFactoryInstance = new DisableSslContextFactory();
        }
        else
        {
            sslContextFactoryInstance = FBUtilities.newSslContextFactory(ssl_context_factory.class_name,
                                                                         sslContextFactoryParameters);
        }
    }

    protected static void putSslContextFactoryParameter(Map<String, Object> existingParameters, ConfigKey configKey, Object value)
    {
        if (value != null) {
            existingParameters.put(configKey.getKeyName(), value);
        }
    }

    private void ensureConfigApplied()
    {
        if (isEnabled == null || isOptional == null)
            throw new IllegalStateException("EncryptionOptions.applyConfig must be called first");
    }

    private void ensureConfigNotApplied()
    {
        if (isEnabled != null || isOptional != null)
            throw new IllegalStateException("EncryptionOptions cannot be changed after configuration applied");
    }

    /**
     * Indicates if the channel should be encrypted. Client and Server uses different logic to determine this
     *
     * @return if the channel should be encrypted
     */
    public Boolean getEnabled()
    {
        ensureConfigApplied();
        return isEnabled;
    }

    /**
     * Sets if encryption should be enabled for this channel. Note that this should only be called by
     * the configuration parser or tests. It is public only for that purpose, mutating enabled state
     * is probably a bad idea.
     * @param enabled value to set
     */
    public void setEnabled(Boolean enabled)
    {
        ensureConfigNotApplied();
        this.enabled = enabled;
    }

    /**
     * Indicates if the channel may be encrypted (but is not required to be).
     * Explicitly providing a value in the configuration take precedent.
     * If no optional value is set and !isEnabled(), then optional connections are allowed
     * if a keystore exists. Without it, it would be impossible to establish the connections.
     *
     * Return type is Boolean even though it can never be null so that snakeyaml can find it
     * @return if the channel may be encrypted
     */
    public Boolean getOptional()
    {
        ensureConfigApplied();
        return isOptional;
    }

    /**
     * Sets if encryption should be optional for this channel. Note that this should only be called by
     * the configuration parser or tests. It is public only for that purpose, mutating enabled state
     * is probably a bad idea.
     * @param optional value to set
     */
    public void setOptional(Boolean optional)
    {
        ensureConfigNotApplied();
        this.optional = optional;
    }

    /**
     * Sets accepted TLS protocol for this channel. Note that this should only be called by
     * the configuration parser or tests. It is public only for that purpose, mutating protocol state
     * is probably a bad idea.
     * @param protocol value to set
     */
    @VisibleForTesting
    public void setProtocol(String protocol)
    {
        this.protocol = protocol;
    }

    public String getProtocol()
    {
        return protocol;
    }

    /**
     * Sets accepted TLS protocols for this channel. Note that this should only be called by
     * the configuration parser or tests. It is public only for that purpose, mutating protocol state
     * is probably a bad idea. The function casing is required for snakeyaml to find this setter for the protected field.
     * @param accepted_protocols value to set
     */
    public void setAcceptedProtocols(List<String> accepted_protocols)
    {
        this.accepted_protocols = accepted_protocols == null ? null : ImmutableList.copyOf(accepted_protocols);
    }

    public List<String> getAcceptedProtocols()
    {
        return sslContextFactoryInstance == null ? null : sslContextFactoryInstance.getAcceptedProtocols();
    }

    public String[] acceptedProtocolsArray()
    {
        List<String> ap = getAcceptedProtocols();
        return ap == null ?  new String[0] : ap.toArray(new String[0]);
    }

    public String[] cipherSuitesArray()
    {
        return cipher_suites == null ? null : cipher_suites.toArray(new String[0]);
    }

    public TlsEncryptionPolicy tlsEncryptionPolicy()
    {
        if (getOptional())
        {
            return TlsEncryptionPolicy.OPTIONAL;
        }
        else if (getEnabled())
        {
            return TlsEncryptionPolicy.ENCRYPTED;
        }
        else
        {
            return TlsEncryptionPolicy.UNENCRYPTED;
        }
    }

    public EncryptionOptions withSslContextFactory(ParameterizedClass sslContextFactoryClass)
    {
        return new EncryptionOptions(sslContextFactoryClass, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites,protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification,enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withKeyStore(String keystore)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites,protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withKeyStorePassword(String keystore_password)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites,protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withTrustStore(String truststore)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withTrustStorePassword(String truststore_password)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withCipherSuites(List<String> cipher_suites)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withCipherSuites(String ... cipher_suites)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, ImmutableList.copyOf(cipher_suites), protocol,
                                     accepted_protocols, algorithm, store_type, require_client_auth,
                                     require_endpoint_verification, enabled, optional).applyConfig();
    }

    public EncryptionOptions withProtocol(String protocol)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }


    public EncryptionOptions withAcceptedProtocols(List<String> accepted_protocols)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites,protocol, accepted_protocols == null ? null :
                                                                                  ImmutableList.copyOf(accepted_protocols),
                                     algorithm, store_type, require_client_auth, require_endpoint_verification,
                                     enabled, optional).applyConfig();
    }


    public EncryptionOptions withAlgorithm(String algorithm)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withStoreType(String store_type)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withRequireClientAuth(boolean require_client_auth)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withRequireEndpointVerification(boolean require_endpoint_verification)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withEnabled(boolean enabled)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    public EncryptionOptions withOptional(Boolean optional)
    {
        return new EncryptionOptions(ssl_context_factory, keystore, keystore_password, truststore,
                                     truststore_password, cipher_suites, protocol, accepted_protocols, algorithm,
                                     store_type, require_client_auth, require_endpoint_verification, enabled,
                                     optional).applyConfig();
    }

    /**
     * The method is being mainly used to cache SslContexts therefore, we only consider
     * fields that would make a difference when the TrustStore or KeyStore files are updated
     */
    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        EncryptionOptions opt = (EncryptionOptions)o;
        return enabled == opt.enabled &&
               optional == opt.optional &&
               require_client_auth == opt.require_client_auth &&
               require_endpoint_verification == opt.require_endpoint_verification &&
               Objects.equals(keystore, opt.keystore) &&
               Objects.equals(keystore_password, opt.keystore_password) &&
               Objects.equals(truststore, opt.truststore) &&
               Objects.equals(truststore_password, opt.truststore_password) &&
               Objects.equals(protocol, opt.protocol) &&
               Objects.equals(accepted_protocols, opt.accepted_protocols) &&
               Objects.equals(algorithm, opt.algorithm) &&
               Objects.equals(store_type, opt.store_type) &&
               Objects.equals(cipher_suites, opt.cipher_suites) &&
               Objects.equals(ssl_context_factory, opt.ssl_context_factory);
    }

    /**
     * The method is being mainly used to cache SslContexts therefore, we only consider
     * fields that would make a difference when the TrustStore or KeyStore files are updated
     */
    @Override
    public int hashCode()
    {
        int result = 0;
        result += 31 * (keystore == null ? 0 : keystore.hashCode());
        result += 31 * (keystore_password == null ? 0 : keystore_password.hashCode());
        result += 31 * (truststore == null ? 0 : truststore.hashCode());
        result += 31 * (truststore_password == null ? 0 : truststore_password.hashCode());
        result += 31 * (protocol == null ? 0 : protocol.hashCode());
        result += 31 * (accepted_protocols == null ? 0 : accepted_protocols.hashCode());
        result += 31 * (algorithm == null ? 0 : algorithm.hashCode());
        result += 31 * (store_type == null ? 0 : store_type.hashCode());
        result += 31 * (enabled == null ? 0 : Boolean.hashCode(enabled));
        result += 31 * (optional == null ? 0 : Boolean.hashCode(optional));
        result += 31 * (cipher_suites == null ? 0 : cipher_suites.hashCode());
        result += 31 * Boolean.hashCode(require_client_auth);
        result += 31 * Boolean.hashCode(require_endpoint_verification);
        result += 31 * (ssl_context_factory == null ? 0 : ssl_context_factory.hashCode());
        return result;
    }

    public static class ServerEncryptionOptions extends EncryptionOptions
    {
        public enum InternodeEncryption
        {
            all, none, dc, rack
        }

        public final InternodeEncryption internode_encryption;
        @Replaces(oldName = "enable_legacy_ssl_storage_port", deprecated = true)
        public final boolean legacy_ssl_storage_port_enabled;
        public final String outbound_keystore;
        @Nullable
        public final String outbound_keystore_password;

        public ServerEncryptionOptions()
        {
            this.internode_encryption = InternodeEncryption.none;
            this.legacy_ssl_storage_port_enabled = false;
            this.outbound_keystore = null;
            this.outbound_keystore_password = null;
        }

        public ServerEncryptionOptions(ParameterizedClass sslContextFactoryClass, String keystore,
                                       String keystore_password,String outbound_keystore,
                                       String outbound_keystore_password, String truststore, String truststore_password,
                                       List<String> cipher_suites, String protocol, List<String> accepted_protocols,
                                       String algorithm, String store_type, boolean require_client_auth,
                                       boolean require_endpoint_verification, Boolean optional,
                                       InternodeEncryption internode_encryption, boolean legacy_ssl_storage_port_enabled)
        {
            super(sslContextFactoryClass, keystore, keystore_password, truststore, truststore_password, cipher_suites,
            protocol, accepted_protocols, algorithm, store_type, require_client_auth, require_endpoint_verification,
            null, optional);
            this.internode_encryption = internode_encryption;
            this.legacy_ssl_storage_port_enabled = legacy_ssl_storage_port_enabled;
            this.outbound_keystore = outbound_keystore;
            this.outbound_keystore_password = outbound_keystore_password;
        }

        public ServerEncryptionOptions(ServerEncryptionOptions options)
        {
            super(options);
            this.internode_encryption = options.internode_encryption;
            this.legacy_ssl_storage_port_enabled = options.legacy_ssl_storage_port_enabled;
            this.outbound_keystore = options.outbound_keystore;
            this.outbound_keystore_password = options.outbound_keystore_password;
        }

        @Override
        protected void fillSslContextParams(Map<String, Object> sslContextFactoryParameters)
        {
            super.fillSslContextParams(sslContextFactoryParameters);
            putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.OUTBOUND_KEYSTORE, this.outbound_keystore);
            putSslContextFactoryParameter(sslContextFactoryParameters, ConfigKey.OUTBOUND_KEYSTORE_PASSWORD, this.outbound_keystore_password);
        }

        @Override
        public EncryptionOptions applyConfig()
        {
            return applyConfigInternal();
        }

        private ServerEncryptionOptions applyConfigInternal()
        {
            super.applyConfig();

            isEnabled = this.internode_encryption != InternodeEncryption.none;

            if (this.enabled != null && this.enabled && !isEnabled)
            {
                logger.warn("Setting server_encryption_options.enabled has no effect, use internode_encryption");
            }

            if (require_client_auth && (internode_encryption == InternodeEncryption.rack || internode_encryption == InternodeEncryption.dc))
            {
                logger.warn("Setting require_client_auth is incompatible with 'rack' and 'dc' internode_encryption values."
                          + " It is possible for an internode connection to pretend to be in the same rack/dc by spoofing"
                          + " its broadcast address in the handshake and bypass authentication. To ensure that mutual TLS"
                          + " authentication is not bypassed, please set internode_encryption to 'all'. Continuing with"
                          + " insecure configuration.");
            }

            // regardless of the optional flag, if the internode encryption is set to rack or dc
            // it must be optional so that unencrypted connections within the rack or dc can be established.
            isOptional = super.isOptional || internode_encryption == InternodeEncryption.rack || internode_encryption == InternodeEncryption.dc;

            return this;
        }

        public boolean shouldEncrypt(InetAddressAndPort endpoint)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            switch (internode_encryption)
            {
                case none:
                    return false; // if nothing needs to be encrypted then return immediately.
                case all:
                    break;
                case dc:
                    if (snitch.getDatacenter(endpoint).equals(snitch.getLocalDatacenter()))
                        return false;
                    break;
                case rack:
                    // for rack then check if the DC's are the same.
                    if (snitch.getRack(endpoint).equals(snitch.getLocalRack())
                        && snitch.getDatacenter(endpoint).equals(snitch.getLocalDatacenter()))
                        return false;
                    break;
            }
            return true;
        }

        /**
         * {@link #isOptional} will be set to {@code true} implicitly for {@code internode_encryption}
         * values of "dc" and "all". This method returns the explicit, raw value of {@link #optional}
         * as set by the user (if set at all).
         */
        public boolean isExplicitlyOptional()
        {
            return optional != null && optional;
        }

        /**
         * The method is being mainly used to cache SslContexts therefore, we only consider
         * fields that would make a difference when the TrustStore or KeyStore files are updated
         */
        @Override
        public boolean equals(Object o)
        {
            if (o == this)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            if (!super.equals(o))
                return false;

            ServerEncryptionOptions opt = (ServerEncryptionOptions) o;
            return internode_encryption == opt.internode_encryption &&
                   legacy_ssl_storage_port_enabled == opt.legacy_ssl_storage_port_enabled &&
                   Objects.equals(outbound_keystore, opt.outbound_keystore) &&
                   Objects.equals(outbound_keystore_password, opt.outbound_keystore_password);
        }

        /**
         * The method is being mainly used to cache SslContexts therefore, we only consider
         * fields that would make a difference when the TrustStore or KeyStore files are updated
         */
        @Override
        public int hashCode()
        {
            int result = 0;
            result += 31 * super.hashCode();
            result += 31 * (internode_encryption == null ? 0 : internode_encryption.hashCode());
            result += 31 * Boolean.hashCode(legacy_ssl_storage_port_enabled);
            result += 31 * (outbound_keystore == null ? 0 : outbound_keystore.hashCode());
            result += 31 * (outbound_keystore_password == null ? 0 : outbound_keystore_password.hashCode());
            return result;
        }

        public ServerEncryptionOptions withSslContextFactory(ParameterizedClass sslContextFactoryClass)
        {
            return new ServerEncryptionOptions(sslContextFactoryClass, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withKeyStore(String keystore)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withKeyStorePassword(String keystore_password)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withTrustStore(String truststore)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withTrustStorePassword(String truststore_password)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withCipherSuites(List<String> cipher_suites)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withCipherSuites(String... cipher_suites)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, Arrays.asList(cipher_suites), protocol,
                                               accepted_protocols, algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withProtocol(String protocol)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withAcceptedProtocols(List<String> accepted_protocols)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withAlgorithm(String algorithm)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withStoreType(String store_type)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withRequireClientAuth(boolean require_client_auth)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withRequireEndpointVerification(boolean require_endpoint_verification)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withOptional(boolean optional)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withInternodeEncryption(InternodeEncryption internode_encryption)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withLegacySslStoragePort(boolean enable_legacy_ssl_storage_port)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               enable_legacy_ssl_storage_port).applyConfigInternal();
        }

        public ServerEncryptionOptions withOutboundKeystore(String outboundKeystore)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outboundKeystore, outbound_keystore_password, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }

        public ServerEncryptionOptions withOutboundKeystorePassword(String outboundKeystorePassword)
        {
            return new ServerEncryptionOptions(ssl_context_factory, keystore, keystore_password,
                                               outbound_keystore, outboundKeystorePassword, truststore,
                                               truststore_password, cipher_suites, protocol, accepted_protocols,
                                               algorithm, store_type, require_client_auth,
                                               require_endpoint_verification, optional, internode_encryption,
                                               legacy_ssl_storage_port_enabled).applyConfigInternal();
        }
    }
}
