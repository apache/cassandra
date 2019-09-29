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

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;

public class EncryptionOptions
{
    public final String keystore;
    public final String keystore_password;
    public final String truststore;
    public final String truststore_password;
    public final List<String> cipher_suites;
    public final String protocol;
    public final String algorithm;
    public final String store_type;
    public final boolean require_client_auth;
    public final boolean require_endpoint_verification;
    public final boolean enabled;
    public final boolean optional;

    public EncryptionOptions()
    {
        keystore = "conf/.keystore";
        keystore_password = "cassandra";
        truststore = "conf/.truststore";
        truststore_password = "cassandra";
        cipher_suites = ImmutableList.of();
        protocol = "TLS";
        algorithm = null;
        store_type = "JKS";
        require_client_auth = false;
        require_endpoint_verification = false;
        enabled = false;
        optional = false;
    }

    public EncryptionOptions(String keystore, String keystore_password, String truststore, String truststore_password, List<String> cipher_suites, String protocol, String algorithm, String store_type, boolean require_client_auth, boolean require_endpoint_verification, boolean enabled, boolean optional)
    {
        this.keystore = keystore;
        this.keystore_password = keystore_password;
        this.truststore = truststore;
        this.truststore_password = truststore_password;
        this.cipher_suites = cipher_suites;
        this.protocol = protocol;
        this.algorithm = algorithm;
        this.store_type = store_type;
        this.require_client_auth = require_client_auth;
        this.require_endpoint_verification = require_endpoint_verification;
        this.enabled = enabled;
        this.optional = optional;
    }

    public EncryptionOptions(EncryptionOptions options)
    {
        keystore = options.keystore;
        keystore_password = options.keystore_password;
        truststore = options.truststore;
        truststore_password = options.truststore_password;
        cipher_suites = options.cipher_suites;
        protocol = options.protocol;
        algorithm = options.algorithm;
        store_type = options.store_type;
        require_client_auth = options.require_client_auth;
        require_endpoint_verification = options.require_endpoint_verification;
        enabled = options.enabled;
        optional = options.optional;
    }

    public EncryptionOptions withKeyStore(String keystore)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withKeyStorePassword(String keystore_password)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withTrustStore(String truststore)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withTrustStorePassword(String truststore_password)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withCipherSuites(List<String> cipher_suites)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withCipherSuites(String ... cipher_suites)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, ImmutableList.copyOf(cipher_suites),
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withProtocol(String protocol)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withAlgorithm(String algorithm)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withStoreType(String store_type)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withRequireClientAuth(boolean require_client_auth)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withRequireEndpointVerification(boolean require_endpoint_verification)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withEnabled(boolean enabled)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
    }

    public EncryptionOptions withOptional(boolean optional)
    {
        return new EncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                           protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                           enabled, optional);
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
               Objects.equals(algorithm, opt.algorithm) &&
               Objects.equals(store_type, opt.store_type) &&
               Objects.equals(cipher_suites, opt.cipher_suites);
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
        result += 31 * (algorithm == null ? 0 : algorithm.hashCode());
        result += 31 * (store_type == null ? 0 : store_type.hashCode());
        result += 31 * Boolean.hashCode(enabled);
        result += 31 * Boolean.hashCode(optional);
        result += 31 * (cipher_suites == null ? 0 : cipher_suites.hashCode());
        result += 31 * Boolean.hashCode(require_client_auth);
        result += 31 * Boolean.hashCode(require_endpoint_verification);
        return result;
    }

    public static class ServerEncryptionOptions extends EncryptionOptions
    {
        public enum InternodeEncryption
        {
            all, none, dc, rack
        }

        public final InternodeEncryption internode_encryption;
        public final boolean enable_legacy_ssl_storage_port;

        public ServerEncryptionOptions()
        {
            this.internode_encryption = InternodeEncryption.none;
            this.enable_legacy_ssl_storage_port = false;
        }
        public ServerEncryptionOptions(String keystore, String keystore_password, String truststore, String truststore_password, List<String> cipher_suites, String protocol, String algorithm, String store_type, boolean require_client_auth, boolean require_endpoint_verification, boolean enabled, boolean optional, InternodeEncryption internode_encryption, boolean enable_legacy_ssl_storage_port)
        {
            super(keystore, keystore_password, truststore, truststore_password, cipher_suites, protocol, algorithm, store_type, require_client_auth, require_endpoint_verification, enabled, optional);
            this.internode_encryption = internode_encryption;
            this.enable_legacy_ssl_storage_port = enable_legacy_ssl_storage_port;
        }

        public ServerEncryptionOptions(ServerEncryptionOptions options)
        {
            super(options);
            this.internode_encryption = options.internode_encryption;
            this.enable_legacy_ssl_storage_port = options.enable_legacy_ssl_storage_port;
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


        public ServerEncryptionOptions withKeyStore(String keystore)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withKeyStorePassword(String keystore_password)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withTrustStore(String truststore)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withTrustStorePassword(String truststore_password)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withCipherSuites(List<String> cipher_suites)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withCipherSuites(String ... cipher_suites)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, ImmutableList.copyOf(cipher_suites),
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withProtocol(String protocol)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withAlgorithm(String algorithm)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withStoreType(String store_type)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withRequireClientAuth(boolean require_client_auth)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withRequireEndpointVerification(boolean require_endpoint_verification)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withEnabled(boolean enabled)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withOptional(boolean optional)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withInternodeEncryption(InternodeEncryption internode_encryption)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

        public ServerEncryptionOptions withLegacySslStoragePort(boolean enable_legacy_ssl_storage_port)
        {
            return new ServerEncryptionOptions(keystore, keystore_password, truststore, truststore_password, cipher_suites,
                                               protocol, algorithm, store_type, require_client_auth, require_endpoint_verification,
                                               enabled, optional, internode_encryption, enable_legacy_ssl_storage_port);
        }

    }
}
