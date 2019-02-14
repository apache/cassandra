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
import java.util.Objects;

public class EncryptionOptions
{
    public String keystore = "conf/.keystore";
    public String keystore_password = "cassandra";
    public String truststore = "conf/.truststore";
    public String truststore_password = "cassandra";
    public String[] cipher_suites = {};
    public String protocol = "TLS";
    public String algorithm = null;
    public String store_type = "JKS";
    public boolean require_client_auth = false;
    public boolean require_endpoint_verification = false;
    public boolean enabled = false;
    public boolean optional = false;

    public EncryptionOptions()
    {   }

    /**
     * Copy constructor
     */
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
               Arrays.equals(cipher_suites, opt.cipher_suites);
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
        result += 31 * Arrays.hashCode(cipher_suites);
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

        public InternodeEncryption internode_encryption = InternodeEncryption.none;
        public boolean enable_legacy_ssl_storage_port = false;

        public ServerEncryptionOptions()
        {   }

        /**
         * Copy constructor
         */
        public ServerEncryptionOptions(ServerEncryptionOptions options)
        {
            super(options);
            internode_encryption = options.internode_encryption;
            enable_legacy_ssl_storage_port = options.enable_legacy_ssl_storage_port;
        }
    }
}
