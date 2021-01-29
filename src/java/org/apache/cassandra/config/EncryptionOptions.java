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

import javax.net.ssl.SSLSocketFactory;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

public abstract class EncryptionOptions
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptionOptions.class);

    public String keystore = "conf/.keystore";
    public String keystore_password = "cassandra";
    public String truststore = "conf/.truststore";
    public String truststore_password = "cassandra";
    public String[] cipher_suites = ((SSLSocketFactory)SSLSocketFactory.getDefault()).getDefaultCipherSuites();
    public String protocol = "TLS";
    public String algorithm = "SunX509";
    public String store_type = "JKS";
    public boolean require_client_auth = false;
    public boolean require_endpoint_verification = false;

    public static class ClientEncryptionOptions extends EncryptionOptions
    {
        public boolean enabled = false;
        public boolean optional = false;
    }

    public static class ServerEncryptionOptions extends EncryptionOptions
    {
        public static enum InternodeEncryption
        {
            all, none, dc, rack
        }

        public InternodeEncryption internode_encryption = InternodeEncryption.none;

        public boolean shouldEncrypt(InetAddress endpoint)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            InetAddress local = FBUtilities.getBroadcastAddress();

            switch (internode_encryption)
            {
                case none:
                    return false; // if nothing needs to be encrypted then return immediately.
                case all:
                    break;
                case dc:
                    if (snitch.getDatacenter(endpoint).equals(snitch.getDatacenter(local)))
                        return false;
                    break;
                case rack:
                    // for rack then check if the DC's are the same.
                    if (snitch.getRack(endpoint).equals(snitch.getRack(local))
                        && snitch.getDatacenter(endpoint).equals(snitch.getDatacenter(local)))
                        return false;
                    break;
            }
            return true;
        }

        public void validate()
        {
            if (require_client_auth && (internode_encryption == InternodeEncryption.rack || internode_encryption == InternodeEncryption.dc))
            {
                logger.warn("Setting require_client_auth is incompatible with 'rack' and 'dc' internode_encryption values."
                          + " It is possible for an internode connection to pretend to be in the same rack/dc by spoofing"
                          + " its broadcast address in the handshake and bypass authentication. To ensure that mutual TLS"
                          + " authentication is not bypassed, please set internode_encryption to 'all'. Continuing with"
                          + " insecure configuration.");
            }
        }
    }
}
