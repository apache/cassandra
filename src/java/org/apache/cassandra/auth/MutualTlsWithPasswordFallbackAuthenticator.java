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

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * This authenticator can be used in optional mTLS mode, If the client doesn't make an mTLS connection
 * this fallbacks to password authentication.
 */
public class MutualTlsWithPasswordFallbackAuthenticator extends PasswordAuthenticator
{
    private final MutualTlsAuthenticator mutualTlsAuthenticator;

    private final Set<AuthenticationMode> AUTHENTICATION_MODES;

    public MutualTlsWithPasswordFallbackAuthenticator(Map<String, String> parameters)
    {
        mutualTlsAuthenticator = new MutualTlsAuthenticator(parameters);
        AUTHENTICATION_MODES = Sets.union(super.getSupportedAuthenticationModes(), mutualTlsAuthenticator.getSupportedAuthenticationModes());
    }

    @Override
    public void setup()
    {
        super.setup();
        mutualTlsAuthenticator.setup();
    }

    @Override
    public boolean supportsEarlyAuthentication()
    {
        return true;
    }

    @Override
    public Set<AuthenticationMode> getSupportedAuthenticationModes()
    {
        return AUTHENTICATION_MODES;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress, Certificate[] certificates)
    {
        if (certificates == null || certificates.length == 0)
        {
            // If no certificates present, fallback to PasswordAuthentication
            return newSaslNegotiator(clientAddress);
        }
        // Otherwise attempt to authenticate using the client-provided certificate.
        return mutualTlsAuthenticator.newSaslNegotiator(clientAddress, certificates);
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {
        Config config = DatabaseDescriptor.getRawConfig();
        if (config.client_encryption_options.getClientAuth() == EncryptionOptions.ClientAuth.NOT_REQUIRED)
        {
            String msg = "MutualTlsWithPasswordFallbackAuthenticator requires client_encryption_options.require_client_auth to be optional/true";
            throw new ConfigurationException(msg);
        }
    }
}
