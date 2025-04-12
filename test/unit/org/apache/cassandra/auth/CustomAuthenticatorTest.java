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
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.AuthenticateMessage;

import static org.assertj.core.api.Assertions.assertThat;


public class CustomAuthenticatorTest
{
    private static final String CUSTOM_AUTHENTICATOR_FQCN = "com.example.auth.CustomAuthenticator";

    @Test
    public void testCustomAuthenticator()
    {
        IAuthenticator authenticator = new CustomAuthenticator();

        AuthenticateMessage message = authenticator.getAuthenticateMessage(ClientState.forInternalCalls());

        assertThat(message.authenticator).isNotEqualTo(authenticator.getClass().getName());
        assertThat(message.authenticator).isEqualTo(CUSTOM_AUTHENTICATOR_FQCN);
    }

    private static class CustomAuthenticator implements IAuthenticator
    {
        @Override
        public boolean requireAuthentication()
        {
            return false;
        }

        @Override
        public Set<? extends IResource> protectedResources()
        {
            return Set.of();
        }

        @Override
        public void validateConfiguration() throws ConfigurationException {}

        @Override
        public void setup() {}

        @Override
        public AuthenticateMessage getAuthenticateMessage(ClientState clientState)
        {
            return new AuthenticateMessage(CUSTOM_AUTHENTICATOR_FQCN);
        }


        @Override
        public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
        {
            return null;
        }

        @Override
        public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
        {
            return null;
        }
    }
}
