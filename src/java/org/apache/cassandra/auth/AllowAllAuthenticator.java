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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

public class AllowAllAuthenticator implements IAuthenticator
{
    private static final SaslNegotiator AUTHENTICATOR_INSTANCE = new Negotiator();

    public boolean requireAuthentication()
    {
        return false;
    }

    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
    }

    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return AUTHENTICATOR_INSTANCE;
    }

    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentialsData)
    {
        return AuthenticatedUser.ANONYMOUS_USER;
    }

    private static class Negotiator implements SaslNegotiator
    {

        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
        {
            return null;
        }

        public boolean isComplete()
        {
            return true;
        }

        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException
        {
            return AuthenticatedUser.ANONYMOUS_USER;
        }
    }
}
