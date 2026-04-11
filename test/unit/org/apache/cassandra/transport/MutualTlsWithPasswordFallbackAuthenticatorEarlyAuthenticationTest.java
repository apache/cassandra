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

package org.apache.cassandra.transport;


import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.transport.messages.AuthenticateMessage;

import static org.junit.Assert.fail;

/**
 * A variant of {@link EarlyAuthenticationTest} that configures
 * {@link org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator}
 * as the configured authenticator.
 * <p>
 * This authenticator has an interesting property such that its underlying
 * {@link IAuthenticator.SaslNegotiator#shouldSendAuthenticateMessage()} will only return false if a client
 * certificate is present, otherwise it will return true, which should cause Cassandra to return an AUTHENTICATE
 * message in response to a STARTUP request sent by a client which will provoke the normal authentication flow.
 */
public class MutualTlsWithPasswordFallbackAuthenticatorEarlyAuthenticationTest extends EarlyAuthenticationTest
{

    @BeforeClass
    public static void setup()
    {
        setupWithAuthenticator(new AuthTestUtils.LocalMutualTlsWithPasswordFallbackAuthenticator(authenticatorParams));
    }

    @Test
    @Override
    public void testNoClientCertificatePresented()
    {
        /*
         * given server is configured with a Fallback password authenticator in that it supports early certificate
         * authentication, but the sasl negotiator is determined based on the presence of client certificates
         *
         * When connecting without a client certificate, we expect the server to prompt us to authenticate.
         */
        testStartupResponse(false, startupResponse -> {
            if (!(startupResponse instanceof AuthenticateMessage))
            {
                fail("Expected an AUTHENTICATE in response to a STARTUP, got: " + startupResponse);
            }
        });
    }
}
