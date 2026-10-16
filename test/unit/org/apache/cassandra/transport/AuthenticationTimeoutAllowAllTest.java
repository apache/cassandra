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

import java.util.Collections;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.apache.cassandra.transport.AuthenticationTimeoutTest.assertAuthenticationTimeoutHandlerRemoved;
import static org.junit.Assert.assertTrue;

public class AuthenticationTimeoutAllowAllTest extends CQLTester
{
    private static final long AUTH_TIMEOUT_MS = 2000L;
    private static long originalAuthTimeout;

    @BeforeClass
    public static void setUp()
    {
        originalAuthTimeout = DatabaseDescriptor.nativeTransportAuthTimeout();
        DatabaseDescriptor.setNativeTransportAuthTimeout(AUTH_TIMEOUT_MS);
        requireNetwork();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setNativeTransportAuthTimeout(originalAuthTimeout);
    }

    @Test
    public void testConnectionIsNotClosedWhenStartupTransitionsDirectlyToReady() throws Exception
    {
        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort))
        {
            client.establishConnection();

            StartupMessage startup = new StartupMessage(
                Collections.singletonMap(StartupMessage.CQL_VERSION,
                                         QueryProcessor.CQL_VERSION.toString()));

            Message.Response response = client.execute(startup);

            assertTrue("Expected READY response", response instanceof ReadyMessage);

            Thread.sleep(AUTH_TIMEOUT_MS + 500);

            assertTrue("Connection should remain open after STARTUP transitions directly to READY",
                       client.channel.isOpen());

            assertAuthenticationTimeoutHandlerRemoved(client);
        }
    }
}