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

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingServiceTest;

public class ReconnectableSnitchHelperTest
{
    static final IInternodeAuthenticator originalAuthenticator = DatabaseDescriptor.getInternodeAuthenticator();

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
    }

    /**
     * Make sure that if a node fails internode authentication and MessagingService returns a null
     * pool that ReconnectableSnitchHelper fails gracefully.
     */
    @Test
    public void failedAuthentication() throws Exception
    {
        DatabaseDescriptor.setInternodeAuthenticator(MessagingServiceTest.ALLOW_NOTHING_AUTHENTICATOR);
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.250");
        //Should tolerate null returns by MS for the connection
        ReconnectableSnitchHelper.reconnect(address, address, null, null);
    }

    @After
    public void replaceAuthenticator()
    {
        DatabaseDescriptor.setInternodeAuthenticator(originalAuthenticator);
    }
}
