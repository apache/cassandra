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

package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExpireEndpointTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testExpireEndpoint() throws UnknownHostException
    {
        InetAddressAndPort hostAddress = InetAddressAndPort.getByName("127.0.0.2");
        UUID hostId = UUID.randomUUID();
        long expireTime = System.currentTimeMillis() - 1;

        Gossiper.instance.initializeNodeUnsafe(hostAddress, hostId, 1);

        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(hostAddress);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(hostAddress, endpointState));
        endpointState.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.removedNonlocal(hostId, expireTime));
        Gossiper.instance.addExpireTimeForEndpoint(hostAddress, expireTime);

        assertTrue("Expiring endpoint not unreachable before status check", Gossiper.instance.getUnreachableMembers().contains(hostAddress));

        Gossiper.instance.doStatusCheck();

        assertFalse("Expired endpoint still part of live members", Gossiper.instance.getLiveMembers().contains(hostAddress));
        assertFalse("Expired endpoint still part of unreachable members", Gossiper.instance.getUnreachableMembers().contains(hostAddress));
        assertNull("Expired endpoint still contain endpoint state", Gossiper.instance.getEndpointStateForEndpoint(hostAddress));
    }
}
