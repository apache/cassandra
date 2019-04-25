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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Test for "Gossip blocks on startup when another node is bootstrapping" (CASSANDRA-12281).
 */
@RunWith(BMUnitRunner.class)
public class PendingRangeCalculatorServiceTest
{
    static ReentrantLock calculationLock = new ReentrantLock();

    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
    }

    @Test
    @BMRule(name = "Block pending range calculation",
            targetClass = "TokenMetadata",
            targetMethod = "calculatePendingRanges",
            targetLocation = "AT INVOKE org.apache.cassandra.locator.AbstractReplicationStrategy.getAddressRanges",
            action = "org.apache.cassandra.gms.PendingRangeCalculatorServiceTest.calculationLock.lock()")
    public void testDelayedResponse() throws UnknownHostException, InterruptedException
    {
        InetAddress otherNodeAddr = InetAddress.getByName("127.0.0.2");
        UUID otherHostId = UUID.randomUUID();

        // introduce node for first major state change
        Gossiper.instance.applyStateLocally(getStates(otherNodeAddr, otherHostId, 1, false));

        // acquire lock to block pending range calculation via byteman
        calculationLock.lock();
        try
        {
            // spawn thread that will trigger handling of a bootstrap state change which in turn will trigger
            // the pending range calculation that will be blocked by our lock
            Thread t1 = new Thread()
            {
                public void run()
                {
                    Gossiper.instance.applyStateLocally(getStates(otherNodeAddr, otherHostId, 2, true));
                }
            };
            t1.start();

            // busy-spin until t1 is blocked by lock
            while (!calculationLock.hasQueuedThreads()) ;

            // trigger further state changes in case we don't want the blocked thread from the
            // expensive range calculation to block us here as well
            Thread t2 = new Thread()
            {
                public void run()
                {
                    Gossiper.instance.applyStateLocally(getStates(otherNodeAddr, otherHostId, 3, false));
                    Gossiper.instance.applyStateLocally(getStates(otherNodeAddr, otherHostId, 4, false));
                    Gossiper.instance.applyStateLocally(getStates(otherNodeAddr, otherHostId, 5, false));
                }
            };
            t2.start();
            t2.join(2000);
            assertFalse("Thread still blocked by pending range calculation", t2.isAlive());
            assertEquals(5, Gossiper.instance.getEndpointStateForEndpoint(otherNodeAddr).getHeartBeatState().getHeartBeatVersion());
        }
        finally
        {
            calculationLock.unlock();
        }
    }

    private Map<InetAddress, EndpointState> getStates(InetAddress otherNodeAddr, UUID hostId, int ver, boolean bootstrapping)
    {
        HeartBeatState hb = new HeartBeatState(1, ver);
        EndpointState state = new EndpointState(hb);
        Collection<Token> tokens = new ArrayList<>();

        tokens.add(new ByteOrderedPartitioner.BytesToken(new byte[]{1,2,3}));
        state.addApplicationState(ApplicationState.TOKENS, StorageService.instance.valueFactory.tokens(tokens));
        state.addApplicationState(ApplicationState.STATUS, bootstrapping ?
                StorageService.instance.valueFactory.bootstrapping(tokens) : StorageService.instance.valueFactory.normal(tokens));
        state.addApplicationState(ApplicationState.HOST_ID, StorageService.instance.valueFactory.hostId(hostId));
        state.addApplicationState(ApplicationState.NET_VERSION, StorageService.instance.valueFactory.networkVersion());

        Map<InetAddress, EndpointState> states = new HashMap<>();
        states.put(otherNodeAddr, state);
        return states;
    }
}
