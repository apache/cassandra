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

package org.apache.cassandra.net;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;

public class StrictStartupConnectivityCheckerTest extends AbstractStartupConnectivityCheckerTest
{
    private StartupConnectivityChecker localQuorumConnectivityChecker;
    private StartupConnectivityChecker globalQuorumConnectivityChecker;
    private StartupConnectivityChecker noopChecker;
    private StartupConnectivityChecker zeroWaitChecker;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Override
    protected void initialize()
    {
        localQuorumConnectivityChecker = new StrictStartupConnectivityChecker(TIMEOUT_NANOS, TimeUnit.NANOSECONDS, false);
        globalQuorumConnectivityChecker = new StrictStartupConnectivityChecker(TIMEOUT_NANOS, TimeUnit.NANOSECONDS, true);
        noopChecker = new StrictStartupConnectivityChecker(-1, TimeUnit.NANOSECONDS, false);
        zeroWaitChecker = new StrictStartupConnectivityChecker(0, TimeUnit.NANOSECONDS, false);
    }

    @Test
    public void execute_HappyPath()
    {
        Sink sink = new Sink(peers, peers, peers);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertTrue(localQuorumConnectivityChecker.execute(peers, this::mapToDatacenter));
    }

    @Test
    public void execute_NotAlive()
    {
        Sink sink = new Sink(Collections.emptySet(), peers, peers);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::mapToDatacenter));
    }

    @Test
    public void execute_NoConnectionsAcks()
    {
        Sink sink = new Sink(peers, Collections.emptySet(), peers);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::mapToDatacenter));
    }

    @Test
    public void execute_LocalQuorum()
    {
        // local peer plus 3 peers from same dc shouldn't pass (4/6)
        Set<InetAddressAndPort> available = new HashSet<>();
        copyCount(dc1NodesMinusLocal, available, NUM_PER_DC - 3);
        checkAvailable(localQuorumConnectivityChecker, available, false);

        // local peer plus 4 peers from same dc should pass (5/6)
        available.clear();
        copyCount(dc1NodesMinusLocal, available, NUM_PER_DC - 2);
        checkAvailable(localQuorumConnectivityChecker, available, true);
    }

    @Test
    public void execute_GlobalQuorum()
    {
        // local dc passing shouldn't pass globally with two hosts down in datacenterB
        Set<InetAddressAndPort> available = new HashSet<>();
        copyCount(dc1NodesMinusLocal, available, NUM_PER_DC - 2);
        copyCount(dc2Nodes, available, NUM_PER_DC - 2);
        copyCount(dc3Nodes, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, false);

        // All three datacenters should be able to have a single node down
        available.clear();
        copyCount(dc1NodesMinusLocal, available, NUM_PER_DC - 2);
        copyCount(dc2Nodes, available, NUM_PER_DC - 1);
        copyCount(dc3Nodes, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, true);

        // Everything being up should work of course
        available.clear();
        copyCount(dc1NodesMinusLocal, available, NUM_PER_DC - 1);
        copyCount(dc2Nodes, available, NUM_PER_DC);
        copyCount(dc3Nodes, available, NUM_PER_DC);
        checkAvailable(globalQuorumConnectivityChecker, available, true);
    }

    @Test
    public void execute_Noop()
    {
        checkAvailable(noopChecker, new HashSet<>(), true);
    }

    @Test
    public void execute_ZeroWaitHasConnections()
    {
        Sink sink = new Sink(peers, peers, new HashSet<>());
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertFalse(zeroWaitChecker.execute(peers, this::mapToDatacenter));
        MessagingService.instance().outboundSink.clear();
    }

    private void checkAvailable(StartupConnectivityChecker checker, Set<InetAddressAndPort> available,
                                boolean shouldPass)
    {
        Sink sink = new Sink(available, available, available);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertEquals(shouldPass, checker.execute(peers, this::mapToDatacenter));
        MessagingService.instance().outboundSink.clear();
    }

    private void copyCount(Set<InetAddressAndPort> source, Set<InetAddressAndPort> dest, int count)
    {
        for (InetAddressAndPort peer : source)
        {
            if (count <= 0)
                break;

            dest.add(peer);
            count -= 1;
        }
    }
}
