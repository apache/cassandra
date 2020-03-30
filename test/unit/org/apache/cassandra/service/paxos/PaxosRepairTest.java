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

package org.apache.cassandra.service.paxos;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;

import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.service.paxos.PaxosRepair.validateVersionCompatibility;
import static org.junit.Assert.*;

import static org.apache.cassandra.service.paxos.PaxosRepairTest.Requirements.*;

public class PaxosRepairTest
{
    private static final String DC0 = "DC0";
    private static final String DC1 = "DC1";
    private static final String DC2 = "DC2";

    private static InetAddressAndPort intToInet(int i)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{127, 0, 0, (byte)i});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    // should make reading the tests slightly easier
    enum Requirements
    {
        NORMAL(false, false),
        STRICT_QUORUM(true, false),
        NO_DC_CHECKS(false, true),
        STRICT_QUORUM_AND_NO_DC_CHECKS(true, true);

        final boolean strict_quorum;
        final boolean no_dc_check;

        Requirements(boolean strict_quorum, boolean no_dc_check)
        {
            this.strict_quorum = strict_quorum;
            this.no_dc_check = no_dc_check;
        }
    }

    private static class Topology
    {
        Set<InetAddressAndPort> dead = new HashSet<>();
        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();


        Topology alive(String dc, int... epNums)
        {
            for (int e : epNums)
            {
                InetAddressAndPort ep = intToInet(e);
                endpointToDc.put(ep, dc);
                dead.remove(ep);
            }
            return this;
        }

        Topology dead(int... eps)
        {
            for (int i : eps)
            {
                InetAddressAndPort ep = intToInet(i);
                assert endpointToDc.containsKey(ep);
                dead.add(ep);
            }
            return this;
        }

        String getDc(InetAddressAndPort ep)
        {
            assert endpointToDc.containsKey(ep);
            return endpointToDc.get(ep);
        }

        Set<InetAddressAndPort> all()
        {
            return new HashSet<>(endpointToDc.keySet());
        }

        Set<InetAddressAndPort> live()
        {
            Set<InetAddressAndPort> eps = all();
            eps.removeAll(dead);
            return eps;
        }

        boolean hasSufficientLiveNodes(Requirements requirements)
        {
            return PaxosRepair.hasSufficientLiveNodesForTopologyChange(all(), live(), this::getDc, requirements.no_dc_check, requirements.strict_quorum);
        }
    }

    static Topology topology(String dc, int... epNums)
    {
        Topology t = new Topology();
        t.alive(dc, epNums);
        return t;
    }

    @Test
    public void singleDcHasSufficientLiveNodesTest()
    {
        assertTrue(topology(DC0, 1).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2).dead(1).hasSufficientLiveNodes(NORMAL));
        assertFalse(topology(DC0, 1, 2).dead(1, 2).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2, 3).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2, 3).dead(1).hasSufficientLiveNodes(NORMAL));
        assertFalse(topology(DC0, 1, 2, 3).dead(1, 2).hasSufficientLiveNodes(NORMAL));
    }

    @Test
    public void multiDcHasSufficientLiveNodesTest()
    {
        assertTrue(topology(DC0, 1, 2, 3).alive(DC1, 4, 5, 6).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2, 3).alive(DC1, 4, 5, 6).dead(1, 4).hasSufficientLiveNodes(NORMAL));
        assertFalse(topology(DC0, 1, 2, 3).alive(DC1, 4, 5, 6).dead(4, 5).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2, 3).alive(DC1, 4, 5, 6).dead(4, 5).hasSufficientLiveNodes(NO_DC_CHECKS));
        assertFalse(topology(DC0, 1, 2, 3).alive(DC1, 4, 5, 6).dead(1, 4, 5).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2).alive(DC1, 3, 4).alive(DC2, 5, 6).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2).alive(DC1, 3, 4).alive(DC2, 5, 6).dead(1).hasSufficientLiveNodes(NORMAL));
        assertFalse(topology(DC0, 1, 2).alive(DC1, 3, 4).alive(DC2, 5, 6).dead(1).hasSufficientLiveNodes(STRICT_QUORUM));
        assertFalse(topology(DC0, 1, 2).alive(DC1, 3, 4).alive(DC2, 5, 6).dead(5, 6).hasSufficientLiveNodes(NORMAL));
        assertTrue(topology(DC0, 1, 2).alive(DC1, 3, 4).alive(DC2, 5, 6).dead(5, 6).hasSufficientLiveNodes(STRICT_QUORUM_AND_NO_DC_CHECKS));
    }

    private static CassandraVersion version(String v)
    {
        return new CassandraVersion(v);
    }

    @Test
    public void versionValidationTest()
    {
        assertTrue(validateVersionCompatibility(version("4.1.0")));
        assertTrue(validateVersionCompatibility(version("4.1.0-SNAPSHOT")));
        assertFalse(validateVersionCompatibility(null));
    }
}
