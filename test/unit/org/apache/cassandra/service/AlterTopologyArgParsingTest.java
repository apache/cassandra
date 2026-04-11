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

package org.apache.cassandra.service;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.AlterTopology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AlterTopologyArgParsingTest
{
    Location loc = new Location("test_dc", "test_rack");
    NodeId id = new NodeId(1);
    Directory dir;

    @Before
    public void setup()
    {
        dir = new Directory();
    }

    @Test
    public void testSingleChangeByInt()
    {
        String arg = "1=test_dc:test_rack";
        Map<NodeId, Location> parsed = AlterTopology.parseArgs(arg, dir);
        assertEquals(1, parsed.size());
        assertEquals(parsed.get(id), loc);
    }

    @Test
    public void testSingleChangeByUUID()
    {
        String arg = String.format("%s=test_dc:test_rack", id.toUUID().toString());
        Map<NodeId, Location> parsed = AlterTopology.parseArgs(arg, dir);
        assertEquals(1, parsed.size());
        assertEquals(parsed.get(id), loc);
    }

    @Test
    public void testSingleChangeByEndpoint()
    {
        InetAddressAndPort ep = MembershipUtils.endpoint(1);
        dir = dir.with(new NodeAddresses(ep), loc); // this will associate NodeId(1) with ep
        String arg = String.format("%s=test_dc:test_rack", ep.getHostAddressAndPort());
        Map<NodeId, Location> parsed = AlterTopology.parseArgs(arg, dir);
        assertEquals(1, parsed.size());
        assertEquals(parsed.get(id), loc);
    }

    @Test
    public void testSingleChangeByEndpointAddress()
    {
        InetAddressAndPort ep = MembershipUtils.endpoint(1);
        dir = dir.with(new NodeAddresses(ep), loc); // this will associate NodeId(1) with ep
        String arg = String.format("%s=test_dc:test_rack", ep.getHostAddress(false));
        Map<NodeId, Location> parsed = AlterTopology.parseArgs(arg, dir);
        assertEquals(1, parsed.size());
        assertEquals(parsed.get(id), loc);
    }

    @Test
    public void testInvalidArg()
    {
        String[] args = new String[]{ "invalid", "1=", "=dc:rack", "1=dc", "1=dc:" };
        for (String invalid : args)
        {
            try
            {
                AlterTopology.parseArgs(invalid, dir);
                fail("Expected exception");
            }
            catch (IllegalArgumentException e)
            {
            }
        }
    }

    @Test
    public void testMultipleChanges()
    {
        NodeId otherId = new NodeId(2);
        InetAddressAndPort ep = MembershipUtils.endpoint(1);
        dir = dir.with(new NodeAddresses(ep), loc); // this will associate NodeId(1) with ep
        String arg = String.format("%s=dc1:rack1,%s=dc2:rack2,3=dc3:rack3,",
                                   ep.getHostAddress(true),
                                   otherId.toUUID().toString());
        Map<NodeId, Location> parsed = AlterTopology.parseArgs(arg, dir);
        assertEquals(3, parsed.size());
        assertEquals(parsed.get(id).datacenter, "dc1");
        assertEquals(parsed.get(id).rack, "rack1");
        assertEquals(parsed.get(otherId).datacenter, "dc2");
        assertEquals(parsed.get(otherId).rack, "rack2");
        assertEquals(parsed.get(new NodeId(3)).datacenter, "dc3");
        assertEquals(parsed.get(new NodeId(3)).rack, "rack3");
    }

    @Test
    public void testMultipleChangesForSameNode()
    {
        InetAddressAndPort ep = MembershipUtils.endpoint(1);
        dir = dir.with(new NodeAddresses(ep), loc); // this will associate NodeId(1) with ep
        String epString = ep.getHostAddress(true);
        String idString = id.toUUID().toString();
        assertIllegalArgument(String.format("%1$s=dc1:rack1,%1$s=dc2:rack2", id.id()));
        assertIllegalArgument(String.format("%s=dc1:rack1,%s=dc2:rack2", id.id(), idString));
        assertIllegalArgument(String.format("%s=dc1:rack1,%s=dc2:rack2", id.id(), epString));
        assertIllegalArgument(String.format("%1$s=dc1:rack1,%1$s=dc2:rack2", epString));
        assertIllegalArgument(String.format("%1$s=dc1:rack1,%1$s=dc2:rack2", idString));
        assertIllegalArgument(String.format("%s=dc1:rack1,%s=dc2:rack2,%s=dc3:rack3", id.id(), idString, epString));
    }

    private void assertIllegalArgument(String arg)
    {
       try
       {
           AlterTopology.parseArgs(arg, dir);
           fail("Expected exception");
       }
       catch (IllegalArgumentException e) {}
    }
}
