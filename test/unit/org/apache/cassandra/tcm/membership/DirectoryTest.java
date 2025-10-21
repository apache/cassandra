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

package org.apache.cassandra.tcm.membership;

import org.junit.Test;

import static org.apache.cassandra.tcm.membership.MembershipUtils.endpoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DirectoryTest
{

    @Test
    public void updateLocationTest()
    {
        Location DC1_R1 = new Location("datacenter1", "rack1");
        Directory dir = new Directory();
        assertTrue(dir.isEmpty());
        assertTrue(dir.knownDatacenters().isEmpty());

        NodeId missing = new NodeId(1000);
        assertInvalidLocationUpdate(dir, missing, DC1_R1, "Node " + missing + " has no registered location to update");

        // add a new node and retrieve its Location
        NodeAddresses addresses = new NodeAddresses(endpoint(1));
        dir = dir.with(addresses, DC1_R1);
        NodeId node = dir.peerId(addresses.broadcastAddress);
        assertEquals(DC1_R1, dir.location(node));
        assertTrue(dir.knownDatacenters().contains("datacenter1"));

        // endpoints by DC & rack are not updated immediately, this is an explicit step when a node joins
        assertTrue(dir.allDatacenterEndpoints().isEmpty());
        assertTrue(dir.allDatacenterRacks().isEmpty());

        // when a node joins, its DC and rack become active
        dir = dir.withRackAndDC(node);
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter1").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter1").get("rack1").contains(addresses.broadcastAddress));

        // update rack
        Location DC1_R2 = new Location("datacenter1", "rack2");
        dir = dir.withUpdatedRackAndDc(node, DC1_R2);
        assertEquals(DC1_R2, dir.location(node));
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter1").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter1").get("rack2").contains(addresses.broadcastAddress));
        // previous rack is no longer present as it was made empty
        assertFalse(dir.allDatacenterRacks().get("datacenter1").containsKey("rack1"));

        // update DC
        Location DC2_R2 = new Location("datacenter2", "rack2");
        dir = dir.withUpdatedRackAndDc(node, DC2_R2);
        assertEquals(DC2_R2, dir.location(node));
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter2").get("rack2").contains(addresses.broadcastAddress));
        // datacenter1 is no longer present as it was made empty
        assertFalse(dir.allDatacenterRacks().containsKey("datacenter1"));
        assertFalse(dir.knownDatacenters().contains("datacenter1"));
        assertTrue(dir.knownDatacenters().contains("datacenter2"));

        // Add a second node in the same dc & rack
        NodeAddresses otherAddresses = new NodeAddresses(endpoint(2));
        dir = dir.with(otherAddresses, DC2_R2);
        NodeId otherNode = dir.peerId(otherAddresses.broadcastAddress);
        dir = dir.withRackAndDC(otherNode);
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(otherAddresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter2").get("rack2").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter2").get("rack2").contains(otherAddresses.broadcastAddress));

        // now updating the rack of the first node should not remove rack2 altogether as it not empty
        Location DC2_R3 = new Location("datacenter2", "rack3");
        dir = dir.withUpdatedRackAndDc(node, DC2_R3);
        assertEquals(DC2_R3, dir.location(node));
        // updated node is removed from rack2 and added to rack3
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter2").get("rack3").contains(addresses.broadcastAddress));
        assertFalse(dir.allDatacenterRacks().get("datacenter2").get("rack2").contains(addresses.broadcastAddress));
        // other node is still present in rack2
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(otherAddresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter2").get("rack2").contains(otherAddresses.broadcastAddress));
        assertFalse(dir.allDatacenterRacks().get("datacenter2").get("rack3").contains(otherAddresses.broadcastAddress));

        // simulate what happens when the nodes leave the cluster
        dir = dir.withoutRackAndDC(otherNode);
        assertFalse(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(otherAddresses.broadcastAddress));
        assertFalse(dir.allDatacenterRacks().get("datacenter2").containsKey("rack2"));
        assertTrue(dir.allDatacenterEndpoints().asMap().get("datacenter2").contains(addresses.broadcastAddress));
        assertTrue(dir.allDatacenterRacks().get("datacenter2").get("rack3").contains(addresses.broadcastAddress));

        dir = dir.withoutRackAndDC(node);
        assertTrue(dir.allDatacenterEndpoints().isEmpty());
        assertTrue(dir.allDatacenterRacks().isEmpty());
    }

    private void assertInvalidLocationUpdate(Directory dir, NodeId nodeId, Location loc, String message)
    {
        try
        {
            dir.withUpdatedRackAndDc(nodeId, loc);
            fail("Expected an exception");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().equals(message));
        }
    }
}
