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

package org.apache.cassandra.tcm.ownership;

import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.tcm.membership.MembershipUtils.endpoint;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.junit.Assert.assertEquals;

public class PrimaryRangeComparatorTest
{
    @Test
    public void tokenOwnerSortsFirst()
    {
        Location location = new Location("dc1", "rack1");
        Directory directory = new Directory();
        TokenMap tokenMap = new TokenMap(Murmur3Partitioner.instance);

        InetAddressAndPort ep1 = endpoint(1);
        directory = directory.with(new NodeAddresses(endpoint(1)), location);
        NodeId id1 = directory.peerId(ep1);
        tokenMap = tokenMap.assignTokens(id1, Collections.singleton(token(100)));

        InetAddressAndPort ep2 = endpoint(2);
        directory = directory.with(new NodeAddresses(endpoint(2)), location);
        NodeId id2 = directory.peerId(ep2);
        tokenMap = tokenMap.assignTokens(id2, Collections.singleton(token(200)));

        InetAddressAndPort ep3 = endpoint(3);
        directory = directory.with(new NodeAddresses(endpoint(3)), location);
        NodeId id3 = directory.peerId(ep3);
        tokenMap = tokenMap.assignTokens(id3, Collections.singleton(token(300)));

        Range<Token> range = new Range<>(token(100), token(200));
        EndpointsForRange replicas = EndpointsForRange.builder(range)
                                                      .add(fullReplica(ep1, range))
                                                      .add(fullReplica(ep2, range))
                                                      .add(fullReplica(ep3, range))
                                                      .build();
        PrimaryRangeComparator c = new PrimaryRangeComparator(tokenMap, directory);
        EndpointsForRange sorted = replicas.sorted(c);
        assertEquals(ep2, sorted.iterator().next().endpoint());
    }

    @Test
    public void whenWraparoundLowestTokenOwnerSortsFirst()
    {
        Location location = new Location("dc1", "rack1");
        Directory directory = new Directory();
        TokenMap tokenMap = new TokenMap(Murmur3Partitioner.instance);

        InetAddressAndPort ep1 = endpoint(1);
        directory = directory.with(new NodeAddresses(endpoint(1)), location);
        NodeId id1 = directory.peerId(ep1);
        tokenMap = tokenMap.assignTokens(id1, Collections.singleton(token(100)));

        InetAddressAndPort ep2 = endpoint(2);
        directory = directory.with(new NodeAddresses(endpoint(2)), location);
        NodeId id2 = directory.peerId(ep2);
        tokenMap = tokenMap.assignTokens(id2, Collections.singleton(token(200)));

        InetAddressAndPort ep3 = endpoint(3);
        directory = directory.with(new NodeAddresses(endpoint(3)), location);
        NodeId id3 = directory.peerId(ep3);
        tokenMap = tokenMap.assignTokens(id3, Collections.singleton(token(300)));

        Range<Token> range = new Range<>(token(300), Murmur3Partitioner.MINIMUM);
        EndpointsForRange replicas = EndpointsForRange.builder(range)
                                                      .add(fullReplica(ep1, range))
                                                      .add(fullReplica(ep2, range))
                                                      .add(fullReplica(ep3, range))
                                                      .build();
        PrimaryRangeComparator c = new PrimaryRangeComparator(tokenMap, directory);
        EndpointsForRange sorted = replicas.sorted(c);
        assertEquals(ep1, sorted.iterator().next().endpoint());
    }
}
