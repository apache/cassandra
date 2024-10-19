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
import java.util.HashSet;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.paxos.Paxos.Electorate;

import static org.junit.Assert.assertEquals;

public class PaxosElectorateTest
{

    @Test
    public void compareElectoratesWithDifferentCollectionTypes() throws UnknownHostException
    {
        Token t = new Murmur3Partitioner.LongToken(0L);
        EndpointsForToken natural = EndpointsForToken.of(t, replica(1),replica(2), replica(3));
        EndpointsForToken pending = EndpointsForToken.of(t, replica(4));
        Electorate first = new Electorate(natural.endpointList(), pending.endpointList());
        Electorate second = new Electorate(new HashSet<>(natural.endpoints()), new HashSet<>(pending.endpoints()));
        assertEquals(first, second);
    }

    private static Replica replica(int i) throws UnknownHostException
    {
        return Replica.fullReplica(InetAddressAndPort.getByName("127.0.0." + i),
                                   Murmur3Partitioner.instance.getMinimumToken(),
                                   Murmur3Partitioner.instance.getMinimumToken());
    }
}
