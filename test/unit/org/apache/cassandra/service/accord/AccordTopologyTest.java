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

package org.apache.cassandra.service.accord;

import java.net.UnknownHostException;
import java.util.Collections;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByRange;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccordTopologyTest
{
    private static final String RACK1 = "RACK1";
    private static final String DC1 = "DC1";
    private static final String KEYSPACE = "ks";
    private static final InetAddressAndPort PEER1 = peer(1);
    private static final InetAddressAndPort PEER2 = peer(2);
    private static final InetAddressAndPort PEER3 = peer(3);
    private static final InetAddressAndPort PEER4 = peer(4);
    private static final InetAddressAndPort PEER5 = peer(5);
    private static final InetAddressAndPort PEER6 = peer(6);

    private static final InetAddressAndPort PEER1A = peer(11);
    private static final InetAddressAndPort PEER4A = peer(14);

    private static final Token TOKEN1 = token(0);
    private static final Token TOKEN2 = token(10);
    private static final Token TOKEN3 = token(20);
    private static final Token TOKEN4 = token(30);
    private static final Token TOKEN5 = token(40);
    private static final Token TOKEN6 = token(50);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        IEndpointSnitch snitch = snitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
    }

    @Test
    public void simpleTranslationTest()
    {

    }

    private void assertPendingRanges(PendingRangeMaps pending, RangesByEndpoint expected)
    {
        RangesByEndpoint.Builder actual = new RangesByEndpoint.Builder();
        pending.iterator().forEachRemaining(pendingRange -> {
            Replica replica = Iterators.getOnlyElement(pendingRange.getValue().iterator());
            actual.put(replica.endpoint(), replica);
        });
        assertRangesByEndpoint(expected, actual.build());
    }

    private void assertPendingRanges(EndpointsByRange pending, RangesByEndpoint expected)
    {
        RangesByEndpoint.Builder actual = new RangesByEndpoint.Builder();
        pending.flattenEntries().forEach(entry -> actual.put(entry.getValue().endpoint(), entry.getValue()));
        assertRangesByEndpoint(expected, actual.build());
    }

    private void assertRangesAtEndpoint(RangesAtEndpoint expected, RangesAtEndpoint actual)
    {
        assertEquals(expected.size(), actual.size());
        assertTrue(Iterables.all(expected, actual::contains));
    }

    private void assertRangesByEndpoint(RangesByEndpoint expected, RangesByEndpoint actual)
    {
        assertEquals(expected.keySet(), actual.keySet());
        for (InetAddressAndPort endpoint : expected.keySet())
        {
            RangesAtEndpoint expectedReplicas = expected.get(endpoint);
            RangesAtEndpoint actualReplicas = actual.get(endpoint);
            assertRangesAtEndpoint(expectedReplicas, actualReplicas);
        }
    }

    private void addNode(TokenMetadata tm, InetAddressAndPort replica, Token token)
    {
        tm.updateNormalTokens(Collections.singleton(token), replica);
    }

    private void replace(InetAddressAndPort toReplace,
                         InetAddressAndPort replacement,
                         Token token,
                         TokenMetadata tm,
                         AbstractReplicationStrategy replicationStrategy)
    {
        assertEquals(toReplace, tm.getEndpoint(token));
        tm.addReplaceTokens(Collections.singleton(token), replacement, toReplace);
        tm.calculatePendingRanges(replicationStrategy, KEYSPACE);
    }

    private static Token token(long token)
    {
        return Murmur3Partitioner.instance.getTokenFactory().fromString(Long.toString(token));
    }

    private static InetAddressAndPort peer(int addressSuffix)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static IEndpointSnitch snitch()
    {
        return new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return RACK1;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return DC1;
            }
        };
    }

    private static AbstractReplicationStrategy simpleStrategy(TokenMetadata tokenMetadata, int replicationFactor)
    {
        return new SimpleStrategy(KEYSPACE,
                                  tokenMetadata,
                                  DatabaseDescriptor.getEndpointSnitch(),
                                  Collections.singletonMap("replication_factor", Integer.toString(replicationFactor)));
    }
}
