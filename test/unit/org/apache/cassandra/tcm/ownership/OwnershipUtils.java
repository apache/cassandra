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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;

import static org.apache.cassandra.tcm.membership.MembershipUtils.randomEndpoint;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class OwnershipUtils
{
    public static Token token(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    public static RangesByEndpoint emptyReplicas()
    {
        return new RangesByEndpoint.Builder().build();
    }

    public static RangesByEndpoint trivialReplicas(InetAddressAndPort endpoint, Range<Token> range)
    {
        RangesByEndpoint.Builder b = new RangesByEndpoint.Builder();
        b.put(endpoint, Replica.fullReplica(endpoint, range));
        return b.build();
    }

    public static PlacementDeltas deltas(DataPlacements first, DataPlacements second)
    {
        assert first.asMap().keySet().equals(second.asMap().keySet());

        PlacementDeltas.Builder deltas = PlacementDeltas.builder(first.size());
        first.asMap().forEach((params, placement) -> {
            deltas.put(params, placement.difference(second.get(params)));
        });
        return deltas.build();
    }

    public static DataPlacements placements(List<Range<Token>> ranges,
                                            Set<ReplicationParams> replication,
                                            Random random)
    {
        DataPlacements.Builder allPlacements = DataPlacements.builder(replication.size());
        replication.forEach((params) -> {
            assertSame("Only simple replication params are necessary/permitted here", SimpleStrategy.class, params.klass);
            String s = params.options.get("replication_factor");
            assertNotNull(s);
            int rf = Integer.parseInt(s);
            DataPlacement.Builder placement = DataPlacement.builder();
            for (Range<Token> range : ranges)
            {
                // pick rf random nodes to be replicas, no duplicates
                Set<InetAddressAndPort> replicas = new HashSet<>(rf);
                while (replicas.size() < rf)
                    replicas.add(randomEndpoint(random));

                replicas.forEach(e -> {
                    Replica replica = Replica.fullReplica(e, range);
                    placement.withReadReplica(replica).withWriteReplica(replica);
                });
            }
            allPlacements.with(params, placement.build());
        });
        return allPlacements.build();
    }

    public static List<Range<Token>> ranges(Random random)
    {
        // min 10, max 40 ranges
        int count = random.nextInt(30) + 10;
        Set<Long> tokens = new HashSet<>(count);
        while(tokens.size() < count-1)
            tokens.add(random.nextLong());
        List<Long> sorted = new ArrayList<>(tokens);
        Collections.sort(sorted);

        List<Range<Token>> ranges = new ArrayList<>(count);
        ranges.add(new Range<>(Murmur3Partitioner.MINIMUM, token(sorted.get(0))));
        for (int i = 1; i < sorted.size(); i++)
            ranges.add(new Range<>(token(sorted.get(i-1)), token(sorted.get(i))));
        ranges.add(new Range<>(token(sorted.get(sorted.size() - 1)), Murmur3Partitioner.MINIMUM));
        return ranges;
    }

    public static DataPlacements randomPlacements(Random random)
    {
        Set<ReplicationParams> replication = ImmutableSet.of(KeyspaceParams.simple(1).replication,
                                                             KeyspaceParams.simple(2).replication,
                                                             KeyspaceParams.simple(3).replication);
        return placements(ranges(random), replication, random);
    }

}
