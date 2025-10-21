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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.broadcastAddress;
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

    public static RangesByEndpoint fullReplicas(InetAddressAndPort endpoint, Range<Token> range)
    {
        RangesByEndpoint.Builder b = new RangesByEndpoint.Builder();
        b.put(endpoint, Replica.fullReplica(endpoint, range));
        return b.build();
    }

    public static RangesByEndpoint transientReplicas(InetAddressAndPort endpoint, Range<Token> range)
    {
        RangesByEndpoint.Builder b = new RangesByEndpoint.Builder();
        b.put(endpoint, Replica.transientReplica(endpoint, range));
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
                    placement.withReadReplica(Epoch.FIRST, replica).withWriteReplica(Epoch.FIRST, replica);
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
        return ranges(count, Murmur3Partitioner.instance, random);
    }

    public static List<Range<Token>> ranges(int count, IPartitioner partitioner, Random random)
    {
        Set<Token> tokens = new HashSet<>(count);
        while(tokens.size() < count-1)
            tokens.add(partitioner.getRandomToken(random));
        return ranges(tokens, partitioner);
    }

    public static List<Range<Token>> ranges(Collection<Token> tokens, IPartitioner partitioner)
    {
        List<Token> sorted = new ArrayList<>(tokens);
        Collections.sort(sorted);

        List<Range<Token>> ranges = new ArrayList<>(tokens.size() + 1);
        ranges.add(new Range<>(partitioner.getMinimumToken(), sorted.get(0)));
        for (int i = 1; i < sorted.size(); i++)
            ranges.add(new Range<>(sorted.get(i-1), sorted.get(i)));
        ranges.add(new Range<>(sorted.get(sorted.size() - 1), partitioner.getMinimumToken()));
        return ranges;
    }

    public static DataPlacements randomPlacements(Random random)
    {
        Set<ReplicationParams> replication = ImmutableSet.of(KeyspaceParams.simple(1).replication,
                                                             KeyspaceParams.simple(2).replication,
                                                             KeyspaceParams.simple(3).replication);
        return placements(ranges(random), replication, random);
    }

    public static void setLocalTokens(int... tokens)
    {
        Set<Token> joiningTokens = new HashSet<>();
        for (int token : tokens)
            joiningTokens.add(token(token));
        ClusterMetadataTestHelper.join(broadcastAddress, joiningTokens);
    }


    public static RangesAtEndpoint generateRangesAtEndpoint(InetAddressAndPort endpoint, int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("generateRangesAtEndpoint argument count should be even");

        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(endpoint);

        for (int i = 0; i < rangePairs.length; i += 2)
        {
            builder.add(Replica.fullReplica(endpoint, generateRange(rangePairs[i], rangePairs[i + 1])));
        }
        return builder.build();
    }

    public static List<Range<Token>> generateRanges(int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("generateRanges argument count should be even");

        List<Range<Token>> ranges = new ArrayList<>();

        for (int i = 0; i < rangePairs.length; i += 2)
        {
            ranges.add(generateRange(rangePairs[i], rangePairs[i + 1]));
        }

        return ranges;
    }

    public static Range<Token> generateRange(int left, int right)
    {
        return new Range<>(token(left), token(right));
    }

    public static Token token(int token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }

    public static Token bytesToken(int token)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(token));
    }
    public static void beginJoin(int... tokens)
    {
        Set<Token> newTokens = new HashSet<>();
        for (int token : tokens)
            newTokens.add(token(token));
        ClusterMetadataTestHelper.joinPartially(broadcastAddress, newTokens);
    }

    public static void beginMove(int... tokens)
    {
        Set<Token> newTokens = new HashSet<>();
        for (int token : tokens)
            newTokens.add(token(token));
        ClusterMetadataTestHelper.movePartially(broadcastAddress, newTokens);
    }

    public static PlacementDeltas randomDeltas(List<Range<Token>> ranges, Random random)
    {
        Set<ReplicationParams> replication = ImmutableSet.of(KeyspaceParams.simple(1).replication,
                                                             KeyspaceParams.simple(2).replication,
                                                             KeyspaceParams.simple(3).replication);
        return deltas(placements(ranges, replication, random), placements(ranges, replication, random));
    }

    public static PlacementDeltas randomDeltas(IPartitioner partitioner, Random random)
    {
        List<Range<Token>> ranges = ranges(10, partitioner, random);
        return randomDeltas(ranges,random);
    }

    public static Set<Token> randomTokens(int numTokens, IPartitioner partitioner, Random random)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while(tokens.size() < numTokens)
            tokens.add(partitioner.getRandomToken(random));
        return tokens;
    }
}
