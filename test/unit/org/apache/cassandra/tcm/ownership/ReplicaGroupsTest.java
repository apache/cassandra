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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.tcm.membership.MembershipUtils.endpoint;

@RunWith(Parameterized.class)
public class ReplicaGroupsTest
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
            { RandomPartitioner.instance },
            { Murmur3Partitioner.instance },
        });
    }

    private final IPartitioner partitioner;

    public ReplicaGroupsTest(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    @Test
    public void testMatchToken()
    {
        qt().forAll(args(partitioner))
            .check(args -> {
                for (Token token : args.tokens)
                    assertEquivalent(args.groups, token);
            });
    }

    @Test
    public void minimumRepro()
    {
        Range<Token> range = new Range<>(new Murmur3Partitioner.LongToken(5), new Murmur3Partitioner.LongToken(10));
        EndpointsForRange.Builder endpointsBuilder = EndpointsForRange.builder(range);
        endpointsBuilder.add(Replica.fullReplica(endpoint((byte) 1), range));

        ReplicaGroups groups = ReplicaGroups.builder()
                                             .withReplicaGroup(VersionedEndpoints.forRange(Epoch.FIRST, endpointsBuilder.build()))
                                             .build();

        Token token = new Murmur3Partitioner.LongToken(5);
        assertEquivalent(groups, token);
    }

    @Test
    public void testMatchTokenWithWraparoundRange()
    {
        // Create a wraparound range (wraps around from high to low token value)
        // For RandomPartitioner, this would be something like (100, minToken] which wraps around
        Token highToken = new RandomPartitioner.BigIntegerToken("100");
        Token minToken = new RandomPartitioner.BigIntegerToken("0");
        Range<Token> wraparoundRange = new Range<>(highToken, minToken);

        EndpointsForRange.Builder endpointsBuilder = EndpointsForRange.builder(wraparoundRange);
        endpointsBuilder.add(Replica.fullReplica(endpoint((byte) 1), wraparoundRange));

        ReplicaGroups groups = ReplicaGroups.builder()
                                             .withReplicaGroup(VersionedEndpoints.forRange(Epoch.FIRST, endpointsBuilder.build()))
                                             .build();

        // Test with minToken - should be in the wraparound range
        assertEquivalent(groups, minToken);

        // Test with a token just after highToken
        Token tokenAfterHigh = new RandomPartitioner.BigIntegerToken("150");
        assertEquivalent(groups, tokenAfterHigh);
    }

    private static Object result(Callable<Object> callable)
    {
        try
        {
            return callable.call();
        }
        catch (Throwable t)
        {
            return t;
        }
    }

    private static void assertEquivalent(ReplicaGroups groups, Token token)
    {
        Object fast = result(() -> groups.matchToken(token));
        Object slow = result(() -> {
            for (VersionedEndpoints.ForRange forRange : groups.endpoints)
            {
                if (forRange.get().range().contains(token))
                    return forRange;
            }
            return null;
        });
        if (fast instanceof Throwable && slow instanceof Throwable)
        {
            Assertions.assertThat(fast).hasSameClassAs(slow);
            Assertions.assertThat(((Throwable) fast).getMessage()).isEqualTo(((Throwable) slow).getMessage());
        }
        else
        {
            Assertions.assertThat(slow).isEqualTo(fast);
        }
    }

    private static class Args
    {
        ReplicaGroups groups;
        Collection<Token> tokens;

        public Args(ReplicaGroups groups, Collection<Token> tokens)
        {
            this.groups = groups;
            this.tokens = tokens;
        }

        @Override
        public String toString()
        {
            return "Args{" +
                   "groups=" + groups +
                   ", tokens=" + tokens +
                   '}';
        }
    }

    private static Gen<Args> args(IPartitioner partitioner)
    {
        return rs -> new Args(replicaGroups(rs, partitioner), tokens(rs, partitioner));
    }

    private static ReplicaGroups replicaGroups(RandomSource rs, IPartitioner partitioner)
    {
        Range<Token> full = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumTokenForSplitting());
        Splitter splitter = partitioner.splitter().get();
        int parts = rs.nextBiasedInt(0, 5, 100);
        Set<Range<Token>> splits = splitter.split(full, parts);

        // Drop some ranges so there are gaps
        List<Token> drops = Gens.lists(AccordGenerators.fromQT(CassandraGenerators.tokensInRange(full))).ofSizeBetween(0, 2).next(rs);

        ReplicaGroups.Builder builder = ReplicaGroups.builder();

        for (Range<Token> range : splits)
        {
            boolean skip = false;
            for (Token drop : drops)
                if (range.contains(drop))
                    skip = true;
            if (skip)
                continue;

            // Generate 1-3 replicas per range
            int numReplicas = rs.nextInt(1, 3);
            EndpointsForRange.Builder endpointsBuilder = EndpointsForRange.builder(range);

            for (int i = 0; i < numReplicas; i++)
            {
                InetAddressAndPort endpoint = endpoint((byte) (i + 1));
                endpointsBuilder.add(Replica.fullReplica(endpoint, range));
            }

            builder.withReplicaGroup(VersionedEndpoints.forRange(Epoch.FIRST, endpointsBuilder.build()));
        }

        return builder.build();
    }

    private static Collection<Token> tokens(RandomSource rs, IPartitioner partitioner)
    {
        Gen<Token> tokenGen = AccordGenerators.fromQT(CassandraGenerators.token(partitioner));
        int numTokens = rs.nextInt(1, 3);
        List<Token> tokens = new ArrayList<>();
        for (int i = 0; i < numTokens; i++)
        {
            tokens.add(tokenGen.next(rs));
        }
        return tokens;
    }
}
