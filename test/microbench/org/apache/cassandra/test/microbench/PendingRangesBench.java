/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.test.microbench;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.apache.cassandra.locator.Replica;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class PendingRangesBench
{
    PendingRangeMaps pendingRangeMaps;
    int maxToken = 256 * 100;

    Multimap<Range<Token>, Replica> oldPendingRanges;

    private Range<Token> genRange(String left, String right)
    {
        return new Range<Token>(new RandomPartitioner.BigIntegerToken(left), new RandomPartitioner.BigIntegerToken(right));
    }

    @Setup
    public void setUp() throws UnknownHostException
    {
        pendingRangeMaps = new PendingRangeMaps();
        oldPendingRanges = HashMultimap.create();

        List<InetAddressAndPort> endpoints = Lists.newArrayList(InetAddressAndPort.getByName("127.0.0.1"),
                                                                InetAddressAndPort.getByName("127.0.0.2"));

        for (int i = 0; i < maxToken; i++)
        {
            for (int j = 0; j < ThreadLocalRandom.current().nextInt(2); j ++)
            {
                Range<Token> range = genRange(Integer.toString(i * 10 + 5), Integer.toString(i * 10 + 15));
                Replica replica = Replica.fullReplica(endpoints.get(j), range);
                pendingRangeMaps.addPendingRange(range, replica);
                oldPendingRanges.put(range, replica);
            }
        }

        // add the wrap around range
        for (int j = 0; j < ThreadLocalRandom.current().nextInt(2); j ++)
        {
            Range<Token> range = genRange(Integer.toString(maxToken * 10 + 5), Integer.toString(5));
            Replica replica = Replica.fullReplica(endpoints.get(j), range);
            pendingRangeMaps.addPendingRange(range, replica);
            oldPendingRanges.put(range, replica);
        }
    }

    @Benchmark
    public void searchToken(final Blackhole bh)
    {
        int randomToken = ThreadLocalRandom.current().nextInt(maxToken * 10 + 5);
        Token searchToken = new RandomPartitioner.BigIntegerToken(Integer.toString(randomToken));
        bh.consume(pendingRangeMaps.pendingEndpointsFor(searchToken));
    }

    @Benchmark
    public void searchTokenForOldPendingRanges(final Blackhole bh)
    {
        int randomToken = ThreadLocalRandom.current().nextInt(maxToken * 10 + 5);
        Token searchToken = new RandomPartitioner.BigIntegerToken(Integer.toString(randomToken));
        Set<Replica> replicas = new HashSet<>();
        for (Map.Entry<Range<Token>, Collection<Replica>> entry : oldPendingRanges.asMap().entrySet())
        {
            if (entry.getKey().contains(searchToken))
                replicas.addAll(entry.getValue());
        }
        bh.consume(replicas);
    }

}
