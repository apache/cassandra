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

package org.apache.cassandra.test.microbench;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.RegistrationStatus;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
public class LocatorBench
{
    static Random random = new Random(1);
    static ClusterMetadata metadata;
    static InetAddressAndPort[] queryEndpoints = new InetAddressAndPort[20000];
    @Setup(Level.Trial)
    public void setup() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        int nodecount = 1000;
        metadata = fakeMetadata(nodecount, 3, 3);
        RegistrationStatus.instance.onRegistration();
        List<InetAddressAndPort> allEndpoints = new ArrayList<>(metadata.directory.allJoinedEndpoints());
        for (int i = 0; i < 20000; i++)
            queryEndpoints[i] = allEndpoints.get(random.nextInt(allEndpoints.size()));
    }

    @Benchmark
    public void bench()
    {
        for (InetAddressAndPort ep : queryEndpoints)
            metadata.locator.location(ep);
    }

    public static ClusterMetadata fakeMetadata(int nodeCount, int dcCount, int rackCount) throws UnknownHostException
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance);
        TokenSupplier tokensupplier = TokenSupplier.evenlyDistributedTokens(nodeCount);
        PlacementProvider placementProvider = new UniformRangePlacement();
        for (int i = 1; i < nodeCount; i++)
        {
            ClusterMetadata.Transformer transformer = metadata.transformer();
            UUID uuid = UUID.randomUUID();
            NodeAddresses addresses = addresses(uuid, i);
            metadata = transformer.register(addresses, new Location("dc" + random.nextInt(dcCount), "rack"+random.nextInt(rackCount)), NodeVersion.CURRENT).build().metadata;
            NodeId nodeId = metadata.directory.peerId(addresses.broadcastAddress);
            metadata = new UnsafeJoin(nodeId, Collections.singleton(new Murmur3Partitioner.LongToken(tokensupplier.token(i))), placementProvider).execute(metadata).success().metadata;
        }

        return metadata;
    }

    static NodeAddresses addresses(UUID uuid, int idx) throws UnknownHostException
    {
        byte [] address = new byte [] {127, 0,
                                       (byte) (((idx + 1) & 0x0000ff00) >> 8),
                                       (byte) ((idx + 1) & 0x000000ff)};

        InetAddressAndPort host = InetAddressAndPort.getByAddress(address);
        return new NodeAddresses(uuid, host, host, host);
    }

    public static void main(String[] args) throws RunnerException, UnknownHostException {
        Options options = new OptionsBuilder()
                          .include(LocatorBench.class.getSimpleName())
                          .build();
        new Runner(options).run();
    }
/*
$ ant microbench -Dbenchmark.name=LocatorBench

Approaches tried - switching out the peers + locations data structures in Directory:
    Original BTreeMap:
     [java] Iteration   1: 4,406 ms/op
     [java] Iteration   2: 4,463 ms/op
     [java] Iteration   3: 4,527 ms/op
     [java] Iteration   4: 4,527 ms/op
     [java] Iteration   5: 4,491 ms/op
    ImmutableMap:
     [java] Iteration   1: 1,822 ms/op
     [java] Iteration   2: 1,820 ms/op
     [java] Iteration   3: 1,822 ms/op
     [java] Iteration   4: 1,800 ms/op
     [java] Iteration   5: 1,793 ms/op
    ImmutableMap with Long.hashCode and id == nodeId.id instead of Objects.equals;
     [java] Iteration   1: 1,389 ms/op
     [java] Iteration   2: 1,370 ms/op
     [java] Iteration   3: 1,381 ms/op
     [java] Iteration   4: 1,405 ms/op
     [java] Iteration   5: 1,408 ms/op
    HashMap:
     [java] Iteration   1: 0,938 ms/op
     [java] Iteration   2: 0,936 ms/op
     [java] Iteration   3: 0,916 ms/op
     [java] Iteration   4: 0,931 ms/op
     [java] Iteration   5: 0,941 ms/op
    Caching locations:
     [java] Iteration   1: 0,189 ms/op
     [java] Iteration   2: 0,188 ms/op
     [java] Iteration   3: 0,189 ms/op
     [java] Iteration   4: 0,192 ms/op
     [java] Iteration   5: 0,191 ms/op
 */

}
