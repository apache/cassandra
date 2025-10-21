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
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
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

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
public class ReplicaGroupsBench
{
    static Token [] queryTokens = new Token[5000];
    static Random random = new Random(1);
    static ReplicaGroups replicaGroups;
/*
new: ReplicaGroupBench.bench  avgt    5  0,317 ± 0,037  ms/op

old: ReplicaGroupBench.bench  avgt    5  10,187 ± 0,040  ms/op

 */
    @Setup(Level.Trial)
    public void setup() throws UnknownHostException
    {
        int nodecount = 1000;
        DatabaseDescriptor.daemonInitialization();
        for (int i = 0; i < 5000; i++)
            queryTokens[i] = Murmur3Partitioner.instance.getRandomToken(random);
        Keyspaces keyspaces = fakeKeyspaces(6);
        ReplicationParams params = keyspaces.get("pfrbench").get().params.replication;
        replicaGroups = new UniformRangePlacement().calculatePlacements(Epoch.FIRST, fakeMetadata(nodecount), keyspaces).get(params).reads;
    }

    @Benchmark
    public void bench()
    {
        for (Token t : queryTokens)
            replicaGroups.forRange(t);
    }

    public ClusterMetadata fakeMetadata(int nodeCount) throws UnknownHostException
    {
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance);
        TokenSupplier tokensupplier = TokenSupplier.evenlyDistributedTokens(nodeCount);
        PlacementProvider placementProvider = new UniformRangePlacement();
        for (int i = 1; i < nodeCount; i++)
        {
            ClusterMetadata.Transformer transformer = metadata.transformer();
            UUID uuid = UUID.randomUUID();
            NodeAddresses addresses = addresses(uuid, i);
            metadata = transformer.register(addresses, new Location("dc1", "rack1"), NodeVersion.CURRENT).build().metadata;
            NodeId nodeId = metadata.directory.peerId(addresses.broadcastAddress);
            metadata = new UnsafeJoin(nodeId, Collections.singleton(new Murmur3Partitioner.LongToken(tokensupplier.token(i))), placementProvider).execute(metadata).success().metadata;
        }

        return metadata;
    }

    NodeAddresses addresses(UUID uuid, int idx) throws UnknownHostException
    {
        byte [] address = new byte [] {127, 0,
                                       (byte) (((idx + 1) & 0x0000ff00) >> 8),
                                       (byte) ((idx + 1) & 0x000000ff)};

        InetAddressAndPort host = InetAddressAndPort.getByAddress(address);
        return new NodeAddresses(uuid, host, host, host);
    }

    public Keyspaces fakeKeyspaces(int rf)
    {
        KeyspaceMetadata metadata = KeyspaceMetadata.create("pfrbench", KeyspaceParams.simple(rf));
        return Keyspaces.of(metadata);
    }
    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                          .include(ReplicaGroupsBench.class.getSimpleName())
                          .build();
        new Runner(options).run();
    }
}
