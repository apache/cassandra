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

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.OwnershipUtils;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.utils.FBUtilities;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G"})
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
public class LocalRangesAllSettledBench
{
    static ClusterMetadata metadata;
    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.syncInstanceForTest());
        metadata = metadata();
    }

    @Benchmark
    public void benchLocalRangesOnlyWithRelevantMSOs()
    {
        Map<KeyspaceMetadata, RangesAtEndpoint> settledByKeyspace = new HashMap<>();
        metadata.unsafeClearLocalRangesAllSettled();
        // This peer is involved in a MOVE operation
        InetAddressAndPort local = InetAddressAndPort.getByNameUnchecked("10.10.9.129:7000");
        FBUtilities.setBroadcastInetAddressAndPort(local);
        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
             settledByKeyspace.put(ksm, metadata.localWriteRangesAllSettled(ksm));
    }

    @Benchmark
    public void benchLocalRangesOnlyNoRelevantMSOs()
    {
        Map<KeyspaceMetadata, RangesAtEndpoint> settledByKeyspace = new HashMap<>();
        metadata.unsafeClearLocalRangesAllSettled();
        // This peer has no involvement in any in-flight MSOs
        InetAddressAndPort local = InetAddressAndPort.getByNameUnchecked("10.10.14.13:7000");
        FBUtilities.setBroadcastInetAddressAndPort(local);
        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
            settledByKeyspace.put(ksm, metadata.localWriteRangesAllSettled(ksm));
    }

    @Benchmark
    public void benchPlacementsAllSettled()
    {
        // Emulates the previous implementation of ClusterMetadata::writePlacementsAllSettled
        // which would be lazily computed during on first access.
        // As this fully applies all in-flight MSOs to derive the final settled placements,
        // the local broadcast address is not significant.
        DataPlacements placementAllSettled = OwnershipUtils.placementsAllSettled(metadata);
        Map<KeyspaceMetadata, RangesAtEndpoint> settledByKeyspace = new HashMap<>();
        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
        {
            DataPlacement placement = placementAllSettled.get(ksm.params.replication);
            settledByKeyspace.put(ksm, placement.writes.byEndpoint().get(FBUtilities.getBroadcastAddressAndPort()));
        }
    }

    public ClusterMetadata metadata() throws Exception
    {
        Path p = Path.of(this.getClass().getClassLoader().getResource("cluster_metadata/CASSANDRA-21144_clustermetadata.gz").toURI());
        try (DataInputStreamPlus in = Util.DataInputStreamPlusImpl.wrap(new GZIPInputStream(new FileInputStream(p.toFile()))))
        {
            ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            return metadata;
        }
    }

    public static void main(String[] args) throws Exception
    {
        Options options = new OptionsBuilder()
                          .include(LocalRangesAllSettledBench.class.getSimpleName())
                          .build();
        new Runner(options).run();
    }
/*
$ ant microbench -Dbenchmark.name=LocalRangesAllSettledBench

     [java] Benchmark                                                        Mode  Cnt      Score     Error  Units
     [java] LocalRangesAllSettledBench.benchLocalRangesOnlyNoRelevantMSOs    avgt    5     18.214 ±   4.350  ms/op
     [java] LocalRangesAllSettledBench.benchLocalRangesOnlyWithRelevantMSOs  avgt    5    274.931 ±  14.193  ms/op
     [java] LocalRangesAllSettledBench.benchPlacementsAllSettled             avgt    5  11465.778 ± 370.754  ms/op

 */
}
