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

package org.apache.cassandra.test.microbench.index.sai.v9.keystore;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.junit.Assert;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v9.V9RowAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.disk.v9.V9SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.v9.WidePrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static org.apache.cassandra.Util.makeKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@BenchmarkMode({ Mode.Throughput })
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = {
"-Xmx512M",
"--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
"--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED"
})
@Threads(1)
@State(Scope.Benchmark)
public class KeyLookupBench
{
    private static final int rows = 1_000_000;
    private static final CQLTester.Randomization random = new CQLTester.Randomization();

    static
    {
        DatabaseDescriptor.toolInitialization();
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    protected TableMetadata metadata;
    protected IndexDescriptor indexDescriptor;
    private PrimaryKeyMap primaryKeyMap;
    private PrimaryKey primaryKey;

    @Param({ "3", "4", "5" })
    public int partitionBlockShift;
    @Param({ "3", "4", "5" })
    public int clusteringBlockShift;
    @Param({ "10", "100", "1000", "10000" })
    public int partitionSize;
    @Param({ "true", "false" })
    public boolean randomClustering;

    @Setup(Level.Trial)
    public void trialSetup() throws Exception
    {
        Version version = Version.LATEST.onOrAfter(Version.GA) ? Version.LATEST : Version.GA;
        SAIUtil.setCurrentVersion(version);
        String keyspaceName = "ks";
        String tableName = this.getClass().getSimpleName();
        metadata = TableMetadata
                   .builder(keyspaceName, tableName)
                   .partitioner(Murmur3Partitioner.instance)
                   .addPartitionKeyColumn("pk1", LongType.instance)
                   .addPartitionKeyColumn("pk2", LongType.instance)
                   .addClusteringColumn("ck1", UTF8Type.instance)
                   .addClusteringColumn("ck2", UTF8Type.instance)
                   .build();

        Descriptor descriptor = new Descriptor(new File(Files.createTempDirectory("jmh").toFile()),
                                               metadata.keyspace,
                                               metadata.name,
                                               Util.newUUIDGen().get());

        indexDescriptor = IndexDescriptor.empty(descriptor, metadata.comparator);

        V9SSTableComponentsWriter.setPartitionBlockShift(partitionBlockShift);
        V9SSTableComponentsWriter.setClusteringBlockShift(clusteringBlockShift);
        Assert.assertTrue("Version must be at least GA", Version.current(keyspaceName).onOrAfter(Version.GA));
        PerSSTableWriter writer = Version.current(keyspaceName).onDiskFormat().newPerSSTableWriter(indexDescriptor);
        V9RowAwarePrimaryKeyFactory factory = new V9RowAwarePrimaryKeyFactory(metadata.comparator);

        PrimaryKey[] primaryKeys = generatePrimaryKeys(factory);
        Arrays.sort(primaryKeys);

        DecoratedKey lastKey = null;
        for (PrimaryKey primaryKey : primaryKeys)
        {
            if (lastKey == null || lastKey.compareTo(primaryKey.partitionKey()) != 0)
            {
                lastKey = primaryKey.partitionKey();
                writer.startPartition(lastKey, -1);
            }
            writer.nextRow(primaryKey);
        }

        writer.complete(Stopwatch.createStarted());

        SSTableReader sstableReader = mock(SSTableReader.class);
        when(sstableReader.metadata()).thenReturn(metadata);

        PrimaryKeyMap.Factory mapFactory = new WidePrimaryKeyMap.Factory(indexDescriptor.perSSTableComponents(), factory, sstableReader);

        primaryKeyMap = mapFactory.newPerSSTablePrimaryKeyMap();

        primaryKey = primaryKeys[rows / 2];
    }

    @Benchmark
    public long advanceToKey()
    {
        return primaryKeyMap.exactRowIdOrInvertedCeiling(primaryKey);
    }

    private PrimaryKey[] generatePrimaryKeys(V9RowAwarePrimaryKeyFactory factory)
    {
        PrimaryKey[] primaryKeys = new PrimaryKey[rows];
        int partition = 0;
        int partitionRowCounter = 0;
        ClusteringStringGenerator clusteringStringGenerator = new ClusteringStringGenerator(metadata);
        for (int index = 0; index < rows; index++)
        {
            primaryKeys[index] = factory.create(makeKey(metadata, (long) partition, (long) partition),
                                                clusteringStringGenerator.nextClustering());
            partitionRowCounter++;
            if (partitionRowCounter == partitionSize)
            {
                partition++;
                partitionRowCounter = 0;
                clusteringStringGenerator = new ClusteringStringGenerator(metadata);
            }
        }
        return primaryKeys;
    }

    /**
     * Generates a sorted list of unique clustering strings during initialization,
     * then returns them in lexicographical order on each call to nextClustering().
     */
    private class ClusteringStringGenerator
    {
        private final List<String> sortedStrings;
        private final TableMetadata table;
        private int currentIndex;

        ClusteringStringGenerator(TableMetadata table)
        {
            // Use TreeSet to maintain sorted order and ensure uniqueness
            TreeSet<String> uniqueStrings = new TreeSet<>();
            this.table = table;
            this.currentIndex = 0;

            sortedStrings = generateUniqueSortedStrings(uniqueStrings);
        }

        private List<String> generateUniqueSortedStrings(TreeSet<String> uniqueStrings)
        {
            while (uniqueStrings.size() < partitionSize)
            {
                String candidate = makeClusteringString();
                uniqueStrings.add(candidate);
            }
            return new ArrayList<>(uniqueStrings);
        }

        private String makeClusteringString()
        {
            if (randomClustering)
                return random.nextTextString(10, 100);
            else
                return String.format("%08d", random.nextIntBetween(0, partitionSize));
        }

        Clustering<?> nextClustering()
        {
            if (!table.hasClustering())
                return Clustering.EMPTY;

            ByteBuffer[] values = new ByteBuffer[table.comparator.size()];
            String nextString = sortedStrings.get(currentIndex++);
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(nextString);
            return Clustering.make(values);
        }
    }
}
