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

package org.apache.cassandra.test.microbench.sai;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.v1.WidePrimaryKeyMap;
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

import static org.apache.cassandra.index.sai.SAITester.getRandom;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class KeyLookupBench
{
    private static final int rows = 1_000_000;

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

    @Param({"3", "4", "5"})
    public int partitionBlockShift;

    @Param({"3", "4", "5"})
    public int clusteringBlockShift;

    @Param({"10", "100", "1000", "10000"})
    public int partitionSize;

    @Param({"true", "false"})
    public boolean randomClustering;

    @Setup(Level.Trial)
    public void trialSetup() throws Exception
    {
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

        indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator);

        CassandraRelevantProperties.SAI_SORTED_TERMS_PARTITION_BLOCK_SHIFT.setInt(partitionBlockShift);
        CassandraRelevantProperties.SAI_SORTED_TERMS_CLUSTERING_BLOCK_SHIFT.setInt(clusteringBlockShift);
        SSTableComponentsWriter writer = new SSTableComponentsWriter(indexDescriptor);

        PrimaryKey.Factory factory = new PrimaryKey.Factory(metadata.partitioner, metadata.comparator);

        PrimaryKey[] primaryKeys = new PrimaryKey[rows];
        int partition = 0;
        int partitionRowCounter = 0;
        for (int index = 0; index < rows; index++)
        {
            primaryKeys[index] = factory.create(makeKey(metadata, (long) partition, (long) partition), makeClustering(metadata));
            partitionRowCounter++;
            if (partitionRowCounter == partitionSize)
            {
                partition++;
                partitionRowCounter = 0;
            }
        }

        Arrays.sort(primaryKeys);

        DecoratedKey lastKey = null;
        for (PrimaryKey primaryKey : primaryKeys)
        {
            if (lastKey == null || lastKey.compareTo(primaryKey.partitionKey()) < 0)
            {
                lastKey = primaryKey.partitionKey();
                writer.startPartition(lastKey);
            }
            writer.nextRow(primaryKey);
        }

        writer.complete();

        SSTableReader sstableReader = mock(SSTableReader.class);
        when(sstableReader.metadata()).thenReturn(metadata);

        PrimaryKeyMap.Factory mapFactory = new WidePrimaryKeyMap.Factory(indexDescriptor, sstableReader);

        primaryKeyMap = mapFactory.newPerSSTablePrimaryKeyMap();

        primaryKey = primaryKeys[500000];
    }

    @Benchmark
    public long advanceToKey()
    {
        return primaryKeyMap.rowIdFromPrimaryKey(primaryKey);
    }

    private static DecoratedKey makeKey(TableMetadata table, Object...partitionKeys)
    {
        ByteBuffer key;
        if (table.partitionKeyType instanceof CompositeType)
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString((String)partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    private Clustering<?> makeClustering(TableMetadata table)
    {
        Clustering<?> clustering;
        if (table.comparator.size() == 0)
            clustering = Clustering.EMPTY;
        else
        {
            ByteBuffer[] values = new ByteBuffer[table.comparator.size()];
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(makeClusteringString());
            clustering = Clustering.make(values);
        }
        return clustering;
    }

    private String makeClusteringString()
    {
        if (randomClustering)
            return getRandom().nextTextString(10, 100);
        else
            return String.format("%08d", getRandom().nextIntBetween(0, partitionSize));
    }
}
