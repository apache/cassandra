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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Measures {@link ClusteringPrefix.Deserializer} with the clustering-type list implementations produced by the
 * memtable and SSTable paths. Before CASSANDRA-21587, adding the SSTable path introduces {@code ArrayList}; after
 * the fix, it shares the multi-column {@code ImmutableList} implementation used by the memtable path. Every
 * benchmark operation reads one non-empty component so that the header sources perform the same work.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3,
      jvmArgsAppend = { "-Xms1G", "-Xmx1G", "-Djmh.executor=CUSTOM",
                         "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(1)
@State(Scope.Thread)
public class ClusteringPrefixDeserializerBench
{
    @Param({ "1", "2", "3" })
    public int headerSources;

    private DeserializerState[] states;
    private int index;

    @Setup
    public void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata oneColumn = schema("one_column", 1);
        TableMetadata twoColumns = schema("two_columns", 2);
        SerializationHeader oneColumnHeader = SerializationHeader.makeWithoutStats(oneColumn);
        SerializationHeader twoColumnHeader = SerializationHeader.makeWithoutStats(twoColumns);
        SerializationHeader sstableHeader = deserializeHeader(twoColumnHeader, twoColumns);

        DeserializerState twoColumn = state(twoColumns.comparator, twoColumnHeader);
        DeserializerState singleColumn = state(oneColumn.comparator, oneColumnHeader);
        DeserializerState sstable = state(twoColumns.comparator, sstableHeader);

        switch (headerSources)
        {
            case 1:
                states = new DeserializerState[] { twoColumn };
                break;
            case 2:
                states = new DeserializerState[] { twoColumn, singleColumn };
                break;
            case 3:
                states = new DeserializerState[] { twoColumn, singleColumn, sstable };
                break;
            default:
                throw new AssertionError("Unsupported number of header sources: " + headerSources);
        }
    }

    /**
     * Rotates through {@link #states} with a conditional reset rather than {@code index++ % states.length}: a modulo
     * by a non-constant divisor costs an integer division (tens of cycles) on an operation that only takes a few tens
     * of nanoseconds, and {@code index++} eventually overflows negative. The reset keeps the exact 1/2/3 mix of header
     * sources, which is what shapes the call-site profile being measured.
     */
    @Benchmark
    public ClusteringBoundOrBoundary<byte[]> deserializeBound() throws IOException
    {
        int i = index;
        DeserializerState state = states[i];
        index = ++i == states.length ? 0 : i;
        state.in.buffer().rewind();
        state.deserializer.prepare(UnfilteredSerializer.IS_MARKER, 0);
        return state.deserializer.deserializeNextBound();
    }

    private static TableMetadata schema(String name, int clusteringColumns)
    {
        TableMetadata.Builder builder = TableMetadata.builder("ClusteringPrefixDeserializerBench", name)
                                                     .addPartitionKeyColumn("k", Int32Type.instance);
        for (int i = 0; i < clusteringColumns; i++)
            builder.addClusteringColumn("c" + i, Int32Type.instance);
        return builder.build();
    }

    private static SerializationHeader deserializeHeader(SerializationHeader header, TableMetadata metadata)
    throws Exception
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            SerializationHeader.serializer.serialize(format.getLatestVersion(), header.toComponent(), out);
            SerializationHeader.Component component = SerializationHeader.serializer.deserialize(format.getLatestVersion(),
                                                                                                   new DataInputBuffer(out.buffer(), true));
            return component.toHeader(metadata);
        }
    }

    private static DeserializerState state(ClusteringComparator comparator, SerializationHeader header)
    throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeByte(ClusteringPrefix.Kind.INCL_START_BOUND.ordinal());
            out.writeUnsignedShort(1);
            out.writeUnsignedVInt32(0);
            header.clusteringTypes()[0].writeValue(ByteBufferUtil.bytes(0), out);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            return new DeserializerState(in, new ClusteringPrefix.Deserializer(comparator, in, header));
        }
    }

    private static class DeserializerState
    {
        final DataInputBuffer in;
        final ClusteringPrefix.Deserializer deserializer;

        private DeserializerState(DataInputBuffer in, ClusteringPrefix.Deserializer deserializer)
        {
            this.in = in;
            this.deserializer = deserializer;
        }
    }
}
