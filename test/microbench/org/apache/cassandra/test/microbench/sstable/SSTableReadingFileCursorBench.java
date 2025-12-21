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

package org.apache.cassandra.test.microbench.sstable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class SSTableReadingFileCursorBench
{
    static
    {
        if(!DatabaseDescriptor.isDaemonInitialized())
            DatabaseDescriptor.toolInitialization(!TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.getBoolean());
    }

    @Param("test/data/compaction/oa-70-big-Data.db")
    String sstableFileName;
    private Descriptor desc;
    private SSTableCursorReader cursor;

    @Setup(Level.Trial)
    public void loadDescriptor() throws FileNotFoundException
    {
        File ssTableFile = new File(sstableFileName);

        if (!ssTableFile.exists())
        {
            throw new FileNotFoundException("Cannot find file " + ssTableFile.absolutePath());
        }
        desc = Descriptor.fromFileWithComponent(ssTableFile, false).left;
    }

    @Setup(Level.Invocation)
    public void prepareReader() throws IOException
    {
        cursor = SSTableCursorReader.fromDescriptor(desc);
    }

    @TearDown(Level.Invocation)
    public void closeReader() throws Exception
    {
        cursor.close();
    }

    long[] counters = new long[4];

    @Benchmark
    public void readPartitionAndUnfiltered() throws IOException
    {
        readPartitionAndUnfiltered(counters, cursor);
    }

    static void readPartitionAndUnfiltered(long[] counters, SSTableCursorReader cursor) throws IOException
    {
        Arrays.fill(counters, 0);
        int state = PARTITION_START;
        PartitionDescriptor pHeader = new PartitionDescriptor(cursor.ssTableReader().getPartitioner().createReusableKey(0));
        UnfilteredDescriptor rHeader = new UnfilteredDescriptor(cursor.ssTableReader().header.clusteringTypes().toArray(AbstractType[]::new));
        while (state != DONE) {
            state = cursor.readPartitionHeader(pHeader);
            counters[0]++;
            state = readThroughPartition(counters, cursor, state, rHeader);
        }
    }

    private static int readThroughPartition(long[] counters, SSTableCursorReader cursor, int state, UnfilteredDescriptor unfilteredDescriptor) throws IOException
    {
        while (state != PARTITION_END) {
            switch (state) {
                case STATIC_ROW_START:
                    counters[1]++;
                    state = readThroughStaticRow(cursor, unfilteredDescriptor);
                    break;
                case ROW_START:
                    counters[2]++;
                    state = readThroughRow(cursor, unfilteredDescriptor);
                    break;
                case TOMBSTONE_START:
                    counters[3]++;
                    state = cursor.readTombstoneMarker(unfilteredDescriptor);
                    break;
            }
        }
        return cursor.continueReading();
    }

    static int readThroughStaticRow(SSTableCursorReader cursor, UnfilteredDescriptor unfilteredDescriptor) throws IOException
    {
        int state = cursor.readStaticRowHeader(unfilteredDescriptor);
        while (state != UNFILTERED_END) {
            state = readThroughCell(cursor);
        }
        return cursor.continueReading();
    }

    static int readThroughRow(SSTableCursorReader cursor, UnfilteredDescriptor unfilteredDescriptor) throws IOException
    {
        int state = cursor.readRowHeader(unfilteredDescriptor);
        while (state != UNFILTERED_END) {
            state = readThroughCell(cursor);
        }
        return cursor.continueReading();
    }

    private static int readThroughCell(SSTableCursorReader cursor) throws IOException
    {
        int state = cursor.readCellHeader();
        if (state == CELL_VALUE_START)
        {
            state = cursor.skipCellValue();
        }
        if (state == CELL_END)
            state = cursor.continueReading();
        return state;
    }

    @Benchmark
    public void readPartitionSkipUnfiltered() throws IOException
    {
        readPartitionSkipUnfiltered(counters, cursor);
    }

    static void readPartitionSkipUnfiltered(long[] counters, SSTableCursorReader cursor) throws IOException
    {
        for (int i = 0; i < counters.length; i++)
        {
            counters[i] = 0;
        }
        int state = PARTITION_START;
        PartitionDescriptor pHeader = new PartitionDescriptor(cursor.ssTableReader().getPartitioner().createReusableKey(0));
        while (state != DONE) {
            state = cursor.readPartitionHeader(pHeader);
            counters[0]++;
            if (state == PARTITION_END) continue;
            while (state != DONE && state != PARTITION_START) {
                switch (state) {
                    case STATIC_ROW_START:
                        counters[1]++;
                        state = cursor.skipStaticRow(true);
                        break;
                    case ROW_START:
                        counters[2]++;
                        state = cursor.skipUnfiltered(true);
                        break;
                    case TOMBSTONE_START:
                        counters[3]++;
                        state = cursor.skipUnfiltered(true);
                        break;
                }
            }
        }
    }

    @Benchmark
    public void skipPartition() throws IOException
    {
        skipPartition(counters, cursor);
    }

    static void skipPartition(long[] counters, SSTableCursorReader cursor) throws IOException
    {
        for (int i = 0; i < counters.length; i++)
        {
            counters[i] = 0;
        }
        while (cursor.skipPartition() != DONE) {
            counters[0]++;
        }
    }
}
