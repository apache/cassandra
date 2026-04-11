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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.Util;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class SSTableReadingFileBench
{
    static
    {
        DatabaseDescriptor.toolInitialization(!TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.getBoolean());
    }
    @Param("test/data/compaction/oa-70-big-Data.db")
    String sstableFileName;
    private Descriptor desc;
    private SSTableReader ssTableReader;

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
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        ssTableReader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
    }

    @TearDown(Level.Invocation)
    public void closeReader() {
        ssTableReader.ref().close();
    }

    long[] counters = new long[4];
    @Benchmark
    public void countPartitionsAndUnfiltered()
    {
        for (int i = 0; i < counters.length; i++)
        {
            counters[i] = 0;
        }
        final ISSTableScanner currentScanner = ssTableReader.getScanner();
        Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner);
        partitions.forEach(unfilteredRowIterator -> {
            counters[0]++;
            Row staticRow = unfilteredRowIterator.staticRow();
            if (staticRow != null) {
                counters[1]++;
            }
            unfilteredRowIterator.forEachRemaining(unfiltered -> {
                if (unfiltered.isRow()) {
                    counters[2]++;
                }
                else
                {
                    counters[3]++;
                }
            });
        });
    }
}
