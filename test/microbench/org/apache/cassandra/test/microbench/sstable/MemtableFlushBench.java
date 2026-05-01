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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.File;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class MemtableFlushBench extends SSTableAbstractBench
{

    @Param("10")
    private int preFillSStableCount;

    @Param("10")
    private int flushingSStableCount;

    @Param("1")
    private int rowCount;

    @Param("200000")
    private int extraFileCount;

    @Param("false")
    private boolean skipCleanup;

    protected void setupData()
    {
        super.setupData();
        // Input some mostly empty flushes
        for (int j = 0; j < preFillSStableCount; j++)
        {
            for (long i = 0; i < rowCount; i++)
                insertForIndex(writeStatement, i);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        // Inflate directory entry count with dummy files to simulate
        // a directory with many more SSTables than we actually flushed
        File dataDir = cfs.getDirectories().getDirectoryForNewSSTables();
        for (int i = 0; i < extraFileCount; i++)
            new File(dataDir, "dummy_" + i + ".db").createFileIfNotExists();
    }

    @Benchmark
    public void doFlushing()
    {
        for (int j = 0; j < flushingSStableCount; j++)
        {
            for (long i = 0; i < rowCount; i++)
                insertForIndex(writeStatement, i);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        if (!skipCleanup)
            super.teardown();
    }
}
