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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.File;

/**
 * Measures the cost of flushing into a data directory that already holds a large number of files, which is where
 * listing the directory once per new sstable writer used to dominate, see CASSANDRA-21345.
 */
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
    private int preFillSSTableCount;

    @Param("10")
    private int flushingSSTableCount;

    // note: named to avoid colliding with SSTableAbstractBench.rowCount, which is a @Param of its own
    @Param("1")
    private int rowsPerFlush;

    @Param("200000")
    private int extraFileCount;

    @Param("false")
    private boolean skipCleanup;

    @Override
    protected void setupData()
    {
        super.setupData();
        // Input some mostly empty flushes
        for (int j = 0; j < preFillSSTableCount; j++)
        {
            for (long i = 0; i < rowsPerFlush; i++)
                insertForIndex(writeStatement, i);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        // Inflate directory entry count with dummy files to simulate a directory with many more sstables than we
        // actually flushed. Deliberately not named like sstable components, so that anything scanning the data
        // directory does not try to interpret them.
        File dataDir = cfs.getDirectories().getDirectoryForNewSSTables();
        for (int i = 0; i < extraFileCount; i++)
            new File(dataDir, "benchpad_" + i).createFileIfNotExists();
    }

    /**
     * Note that the sstables flushed here are left behind, so the directory grows over the course of a trial. That
     * drift is deliberately not corrected for: it is a handful of files per invocation against {@code extraFileCount}
     * entries, and re-creating the padding per iteration would cost far more than it buys.
     */
    @Benchmark
    public void doFlushing()
    {
        for (int j = 0; j < flushingSSTableCount; j++)
        {
            for (long i = 0; i < rowsPerFlush; i++)
                insertForIndex(writeStatement, i);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }

    @Override
    protected boolean shouldSkipCleanup()
    {
        return skipCleanup;
    }
}
