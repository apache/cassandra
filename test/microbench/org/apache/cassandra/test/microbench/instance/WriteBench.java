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

package org.apache.cassandra.test.microbench.instance;


import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class WriteBench extends SimpleTableWriter
{

    public enum EndOp
    {
        INMEM, TRUNCATE, FLUSH
    }

    @Param({"INMEM", "TRUNCATE", "FLUSH"})
    EndOp flush = EndOp.INMEM;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        super.commonSetup();
    }

    @Benchmark
    public void writeTable() throws Throwable
    {
        long i;
        for (i = 0; i <= count - BATCH; i += BATCH)
            performWrite(i, BATCH);
        if (i < count)
            performWrite(i, Math.toIntExact(count - i));

        switch (flush)
        {
        case FLUSH:
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            // if we flush we also must truncate to avoid accummulating sstables
        case TRUNCATE:
            execute("TRUNCATE TABLE " + table);
            // note: we turn snapshotting and durable writes (which would have caused a flush) off for this benchmark
            break;
        case INMEM:
            if (!cfs.getLiveSSTables().isEmpty())
                throw new AssertionError("SSTables created for INMEM test.");
            // leave unflushed, i.e. next iteration will overwrite data
        default:
        }
    }

    public Object[] writeArguments(long i)
    {
        return new Object[] { i, i, i };
    }

    void doExtraChecks()
    {
        if (flush == WriteBench.EndOp.INMEM && !cfs.getLiveSSTables().isEmpty())
            throw new AssertionError("SSTables created for INMEM test.");
    }

    String extraInfo()
    {
        return " flush " + flush;
    }
}
