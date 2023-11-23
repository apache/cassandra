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


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public abstract class ReadBenchBase extends SimpleTableWriter
{
    public enum Flush
    {
        INMEM, NO, YES
    }

    @Param({"INMEM", "YES"})
    Flush flush = Flush.INMEM;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        super.commonSetup();

        // Write the data we are going to read.
        long writeStart = System.currentTimeMillis();
        System.err.println("Writing " + count);
        long i;
        for (i = 0; i <= count - BATCH; i += BATCH)
            performWrite(i, BATCH);
        if (i < count)
            performWrite(i, (int) (count - i));
        long writeLength = System.currentTimeMillis() - writeStart;
        System.err.format("... done in %.3f s.\n", writeLength / 1000.0);

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
        System.err.format("%s in %s mode: %d ops, %s serialized bytes, %s\n",
                          memtable.getClass().getSimpleName(),
                          DatabaseDescriptor.getMemtableAllocationType(),
                          memtable.operationCount(),
                          FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                          usage);

        switch (flush)
        {
        case YES:
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            break;
        case INMEM:
            if (!cfs.getLiveSSTables().isEmpty())
                throw new AssertionError("SSTables created for INMEM test.");
        default:
            // don't flush
        }

        System.err.format("%s sstables, total %s, %,d partitions. Mean write latency %.2f ms\n",
                          cfs.getLiveSSTables().size(),
                          FBUtilities.prettyPrintMemory(cfs.metric.liveDiskSpaceUsed.getCount()),
                          cfs.metric.estimatedPartitionCount.getValue(),
                          cfs.metric.writeLatency.latency.getSnapshot().getMean());
        // Needed to stabilize sstable count for off-cache sized tests (e.g. count = 100_000_000)
        while (cfs.getLiveSSTables().size() >= 15)
        {
            cfs.enableAutoCompaction(true);
            cfs.disableAutoCompaction();
            System.err.format("%s sstables, total %s, %,d partitions. Mean write latency %.2f ms\n",
                              cfs.getLiveSSTables().size(),
                              FBUtilities.prettyPrintMemory(cfs.metric.liveDiskSpaceUsed.getCount()),
                              cfs.metric.estimatedPartitionCount.getValue(),
                              cfs.metric.writeLatency.latency.getSnapshot().getMean());
        }
    }

    public Object performReadSerial(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        long sum = 0;
        for (int i = 0; i < BATCH; ++i)
            sum += execute(readStatement, supplier.get()).size();
        return sum;
    }

    public Object performReadThreads(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        List<Future<Integer>> futures = new ArrayList<>();
        for (long i = 0; i < BATCH; ++i)
        {
            futures.add(executorService.submit(() ->
                                               {
                                                   try
                                                   {
                                                       return execute(readStatement, supplier.get()).size();
                                                   }
                                                   catch (Throwable throwable)
                                                   {
                                                       Throwables.throwIfUnchecked(throwable);
                                                       throw new RuntimeException(throwable);
                                                   }
                                               }));
        }
        long done = 0;
        for (Future<Integer> f : futures)
            done += f.get();
        return done;
    }

    public Object performReadSerialNet(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        long sum = 0;
        for (int i = 0; i < BATCH; ++i)
            sum += executeNet(getDefaultVersion(), readStatement, supplier.get())
                           .getAvailableWithoutFetching();
        return sum;
    }

    public long performReadThreadsNet(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        List<Future<Integer>> futures = new ArrayList<>();
        for (long i = 0; i < BATCH; ++i)
        {
            futures.add(executorService.submit(() ->
                                               {
                                                   try
                                                   {
                                                       return executeNet(getDefaultVersion(), readStatement, supplier.get())
                                                              .getAvailableWithoutFetching();
                                                   }
                                                   catch (Throwable throwable)
                                                   {
                                                       Throwables.throwIfUnchecked(throwable);
                                                       throw new RuntimeException(throwable);
                                                   }
                                               }));
        }
        long done = 0;
        for (Future<Integer> f : futures)
            done += f.get();
        return done;
    }


    public Object performRead(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        if (useNet)
        {
            if (threadCount == 1)
                return performReadSerialNet(readStatement, supplier);
            else
                return performReadThreadsNet(readStatement, supplier);
        }
        else
        {
            if (threadCount == 1)
                return performReadSerial(readStatement, supplier);
            else
                return performReadThreads(readStatement, supplier);
        }
    }

    void doExtraChecks()
    {
        if (flush == Flush.INMEM && !cfs.getLiveSSTables().isEmpty())
            throw new AssertionError("SSTables created for INMEM test.");
    }

    String extraInfo()
    {
        return " flush " + flush;
    }
}
