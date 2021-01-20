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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public abstract class ReadTest extends CQLTester
{
    static String keyspace;
    String table;
    ColumnFamilyStore cfs;
    Random rand;

    @Param({"1000"})
    int BATCH = 1_000;

    public enum Flush
    {
        INMEM, NO, YES
    }

    @Param({"1000000"})
    int count = 1_000_000;

    @Param({"INMEM", "YES"})
    Flush flush = Flush.INMEM;

    @Param({""})
    String memtableClass = "";

    @Param({"false"})
    boolean useNet = false;

    @Param({"1"})
    int threadCount = 1;

    ExecutorService executorService;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        rand = new Random(1);
        executorService = Executors.newFixedThreadPool(threadCount);
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        DatabaseDescriptor.setAutoSnapshot(false);
        System.err.println("setupClass done.");
        String memtableSetup = "";
        if (!memtableClass.isEmpty())
            memtableSetup = String.format(" AND memtable = { 'class': '%s' }", memtableClass);
        keyspace = createKeyspace(
        "CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace,
                            "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}" +
                            memtableSetup);
        execute("use " + keyspace + ";");
        if (useNet)
        {
            CQLTester.requireNetwork();
            executeNet(getDefaultVersion(), "use " + keyspace + ";");
        }
        String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";
        System.err.println("Prepared, batch " + BATCH + " threads " + threadCount + " flush " + flush);
        System.err.println("Disk access mode " + DatabaseDescriptor.getDiskAccessMode() +
                           " index " + DatabaseDescriptor.getIndexAccessMode());

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        //Warm up
        long writeStart = System.currentTimeMillis();
        System.err.println("Writing " + count);
        long i;
        for (i = 0; i <= count - BATCH; i += BATCH)
            performWrite(writeStatement, i, BATCH);
        if (i < count)
            performWrite(writeStatement, i, count - i);
        long writeLength = System.currentTimeMillis() - writeStart;
        System.err.format("... done in %.3f s.\n", writeLength / 1000.0);

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
        System.err.format("%s in %s mode: %d ops, %s serialized bytes, %s\n",
                          memtable.getClass().getSimpleName(),
                          DatabaseDescriptor.getMemtableAllocationType(),
                          memtable.getOperations(),
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

        // Needed to stabilize sstable count for off-cache sized tests (e.g. count = 100_000_000)
        while (cfs.getLiveSSTables().size() >= 15)
        {
            cfs.enableAutoCompaction(true);
            cfs.disableAutoCompaction();
        }
    }

    abstract Object[] writeArguments(long i);

    public void performWrite(String writeStatement, long ofs, long count) throws Throwable
    {
        if (threadCount == 1)
            performWriteSerial(writeStatement, ofs, count);
        else
            performWriteThreads(writeStatement, ofs, count);
    }

    public void performWriteSerial(String writeStatement, long ofs, long count) throws Throwable
    {
        for (long i = ofs; i < ofs + count; ++i)
            execute(writeStatement, writeArguments(i));
    }

    public void performWriteThreads(String writeStatement, long ofs, long count) throws Throwable
    {
        List<Future<Integer>> futures = new ArrayList<>();
        for (long i = 0; i < count; ++i)
        {
            long pos = ofs + i;
            futures.add(executorService.submit(() ->
                                               {
                                                   try
                                                   {
                                                       execute(writeStatement, writeArguments(pos));
                                                       return 1;
                                                   }
                                                   catch (Throwable throwable)
                                                   {
                                                       throw Throwables.propagate(throwable);
                                                   }
                                               }));
        }
        long done = 0;
        for (Future<Integer> f : futures)
            done += f.get();
        assert count == done;
    }

    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException
    {
        if (flush == Flush.INMEM && !cfs.getLiveSSTables().isEmpty())
            throw new AssertionError("SSTables created for INMEM test.");

        executorService.shutdown();
        executorService.awaitTermination(15, TimeUnit.SECONDS);

        // do a flush to print sizes
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        CommitLog.instance.shutdownBlocking();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
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
                                                       throw Throwables.propagate(throwable);
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
                                                       throw Throwables.propagate(throwable);
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
}
