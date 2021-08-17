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


import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 2, timeUnit = TimeUnit.SECONDS)
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

    public enum Execution
    {
        SERIAL,
        SERIAL_NET,
        PARALLEL,
        PARALLEL_NET,
    }

    @Param({"PARALLEL"})
    Execution async = Execution.PARALLEL;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        rand = new Random(1);
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        System.err.println("setupClass done.");
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use "+keyspace+";");
        switch (async)
        {
            case SERIAL_NET:
            case PARALLEL_NET:
                CQLTester.requireNetwork();
                executeNet(getDefaultVersion(), "use " + keyspace + ";");
        }
        String writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        System.err.println("Prepared, batch " + BATCH + " flush " + flush);
        System.err.println("Disk access mode " + DatabaseDescriptor.getDiskAccessMode() + " index " + DatabaseDescriptor.getIndexAccessMode());

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush();

        //Warm up
        System.err.println("Writing " + count);
        long i;
        for (i = 0; i <= count - BATCH; i += BATCH)
            performWrite(writeStatement, i, BATCH);
        if (i < count)
            performWrite(writeStatement, i, count - i);

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        System.err.format("Memtable in %s mode: %d ops, %s serialized bytes, %s (%.0f%%) on heap, %s (%.0f%%) off-heap\n",
                          DatabaseDescriptor.getMemtableAllocationType(),
                          memtable.getOperations(),
                          FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                          FBUtilities.prettyPrintMemory(memtable.getAllocator().onHeap().owns()),
                          100 * memtable.getAllocator().onHeap().ownershipRatio(),
                          FBUtilities.prettyPrintMemory(memtable.getAllocator().offHeap().owns()),
                          100 * memtable.getAllocator().offHeap().ownershipRatio());

        switch (flush)
        {
        case YES:
            cfs.forceBlockingFlush();
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
        for (long i = ofs; i < ofs + count; ++i)
            execute(writeStatement, writeArguments(i));
    }


    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException
    {
        if (flush == Flush.INMEM && !cfs.getLiveSSTables().isEmpty())
            throw new AssertionError("SSTables created for INMEM test.");

        // do a flush to print sizes
        cfs.forceBlockingFlush();

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
        return IntStream.range(0, BATCH)
                        .parallel()
                        .mapToLong(i ->
                                   {
                                       try
                                       {
                                           return execute(readStatement, supplier.get()).size();
                                       }
                                       catch (Throwable throwable)
                                       {
                                           throw Throwables.propagate(throwable);
                                       }
                                   })
                        .sum();
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
        return IntStream.range(0, BATCH)
                        .parallel()
                        .mapToLong(i ->
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
                                   })
                        .sum();
    }


    public Object performRead(String readStatement, Supplier<Object[]> supplier) throws Throwable
    {
        switch (async)
        {
            case SERIAL:
                return performReadSerial(readStatement, supplier);
            case SERIAL_NET:
                return performReadSerialNet(readStatement, supplier);
            case PARALLEL:
                return performReadThreads(readStatement, supplier);
            case PARALLEL_NET:
                return performReadThreadsNet(readStatement, supplier);
        }
        return null;
    }
}
