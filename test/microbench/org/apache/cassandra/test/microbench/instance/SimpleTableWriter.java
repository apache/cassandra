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

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public abstract class SimpleTableWriter extends CQLTester
{
    static String keyspace;
    String table;
    ColumnFamilyStore cfs;
    Random rand;
    String writeStatement;
    ExecutorService executorService;

    @Param({"1000000"})
    int count = 1_000_000;

    @Param({ "1000" })
    int BATCH = 1_000;

    @Param({ "default" })
    String memtableClass = "default";

    @Param({ "false" })
    boolean useNet = false;

    @Param({ "32" })
    int threadCount;

    public void commonSetup() throws Throwable
    {
        rand = new Random(1);
        executorService = Executors.newFixedThreadPool(threadCount);
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        DatabaseDescriptor.setAutoSnapshot(false);
        System.err.println("setupClass done.");
        String memtableSetup = "";
        if (!memtableClass.isEmpty())
            memtableSetup = String.format(" AND memtable = '%s'", memtableClass);
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace,
                            "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}" +
                            memtableSetup);
        execute("use " + keyspace + ";");
        if (useNet)
        {
            CQLTester.requireNetwork();
            executeNet(getDefaultVersion(), "use " + keyspace + ";");
        }
        writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";
        System.err.println("Prepared, batch " + BATCH + " threads " + threadCount + extraInfo());
        System.err.println("Disk access mode " + DatabaseDescriptor.getDiskAccessMode() +
                           " index " + DatabaseDescriptor.getIndexAccessMode());

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
    }

    abstract Object[] writeArguments(long i);

    public void performWrite(long ofs, int count) throws Throwable
    {
        if (useNet)
        {
            if (threadCount == 1)
                performWriteSerialNet(ofs, count);
            else
                performWriteThreadsNet(ofs, count);
        }
        else
        {
            if (threadCount == 1)
                performWriteSerial(ofs, count);
            else
                performWriteThreads(ofs, count);
        }
    }

    public void performWriteSerial(long ofs, int count) throws Throwable
    {
        for (long i = ofs; i < ofs + count; ++i)
            execute(writeStatement, writeArguments(i));
    }

    public void performWriteThreads(long ofs, int count) throws Throwable
    {
        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < count; ++i)
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
                                                       Throwables.throwIfUnchecked(throwable);
                                                       throw new RuntimeException(throwable);
                                                   }
                                               }));
        }
        int done = 0;
        for (Future<Integer> f : futures)
            done += f.get();
        assert count == done;
    }

    public void performWriteSerialNet(long ofs, int count) throws Throwable
    {
        for (long i = ofs; i < ofs + count; ++i)
            sessionNet().execute(writeStatement, writeArguments(i));
    }

    public void performWriteThreadsNet(long ofs, int count) throws Throwable
    {
        List<Future<Integer>> futures = new ArrayList<>();
        for (long i = 0; i < count; ++i)
        {
            long pos = ofs + i;
            futures.add(executorService.submit(() ->
                                               {
                                                   try
                                                   {
                                                       sessionNet().execute(writeStatement, writeArguments(pos));
                                                       return 1;
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
        assert count == done;
    }

    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException
    {
        executorService.shutdown();
        executorService.awaitTermination(15, TimeUnit.SECONDS);

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
        System.err.format("\n%s in %s mode: %d ops, %s serialized bytes, %s\n",
                          memtable.getClass().getSimpleName(),
                          DatabaseDescriptor.getMemtableAllocationType(),
                          memtable.operationCount(),
                          FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                          usage);

        doExtraChecks();

        // do a flush to print sizes
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        CommitLog.instance.shutdownBlocking();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    abstract void doExtraChecks();
    abstract String extraInfo();
}
