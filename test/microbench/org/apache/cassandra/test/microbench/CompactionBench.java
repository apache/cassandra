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
import java.util.List;
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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.tcm.ClusterMetadataService;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 25, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CompactionBench extends CQLTester
{
    protected static String keyspace;
    protected String table;
    protected String writeStatement;
    protected ColumnFamilyStore cfs;
    protected List<File> snapshotFiles;

    @Param("2")
    protected int sstableCount = 2;

    @Param("50000")
    protected int rowCount = 50000;

    @Param("NONE")
    protected String overlap = "NONE";

    @Param("true")
    protected boolean isCursor = true;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setCursorCompactionEnabled(isCursor);
        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(10*1024); // no rate limiting
        createSStables();
        takeSnapshot();
    }

    protected void createSStables()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        for (int j = 0; j < sstableCount; j++)
        {
            int pPrefix = overlap.startsWith("PK") ? 0 : j * rowCount;
            int rPrefix = overlap.startsWith("PK.ROW") ? 0 : j * rowCount;
            for (long i = 0; i < rowCount; i++)
            {
                execute(writeStatement, (pPrefix + i), (rPrefix + i), j * rowCount + i);
            }

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }

    private void takeSnapshot()
    {
        SnapshotManager.instance.takeSnapshot("originals", cfs.getKeyspaceTableName());
        snapshotFiles = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots("originals").listFiles();
        long[] sum = new long[1];
        snapshotFiles.forEach(f -> sum[0] += f.length());
        System.out.println("Total input size: " + sum[0]);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        int active = Thread.currentThread().getThreadGroup().activeCount();
        Thread[] threads = new Thread[active];
        Thread.currentThread().getThreadGroup().enumerate(threads);
        for (Thread t : threads)
        {
            if (!t.isDaemon())
                System.err.println("Thread "+t.getName());
        }

        CommitLog.instance.shutdownBlocking();
        ClusterMetadataService.instance().log().close();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }


    @TearDown(Level.Invocation)
    public void resetSnapshot() throws IOException, InterruptedException
    {
        cfs.truncateBlocking();

        List<File> directories = cfs.getDirectories().getCFDirectories();
        // Sometimes deletes are unreliable...
        int deleted = 0;
        do
        {
            deleted = 0;
            for (File file : directories)
            {
                for (File f : file.tryList())
                {
                    if (f.isDirectory())
                        continue;
                    f.tryDelete();
                    deleted++;
                }
            }
            Thread.sleep(10);
        } while (deleted != 0);

        for (File file : snapshotFiles)
            FileUtils.createHardLink(file, new File(new File(file.toPath().getParent().getParent().getParent()), file.name()));

        cfs.loadNewSSTables();
    }

    @Benchmark
    public void compactTest() throws Throwable
    {
        cfs.forceMajorCompaction();
    }
}
