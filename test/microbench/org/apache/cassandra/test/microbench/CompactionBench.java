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


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 25, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CompactionBench extends CQLTester
{
    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    ColumnFamilyStore cfs;
    List<File> snapshotFiles;
    List<Descriptor> liveFiles;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.prepareServer();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";
        readStatement = "SELECT * from "+table+" limit 100";

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        //Warm up
        System.err.println("Writing 50k");
        for (long i = 0; i < 50000; i++)
            execute(writeStatement, i, i, i );


        cfs.forceBlockingFlush();

        System.err.println("Writing 50k again...");
        for (long i = 0; i < 50000; i++)
            execute(writeStatement, i, i, i );

        cfs.forceBlockingFlush();

        cfs.snapshot("originals");

        snapshotFiles = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots("originals").listFiles();
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

        CQLTester.cleanup();
    }


    @TearDown(Level.Invocation)
    public void resetSnapshot()
    {
        cfs.truncateBlocking();

        List<File> directories = cfs.getDirectories().getCFDirectories();

        for (File file : directories)
        {
            for (File f : file.listFiles())
            {
                if (f.isDirectory())
                    continue;

                FileUtils.delete(f);
            }
        }


        for (File file : snapshotFiles)
            FileUtils.createHardLink(file, new File(file.toPath().getParent().getParent().getParent().toFile(), file.getName()));

        cfs.loadNewSSTables();
    }

    @Benchmark
    public void compactTest() throws Throwable
    {
        cfs.forceMajorCompaction();
    }
}
