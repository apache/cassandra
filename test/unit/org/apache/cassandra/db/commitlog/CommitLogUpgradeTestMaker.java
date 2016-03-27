package org.apache.cassandra.db.commitlog;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.commitlog.CommitLogUpgradeTest.*;

public class CommitLogUpgradeTestMaker
{
    public static ByteBuffer dataSource;

    private static int NUM_THREADS = 4 * Runtime.getRuntime().availableProcessors() - 1;
    public static int numCells = 1;
    public static int cellSize = 256;
    public static int rateLimit = 0;
    public static int runTimeMs = 1000;

    public static void main(String[] args) throws Exception
    {
        try
        {
            initialize();

            CommitLogUpgradeTestMaker tester = new CommitLogUpgradeTestMaker();
            tester.makeLog();
        }
        catch (Throwable e)
        {
            e.printStackTrace(System.err);
        }
        finally
        {
            System.exit(0);
        }
    }

    volatile boolean stop = false;
    boolean randomSize = true;

    static public void initialize() throws IOException, ConfigurationException
    {
        try (FileInputStream fis = new FileInputStream("CHANGES.txt"))
        {
            dataSource = ByteBuffer.allocateDirect((int) fis.getChannel().size());
            while (dataSource.hasRemaining())
            {
                fis.getChannel().read(dataSource);
            }
            dataSource.flip();
        }

        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata);
    }

    public void makeLog() throws IOException, InterruptedException
    {
        CommitLog commitLog = CommitLog.instance;
        System.out.format("\nUsing commit log size: %dmb, compressor: %s, encryption: %s, sync: %s, %s\n",
                          mb(DatabaseDescriptor.getCommitLogSegmentSize()),
                          commitLog.configuration.getCompressorName(),
                          commitLog.configuration.useEncryption(),
                          commitLog.executor.getClass().getSimpleName(),
                          randomSize ? "random size" : "");
        final List<CommitlogExecutor> threads = new ArrayList<>();
        ScheduledExecutorService scheduled = startThreads(commitLog, threads);

        Thread.sleep(runTimeMs);
        stop = true;
        scheduled.shutdown();
        scheduled.awaitTermination(2, TimeUnit.SECONDS);

        int hash = 0;
        int cells = 0;
        for (CommitlogExecutor t : threads)
        {
            t.join();
            hash += t.hash;
            cells += t.cells;
        }
        commitLog.shutdownBlocking();

        File dataDir = new File(CommitLogUpgradeTest.DATA_DIR + FBUtilities.getReleaseVersionString());
        System.out.format("Data will be stored in %s\n", dataDir);
        if (dataDir.exists())
            FileUtils.deleteRecursive(dataDir);

        dataDir.mkdirs();
        for (File f : new File(DatabaseDescriptor.getCommitLogLocation()).listFiles())
            FileUtils.createHardLink(f, new File(dataDir, f.getName()));

        Properties prop = new Properties();
        prop.setProperty(CFID_PROPERTY, Schema.instance.getId(KEYSPACE, TABLE).toString());
        prop.setProperty(CELLS_PROPERTY, Integer.toString(cells));
        prop.setProperty(HASH_PROPERTY, Integer.toString(hash));
        prop.store(new FileOutputStream(new File(dataDir, PROPERTIES_FILE)),
                   "CommitLog upgrade test, version " + FBUtilities.getReleaseVersionString());
        System.out.println("Done");
    }

    public ScheduledExecutorService startThreads(CommitLog commitLog, final List<CommitlogExecutor> threads)
    {
        stop = false;
        for (int ii = 0; ii < NUM_THREADS; ii++)
        {
            final CommitlogExecutor t = new CommitlogExecutor(commitLog);
            threads.add(t);
            t.start();
        }

        final long start = System.currentTimeMillis();
        Runnable printRunnable = new Runnable()
        {
            long lastUpdate = 0;

            public void run()
            {
                Runtime runtime = Runtime.getRuntime();
                long maxMemory = mb(runtime.maxMemory());
                long allocatedMemory = mb(runtime.totalMemory());
                long freeMemory = mb(runtime.freeMemory());
                long temp = 0;
                long sz = 0;
                for (CommitlogExecutor cle : threads)
                {
                    temp += cle.counter.get();
                    sz += cle.dataSize;
                }
                double time = (System.currentTimeMillis() - start) / 1000.0;
                double avg = (temp / time);
                System.out.println(
                        String.format("second %d mem max %dmb allocated %dmb free %dmb mutations %d since start %d avg %.3f transfer %.3fmb",
                                      ((System.currentTimeMillis() - start) / 1000),
                                      maxMemory,
                                      allocatedMemory,
                                      freeMemory,
                                      (temp - lastUpdate),
                                      lastUpdate,
                                      avg,
                                      mb(sz / time)));
                lastUpdate = temp;
            }
        };
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(printRunnable, 1, 1, TimeUnit.SECONDS);
        return scheduled;
    }

    private static long mb(long maxMemory)
    {
        return maxMemory / (1024 * 1024);
    }

    private static double mb(double maxMemory)
    {
        return maxMemory / (1024 * 1024);
    }

    public static ByteBuffer randomBytes(int quantity, ThreadLocalRandom tlr)
    {
        ByteBuffer slice = ByteBuffer.allocate(quantity);
        ByteBuffer source = dataSource.duplicate();
        source.position(tlr.nextInt(source.capacity() - quantity));
        source.limit(source.position() + quantity);
        slice.put(source);
        slice.flip();
        return slice;
    }

    public class CommitlogExecutor extends Thread
    {
        final AtomicLong counter = new AtomicLong();
        int hash = 0;
        int cells = 0;
        int dataSize = 0;
        final CommitLog commitLog;

        volatile CommitLogPosition clsp;

        public CommitlogExecutor(CommitLog commitLog)
        {
            this.commitLog = commitLog;
        }

        public void run()
        {
            RateLimiter rl = rateLimit != 0 ? RateLimiter.create(rateLimit) : null;
            final ThreadLocalRandom tlr = ThreadLocalRandom.current();
            while (!stop)
            {
                if (rl != null)
                    rl.acquire();
                ByteBuffer key = randomBytes(16, tlr);

                UpdateBuilder builder = UpdateBuilder.create(Schema.instance.getCFMetaData(KEYSPACE, TABLE), Util.dk(key));

                for (int ii = 0; ii < numCells; ii++)
                {
                    int sz = randomSize ? tlr.nextInt(cellSize) : cellSize;
                    ByteBuffer bytes = randomBytes(sz, tlr);
                    builder.newRow(CommitLogUpgradeTest.CELLNAME + ii).add("val", bytes);
                    hash = hash(hash, bytes);
                    ++cells;
                    dataSize += sz;
                }

                clsp = commitLog.add((Mutation)builder.makeMutation());
                counter.incrementAndGet();
            }
        }
    }
}
