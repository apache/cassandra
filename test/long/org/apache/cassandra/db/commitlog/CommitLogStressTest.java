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


import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FastByteArrayInputStream;

public class CommitLogStressTest
{

    public static ByteBuffer dataSource;
    
    public static int NUM_THREADS = 4 * Runtime.getRuntime().availableProcessors() - 1;

    public static int numCells = 1;

    public static int cellSize = 1024;
    
    public static int rateLimit = 0;
    
    public static int runTimeMs = 10000;
    
    public static String location = DatabaseDescriptor.getCommitLogLocation() + "/stress";
    
    public static int hash(int hash, ByteBuffer bytes)
    {
        int shift = 0;
        for (int i=0; i<bytes.limit(); i++) {
            hash += (bytes.get(i) & 0xFF) << shift;
            shift = (shift + 8) & 0x1F;
        }
        return hash;
    }
    
    public static void main(String[] args) throws Exception {
        try {
            if (args.length >= 1) {
                NUM_THREADS = Integer.parseInt(args[0]);
                System.out.println("Setting num threads to: " + NUM_THREADS);
            }
    
            if (args.length >= 2) {
                numCells = Integer.parseInt(args[1]);
                System.out.println("Setting num cells to: " + numCells);
            }
    
            if (args.length >= 3) {
                cellSize = Integer.parseInt(args[1]);
                System.out.println("Setting cell size to: " + cellSize + " be aware the source corpus may be small");
            }
    
            if (args.length >= 4) {
                rateLimit = Integer.parseInt(args[1]);
                System.out.println("Setting per thread rate limit to: " + rateLimit);
            }
            initialize();
            
            CommitLogStressTest tester = new CommitLogStressTest();
            tester.testFixedSize();
        }
        catch (Exception e)
        {
            e.printStackTrace(System.err);
        }
        finally {
            System.exit(0);
        }
    }
    
    boolean failed = false;
    volatile boolean stop = false;
    boolean randomSize = false;
    boolean discardedRun = false;
    ReplayPosition discardedPos;
    
    @BeforeClass
    static public void initialize() throws FileNotFoundException, IOException, InterruptedException
    {
        try (FileInputStream fis = new FileInputStream("CHANGES.txt"))
        {
            dataSource = ByteBuffer.allocateDirect((int)fis.getChannel().size());
            while (dataSource.hasRemaining()) {
                fis.getChannel().read(dataSource);
            }
            dataSource.flip();
        }

        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(""); // leave def. blank to maintain old behaviour

        File dir = new File(location);
        if (dir.isDirectory())
        {
            File[] files = dir.listFiles();
    
            for (File f : files)
                if (!f.delete())
                    Assert.fail("Failed to delete " + f);
        } else {
            dir.mkdir();
        }
    }

    @Test
    public void testRandomSize() throws Exception
    {
        randomSize = false;
        discardedRun = false;
        testAllLogConfigs();
    }

    @Test
    public void testFixedSize() throws Exception
    {
        randomSize = false;
        discardedRun = false;

        testAllLogConfigs();
    }

    @Test
    public void testDiscardedRun() throws Exception
    {
        discardedRun = true;
        randomSize = true;

        testAllLogConfigs();
    }

    public void testAllLogConfigs() throws IOException, InterruptedException
    {
        failed = false;
        DatabaseDescriptor.setCommitLogSyncBatchWindow(1);
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogSegmentSize(32);
        for (ParameterizedClass compressor : new ParameterizedClass[] {
                null,
                new ParameterizedClass("LZ4Compressor", null),
                new ParameterizedClass("SnappyCompressor", null),
                new ParameterizedClass("DeflateCompressor", null)}) {
            DatabaseDescriptor.setCommitLogCompression(compressor);
            for (CommitLogSync sync : CommitLogSync.values())
            {
                DatabaseDescriptor.setCommitLogSync(sync);
                CommitLog commitLog = new CommitLog(location, CommitLog.instance.archiver);
                testLog(commitLog);
            }
        }
        assert !failed;
    }

    public void testLog(CommitLog commitLog) throws IOException, InterruptedException {
        System.out.format("\nTesting commit log size %dmb, compressor %s, sync %s%s%s\n",
                           mb(DatabaseDescriptor.getCommitLogSegmentSize()),
                           commitLog.compressor != null ? commitLog.compressor.getClass().getSimpleName() : "none",
                           commitLog.executor.getClass().getSimpleName(),
                           randomSize ? " random size" : "",
                           discardedRun ? " with discarded run" : "");
        commitLog.allocator.enableReserveSegmentCreation();
        
        final List<CommitlogExecutor> threads = new ArrayList<>();
        ScheduledExecutorService scheduled = startThreads(commitLog, threads);

        discardedPos = ReplayPosition.NONE;
        if (discardedRun) {
            // Makes sure post-break data is not deleted, and that replayer correctly rejects earlier mutations.
            Thread.sleep(runTimeMs / 3);
            stop = true;
            scheduled.shutdown();
            scheduled.awaitTermination(2, TimeUnit.SECONDS);

            for (CommitlogExecutor t: threads)
            {
                t.join();
                CommitLog.instance.discardCompletedSegments( Schema.instance.getCFMetaData("Keyspace1", "Standard1").cfId, t.rp);
                if (t.rp.compareTo(discardedPos) > 0)
                    discardedPos = t.rp;
            }
            threads.clear();
            System.out.format("Discarded at %s\n", discardedPos);

            scheduled = startThreads(commitLog, threads);
        }

        
        Thread.sleep(runTimeMs);
        stop = true;
        scheduled.shutdown();
        scheduled.awaitTermination(2, TimeUnit.SECONDS);

        int hash = 0;
        int cells = 0;
        for (CommitlogExecutor t: threads) {
            t.join();
            hash += t.hash;
            cells += t.cells;
        }
        
        commitLog.shutdownBlocking();

        System.out.print("Stopped. Replaying... "); System.out.flush();
        Replayer repl = new Replayer();
        File[] files = new File(location).listFiles();
        repl.recover(files);

        for (File f : files)
            if (!f.delete())
                Assert.fail("Failed to delete " + f);
        
        if (hash == repl.hash && cells == repl.cells)
            System.out.println("Test success.");
        else
        {
            System.out.format("Test failed. Cells %d expected %d, hash %d expected %d.\n", repl.cells, cells, repl.hash, hash);
            failed = true;
        }
    }

    public ScheduledExecutorService startThreads(CommitLog commitLog, final List<CommitlogExecutor> threads)
    {
        stop = false;
        for (int ii = 0; ii < NUM_THREADS; ii++) {
            final CommitlogExecutor t = new CommitlogExecutor(commitLog);
            threads.add(t);
            t.start();
        }

        final long start = System.currentTimeMillis();
        Runnable printRunnable = new Runnable() {
            long lastUpdate = 0;

            public void run() {
              Runtime runtime = Runtime.getRuntime();
              long maxMemory = mb(runtime.maxMemory());
              long allocatedMemory = mb(runtime.totalMemory());
              long freeMemory = mb(runtime.freeMemory());
              long temp = 0;
              long sz = 0;
              for (CommitlogExecutor cle : threads) {
                  temp += cle.counter.get();
                  sz += cle.dataSize;
              }
              double time = (System.currentTimeMillis() - start) / 1000.0;
              double avg = (temp / time);
              System.out.println(String.format("second %d mem max %dmb allocated %dmb free %dmb mutations %d since start %d avg %.3f transfer %.3fmb",
                      ((System.currentTimeMillis() - start) / 1000),
                      maxMemory, allocatedMemory, freeMemory, (temp - lastUpdate), lastUpdate, avg, mb(sz / time)));
              lastUpdate = temp;
            }
        };
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        scheduled.scheduleAtFixedRate(printRunnable, 1, 1, TimeUnit.SECONDS);
        return scheduled;
    }

    private static long mb(long maxMemory) {
        return maxMemory / (1024 * 1024);
    }

    private static double mb(double maxMemory) {
        return maxMemory / (1024 * 1024);
    }

    public static ByteBuffer randomBytes(int quantity, ThreadLocalRandom tlr) {
        ByteBuffer slice = ByteBuffer.allocate(quantity);
        ByteBuffer source = dataSource.duplicate();
        source.position(tlr.nextInt(source.capacity() - quantity));
        source.limit(source.position() + quantity);
        slice.put(source);
        slice.flip();
        return slice;
    }

    public class CommitlogExecutor extends Thread {
        final AtomicLong counter = new AtomicLong();
        int hash = 0;
        int cells = 0;
        int dataSize = 0;
        final CommitLog commitLog;

        volatile ReplayPosition rp;

        public CommitlogExecutor(CommitLog commitLog)
        {
            this.commitLog = commitLog;
        }

        public void run() {
            RateLimiter rl = rateLimit != 0 ? RateLimiter.create(rateLimit) : null;
            final ThreadLocalRandom tlr = ThreadLocalRandom.current();
            while (!stop) {
                if (rl != null)
                    rl.acquire();
                String ks = "Keyspace1";
                ByteBuffer key = randomBytes(16, tlr);
                Mutation mutation = new Mutation(ks, key);

                for (int ii = 0; ii < numCells; ii++) {
                    int sz = randomSize ? tlr.nextInt(cellSize) : cellSize;
                    ByteBuffer bytes = randomBytes(sz, tlr);
                    mutation.add("Standard1", Util.cellname("name" + ii), bytes,
                            System.currentTimeMillis());
                    hash = hash(hash, bytes);
                    ++cells;
                    dataSize += sz;
                }
                rp = commitLog.add(mutation);
                counter.incrementAndGet();
            }
        }
    }
    
    class Replayer extends CommitLogReplayer {

        Replayer()
        {
            super(discardedPos, null);
        }

        int hash = 0;
        int cells = 0;

        @Override
        void replayMutation(byte[] inputBuffer, int size,
                final long entryLocation, final CommitLogDescriptor desc, final ReplayFilter replayFilter)
        {
            if (desc.id < discardedPos.segment) {
                System.out.format("Mutation from discarded segment, segment %d pos %d\n", desc.id, entryLocation);
                return;
            } else if (desc.id == discardedPos.segment && entryLocation <= discardedPos.position)
                // Skip over this mutation.
                return;
                
            FastByteArrayInputStream bufIn = new FastByteArrayInputStream(inputBuffer, 0, size);
            Mutation mutation;
            try
            {
                mutation = Mutation.serializer.deserialize(new DataInputStream(bufIn),
                                                               desc.getMessagingVersion(),
                                                               ColumnSerializer.Flag.LOCAL);
            }
            catch (IOException e)
            {
                // Test fails.
                throw new AssertionError(e);
            }

            for (ColumnFamily cf : mutation.getColumnFamilies()) {
                for (Cell c : cf.getSortedColumns()) {
                    if (new String(c.name().toByteBuffer().array(), StandardCharsets.UTF_8).startsWith("name"))
                    {
                        hash = hash(hash, c.value());
                        ++cells;
                    }
                }
            }
        }
        
    }
}
