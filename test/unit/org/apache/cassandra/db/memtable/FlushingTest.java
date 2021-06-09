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

package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(BMUnitRunner.class)
@BMUnitConfig(debug = true)
public class FlushingTest extends CQLTester
{
    List<PartitionPosition> ranges;
    List<Directories.DataDirectory> locations;
    ColumnFamilyStore cfs;
    Memtable memtable;
    ExecutorService executor;
    int nThreads;

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");

        for (int i = 0; i < 10000; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, i);

        cfs = getCurrentColumnFamilyStore();
        memtable = cfs.getTracker().getView().getCurrentMemtable();

        OpOrder.Barrier barrier = cfs.keyspace.writeOrder.newBarrier();
        Memtable.LastCommitLogPosition position = new Memtable.LastCommitLogPosition(CommitLog.instance.getCurrentPosition());
        memtable.switchOut(barrier, new AtomicReference<>(position));
        barrier.issue();

        ranges = new ArrayList<>();
        locations = new ArrayList<>();
        // this determines the number of flush writers created, the FlushRunnable will convert a null location into an sstable location for us
        int rangeCount = 24;
        for (int i = 0; i < rangeCount; ++i)
        {
            // split the range to ensure there are partitions to write
            ranges.add(cfs.getPartitioner().split(cfs.getPartitioner().getMinimumToken(),
                                                  cfs.getPartitioner().getMaximumToken(),
                                                  (i+1) * 1.0 / rangeCount).minKeyBound());
            locations.add(null);
        }
        nThreads = locations.size() / 2;
        executor = Executors.newFixedThreadPool(nThreads);
    }

    @Test
    public void testAbortingFlushRunnablesWithoutStarting() throws Throwable
    {
        // abort without starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Flushing.FlushRunnable> flushRunnables = Flushing.flushRunnables(cfs, memtable, ranges, locations, txn);
            assertNotNull(flushRunnables);

            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Flushing.FlushRunnableWriterState.IDLE, flushRunnable.state());

            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));

            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Flushing.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }
    }

    static Semaphore stopSignal = null;
    static Semaphore continueSignal;

    public static void stopAndWait() throws InterruptedException
    {
        if (stopSignal != null)
        {
            stopSignal.release();
            continueSignal.acquire();
        }
    }

    @Test
    @BMRule(name = "Wait before loop",
    targetClass = "Flushing$FlushRunnable",
    targetMethod = "writeSortedContents",
    targetLocation = "AT ENTRY",
    action = "org.apache.cassandra.db.memtable.FlushingTest.stopAndWait()")
    public void testAbortingFlushRunnablesAfterStarting() throws Throwable
    {
        // abort after starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Flushing.FlushRunnable> flushRunnables = Flushing.flushRunnables(cfs, memtable, ranges, locations, txn);

            stopSignal = new Semaphore(0);
            continueSignal = new Semaphore(0);

            List<Future<SSTableMultiWriter>> futures = flushRunnables.stream().map(executor::submit).collect(Collectors.toList());

            stopSignal.acquire(nThreads);
            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));
            continueSignal.release(flushRunnables.size());  // release all, including the ones that have not started yet

            FBUtilities.waitOnFutures(futures);

            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Flushing.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }
    }

    @Test
    public void testAbortingFlushRunnablesBeforeStarting() throws Throwable
    {
        // abort before starting
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
        {
            List<Flushing.FlushRunnable> flushRunnables = Flushing.flushRunnables(cfs, memtable, ranges, locations, txn);

            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertNull(flushRunnable.abort(null));

            List<Future<SSTableMultiWriter>> futures = flushRunnables.stream().map(executor::submit).collect(Collectors.toList());

            FBUtilities.waitOnFutures(futures);

            for (Flushing.FlushRunnable flushRunnable : flushRunnables)
                assertEquals(Flushing.FlushRunnableWriterState.ABORTED, flushRunnable.state());
        }
    }
}
