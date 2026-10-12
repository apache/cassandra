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

package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link CursorCompactor#close()} must close every cursor, whether or not the compaction was stopped
 * and whether or not finishing the output fails. The table writer must close its cursor index writer
 * when the output is committed and when it is aborted.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "count opened cursors",
            targetClass = "StatefulCursor",
            targetMethod = "<init>",
            targetLocation = "AT EXIT",
            action = "org.apache.cassandra.db.compaction.CursorCompactorCloseTest.opened.incrementAndGet()"),
    @BMRule(name = "count closed cursors",
            targetClass = "org.apache.cassandra.io.sstable.SSTableCursorReader",
            targetMethod = "close",
            targetLocation = "AT EXIT",
            action = "org.apache.cassandra.db.compaction.CursorCompactorCloseTest.closed.incrementAndGet()"),
    @BMRule(name = "count closed cursor index writers",
            targetClass = "^org.apache.cassandra.io.sstable.CursorIndexWriter",
            targetMethod = "close",
            targetLocation = "AT ENTRY",
            action = "org.apache.cassandra.db.compaction.CursorCompactorCloseTest.indexWritersClosed.incrementAndGet()"),
    @BMRule(name = "note that a partition was started",
            targetClass = "org.apache.cassandra.io.sstable.SSTableCursorWriter",
            targetMethod = "writePartitionStart",
            targetLocation = "AT EXIT",
            action = "org.apache.cassandra.db.compaction.CursorCompactorCloseTest.partitionStarted = true"),
    @BMRule(name = "stop once a partition is started",
            targetClass = "org.apache.cassandra.db.compaction.CompactionInfo$Holder",
            targetMethod = "isStopRequested",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.CursorCompactorCloseTest.stopInFirstPartition " +
                        "&& org.apache.cassandra.db.compaction.CursorCompactorCloseTest.partitionStarted",
            action = "return true"),
    @BMRule(name = "fail while finishing the output",
            targetClass = "CursorCompactor",
            targetMethod = "writerRollover",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.CursorCompactorCloseTest.failFinish",
            action = "throw new RuntimeException(\"injected finish failure\")")
})
public class CursorCompactorCloseTest extends CQLTester
{
    public static final AtomicInteger opened = new AtomicInteger();
    public static final AtomicInteger closed = new AtomicInteger();
    public static final AtomicInteger indexWritersClosed = new AtomicInteger();
    public static volatile boolean partitionStarted;
    public static volatile boolean stopInFirstPartition;
    public static volatile boolean failFinish;

    private final List<Object> leaks = new CopyOnWriteArrayList<>();
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void reset()
    {
        opened.set(0);
        closed.set(0);
        indexWritersClosed.set(0);
        partitionStarted = false;
        stopInFirstPartition = false;
        failFinish = false;
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        Ref.setOnLeak(leaks::add);
    }

    @After
    public void restore()
    {
        stopInFirstPartition = false;
        failFinish = false;
        Ref.setOnLeak(null);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void stopRightAfterFirstPartitionBig() throws Throwable
    {
        stopRightAfterFirstPartition("big");
    }

    @Test
    public void stopRightAfterFirstPartitionBti() throws Throwable
    {
        stopRightAfterFirstPartition("bti");
    }

    @Test
    public void stopDuringFirstPartitionBig() throws Throwable
    {
        stopDuringFirstPartition("big");
    }

    @Test
    public void stopDuringFirstPartitionBti() throws Throwable
    {
        stopDuringFirstPartition("bti");
    }

    @Test
    public void finishFailureStillClosesCursorsBig() throws Throwable
    {
        finishFailureStillClosesCursors("big");
    }

    @Test
    public void finishFailureStillClosesCursorsBti() throws Throwable
    {
        finishFailureStillClosesCursors("bti");
    }

    @Test
    public void cursorIndexWriterClosedOnCommitBig() throws Throwable
    {
        cursorIndexWriterClosedOnCommit("big");
    }

    @Test
    public void cursorIndexWriterClosedOnCommitBti() throws Throwable
    {
        cursorIndexWriterClosedOnCommit("bti");
    }

    private void cursorIndexWriterClosedOnCommit(String format) throws Throwable
    {
        ColumnFamilyStore cfs = twoInputs(format);
        boolean cursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        try
        {
            CompactionPipelineCounts before = CompactionPipelineCounts.mark();
            cfs.forceMajorCompaction();
            CompactionPipelineCounts.assertPipelineRan(true, before);
        }
        finally
        {
            DatabaseDescriptor.setCursorCompactionEnabled(cursorEnabled);
        }
        assertEquals("one cursor index writer per committed output must be closed",
                     cfs.getLiveSSTables().size(), indexWritersClosed.get());
        assertCursorsClosedAndNoLeaks();
    }

    /** Like nodetool stop COMPACTION after the first output partition is written. */
    private void stopRightAfterFirstPartition(String format) throws Throwable
    {
        ColumnFamilyStore cfs = twoInputs(format);
        assertNull("close() threw", compactAndClose(cfs, compactor -> {
            assertTrue("first partition must be written", compactor.writeNextPartition(writerUnderTest));
            compactor.stop();
            assertInterrupted(compactor);
        }));
        assertAbortedOutputClosedItsIndexWriter();
        assertCursorsClosedAndNoLeaks();
    }

    /** The stop lands after the first partition is started and before it is finished. */
    private void stopDuringFirstPartition(String format) throws Throwable
    {
        ColumnFamilyStore cfs = twoInputs(format);
        stopInFirstPartition = true;
        assertNull("close() threw", compactAndClose(cfs, this::assertInterrupted));
        assertTrue("the stop did not land inside the first partition", partitionStarted);
        assertAbortedOutputClosedItsIndexWriter();
        assertCursorsClosedAndNoLeaks();
    }

    /** close() rethrows the failure from finishing the output, after it has closed the cursors. */
    private void finishFailureStillClosesCursors(String format) throws Throwable
    {
        ColumnFamilyStore cfs = twoInputs(format);
        Throwable fromClose = compactAndClose(cfs, compactor -> {
            assertTrue("first partition must be written", compactor.writeNextPartition(writerUnderTest));
            failFinish = true;
        });
        assertNotNull("close() did not rethrow the failure from finish()", fromClose);
        assertEquals("injected finish failure", fromClose.getMessage());
        assertAbortedOutputClosedItsIndexWriter();
        assertCursorsClosedAndNoLeaks();
    }

    private void assertInterrupted(CursorCompactor compactor) throws Exception
    {
        try
        {
            compactor.writeNextPartition(writerUnderTest);
            fail("expected the stop to interrupt the compaction");
        }
        catch (CompactionInterruptedException expected)
        {
            // CompactionTask's try-with-resources now calls close()
        }
    }

    private interface CompactionSteps
    {
        void run(CursorCompactor compactor) throws Exception;
    }

    private DefaultCompactionWriter writerUnderTest;

    /** Runs the steps on a new cursor compaction, then closes it and returns what close() threw, if anything. */
    private Throwable compactAndClose(ColumnFamilyStore cfs, CompactionSteps steps) throws Throwable
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull(txn);
        Throwable fromClose = null;
        try (CompactionController controller = new CompactionController(cfs, inputs, gcBefore);
             AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(inputs);
             DefaultCompactionWriter writer = new DefaultCompactionWriter(cfs, cfs.getDirectories(), txn, inputs))
        {
            assertTrue("cursor path must be supported for this table", CursorCompactor.isSupported(scanners, controller));
            writerUnderTest = writer;
            CursorCompactor compactor = new CursorCompactor(OperationType.COMPACTION, scanners.scanners, controller,
                                                            FBUtilities.nowInSeconds(), TimeUUID.Generator.nextTimeUUID());
            steps.run(compactor);
            try
            {
                compactor.close();
            }
            catch (Throwable t)
            {
                fromClose = t;
            }
        }
        finally
        {
            writerUnderTest = null;
            stopInFirstPartition = false;
            failFinish = false;
            try
            {
                txn.abort();
            }
            catch (Throwable ignored)
            {
                // the writer's abort may already have aborted the transaction
            }
        }
        return fromClose;
    }

    private void assertAbortedOutputClosedItsIndexWriter()
    {
        assertEquals("the aborted output must close its cursor index writer", 1, indexWritersClosed.get());
    }

    private void assertCursorsClosedAndNoLeaks() throws InterruptedException
    {
        assertTrue("no cursor was opened", opened.get() > 0);
        assertEquals("every cursor must be closed", opened.get(), closed.get());
        for (int i = 0; i < 10; i++)
        {
            System.gc();
            Thread.sleep(100);
        }
        Thread.sleep(500);
        assertTrue("LEAK DETECTED: " + leaks, leaks.isEmpty());
    }

    private ColumnFamilyStore twoInputs(String format) throws Throwable
    {
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 20; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "v" + round);
            flush();
        }
        return cfs;
    }
}
