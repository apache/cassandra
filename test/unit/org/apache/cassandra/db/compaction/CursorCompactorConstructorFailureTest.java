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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A CursorCompactor constructor that throws after it has opened its cursors must close them,
 * because the caller never gets an instance to close. It must also finish the compaction it
 * began in the {@link ActiveCompactionsTracker}, including when opening a cursor fails.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
    @BMRule(name = "count opened cursors",
            targetClass = "StatefulCursor",
            targetMethod = "<init>",
            targetLocation = "AT EXIT",
            action = "org.apache.cassandra.db.compaction.CursorCompactorConstructorFailureTest.opened.incrementAndGet()"),
    @BMRule(name = "count closed cursors",
            targetClass = "org.apache.cassandra.io.sstable.SSTableCursorReader",
            targetMethod = "close",
            targetLocation = "AT EXIT",
            action = "org.apache.cassandra.db.compaction.CursorCompactorConstructorFailureTest.closed.incrementAndGet()"),
    @BMRule(name = "fail after the cursors are open",
            targetClass = "CursorCompactor$Purger",
            targetMethod = "<init>",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.CursorCompactorConstructorFailureTest.failConstruction",
            action = "throw new RuntimeException(\"injected constructor failure\")"),
    @BMRule(name = "fail opening the second cursor of the test's table",
            targetClass = "StatefulCursor",
            targetMethod = "<init>(org.apache.cassandra.io.sstable.format.SSTableReader,java.util.Collection,org.apache.cassandra.config.Config$DiskAccessMode)",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.db.compaction.CursorCompactorConstructorFailureTest.failCursorOpen($1.descriptor.cfname)",
            action = "throw new RuntimeException(\"injected cursor open failure\")")
})
public class CursorCompactorConstructorFailureTest extends CQLTester
{
    public static final AtomicInteger opened = new AtomicInteger();
    public static final AtomicInteger closed = new AtomicInteger();
    public static volatile boolean failConstruction;
    public static volatile String cursorOpenFailureTable;
    public static final AtomicInteger cursorOpenAttempts = new AtomicInteger();

    /** True on the second cursor opened for the given table, so one cursor is already open when it fails. */
    public static boolean failCursorOpen(String table)
    {
        return table.equals(cursorOpenFailureTable) && cursorOpenAttempts.incrementAndGet() == 2;
    }

    private SSTableFormat<?, ?> originalFormat;

    @After
    public void restore()
    {
        failConstruction = false;
        cursorOpenFailureTable = null;
        if (originalFormat != null)
            DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void cursorsClosedWhenConstructorFailsBig() throws Throwable
    {
        assertCursorsClosedOnConstructorFailure("big");
    }

    @Test
    public void cursorsClosedWhenConstructorFailsBti() throws Throwable
    {
        assertCursorsClosedOnConstructorFailure("bti");
    }

    @Test
    public void compactionFinishedWhenCursorOpenFailsBig() throws Throwable
    {
        assertCompactionFinishedOnCursorOpenFailure("big");
    }

    @Test
    public void compactionFinishedWhenCursorOpenFailsBti() throws Throwable
    {
        assertCompactionFinishedOnCursorOpenFailure("bti");
    }

    private void assertCursorsClosedOnConstructorFailure(String format) throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithThreeSSTables(format);
        Set<SSTableReader> inputs = cfs.getLiveSSTables();

        opened.set(0);
        closed.set(0);
        failConstruction = true;
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds());
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            new CursorCompactor(OperationType.COMPACTION, scanners.scanners, controller,
                                FBUtilities.nowInSeconds(), TimeUUID.Generator.nextTimeUUID());
            fail("the injected failure did not reach the constructor");
        }
        catch (RuntimeException e)
        {
            if (!"injected constructor failure".equals(e.getMessage()))
                throw e;
        }
        finally
        {
            failConstruction = false;
        }

        assertTrue("the constructor opened no cursor before the failure point", opened.get() > 0);
        assertEquals("every cursor the failed constructor opened must be closed", opened.get(), closed.get());
    }

    private void assertCompactionFinishedOnCursorOpenFailure(String format) throws Throwable
    {
        ColumnFamilyStore cfs = createTableWithThreeSSTables(format);
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        Map<SSTableReader, Integer> refsBefore = new HashMap<>();
        for (SSTableReader reader : inputs)
            refsBefore.put(reader, reader.selfRef().globalCount());

        RecordingTracker tracker = new RecordingTracker();
        opened.set(0);
        closed.set(0);
        cursorOpenAttempts.set(0);
        cursorOpenFailureTable = cfs.name;
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds());
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            newCursorCompactor(scanners.scanners, controller, tracker);
            fail("the injected cursor open failure did not reach the constructor");
        }
        catch (RuntimeException e)
        {
            if (!"injected cursor open failure".equals(e.getMessage()))
                throw e;
        }
        finally
        {
            cursorOpenFailureTable = null;
        }

        assertEquals("the failure must come on the second cursor", 2, cursorOpenAttempts.get());
        assertEquals("the constructor must begin the compaction once", 1, tracker.begun);
        assertEquals("the failed constructor must finish the compaction it began", 1, tracker.finished);
        assertTrue("no cursor was open when the cursor open failed", opened.get() > 0);
        assertEquals("every cursor the failed constructor opened must be closed", opened.get(), closed.get());
        for (SSTableReader reader : inputs)
            assertEquals("the failed constructor must not keep a reference to " + reader,
                         (int) refsBefore.get(reader), reader.selfRef().globalCount());
    }

    private ColumnFamilyStore createTableWithThreeSSTables(String format) throws Throwable
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 10; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "v" + round);
            flush();
        }
        assertEquals(3, cfs.getLiveSSTables().size());
        return cfs;
    }

    /** The constructor that takes a tracker is private; production callers all pass NOOP. */
    private static void newCursorCompactor(List<ISSTableScanner> scanners, CompactionController controller,
                                           ActiveCompactionsTracker tracker) throws Throwable
    {
        Constructor<CursorCompactor> constructor =
            CursorCompactor.class.getDeclaredConstructor(OperationType.class, List.class, AbstractCompactionController.class,
                                                         long.class, TimeUUID.class, ActiveCompactionsTracker.class);
        constructor.setAccessible(true);
        try
        {
            constructor.newInstance(OperationType.COMPACTION, scanners, controller, FBUtilities.nowInSeconds(),
                                    TimeUUID.Generator.nextTimeUUID(), tracker);
        }
        catch (InvocationTargetException e)
        {
            throw e.getCause();
        }
    }

    private static class RecordingTracker implements ActiveCompactionsTracker
    {
        int begun;
        int finished;

        public void beginCompaction(CompactionInfo.Holder ci)
        {
            begun++;
        }

        public void finishCompaction(CompactionInfo.Holder ci)
        {
            finished++;
        }
    }
}
