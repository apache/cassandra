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

import java.util.ArrayList;
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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A CursorCompactor constructor that throws after it has opened its cursors must close them,
 * because the caller never gets an instance to close.
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
            action = "throw new RuntimeException(\"injected constructor failure\")")
})
public class CursorCompactorConstructorFailureTest extends CQLTester
{
    public static final AtomicInteger opened = new AtomicInteger();
    public static final AtomicInteger closed = new AtomicInteger();
    public static volatile boolean failConstruction;

    private SSTableFormat<?, ?> originalFormat;

    @After
    public void restore()
    {
        failConstruction = false;
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

    private void assertCursorsClosedOnConstructorFailure(String format) throws Throwable
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
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        assertEquals(3, inputs.size());

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
}
