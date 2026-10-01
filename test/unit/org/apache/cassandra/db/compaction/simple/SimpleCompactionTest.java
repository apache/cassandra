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

package org.apache.cassandra.db.compaction.simple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TestHelper;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


@Ignore
@RunWith(Parameterized.class)
public abstract class SimpleCompactionTest extends CQLTester
{
    /** One run of every scenario: how compaction reads, which path it takes, and the output format. */
    public static final class Params
    {
        public final DiskAccessMode diskAccessMode;
        public final boolean cursorCompaction;
        public final String sstableFormat;

        Params(DiskAccessMode diskAccessMode, boolean cursorCompaction, String sstableFormat)
        {
            this.diskAccessMode = diskAccessMode;
            this.cursorCompaction = cursorCompaction;
            this.sstableFormat = sstableFormat;
        }

        @Override
        public String toString()
        {
            return "diskAccessMode=" + diskAccessMode + ",cursor=" + cursorCompaction + ",format=" + sstableFormat;
        }
    }

    @Parameterized.Parameter
    public Params params;

    /** Every scenario runs on every combination of disk access mode, compaction path, and output format. */
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Params> params()
    {
        List<Params> all = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (DiskAccessMode mode : new DiskAccessMode[]{ DiskAccessMode.standard, DiskAccessMode.direct })
                for (boolean cursor : new boolean[]{ true, false })
                    all.add(new Params(mode, cursor, format));
        return all;
    }

    private DiskAccessMode originalDiskAccessMode;
    private boolean originalCursorCompactionEnabled;
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void setCompactionParams()
    {
        originalDiskAccessMode = DatabaseDescriptor.getCompactionReadDiskAccessMode();
        originalCursorCompactionEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setCompactionReadDiskAccessMode(params.diskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(params.cursorCompaction);
        DatabaseDescriptor.setSelectedSSTableFormat(params.sstableFormat);
    }

    @After
    public void restoreCompactionParams()
    {
        DatabaseDescriptor.setCompactionReadDiskAccessMode(originalDiskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(originalCursorCompactionEnabled);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @AfterClass
    public static void teardown() throws IOException, InterruptedException, ExecutionException
    {
        TestHelper.teardown();
    }

    /** Fails before compacting if cursor compaction is unsupported for this table. */
    protected void assertCursorPathWillRun(ColumnFamilyStore cfs)
    {
        if (!params.cursorCompaction)
            return;

        // A format without cursor-compaction support correctly runs the iterator path, so skip.
        if (!DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction())
            return;

        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        try (CompactionController controller = new CompactionController(cfs, inputs, cfs.gcBefore(FBUtilities.nowInSeconds()));
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            assertTrue("cursor compaction is not supported for this scenario, so it would only " +
                       "exercise the iterator path",
                       CursorCompactor.isSupported(scanners, controller));
        }
    }

    /** Major-compacts and asserts the compaction ran the pipeline this parameterization asked for. */
    protected void majorCompact(ColumnFamilyStore cfs)
    {
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        cfs.forceMajorCompaction();
        assertExpectedPipelineRan(before);
    }

    /** A time an hour ahead, so short TTLs have expired and gc_grace_seconds=0 tombstones can be purged. */
    protected static long anHourFromNow()
    {
        return FBUtilities.nowInSeconds() + 3600;
    }

    /**
     * Compacts all live sstables into one as if the time were nowInSec, so tests need not sleep
     * for TTLs or gc_grace_seconds to pass. Asserts the expected pipeline ran.
     */
    protected void majorCompactAt(ColumnFamilyStore cfs, long nowInSec)
    {
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        long gcBefore = cfs.getDefaultGcBefore(nowInSec);
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION))
        {
            assertNotNull("could not mark the live sstables as compacting", txn);
            new CompactionTask(cfs, txn, gcBefore).setNowInSecondsSupplier(() -> nowInSec)
                                                  .execute(CompactionManager.instance.active);
        }
        assertExpectedPipelineRan(before);
    }

    private void assertExpectedPipelineRan(CompactionPipelineCounts before)
    {
        CompactionPipelineCounts.assertPipelineRan(params.cursorCompaction &&
                                                   DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction(),
                                                   before);
    }
}
