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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TestHelper;

import static org.junit.Assert.assertTrue;


@Ignore
@RunWith(Parameterized.class)
public abstract class SimpleCompactionTest extends CQLTester
{
    @Parameterized.Parameter(0)
    public DiskAccessMode compactionReadDiskAccessMode;

    @Parameterized.Parameter(1)
    public boolean cursorCompactionEnabled;

    @Parameterized.Parameter(2)
    public String sstableFormat;

    @Parameterized.Parameters(name = "diskAccessMode={0},cursor={1},format={2}")
    public static Collection<Object[]> params()
    {
        // Every scenario runs on both output formats, BIG and BTI.
        return Arrays.asList(new Object[]{ DiskAccessMode.standard, true, "big" },
                             new Object[]{ DiskAccessMode.standard, false, "big" },
                             new Object[]{ DiskAccessMode.direct, true, "big" },
                             new Object[]{ DiskAccessMode.direct, false, "big" },
                             new Object[]{ DiskAccessMode.standard, true, "bti" },
                             new Object[]{ DiskAccessMode.standard, false, "bti" },
                             new Object[]{ DiskAccessMode.direct, true, "bti" },
                             new Object[]{ DiskAccessMode.direct, false, "bti" });
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
        DatabaseDescriptor.setCompactionReadDiskAccessMode(compactionReadDiskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(cursorCompactionEnabled);
        DatabaseDescriptor.setSelectedSSTableFormat(sstableFormat);
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
        if (!cursorCompactionEnabled)
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
        CompactionPipelineCounts.assertPipelineRan(cursorCompactionEnabled &&
                                                   DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction(),
                                                   before);
    }
}
