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
        // Every scenario runs on both output formats.  The test pins the format itself, so the
        // corpus behaves the same way under any ambient config (for example ant test-latest, whose
        // overlay selects BTI).  Both BIG and BTI support cursor compaction.
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

    /**
     * Fails before the compaction if cursor compaction is unsupported for this table, so the failure
     * names the reason. {@link #majorCompact} catches the same case afterwards, as a pipeline-count
     * mismatch with no explanation. Call this on the input sstables before compacting.
     */
    protected void assertCursorPathWillRun(ColumnFamilyStore cfs)
    {
        if (!cursorCompactionEnabled)
            return;

        // A format that does not support cursor compaction refuses the cursor path by design, so the
        // iterator pipeline is then the correct outcome, not a defect. Both BIG and BTI — the formats
        // this test pins — support it, so this guard passes through; keep the assertion below for
        // every other unsupported-ness reason.
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

    /**
     * Major-compacts and asserts the compaction really went through the pipeline this
     * parameterization asked for. {@code cfs.forceMajorCompaction()} on its own cannot tell the two
     * apart, and neither can a supportability precheck: pipeline selection also consults
     * {@code cursorCompactionEnabled}, which {@code CursorCompactor.isSupported} never reads. A
     * scenario that requested the cursor path and was served by the iterator one would assert
     * nothing about the cursor reader or writer and still pass.
     * <p>
     * The expectation is derived from the parameterization and the pinned format's cursor-compaction
     * capability rather than from {@code isSupported}, so that it cannot become a tautology restating
     * the predicate the pipeline itself consults. A format that does not support cursor compaction
     * refuses the cursor path, so the iterator pipeline is the correct expectation there; BIG and BTI
     * both support it, so under {@code cursor=true} the cursor pipeline is expected on both.
     */
    protected void majorCompact(ColumnFamilyStore cfs)
    {
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        cfs.forceMajorCompaction();
        CompactionPipelineCounts.assertPipelineRan(cursorCompactionEnabled &&
                                                   DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction(),
                                                   before);
    }
}
