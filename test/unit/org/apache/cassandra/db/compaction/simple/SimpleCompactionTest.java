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
import org.junit.Assume;
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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
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

    @Parameterized.Parameters(name = "diskAccessMode={0},cursor={1}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(new Object[]{ DiskAccessMode.standard, true },
                             new Object[]{ DiskAccessMode.standard, false },
                             new Object[]{ DiskAccessMode.direct, true },
                             new Object[]{ DiskAccessMode.direct, false });
    }

    private DiskAccessMode originalDiskAccessMode;
    private boolean originalCursorCompactionEnabled;

    @Before
    public void setCompactionParams()
    {
        originalDiskAccessMode = DatabaseDescriptor.getCompactionReadDiskAccessMode();
        originalCursorCompactionEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCompactionReadDiskAccessMode(compactionReadDiskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(cursorCompactionEnabled);
    }

    @After
    public void restoreCompactionParams()
    {
        DatabaseDescriptor.setCompactionReadDiskAccessMode(originalDiskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(originalCursorCompactionEnabled);
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

        // Cursor compaction only supports BIG output, so under a non-BIG selected format — which is
        // what `ant test-latest` runs — the assertion below would fail for a reason that is not a
        // defect. Skip, and keep the assertion for every other unsupported-ness reason.
        Assume.assumeTrue("cursor compaction requires the BIG sstable format; selected=" +
                          DatabaseDescriptor.getSelectedSSTableFormat().name(),
                          BigFormat.isSelected());

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
     * The expectation is derived from the parameterization and the selected format rather than from
     * {@code isSupported}, so that it cannot become a tautology restating the predicate the pipeline
     * itself consults. Under a non-BIG selected format — {@code ant test-latest} selects BTI — the
     * cursor path is refused outright, so the iterator pipeline is the correct expectation there
     * rather than a skip: asserting it is true, cheap, and still non-vacuous.
     */
    protected void majorCompact(ColumnFamilyStore cfs)
    {
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        cfs.forceMajorCompaction();
        CompactionPipelineCounts.assertPipelineRan(cursorCompactionEnabled && BigFormat.isSelected(), before);
    }
}
