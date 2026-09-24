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

package org.apache.cassandra.db.compaction.differential;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertTrue;

/**
 * Burn counterpart to {@link ParameterizedCompactionDifferentialTest}: runs the same
 * {@link DifferentialSchemas#minimalCorpus() corpus} at a large scale, under BTI only, so at least
 * {@link #MIN_TOTAL_ROWS} rows flow through the cursor-vs-iterator differential in one run.
 *
 * <p>Kept a separate class from the fast matrix so the matrix stays fast. The scale is read from the
 * {@value #SCALE_PROP} system property, defaulting to {@link #DEFAULT_SCALE}. The dump is captured as a
 * streaming digest ({@link #scaleCapture()}), so memory stays flat regardless of row count.
 */
public class BurnCompactionDifferentialTest extends DifferentialCompactionTester
{
    /** System property overriding the burn scale, for even larger local runs. */
    static final String SCALE_PROP = "cassandra.test.compaction_burn_scale";

    /** Default scale, chosen so the corpus yields at least {@link #MIN_TOTAL_ROWS} rows. */
    static final int DEFAULT_SCALE = 80;

    /** The floor the run must clear, proving it is a real burn and not a shrunken matrix. */
    static final long MIN_TOTAL_ROWS = 100_000;

    /** Digest-mode capture: the logical dump is streamed into a SHA-256, so capture memory stays flat. */
    @Override
    protected boolean scaleCapture()
    {
        return true;
    }

    @Before
    public void selectBti()
    {
        selectSSTableFormat("bti");
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    @Test
    public void burnCorpusUnderBti() throws Exception
    {
        int scale = Integer.getInteger(SCALE_PROP, DEFAULT_SCALE);
        assertTrue("burn scale must be positive", scale > 0);

        long totalRows = 0;
        for (DifferentialSchema schema : DifferentialSchemas.minimalCorpus())
        {
            ColumnFamilyStore cfs = writeAndGuardShape(schema, scale);
            totalRows += rowsOnDisk(cfs);
            assertCursorMatchesIterator(cfs);
        }

        logger.info("burn run at scale {} processed {} rows on disk across the corpus", scale, totalRows);
        assertTrue("burn run processed only " + totalRows + " rows at scale " + scale + "; expected at least " +
                   MIN_TOTAL_ROWS + " (raise -D" + SCALE_PROP + ")",
                   totalRows >= MIN_TOTAL_ROWS);
    }

    /** Sum of the per-sstable row counts across the live set (overlaps counted per sstable, as written). */
    private static long rowsOnDisk(ColumnFamilyStore cfs)
    {
        long rows = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            rows += sstable.getSSTableMetadata().totalRows;
        return rows;
    }
}
