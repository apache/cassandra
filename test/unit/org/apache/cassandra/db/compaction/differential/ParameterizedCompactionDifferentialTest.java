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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Runs the path-agnostic {@link DifferentialSchemas#minimalCorpus() minimal schema corpus} through the
 * cursor-vs-iterator compaction differential across BOTH sstable formats (BIG and BTI). Each
 * (format, shape) cell is a real cursor-vs-iterator comparison: the harness asserts the cursor path
 * actually ran and that the output is in the selected format, then asserts byte + logical equivalence
 * against the iterator path.
 *
 * <p>The corpus itself carries no compaction, flush or read dependency, so the same fixtures can feed
 * read-path (20428) and flush-path (21554) differential engines on higher branches. The burn counterpart
 * is {@link BurnCompactionDifferentialTest}.
 */
@RunWith(Parameterized.class)
public class ParameterizedCompactionDifferentialTest extends DifferentialCompactionTester
{
    /** Fast-matrix scale: base shape counts, unmultiplied. */
    static final int MATRIX_SCALE = 1;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public DifferentialSchema schema;

    @Parameterized.Parameters(name = "{0}-{1}")
    public static List<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (DifferentialSchema schema : DifferentialSchemas.minimalCorpus())
                params.add(new Object[]{ format, schema });
        return params;
    }

    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void selectFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    @Test
    public void cursorMatchesIteratorForShape() throws Exception
    {
        createTable(schema.tableDefinition());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // adapter over this test's inherited CQLTester execute()/flush()
        DifferentialWorkload workload = new DifferentialWorkload()
        {
            @Override
            public void execute(String cql, Object... args)
            {
                ParameterizedCompactionDifferentialTest.this.execute(cql, args);
            }

            @Override
            public void flush()
            {
                ParameterizedCompactionDifferentialTest.this.flush();
            }
        };
        schema.write(workload, MATRIX_SCALE);

        assertLiveSSTablesOverlap(cfs, schema);
        if (schema.spansMultipleIndexBlocks())
            assertSomePartitionSpansMultipleBlocks(cfs, schema);

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Asserts at least two live sstables have overlapping key ranges, so a shape can never silently
     * degrade into a non-overlapping no-op merge. The weak "&gt;= 2 sstables" count is not enough: two
     * disjoint sstables would merge nothing.
     */
    static void assertLiveSSTablesOverlap(ColumnFamilyStore cfs, DifferentialSchema schema)
    {
        List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
        if (live.size() < 2)
            throw new AssertionError("shape '" + schema.name() + "' must leave at least two sstables to " +
                                     "merge; got " + live.size());
        for (int i = 0; i < live.size(); i++)
            for (int j = i + 1; j < live.size(); j++)
                if (rangesOverlap(live.get(i), live.get(j)))
                    return;
        throw new AssertionError("shape '" + schema.name() + "' left " + live.size() + " sstables but none " +
                                 "have overlapping key ranges: the merge would be a no-op concatenation, " +
                                 "not a real reconciliation");
    }

    /** Two sstables overlap when neither's key range ends before the other's begins. */
    private static boolean rangesOverlap(SSTableReader a, SSTableReader b)
    {
        return a.getFirst().compareTo(b.getLast()) <= 0 && b.getFirst().compareTo(a.getLast()) <= 0;
    }

    /**
     * Asserts at least one partition of some live sstable carries a promoted row index of more than one
     * block, proving the shape actually exercises the block-navigation path rather than silently shrinking
     * to a single-block partition.
     */
    static void assertSomePartitionSpansMultipleBlocks(ColumnFamilyStore cfs, DifferentialSchema schema)
    {
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    DecoratedKey key;
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        key = partition.partitionKey();
                    }
                    AbstractRowIndexEntry entry = sstable.getRowIndexEntry(key, SSTableReader.Operator.EQ);
                    if (entry != null && entry.blockCount() > 1)
                        return;
                }
            }
        }
        throw new AssertionError("shape '" + schema.name() + "' declares it spans multiple index blocks, " +
                                 "but no partition of the flushed sstables carries a promoted row index of " +
                                 "more than one block: the block-navigation path is not being exercised");
    }
}
