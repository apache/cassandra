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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The cursor path accepts a partial-range scanner. */
public class CursorPartialRangeGateTest extends CQLTester
{
    private ColumnFamilyStore twoSSTablesOfManyPartitions()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 40; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, 1, ?)", pk, "a" + pk);
        flush();
        for (long pk = 40; pk < 80; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, 1, ?)", pk, "b" + pk);
        flush();
        return cfs;
    }

    /** The token range covering everything. */
    private List<Range<Token>> fullRange(ColumnFamilyStore cfs)
    {
        Token min = cfs.getPartitioner().getMinimumToken();
        return Collections.singletonList(new Range<>(min, min));
    }

    /** A range that splits the ring, so at least one sstable is read only in part. */
    private List<Range<Token>> halfRange(ColumnFamilyStore cfs)
    {
        List<Token> tokens = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            tokens.add(sstable.getFirst().getToken());
            tokens.add(sstable.getLast().getToken());
        }
        Collections.sort(tokens);
        Token min = cfs.getPartitioner().getMinimumToken();
        return Collections.singletonList(new Range<>(min, tokens.get(tokens.size() / 2)));
    }

    /** Whether the gate can accept any compaction under the running configuration. */
    private static boolean cursorSupportsSelectedFormat()
    {
        return DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction();
    }

    private boolean isSupportedOver(ColumnFamilyStore cfs, List<Range<Token>> ranges) throws Exception
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        try (CompactionController controller = new CompactionController(cfs, inputs, FBUtilities.nowInSeconds());
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), ranges))
        {
            return CursorCompactor.isSupported(scanners, controller);
        }
    }

    /** A whole-ring compaction is supported. */
    @Test
    public void aFullRangeCompactionIsSupported() throws Exception
    {
        ColumnFamilyStore cfs = twoSSTablesOfManyPartitions();
        assertEquals("a full-range compaction is cursor-supported whenever the selected format is",
                     cursorSupportsSelectedFormat(), isSupportedOver(cfs, fullRange(cfs)));
    }

    /** The half-range fixture must actually produce a partial scanner. */
    @Test
    public void theHalfRangeFixtureProducesAPartialScanner()
    {
        ColumnFamilyStore cfs = twoSSTablesOfManyPartitions();
        List<Range<Token>> ranges = halfRange(cfs);

        boolean sawPartial = false;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(ranges);
            if (bounds.isEmpty())
            {
                sawPartial = true;
                continue;
            }
            long covered = 0;
            for (PartitionPositionBounds b : bounds)
                covered += b.upperPosition - b.lowerPosition;
            if (covered < sstable.uncompressedLength())
                sawPartial = true;
        }
        assertTrue("the half-range fixture must leave at least one sstable partly covered, "
                   + "or the gate test below proves nothing", sawPartial);
    }

    /** A partial-range compaction is supported. */
    @Test
    public void aPartialRangeCompactionIsSupported() throws Exception
    {
        ColumnFamilyStore cfs = twoSSTablesOfManyPartitions();
        assertEquals("a partial-range compaction is cursor-supported whenever the selected format is: a shard "
                     + "task whose sstables straddle a shard boundary would otherwise fall back to the iterator path",
                     cursorSupportsSelectedFormat(), isSupportedOver(cfs, halfRange(cfs)));
    }

}
