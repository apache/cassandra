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

import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The cursor pairs every range tombstone open with a close, and reports one that is left open when
 * its partition ends.
 * <p>
 * A range never spans a partition, so the pairing state is per partition. If it were carried across
 * a partition boundary, a well-formed sstable would be reported as corrupt: the first close of the
 * next partition would look unmatched, or its first open would look like a double open. These tests
 * hold the well-formed cases clean, which is the direction a mistake in that reset would break.
 */
public class CursorRangeTombstonePairingTest extends CQLTester
{
    /** Walks the whole sstable, counting the markers it surfaces. */
    private static int driveCountingMarkers(StatefulCursor cursor)
    {
        int markers = 0;
        int state = cursor.readPartitionHeader();
        while (!isState(state, DONE))
        {
            if (isState(state, PARTITION_START))
            {
                state = cursor.readPartitionHeader();
            }
            else if (isState(state, STATIC_ROW_START))
            {
                cursor.readStaticRowHeader();
                state = cursor.state();
            }
            else if (isState(state, ROW_START))
            {
                cursor.readRowHeader();
                state = cursor.state();
            }
            else if (isState(state, TOMBSTONE_START))
            {
                cursor.readTombstoneMarker();
                markers++;
                state = cursor.state();
            }
            else if (isState(state, CELL_HEADER_START))
            {
                state = cursor.readCellHeader();
            }
            else if (isState(state, CELL_VALUE_START))
            {
                state = cursor.skipCellValue();
            }
            else
            {
                state = cursor.continueReading();
            }
        }
        return markers;
    }

    private SSTableReader theOnlySSTable(ColumnFamilyStore cfs)
    {
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        return cfs.getLiveSSTables().iterator().next();
    }

    /**
     * Several partitions, each with its own complete range. The pairing state has to be clean at
     * each partition header, or the second partition's open looks like a double open.
     */
    @Test
    public void aRangePerPartitionReadsClean()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int pk = 1; pk <= 4; pk++)
        {
            for (int ck = 1; ck <= 6; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", (long) pk, (long) ck, "v" + ck);
            execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck < ?", (long) pk, 2L, 5L);
        }
        flush();

        SSTableReader sstable = theOnlySSTable(cfs);
        int markers;
        try (StatefulCursor cursor = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            markers = driveCountingMarkers(cursor);
        }

        assertTrue("expected each of the four partitions to carry an open and a close, got " + markers,
                   markers >= 8);
        assertFalse("a well-formed sstable must not be marked suspect by the pairing check",
                    sstable.isMarkedSuspect());
    }

    /**
     * A partition whose range runs to the end of the partition, followed by another partition. This
     * is the shape that a stale pairing flag would misreport: the range closes at the partition's
     * last clustering, and the next partition opens its own.
     */
    @Test
    public void aRangeEndingAtThePartitionEndReadsClean()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int pk = 1; pk <= 3; pk++)
        {
            for (int ck = 1; ck <= 4; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", (long) pk, (long) ck, "v" + ck);
            // open-ended: the range has no upper bound within the partition
            execute("DELETE FROM %s WHERE pk = ? AND ck > ?", (long) pk, 2L);
        }
        flush();

        SSTableReader sstable = theOnlySSTable(cfs);
        try (StatefulCursor cursor = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            driveCountingMarkers(cursor);
        }
        assertFalse("an open-ended range that the partition closes must not be reported as unclosed",
                    sstable.isMarkedSuspect());
    }

    /**
     * Adjacent ranges that the writer folds into a boundary marker, which both closes and opens.
     * The pairing check treats a boundary as leaving a range open, so this pins that a boundary
     * followed by a real close still ends the partition clean.
     */
    @Test
    public void adjacentRangesFormingABoundaryReadClean()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int ck = 1; ck <= 12; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", (long) ck, "v" + ck);
        execute("DELETE FROM %s WHERE pk = 1 AND ck > 2 AND ck < 7");
        execute("DELETE FROM %s WHERE pk = 1 AND ck > 5 AND ck < 10");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 'other')");
        flush();

        SSTableReader sstable = theOnlySSTable(cfs);
        try (StatefulCursor cursor = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            driveCountingMarkers(cursor);
        }
        assertFalse("overlapping ranges folded into a boundary must read clean",
                    sstable.isMarkedSuspect());
    }
}
