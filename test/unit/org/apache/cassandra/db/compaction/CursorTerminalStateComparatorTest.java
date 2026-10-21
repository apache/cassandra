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

import static org.apache.cassandra.db.compaction.CursorCompactor.NO_TERMINAL_DECISION;
import static org.apache.cassandra.db.compaction.CursorCompactor.compareByTerminalState;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins the prologue every cursor comparator shares. A cursor at its terminal state sorts after one
 * that is not, and two live cursors are left for the caller to compare.
 *
 * Each comparator passes its own terminal state, so the rule is exercised with all three that the
 * production comparators use.
 */
public class CursorTerminalStateComparatorTest
{
    @Test
    public void bothTerminalCompareEqual()
    {
        assertEquals(0, compareByTerminalState(DONE, DONE, DONE));
        assertEquals(0, compareByTerminalState(PARTITION_END, PARTITION_END, PARTITION_END));
        assertEquals(0, compareByTerminalState(UNFILTERED_END, UNFILTERED_END, UNFILTERED_END));
    }

    @Test
    public void aTerminalCursorSortsAfterALiveOne()
    {
        assertEquals(1, compareByTerminalState(DONE, ROW_START, DONE));
        assertEquals(-1, compareByTerminalState(ROW_START, DONE, DONE));

        assertEquals(1, compareByTerminalState(PARTITION_END, ROW_START, PARTITION_END));
        assertEquals(-1, compareByTerminalState(ROW_START, PARTITION_END, PARTITION_END));
    }

    @Test
    public void twoLiveCursorsAreLeftToTheCaller()
    {
        assertEquals(NO_TERMINAL_DECISION, compareByTerminalState(ROW_START, CELL_HEADER_START, DONE));
        assertEquals(NO_TERMINAL_DECISION, compareByTerminalState(ROW_START, ROW_START, PARTITION_END));
    }

    /**
     * Each comparator asks about its own terminal state only. A cursor sitting at some other
     * terminal state is live as far as that comparator is concerned, so the caller decides.
     */
    @Test
    public void onlyTheGivenTerminalStateCounts()
    {
        assertEquals(NO_TERMINAL_DECISION, compareByTerminalState(PARTITION_END, ROW_START, DONE));
        assertEquals(NO_TERMINAL_DECISION, compareByTerminalState(DONE, ROW_START, PARTITION_END));
        assertEquals(NO_TERMINAL_DECISION, compareByTerminalState(UNFILTERED_END, ROW_START, DONE));
    }

    /** The sentinel must not collide with a real comparison result. */
    @Test
    public void theSentinelIsOutsideEveryRealResult()
    {
        assertTrue(NO_TERMINAL_DECISION < -1);
        assertEquals(Integer.MIN_VALUE, NO_TERMINAL_DECISION);
    }
}
