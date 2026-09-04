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

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The merge rule for a complex cell is "keep the highest timestamp", not "keep the cell from the
 * newest sstable". Every other scenario in this package writes ascending timestamps in sstable
 * order, so the two rules give the same answer everywhere and neither is distinguished. A cursor
 * that returned the last cell it read, rather than the one with the greater timestamp, passes all
 * of them.
 *
 * Each scenario here writes the LOWER timestamp into the LATER sstable, so the two rules disagree,
 * and asserts absolutely which value survived. Byte equivalence alone cannot catch this: both paths
 * could share the mistake. The assertions name the winning value.
 */
public class TimestampInversionDifferentialCompactionTest extends DifferentialCompactionTester
{
    /**
     * A map entry at one key, written with the greater timestamp first and the lesser second. The
     * merge must keep the first sstable's value.
     */
    @Test
    public void mapCellFromTheOlderSSTableWinsOnTimestamp() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET m['k'] = ?, v = ? WHERE pk = 0 AND ck = ?",
                    "high-ts-wins" + ck, "row" + ck, ck);
        flush();

        // Lower timestamp, later sstable: "newest sstable wins" would keep this one.
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ? WHERE pk = 0 AND ck = ?",
                    "low-ts-loses" + ck, ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            assertTrue("the greater timestamp must win even though it is in the older sstable, ck " + ck,
                       json.contains(cellValue("high-ts-wins" + ck)));
            assertFalse("the later sstable's lesser timestamp must not win, ck " + ck,
                        json.contains(cellValue("low-ts-loses" + ck)));
            assertEquals("exactly one cell must survive at this path, ck " + ck,
                         1, countOccurrences(json, cellValue("high-ts-wins" + ck)));
        }
    }

    /**
     * The same inversion over a set. A set element's value is empty and its identity is the cell
     * path, so the surviving cell is pinned through the row's simple column and the element count
     * rather than through a value.
     */
    @Test
    public void setElementFromTheOlderSSTableWinsOnTimestamp() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s set<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // The whole-collection assignment writes a complex deletion at TIMESTAMP 3000, so the
        // later, lower-timestamped add below is shadowed by the deletion as well as by the cell.
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET s = {'kept'}, v = ? WHERE pk = 0 AND ck = ?",
                    "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET s = s + {'shadowed'} WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the element added under the older complex deletion must not survive",
                     0, countOccurrences(json, "\"shadowed\""));
        assertEquals("the element written with the greater timestamp must survive in every row",
                     6, countOccurrences(json, "\"kept\""));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
    }

    /**
     * Three sstables where the middle one holds the winning timestamp. A cursor that tracked "the
     * last source to produce a cell at this path" keeps the third sstable's value; the rule keeps
     * the second's.
     */
    @Test
    public void middleSSTableHoldsTheWinningTimestamp() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET m['k'] = ?, v = ? WHERE pk = 0 AND ck = ?",
                    "first-2000" + ck, "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 5000 SET m['k'] = ? WHERE pk = 0 AND ck = ?",
                    "middle-5000-wins" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 3000 SET m['k'] = ? WHERE pk = 0 AND ck = ?",
                    "last-3000" + ck, ck);
        flush();

        assertEquals("scenario needs three input sstables", 3, cfs.getLiveSSTables().size());

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            assertTrue("the middle sstable's greater timestamp must win, ck " + ck,
                       json.contains(cellValue("middle-5000-wins" + ck)));
            assertFalse("the last sstable's lesser timestamp must not win, ck " + ck,
                        json.contains(cellValue("last-3000" + ck)));
            assertFalse("the first sstable's lesser timestamp must not win, ck " + ck,
                        json.contains(cellValue("first-2000" + ck)));
        }
    }

    /**
     * A complex deletion whose marked-for-delete timestamp is greater than a cell written into a
     * LATER sstable. The deletion must still shadow that cell.
     */
    @Test
    public void olderSSTableComplexDeletionShadowsANewerSSTableCell() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
        {
            execute("UPDATE %s USING TIMESTAMP 1000 SET v = ? WHERE pk = 0 AND ck = ?", "row" + ck, ck);
            execute("DELETE m FROM %s USING TIMESTAMP 4000 WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET m['k'] = ? WHERE pk = 0 AND ck = ?",
                    "under-deletion" + ck, ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            assertFalse("a cell below the older sstable's complex deletion must not survive, ck " + ck,
                        json.contains(cellValue("under-deletion" + ck)));
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
        }
    }
}
