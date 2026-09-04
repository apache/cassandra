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
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Element-level operations on a collection, which the rest of this package leaves almost entirely
 * untested.
 *
 * A set or list element tombstone reaches the cell merge only through {@code s = s - {...}},
 * {@code l = l - [...]}, {@code DELETE l[i]} or {@code m = m - {k}}. A map key tombstone also
 * comes from {@code DELETE m[k]}.
 *
 * Two live cells at the SAME path in different sstables reach the cell-level timestamp compare
 * only when no complex deletion sits between them. A whole-collection write emits one, which
 * shadows the older cell instead of reconciling it, so the adding forms above are what put two
 * live cells at one path.
 *
 * Each scenario asserts absolutely which cells survived. The differential harness proves the two
 * paths agree; it cannot see a rule they both get wrong.
 */
public class CollectionElementMergeDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** {@code s = s - {...}}: a set element tombstone over a live cell written earlier. */
    @Test
    public void setElementRemovalAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s set<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET s = s + {'removed', 'survivor'}, v = ? " +
                    "WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET s = s - {'removed'} WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the removed element must not survive as a LIVE cell in any row",
                     0, countOccurrences(json, liveElement("removed")));
        assertEquals("the removal must leave an element tombstone in every row",
                     6, countOccurrences(json, deletedElement("removed")));
        assertEquals("the untouched element must survive in every row",
                     6, countOccurrences(json, liveElement("survivor")));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
    }

    /**
     * The reverse order: the element tombstone is written FIRST, at the lower timestamp, and the
     * live cell arrives later at the higher timestamp. The live cell must win.
     */
    @Test
    public void setElementTombstoneMeetsALaterLiveCell() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s set<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
        {
            execute("UPDATE %s USING TIMESTAMP 1000 SET v = ? WHERE pk = 0 AND ck = ?", "row" + ck, ck);
            execute("UPDATE %s USING TIMESTAMP 1000 SET s = s - {'resurrected'} WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET s = s + {'resurrected'} WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("a later live element must win over an earlier element tombstone",
                     6, countOccurrences(json, "\"resurrected\""));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
    }

    /**
     * Two LIVE set element cells at the same path in different sstables, with no complex deletion
     * between them. A whole-collection write emits a deletion that shadows the older cell, so only
     * the adding form reaches the cell-level compare.
     */
    @Test
    public void setLiveCellsAtOnePathWithNoInterveningDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s set<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // s + {...} adds cells without a complex deletion, unlike SET s = {...}.
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET s = s + {'shared'}, v = ? WHERE pk = 0 AND ck = ?",
                    "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET s = s + {'shared'} WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("two live cells at one set path must merge to exactly one per row",
                     6, countOccurrences(json, "\"shared\""));
    }

    /** A set element expiring by TTL while another element of the same set stays live. */
    @Test
    public void setElementTtlAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s set<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET s = s + {'stays'}, v = ? WHERE pk = 0 AND ck = ?",
                    "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 AND TTL 1 SET s = s + {'expires'} WHERE pk = 0 AND ck = ?", ck);
        flush();

        long pinnedNow = FBUtilities.nowInSeconds() + 60;
        assertSomethingExpiredAt(cfs, pinnedNow);
        String json = allJson(assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                          taskWithFixedNow(pinnedNow),
                                                          cfs.getDefaultGcBefore(pinnedNow)));
        assertEquals("the expired set element must not survive as a live cell",
                     0, countOccurrences(json, cellValue("expires")));
        assertEquals("the live element of the same set must survive in every row",
                     6, countOccurrences(json, "\"stays\""));
    }

    /** {@code l = l - [...]}: removal by value, which deletes every element holding that value. */
    @Test
    public void listElementRemovalByValueAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET l = l + ['removed', 'survivor'], v = ? " +
                    "WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        flush();

        // Removal by value reads the list first, so it needs its own timestamp to sort after.
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET l = l - ['removed'] WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the removed list element must not survive as a live cell",
                     0, countOccurrences(json, cellValue("removed")));
        assertEquals("the untouched list element must survive in every row",
                     6, countOccurrences(json, cellValue("survivor")));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
    }

    /**
     * {@code DELETE l[i]} and {@code SET l[i] = ?}: the index forms, which resolve an index to a
     * timeuuid cell path by reading the list first. Neither appeared anywhere in the tree.
     */
    @Test
    public void listElementDeleteAndOverwriteByIndexAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET l = ['doomed', 'rewritten', 'survivor'], v = ? " +
                    "WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        flush();

        // Index forms resolve against the list as it stands when the statement runs, so the delete
        // would renumber the list under the overwrite. Overwrite first, then delete index 0.
        for (long ck = 0; ck < 6; ck++)
        {
            execute("UPDATE %s USING TIMESTAMP 2000 SET l[1] = ? WHERE pk = 0 AND ck = ?", "replacement", ck);
            execute("DELETE l[0] FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the element deleted by index must not survive",
                     0, countOccurrences(json, cellValue("doomed")));
        assertEquals("the element overwritten by index must not keep its old value",
                     0, countOccurrences(json, cellValue("rewritten")));
        assertEquals("the replacement value must survive in every row",
                     6, countOccurrences(json, cellValue("replacement")));
        assertEquals("the untouched element must survive in every row",
                     6, countOccurrences(json, cellValue("survivor")));
    }

    /** A list element expiring by TTL while another element of the same list stays live. */
    @Test
    public void listElementTtlAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET l = l + ['stays'], v = ? WHERE pk = 0 AND ck = ?",
                    "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 AND TTL 1 SET l = l + ['expires'] WHERE pk = 0 AND ck = ?", ck);
        flush();

        long pinnedNow = FBUtilities.nowInSeconds() + 60;
        assertSomethingExpiredAt(cfs, pinnedNow);
        String json = allJson(assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                          taskWithFixedNow(pinnedNow),
                                                          cfs.getDefaultGcBefore(pinnedNow)));
        assertEquals("the expired list element must not survive as a live cell",
                     0, countOccurrences(json, cellValue("expires")));
        assertEquals("the live element of the same list must survive in every row",
                     6, countOccurrences(json, cellValue("stays")));
    }

    /**
     * {@code m = m - {k}}: the set-subtraction form of a map key delete. It produces the same cell
     * tombstone as {@code DELETE m[k]}, which is covered, but the statement form itself appeared in
     * no running test.
     */
    @Test
    public void mapKeyRemovalBySubtractionAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m = m + ?, v = ? WHERE pk = 0 AND ck = ?",
                    map("gone", "removed" + ck, "kept", "survivor" + ck), "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET m = m - {'gone'} WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            assertFalse("the subtracted map key must not survive, ck " + ck,
                        json.contains(cellValue("removed" + ck)));
            assertTrue("the untouched map key must survive, ck " + ck,
                       json.contains(cellValue("survivor" + ck)));
        }
    }

    /**
     * {@code SET u.b = null}: a single UDT field set to null, which writes a cell tombstone at that
     * field's path while the column's other fields stay live. The field-level counterpart of a
     * collection element tombstone, and absent from the branch.
     */
    @Test
    public void udtFieldSetToNullAcrossSSTables() throws Exception
    {
        String udt = createType("CREATE TYPE %s (f1 text, f2 text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET u.f1 = ?, u.f2 = ?, v = ? WHERE pk = 0 AND ck = ?",
                    "nulled" + ck, "survivor" + ck, "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET u.f1 = null WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            assertFalse("the nulled UDT field must not survive, ck " + ck,
                        json.contains(cellValue("nulled" + ck)));
            assertTrue("the other UDT field must survive, ck " + ck,
                       json.contains(cellValue("survivor" + ck)));
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
        }
    }

    /**
     * A set element's identity is its cell PATH, not its value, so a tombstoned element still
     * renders its path in the dump. These needles separate a live element from a deleted one.
     * JsonTransformer.serializeCell writes cells compactly, so the fields are adjacent.
     */
    private static String liveElement(String element)
    {
        return "\"path\":[\"" + element + "\"],\"value\"";
    }

    private static String deletedElement(String element)
    {
        return "\"path\":[\"" + element + "\"],\"deletion_info\"";
    }
}
