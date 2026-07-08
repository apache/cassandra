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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.big.BigFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Validates the logical correctness of cursor compaction's resolveRegular() same-timestamp
 * tie-break, the bug CASSANDRA-21356 owns: a tombstone cell must beat an expiring cell at an
 * identical timestamp.
 *
 * Before the fix, ReusableLivenessInfo.isExpiring() checked {@code localExpirationTime !=
 * NO_EXPIRATION_TIME} instead of {@code ttl != NO_TTL}. A tombstone cell also has a non-default
 * localExpirationTime (it stores the deletion timestamp there), so isExpiring() returned true for
 * both tombstone and expiring cells. resolveRegular() used {@code !isExpiring()} to identify
 * tombstones, so both cells looked identical to it and it fell through to comparing
 * localExpirationTime values — an expiring cell's is a future timestamp, a tombstone's is a past
 * deletion timestamp, so the expiring cell always won, resurrecting an explicitly deleted column.
 * See ReusableLivenessInfoTest for direct unit coverage of the root-cause isExpiring() check
 * itself (the general tombstone case, independent of any tie-break).
 *
 * This is asserted by querying the compacted table back rather than by comparing raw Data.db /
 * Index.db bytes against the iterator compaction path. SSTableCursorWriter has other, unrelated
 * byte-encoding gaps (tracked separately as CASSANDRA-21336, CASSANDRA-21357, and CASSANDRA-21358)
 * that make cursor and iterator compaction produce different raw bytes for reasons that have
 * nothing to do with this bug — a byte-for-byte comparison here would fail regardless of whether
 * this specific bug is fixed, so this test checks queryable behavior instead.
 */
public class CursorCompactionEquivalenceTest extends CQLTester
{
    private boolean origCursorEnabled;

    @Before
    public void guardAndSave()
    {
        assumeTrue("Cursor compaction requires BIG SSTable format", BigFormat.isSelected());
        origCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCursorCompactionEnabled(true);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setCursorCompactionEnabled(origCursorEnabled);
    }

    // ── same-timestamp tombstone vs expiring cell tie-break ──────────────────────
    // Exercises resolveRegular(): tombstone must beat expiring cell at identical timestamp.
    // Without the fix, ReusableLivenessInfo.isExpiring() returns true for tombstones,
    // causing resolveRegular() to misidentify the tombstone and pick the expiring cell instead.

    @Test
    public void testSameTimestampTieBreak() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))" +
                    " WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // SSTable 1: tombstone cell for v at timestamp 100
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, null) USING TIMESTAMP 100");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // SSTable 2: expiring cell for v at the SAME timestamp 100 — tombstone must win
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 0, 'x') USING TIMESTAMP 100 AND TTL 3600");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        cfs.forceMajorCompaction();

        UntypedResultSet rs = execute("SELECT v FROM %s WHERE pk = 0 AND ck = 0");
        assertEquals(1, rs.size());
        assertFalse("Tombstone must beat the expiring cell at an identical timestamp",
                    rs.one().has("v"));
    }
}
