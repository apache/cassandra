/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.memtable;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;

/**
 * Partition and range deletions with the same timestamp, written to the same memtable.
 */
@RunWith(Parameterized.class)
public class EqualTimestampTombstoneTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        // "default" sets no memtable option
        return Arrays.asList("default", "trie", "trie_stage1", "trie_stage2", "trie_stage3", "skiplist");
    }

    private static final String ASC = "CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))";
    private static final String DESC = ASC + " WITH CLUSTERING ORDER BY (ck DESC)";

    private static final String[] ASC_DELETES = {
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1",
        "DELETE FROM %s USING TIMESTAMP 4 WHERE pk = 1 AND ck = 3",
        "DELETE FROM %s USING TIMESTAMP 2 WHERE pk = 1 AND ck >= 3",
        "DELETE FROM %s USING TIMESTAMP 3 WHERE pk = 1 AND ck > 1"
    };

    private static final String[] DESC_DELETES = {
        "DELETE FROM %s USING TIMESTAMP 6 WHERE pk = 1",
        "DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck >= 0 AND ck <= 5",
        "DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck = 7",
        "DELETE FROM %s USING TIMESTAMP 6 WHERE pk = 1 AND ck >= 1 AND ck < 7"
    };

    /// [#DESC_DELETES] on an ascending table.
    private static final String[] ASC_MIRROR_DELETES = {
        "DELETE FROM %s USING TIMESTAMP 6 WHERE pk = 1",
        "DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck >= 3 AND ck <= 8",
        "DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck = 1",
        "DELETE FROM %s USING TIMESTAMP 6 WHERE pk = 1 AND ck > 1 AND ck <= 7"
    };

    /// A tied range that covers a newer range on both sides.
    private static final String[] NEVER_CLOSED_DELETES = {
        "DELETE FROM %s USING TIMESTAMP 6 WHERE pk = 1",
        "DELETE FROM %s USING TIMESTAMP 10 WHERE pk = 1 AND ck >= 3 AND ck <= 5",
        "DELETE FROM %s USING TIMESTAMP 6 WHERE pk = 1 AND ck >= 1 AND ck <= 8"
    };

    private String withMemtable(String createTable)
    {
        if ("default".equals(memtable))
            return createTable;
        return createTable + (createTable.contains(" WITH ") ? " AND" : " WITH") + " memtable = '" + memtable + '\'';
    }

    private void writeAndDelete(String createTable, String[] deletes)
    {
        writeAndDelete(createTable, deletes, true);
    }

    /// With `flushRows`, the rows are flushed before the deletions are written.
    private void writeAndDelete(String createTable, String[] deletes, boolean flushRows)
    {
        createTable(withMemtable(createTable));
        disableCompaction();
        for (int ck = 0; ck < 10; ++ck)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?) USING TIMESTAMP 1", ck, ck);
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 0, 0) USING TIMESTAMP 1");
        if (flushRows)
            flush();

        for (String delete : deletes)
            execute(delete);
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 4, 40) USING TIMESTAMP 20");
    }

    private void assertSurvivors()
    {
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1"), row(1, 4, 40));
        // one of the two orders is a reversed read of the partition
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1 ORDER BY ck ASC"), row(1, 4, 40));
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 1 ORDER BY ck DESC"), row(1, 4, 40));
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 2"), row(2, 0, 0));
        assertRowCount(execute("SELECT * FROM %s"), 2);
    }

    private void checkFlushAndCompaction()
    {
        assertSurvivors();
        flush();
        assertSurvivors();
        compact();
        assertSurvivors();
    }

    @Test
    public void testAscendingFlush()
    {
        writeAndDelete(ASC, ASC_DELETES);
        checkFlushAndCompaction();
    }

    @Test
    public void testDescendingFlush()
    {
        writeAndDelete(DESC, DESC_DELETES);
        checkFlushAndCompaction();
    }

    @Test
    public void testAscendingMirrorFlush()
    {
        writeAndDelete(ASC, ASC_MIRROR_DELETES);
        checkFlushAndCompaction();
    }

    @Test
    public void testNeverClosedRange()
    {
        writeAndDelete(ASC, NEVER_CLOSED_DELETES);
        checkFlushAndCompaction();
    }

    @Test
    public void testPartitionDeletionLast()
    {
        writeAndDelete(DESC, new String[]{ DESC_DELETES[1], DESC_DELETES[2], DESC_DELETES[3], DESC_DELETES[0] });
        checkFlushAndCompaction();
    }

    /// A deletion of a complex column inside a range, with a tied partition and range deletion around it.
    @Test
    public void testComplexColumnDeletion()
    {
        createTable(withMemtable("CREATE TABLE %s (pk int, ck1 int, ck2 int, c2 set<int>, PRIMARY KEY (pk, ck1, ck2))"));
        disableCompaction();
        execute("DELETE c2 FROM %s USING TIMESTAMP 5 WHERE pk = 0 AND ck1 = 3 AND ck2 = 2");
        execute("DELETE FROM %s USING TIMESTAMP 4 WHERE pk = 0");
        execute("DELETE FROM %s USING TIMESTAMP 4 WHERE pk = 0 AND (ck1, ck2) > (2, 1)");
        execute("DELETE FROM %s USING TIMESTAMP 5 WHERE pk = 0 AND ck1 >= 0");

        assertEmpty(execute("SELECT * FROM %s WHERE pk = 0"));
        flush();
        assertEmpty(execute("SELECT * FROM %s WHERE pk = 0"));
        compact();
        assertEmpty(execute("SELECT * FROM %s WHERE pk = 0"));
    }

    private void checkSaiQueries(String createTable, String[] deletes)
    {
        createTable(withMemtable(createTable));
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        disableCompaction();
        for (int ck = 0; ck < 10; ++ck)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, 5) USING TIMESTAMP 1", ck);
        for (String delete : deletes)
            execute(delete);
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 4, 5) USING TIMESTAMP 20");

        assertRows(execute("SELECT pk, ck FROM %s WHERE v = 5"), row(1, 4));
        flush();
        assertRows(execute("SELECT pk, ck FROM %s WHERE v = 5"), row(1, 4));
        compact();
        assertRows(execute("SELECT pk, ck FROM %s WHERE v = 5"), row(1, 4));
    }

    @Test
    public void testSaiAscending()
    {
        checkSaiQueries(ASC, ASC_DELETES);
    }

    /// Replay may apply the mutations in a different order than they were written.
    @Test
    public void testCommitLogReplay() throws Throwable
    {
        writeAndDelete(DESC, DESC_DELETES, false);
        assertSurvivors();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.clearUnsafe(); // drops the memtable
        assertEmpty(execute("SELECT * FROM %s"));
        CommitLog.instance.resetUnsafe(false);

        assertSurvivors();
        flush();
        assertSurvivors();
        compact();
        assertSurvivors();
    }
}
