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

package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.Collection;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RangeTombstoneBoundaryTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
    }

    @Parameterized.Parameter
    public boolean usCursorCompaction;

    @Parameterized.Parameters(name="cursorCompaction={0}")
    public static Collection<Object[]> parameterizations()
    {
        return Arrays.asList(new Object[][] { { false }, { true }});
    }

    @Test
    public void testOpenRangeTombstoneInLastBlock()
    {
        DatabaseDescriptor.setCursorCompactionEnabled(usCursorCompaction);

        createTable("CREATE TABLE %s (pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" +
                    " WITH compression = {'enabled': 'false'} AND compaction = {'class': 'LeveledCompactionStrategy', 'enabled': false}");

        // 30 rows with ~100-byte values → ~3 index blocks at 1 KiB each
        String pad = "x".repeat(80);
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", "p", String.format("r%03d", i), pad + i);

        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Open-ended delete: removes rows >= 27 (last block)
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ?", "p", "r027");

        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        getCurrentColumnFamilyStore().forceMajorCompaction();

        UntypedResultSet range = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r023");
        assertEquals("range select [r023, r029] should return 4 rows (23, 24, 25, 26)", 4, range.size());

        range = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r027");
        assertEquals(0, range.size());
    }

    @Test
    public void testOpenRangeTombstoneInMiddleBlock()
    {
        DatabaseDescriptor.setCursorCompactionEnabled(usCursorCompaction);

        createTable("CREATE TABLE %s (pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" +
                    " WITH compression = {'enabled': 'false'} AND compaction = {'class': 'LeveledCompactionStrategy', 'enabled': false}");

        // 30 rows with ~100-byte values → ~3 index blocks at 1 KiB each
        String pad = "x".repeat(80);
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", "p", String.format("r%03d", i), pad + i);

        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Open-ended delete: removes rows >= 15 (middle block)
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ?", "p", "r015");

        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        getCurrentColumnFamilyStore().forceMajorCompaction();

        UntypedResultSet range = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r012");
        assertEquals(3, range.size());

        range = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r023");
        assertEquals(0, range.size());
    }

    @Test
    public void testMidBlockRangeTombstoneInLastBlock()
    {
        DatabaseDescriptor.setCursorCompactionEnabled(usCursorCompaction);

        createTable("CREATE TABLE %s (pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" +
                    " WITH compression = {'enabled': 'false'} AND compaction = {'class': 'LeveledCompactionStrategy', 'enabled': false}");

        // 30 rows with ~100-byte values → ~3 index blocks at 1 KiB each
        String pad = "x".repeat(80);
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TIMESTAMP 1", "p", String.format("r%03d", i), pad + i);

        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Mid-block delete: removes rows 24-25 (last block)
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ? AND ck <= ?", "p", "r024", "r025");

        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        getCurrentColumnFamilyStore().forceMajorCompaction();

        UntypedResultSet range = execute("SELECT * FROM %s WHERE pk = ? AND ck >= ?", "p", "r023");
        assertEquals("range select [r023, r029] should return 5 rows (23, 26, 27, 28, 29)", 5, range.size());
    }
}
