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

package org.apache.cassandra.db.memtable;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// Trie memtables identify columns by their unique id, and the order of the ids differs from the order in which
/// columns are presented. Check that the consumers of memtable data receive the columns in the correct order, and
/// that memtable rows can be accessed using column definitions that do not have a unique id.
public class TrieMemtableColumnIdTest extends CQLTester
{
    /// Creates a table where the complex column `m` has a lower id than the simple column `v`, which must be
    /// presented before it.
    private void createTableWithIdsNotInColumnOrder()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, int>) WITH memtable = 'trie'");
        alterTable("ALTER TABLE %s ADD v int");
        TableMetadata metadata = currentTableMetadata();
        assertTrue(metadata.getColumn(ByteBufferUtil.bytes("m")).uniqueId < metadata.getColumn(ByteBufferUtil.bytes("v")).uniqueId);
    }

    @Test
    public void testStorageAttachedIndex() throws Throwable
    {
        createTableWithIdsNotInColumnOrder();
        createIndex("CREATE INDEX ON %s(v) USING 'SAI'");
        testIndex();
    }

    @Test
    public void testLegacyIndex() throws Throwable
    {
        createTableWithIdsNotInColumnOrder();
        createIndex("CREATE INDEX ON %s(v)");
        testIndex();
    }

    private void testIndex() throws Throwable
    {
        assertEquals(TrieMemtable.class, getCurrentColumnFamilyStore().getCurrentMemtable().getClass());
        execute("INSERT INTO %s (k, v, m) VALUES (0, 1, {2: 3})");
        execute("INSERT INTO %s (k, v, m) VALUES (1, 1, {12: 13})");
        execute("INSERT INTO %s (k, v, m) VALUES (2, 2, {2: 3})");
        // Change the indexed value of a row, which must remove the old index entry.
        execute("UPDATE %s SET v = 2, m = m + {4: 5} WHERE k = 1");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT k, v, m FROM %s WHERE v = 1"),
                                    row(0, 1, map(2, 3)));
            assertRowsIgnoringOrder(execute("SELECT k, v, m FROM %s WHERE v = 2"),
                                    row(1, 2, map(4, 5, 12, 13)),
                                    row(2, 2, map(2, 3)));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 2 AND m CONTAINS 3 ALLOW FILTERING"),
                                    row(2));
            assertRowsIgnoringOrder(execute("SELECT k FROM %s WHERE v = 2 AND m CONTAINS KEY 4 ALLOW FILTERING"),
                                    row(1));
        });
    }

    @Test
    public void testColumnDefinitionsWithoutUniqueId() throws Throwable
    {
        createTableWithIdsNotInColumnOrder();
        execute("INSERT INTO %s (k, v, m) VALUES (0, 1, {2: 3, 4: 5})");

        MapType<Integer, Integer> mapType = MapType.getInstance(Int32Type.instance, Int32Type.instance, true);
        ColumnMetadata v = ColumnMetadata.regularColumn(KEYSPACE, currentTable(), "v", Int32Type.instance, ColumnMetadata.NO_UNIQUE_ID);
        ColumnMetadata m = ColumnMetadata.regularColumn(KEYSPACE, currentTable(), "m", mapType, ColumnMetadata.NO_UNIQUE_ID);
        ColumnMetadata missing = ColumnMetadata.regularColumn(KEYSPACE, currentTable(), "x", Int32Type.instance, ColumnMetadata.NO_UNIQUE_ID);

        Memtable memtable = getCurrentColumnFamilyStore().getCurrentMemtable();
        DecoratedKey key = currentTableMetadata().partitioner.decorateKey(Int32Type.instance.decompose(0));
        try (UnfilteredRowIterator partition = memtable.rowIterator(key))
        {
            Row row = (Row) partition.next();
            assertEquals(1, (int) Int32Type.instance.compose(row.getCell(v).buffer()));
            assertEquals(2, row.getComplexColumnData(m).cellsCount());
            assertNull(row.getCell(missing));
        }
    }
}
