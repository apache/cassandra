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

package org.apache.cassandra.db;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public abstract class AbstractSerializationHeaderCQLTest extends CQLTester
{
    @Test
    public void testDroppedColumnPresenceInSerialisationHeader()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, a text, b text, c text)");
        execute("INSERT INTO %s (id, a, b, c) VALUES (1, 'a1', 'b1', 'c1')");
        flush(keyspace());
        alterTable("ALTER TABLE %s DROP b");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTrue("Column 'b' should be in the flushed SSTable header",
                   sstableHeaderContainsColumn(sstable, "b"));

        // same but alter before flush

        createTable("CREATE TABLE %s (id int PRIMARY KEY, a text, b text, c text)");
        execute("INSERT INTO %s (id, a, b, c) VALUES (1, 'a1', 'b1', 'c1')");
        alterTable("ALTER TABLE %s DROP b");
        flush(keyspace());

        ColumnFamilyStore cfs2 = getCurrentColumnFamilyStore();
        assertEquals(1, cfs2.getLiveSSTables().size());

        SSTableReader sstable2 = cfs2.getLiveSSTables().iterator().next();
        assertTrue("Column 'b' should be in the flushed SSTable header",
                   sstableHeaderContainsColumn(sstable2, "b"));

        // flush but without dropped column

        createTable("CREATE TABLE %s (id int PRIMARY KEY, a text, b text, c text)");
        // we are not populating column we go to drop on purpose
        execute("INSERT INTO %s (id, a, c) VALUES (1, 'a1', 'c1')");
        alterTable("ALTER TABLE %s DROP b");
        flush(keyspace());

        ColumnFamilyStore cfs3 = getCurrentColumnFamilyStore();
        assertEquals(1, cfs3.getLiveSSTables().size());

        SSTableReader sstable3 = cfs3.getLiveSSTables().iterator().next();
        assertFalse("Column 'b' should not be in the flushed SSTable header",
                    sstableHeaderContainsColumn(sstable3, "b"));
    }

    @Test
    public void testDroppedColumnNotInCompactedSerialisationHeader()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, a text, b text, c text)");
        execute("INSERT INTO %s (id, a, b, c) VALUES (1, 'a1', 'b1', 'c1')");
        alterTable("ALTER TABLE %s DROP b");
        flush(keyspace());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        assertTrue("Column 'b' should be in the flushed SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "b"));

        compact(keyspace(), currentTable());

        assertEquals(1, cfs.getLiveSSTables().size());

        // column dropped for a header after compaction
        assertFalse("Dropped column 'b' should not be in the compacted SSTable header",
                    sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "b"));

        assertTrue("Column 'a' should be in the compacted SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "a"));
        assertTrue("Column 'c' should be in the compacted SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "c"));

        assertRows(execute("SELECT id, a, c FROM %s"), row(1, "a1", "c1"));
    }

    @Test
    public void testReAddedColumnInCompactedSerializationHeader()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, a text, b text, c text)");
        execute("INSERT INTO %s (id, a, b, c) VALUES (1, 'a1', 'b1', 'c1')");
        alterTable("ALTER TABLE %s DROP b");
        alterTable("ALTER TABLE %s ADD b text");
        flush(keyspace());

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(1, cfs.getLiveSSTables().size());
        assertTrue("Column 'b' should be in the flushed SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "b"));

        compact(keyspace(), currentTable());

        assertEquals(1, cfs.getLiveSSTables().size());

        // re-added column b
        assertTrue("Dropped column 'b' should be in the compacted SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "b"));

        assertTrue("Column 'a' should be in the compacted SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "a"));
        assertTrue("Column 'c' should be in the compacted SSTable header",
                   sstableHeaderContainsColumn(cfs.getLiveSSTables().iterator().next(), "c"));

        assertRows(execute("SELECT id, a, b, c FROM %s"), row(1, "a1", null, "c1"));
    }

    /**
     * Checks if a column with the given name exists in the SSTable header.
     */
    private boolean sstableHeaderContainsColumn(SSTableReader sstable, String columnName)
    {
        for (ColumnMetadata column : sstable.header.columns())
        {
            if (column.name.toString().equals(columnName))
                return true;
        }
        return false;
    }
}
