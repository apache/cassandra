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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReader;

public class RangeTombstoneMergeTest extends CQLTester
{
    @Before
    public void before() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                "  key text," +
                "  column text," +
                "  data text," +
                "  extra text," +
                "  PRIMARY KEY(key, column)" +
                ");");

        // If the sstable only contains tombstones during compaction it seems that the sstable either gets removed or isn't created (but that could probably be a separate JIRA issue).
        execute("INSERT INTO %s (key, column, data) VALUES (?, ?, ?)", "1", "1", "1");
    }

    @Test
    public void testEqualMerge() throws Throwable
    {
        addRemoveAndFlush();

        for (int i=0; i<3; ++i)
        {
            addRemoveAndFlush();
            compact();
        }

        assertOneTombstone();
    }

    @Test
    public void testRangeMerge() throws Throwable
    {
        addRemoveAndFlush();

        execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", "1", "2", "2", "2");
        execute("DELETE extra FROM %s WHERE key=? AND column=?", "1", "2");

        flush();
        compact();

        execute("DELETE FROM %s WHERE key=? AND column=?", "1", "2");

        flush();
        compact();

        assertOneTombstone();
    }

    void assertOneTombstone() throws Throwable
    {
        assertRows(execute("SELECT column FROM %s"),
                   row("1"));
        assertAllRows(row("1", "1", "1", null));

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        ColumnFamily cf = cfs.getColumnFamily(Util.dk("1"), Composites.EMPTY, Composites.EMPTY, false, 100, System.currentTimeMillis());
        assertTrue(cf.deletionInfo().hasRanges());
        assertEquals(1, cf.deletionInfo().rangeCount());    // Ranges merged during CF construction

        assertEquals(1, cfs.getSSTables().size());
        SSTableReader reader = Iterables.get(cfs.getSSTables(), 0);
        assertEquals(1, countTombstones(reader));           // See CASSANDRA-7953.
    }

    void addRemoveAndFlush() throws Throwable
    {
        execute("INSERT INTO %s (key, column, data) VALUES (?, ?, ?)", "1", "2", "2");
        execute("DELETE FROM %s WHERE key=? AND column=?", "1", "2");
        flush();
    }

    int countTombstones(SSTableReader reader)
    {
        int tombstones = 0;
        ISSTableScanner partitions = reader.getScanner();
        while (partitions.hasNext())
        {
            OnDiskAtomIterator iter = partitions.next();
            while (iter.hasNext())
            {
                OnDiskAtom atom = iter.next();
                if (atom instanceof RangeTombstone)
                    ++tombstones;
            }
        }
        return tombstones;
    }
}
