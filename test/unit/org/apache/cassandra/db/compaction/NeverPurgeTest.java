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

import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NeverPurgeTest extends CQLTester
{
    static
    {
        System.setProperty("cassandra.never_purge_tombstones", "true");
    }

    @Test
    public void neverPurgeCellTombstoneTest() throws Throwable
    {
        testHelper("UPDATE %s SET c = null WHERE a=1 AND b=2");
    }

    @Test
    public void neverPurgeRowTombstoneTest() throws Throwable
    {
        testHelper("DELETE FROM %s WHERE a=1 AND b=2");
    }

    @Test
    public void neverPurgePartitionTombstoneTest() throws Throwable
    {
        testHelper("DELETE FROM %s WHERE a=1");
    }

    @Test
    public void minorNeverPurgeTombstonesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        for (int i = 0; i < 4; i++)
        {
            for (int j = 0; j < 1000; j++)
            {
                execute("INSERT INTO %s (a, b, c) VALUES (" + j + ", 2, '3')");
            }
            cfs.forceBlockingFlush();
        }

        execute("UPDATE %s SET c = null WHERE a=1 AND b=2");
        execute("DELETE FROM %s WHERE a=2 AND b=2");
        execute("DELETE FROM %s WHERE a=3");
        cfs.forceBlockingFlush();
        cfs.enableAutoCompaction();
        while (cfs.getLiveSSTables().size() > 1 || !cfs.getTracker().getCompacting().isEmpty())
            Thread.sleep(100);
        verifyContainsTombstones(cfs.getLiveSSTables(), 3);
    }

    private void testHelper(String deletionStatement) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b)) WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, '3')");
        execute(deletionStatement);
        Thread.sleep(1000);
        cfs.forceBlockingFlush();
        cfs.forceMajorCompaction();
        verifyContainsTombstones(cfs.getLiveSSTables(), 1);
    }

    private void verifyContainsTombstones(Collection<SSTableReader> sstables, int expectedTombstoneCount) throws Exception
    {
        assertTrue(sstables.size() == 1); // always run a major compaction before calling this
        SSTableReader sstable = sstables.iterator().next();
        int tombstoneCount = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    if (!iter.partitionLevelDeletion().isLive())
                        tombstoneCount++;

                    while (iter.hasNext())
                    {
                        Unfiltered atom = iter.next();
                        if (atom.isRow())
                        {
                            Row r = (Row)atom;
                            if (!r.deletion().isLive())
                                tombstoneCount++;
                            for (Cell c : r.cells())
                                if (c.isTombstone())
                                    tombstoneCount++;
                        }
                    }
                }
            }
        }
        assertEquals(tombstoneCount, expectedTombstoneCount);
    }
}
