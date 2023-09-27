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

package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class ForceCompactionTest extends CQLTester
{
    private final static int NUM_PARTITIONS = 10;
    private final static int NUM_ROWS = 100;

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c1 text, c2 text, c3 text, PRIMARY KEY (key, c1))");

        for (int partitionCount = 0; partitionCount < NUM_PARTITIONS; partitionCount++)
        {
            for (int rowCount = 0; rowCount < NUM_ROWS; rowCount++)
            {
                execute("INSERT INTO %s (key, c1, c2, c3) VALUES (?, ?, ?, ?)",
                        "k" + partitionCount, "c1_" + rowCount, "c2_" + rowCount, "c3_" + rowCount);
            }
        }

        // Disable auto compaction
        // NOTE: We can only disable the auto compaction once the table is created because the setting is on
        // the table level. And we don't need to re-enable it back because the table will be dropped after the test.
        disableCompaction();
    }

    @Test
    public void forceCompactPartitionTombstoneTest() throws Throwable
    {
        String keyToPurge = "k0";

        testHelper("DELETE FROM %s WHERE key = ?", keyToPurge);
    }

    @Test
    public void forceCompactMultiplePartitionsTombstoneTest() throws Throwable
    {
        List<String> keysToPurge = new ArrayList<>();
        Random rand = new Random();

        int numPartitionsToPurge = 1 + rand.nextInt(NUM_PARTITIONS);
        for (int count = 0; count < numPartitionsToPurge; count++)
        {
            String key = "k" + rand.nextInt(NUM_PARTITIONS);

            execute("DELETE FROM %s WHERE key = ?", key);
            keysToPurge.add(key);
        }

        flush();

        String[] keys = new String[keysToPurge.size()];
        keys = keysToPurge.toArray(keys);
        forceCompact(keys);

        verifyNotContainsTombstones();
    }

    @Test
    public void forceCompactRowTombstoneTest() throws Throwable
    {
        String keyToPurge = "k0";

        testHelper("DELETE FROM %s WHERE key = ? AND c1 = 'c1_0'", keyToPurge);
    }

    @Test
    public void forceCompactMultipleRowsTombstoneTest() throws Throwable
    {
        List<String> keysToPurge = new ArrayList<>();

        Random randPartition = new Random();
        Random randRow = new Random();

        for (int count = 0; count < 10; count++)
        {
            String partitionKey = "k" + randPartition.nextInt(NUM_PARTITIONS);
            String clusteringKey = "c1_" + randRow.nextInt(NUM_ROWS);

            execute("DELETE FROM %s WHERE key = ? AND c1 = ?", partitionKey, clusteringKey);
            keysToPurge.add(partitionKey);
        }

        flush();

        String[] keys = new String[keysToPurge.size()];
        keys = keysToPurge.toArray(keys);
        forceCompact(keys);

        verifyNotContainsTombstones();
    }

    @Test
    public void forceCompactCellTombstoneTest() throws Throwable
    {
        String keyToPurge = "k0";
        testHelper("DELETE c2 FROM %s WHERE key = ? AND c1 = 'c1_0'", keyToPurge);
    }

    @Test
    public void forceCompactMultipleCellsTombstoneTest() throws Throwable
    {
        List<String> keysToPurge = new ArrayList<>();

        Random randPartition = new Random();
        Random randRow = new Random();

        for (int count = 0; count < 10; count++)
        {
            String partitionKey = "k" + randPartition.nextInt(NUM_PARTITIONS);
            String clusteringKey = "c1_" + randRow.nextInt(NUM_ROWS);

            execute("DELETE c2, c3 FROM %s WHERE key = ? AND c1 = ?", partitionKey, clusteringKey);
            keysToPurge.add(partitionKey);
        }

        flush();

        String[] keys = new String[keysToPurge.size()];
        keys = keysToPurge.toArray(keys);
        forceCompact(keys);

        verifyNotContainsTombstones();
    }

    @Test
    public void forceCompactUpdateCellTombstoneTest() throws Throwable
    {
        String keyToPurge = "k0";
        testHelper("UPDATE %s SET c2 = null WHERE key = ? AND c1 = 'c1_0'", keyToPurge);
    }

    @Test
    public void forceCompactTTLExpiryTest() throws Throwable
    {
        int ttlSec = 2;
        String keyToPurge = "k0";

        execute("UPDATE %s USING TTL ? SET c2 = 'bbb' WHERE key = ? AND c1 = 'c1_0'", ttlSec, keyToPurge);

        flush();

        // Wait until the TTL has been expired
        // NOTE: we double the wait time of the ttl to be on the safer side and avoid the flakiness of the test
        Thread.sleep(ttlSec * 1000 * 2);

        String[] keysToPurge = new String[]{keyToPurge};
        forceCompact(keysToPurge);

        verifyNotContainsTombstones();
    }

    @Test
    public void forceCompactCompositePartitionKeysTest() throws Throwable
    {
        createTable("CREATE TABLE %s (key1 text, key2 text, c1 text, c2 text, PRIMARY KEY ((key1, key2), c1))");

        for (int partitionCount = 0; partitionCount < NUM_PARTITIONS; partitionCount++)
        {
            for (int rowCount = 0; rowCount < NUM_ROWS; rowCount++)
            {
                execute("INSERT INTO %s (key1, key2, c1, c2) VALUES (?, ?, ?, ?)",
                        "k1_" + partitionCount, "k2_" + partitionCount, "c1_" + rowCount, "c2_" + rowCount);
            }
        }

        // Disable auto compaction
        // NOTE: We can only disable the auto compaction once the table is created because the setting is on
        // the table level. And we don't need to re-enable it back because the table will be dropped after the test.
        disableCompaction();

        String keyToPurge = "k1_0:k2_0";

        execute("DELETE FROM %s WHERE key1 = 'k1_0' and key2 = 'k2_0'");

        flush();

        String[] keysToPurge = new String[]{keyToPurge};
        forceCompact(keysToPurge);

        verifyNotContainsTombstones();
    }

    private void testHelper(String cqlStatement, String keyToPurge) throws Throwable
    {
        execute(cqlStatement, keyToPurge);

        flush();

        String[] keysToPurge = new String[]{keyToPurge};
        forceCompact(keysToPurge);

        verifyNotContainsTombstones();
    }

    private void forceCompact(String[] partitionKeysIgnoreGcGrace)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        if (cfs != null)
        {
            cfs.forceMajorCompaction();
            cfs.forceCompactionKeysIgnoringGcGrace(partitionKeysIgnoreGcGrace);
        }
    }

    private void verifyNotContainsTombstones()
    {
        // Get sstables
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());

        // always run a major compaction before calling this
        Util.spinAssertEquals("Too many sstables: " + cfs.getLiveSSTables().toString(),
                              Boolean.TRUE,
                              () -> cfs.getLiveSSTables().size() == 1,
                              60,
                              TimeUnit.SECONDS);

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        SSTableReader sstable = sstables.iterator().next();
        int actualPurgedTombstoneCount = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    // Partition should be all alive
                    assertTrue(iter.partitionLevelDeletion().isLive());

                    while (iter.hasNext())
                    {
                        Unfiltered atom = iter.next();
                        if (atom.isRow())
                        {
                            Row r = (Row)atom;

                            // Row should be all alive
                            assertTrue(r.deletion().isLive());

                            // Cell should be alive as well
                            for (Cell c : r.cells())
                            {
                                assertFalse(c.isTombstone());
                            }
                        }
                    }
                }
            }
        }
    }
}