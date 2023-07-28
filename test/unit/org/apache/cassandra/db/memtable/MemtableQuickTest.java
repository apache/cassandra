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

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Refs;

@RunWith(Parameterized.class)
public class MemtableQuickTest extends CQLTester
{
    static final Logger logger = LoggerFactory.getLogger(MemtableQuickTest.class);

    static final int partitions = 50_000;
    static final int rowsPerPartition = 4;

    static final int deletedPartitionsStart = 20_000;
    static final int deletedPartitionsEnd = deletedPartitionsStart + 10_000;

    static final int deletedRowsStart = 40_000;
    static final int deletedRowsEnd = deletedRowsStart + 5_000;

    @Parameterized.Parameter()
    public String memtableClass;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("skiplist",
                                "skiplist_sharded",
                                "skiplist_sharded_locking",
                                "trie");
    }

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        CQLTester.disablePreparedReuseForTest();
        logger.info("setupClass done.");
    }

    @Test
    public void testMemtable() throws Throwable
    {

        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                             " with compression = {'enabled': false}" +
                                             " and memtable = '" + memtableClass + "'" +
                                             " and compaction = { 'class': 'UnifiedCompactionStrategy', 'base_shard_count': '4' }"); // to trigger splitting of sstables, CASSANDRA-18123
        execute("use " + keyspace + ';');

        String writeStatement = "INSERT INTO "+table+"(userid,picid,commentid)VALUES(?,?,?)";

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        Util.flush(cfs);

        long i;
        long limit = partitions;
        logger.info("Writing {} partitions of {} rows", partitions, rowsPerPartition);
        for (i = 0; i < limit; ++i)
        {
            for (long j = 0; j < rowsPerPartition; ++j)
                execute(writeStatement, i, j, i + j);
        }

        logger.info("Deleting partitions between {} and {}", deletedPartitionsStart, deletedPartitionsEnd);
        for (i = deletedPartitionsStart; i < deletedPartitionsEnd; ++i)
        {
            // no partition exists, but we will create a tombstone
            execute("DELETE FROM " + table + " WHERE userid = ?", i);
        }

        logger.info("Deleting rows between {} and {}", deletedRowsStart, deletedRowsEnd);
        for (i = deletedRowsStart; i < deletedRowsEnd; ++i)
        {
            // no row exists, but we will create a tombstone (and partition)
            execute("DELETE FROM " + table + " WHERE userid = ? AND picid = ?", i, 0L);
        }

        logger.info("Reading {} partitions", partitions);
        for (i = 0; i < limit; ++i)
        {
            UntypedResultSet result = execute("SELECT * FROM " + table + " WHERE userid = ?", i);
            if (i >= deletedPartitionsStart && i < deletedPartitionsEnd)
                assertEmpty(result);
            else
            {
                int start = 0;
                if (i >= deletedRowsStart && i < deletedRowsEnd)
                    start = 1;
                Object[][] rows = new Object[rowsPerPartition - start][];
                for (long j = start; j < rowsPerPartition; ++j)
                    rows[(int) (j - start)] = row(i, j, i + j);
                assertRows(result, rows);
            }
        }

        int deletedPartitions = deletedPartitionsEnd - deletedPartitionsStart;
        int deletedRows = deletedRowsEnd - deletedRowsStart;
        logger.info("Selecting *");
        UntypedResultSet result = execute("SELECT * FROM " + table);
        assertRowCount(result, rowsPerPartition * (partitions - deletedPartitions) - deletedRows);

        Util.flush(cfs);

        logger.info("Selecting *");
        result = execute("SELECT * FROM " + table);
        assertRowCount(result, rowsPerPartition * (partitions - deletedPartitions) - deletedRows);

        try (Refs<SSTableReader> refs = new Refs())
        {
            Collection<SSTableReader> sstables = cfs.getLiveSSTables();
            if (sstables.isEmpty()) // persistent memtables won't flush
            {
                assert cfs.streamFromMemtable();
                cfs.writeAndAddMemtableRanges(null,
                                              () -> ImmutableList.of(new Range(Util.testPartitioner().getMinimumToken().minKeyBound(),
                                                                               Util.testPartitioner().getMinimumToken().minKeyBound())),
                                              refs);
                sstables = refs;
                Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
            }

            // make sure the row counts are correct in both the metadata as well as the cardinality estimator
            // (see CASSANDRA-18123)
            long totalPartitions = 0;
            for (SSTableReader sstable : sstables)
            {
                long sstableKeys = sstable.estimatedKeys();
                long cardinality = SSTableReader.getApproximateKeyCount(ImmutableList.of(sstable));
                // should be within 10% of each other
                Assert.assertEquals((double) sstableKeys, (double) cardinality, sstableKeys * 0.1);
                totalPartitions += sstableKeys;
            }
            Assert.assertEquals((double) partitions, (double) totalPartitions, partitions * 0.1);
        }
    }
}