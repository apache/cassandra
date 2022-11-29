/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.SSTableExpiredBlockers;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TTLExpiryTest
{
    public static final String KEYSPACE1 = "TTLExpiryTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS.setInt(1);

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    TableMetadata.builder(KEYSPACE1, CF_STANDARD1)
                                                 .addPartitionKeyColumn("pKey", AsciiType.instance)
                                                 .addRegularColumn("col1", AsciiType.instance)
                                                 .addRegularColumn("col", AsciiType.instance)
                                                 .addRegularColumn("col311", AsciiType.instance)
                                                 .addRegularColumn("col2", AsciiType.instance)
                                                 .addRegularColumn("col3", AsciiType.instance)
                                                 .addRegularColumn("col7", AsciiType.instance)
                                                 .addRegularColumn("col8", MapType.getInstance(AsciiType.instance, AsciiType.instance, true))
                                                 .addRegularColumn("shadow", AsciiType.instance)
                                                 .gcGraceSeconds(0));
    }

    @Test
    public void testAggressiveFullyExpired()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(0).build());
        String key = "ttl";
        new RowUpdateBuilder(cfs.metadata(), 1L, 1, key)
                    .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 3L, 1, key)
                    .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
        Util.flush(cfs);
        new RowUpdateBuilder(cfs.metadata(), 2L, 1, key)
                    .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 5L, 1, key)
                    .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), 4L, 1, key)
                    .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 7L, 1, key)
                    .add("shadow", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        Util.flush(cfs);


        new RowUpdateBuilder(cfs.metadata(), 6L, 3, key)
                    .add("shadow", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata(), 8L, 1, key)
                    .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        Util.flush(cfs);

        Set<SSTableReader> sstables = Sets.newHashSet(cfs.getLiveSSTables());
        long now = FBUtilities.nowInSeconds();
        long gcBefore = now + 2;
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(
                cfs,
                sstables,
                Collections.EMPTY_SET,
                gcBefore);
        assertEquals(2, expired.size());

        cfs.clearUnsafe();
    }

    @Test
    public void testSimpleExpire() throws InterruptedException
    {
        testSimpleExpire(false);
    }

    @Test
    public void testBug10944() throws InterruptedException
    {
        // Reproduction for CASSANDRA-10944 (at the time of the bug)
        testSimpleExpire(true);
    }

    public void testSimpleExpire(boolean force10944Bug) throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        // To reproduce #10944, we need our gcBefore to be equal to the locaDeletionTime. A gcGrace of 1 will (almost always) give us that.
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(force10944Bug ? 1 : 0).build());
        long timestamp = System.currentTimeMillis();
        String key = "ttl";
        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
                        .add("col", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .add("col7", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .build()
                        .applyUnsafe();

        Util.flush(cfs);

        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
            .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .add("col8", Collections.singletonMap("bar", "foo"))
            .delete("col1")
            .build()
            .applyUnsafe();


        Util.flush(cfs);
        // To reproduce #10944, we need to avoid the optimization that get rid of full sstable because everything
        // is known to be gcAble, so keep some data non-expiring in that case.
        new RowUpdateBuilder(cfs.metadata(), timestamp, force10944Bug ? 0 : 1, key)
                    .add("col3", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();


        Util.flush(cfs);
        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
                            .add("col311", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                            .build()
                            .applyUnsafe();


        Util.flush(cfs);
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getLiveSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(force10944Bug ? 1 : 0, cfs.getLiveSSTables().size());
    }

    @Test
    public void testNoExpire() throws InterruptedException, IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(0).build());
        long timestamp = System.currentTimeMillis();
        String key = "ttl";
        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
            .add("col", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .add("col7", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        Util.flush(cfs);
        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
            .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        Util.flush(cfs);
        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
            .add("col3", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        Util.flush(cfs);
        String noTTLKey = "nottl";
        new RowUpdateBuilder(cfs.metadata(), timestamp, noTTLKey)
            .add("col311", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        // also write to other key to ensure overlap for UCS
        new RowUpdateBuilder(cfs.metadata(), timestamp, 1, key)
            .add("col7", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        Util.flush(cfs);
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getLiveSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        UnfilteredPartitionIterator scanner = sstable.partitionIterator(ColumnFilter.all(cfs.metadata()),
                                                                        DataRange.allData(cfs.getPartitioner()),
                                                                        SSTableReadsListener.NOOP_LISTENER);
        assertTrue(scanner.hasNext());
        while(scanner.hasNext())
        {
            UnfilteredRowIterator iter = scanner.next();
            assertEquals(Util.dk(noTTLKey), iter.partitionKey());
        }
        scanner.close();
    }

    @Test
    public void testCheckForExpiredSSTableBlockers() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        SchemaTestUtil.announceTableUpdate(cfs.metadata().unbuild().gcGraceSeconds(0).build());

        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), "test")
                .noRowMarker()
                .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();

        Util.flush(cfs);
        SSTableReader blockingSSTable = cfs.getSSTables(SSTableSet.LIVE).iterator().next();
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), "test")
                            .noRowMarker()
                            .delete("col1")
                            .build()
                            .applyUnsafe();
            Util.flush(cfs);
        }
        Multimap<SSTableReader, SSTableReader> blockers = SSTableExpiredBlockers.checkForExpiredSSTableBlockers(cfs.getSSTables(SSTableSet.LIVE), (int) (System.currentTimeMillis() / 1000) + 100);
        assertEquals(1, blockers.keySet().size());
        assertTrue(blockers.keySet().contains(blockingSSTable));
        assertEquals(10, blockers.get(blockingSSTable).size());
    }
}
