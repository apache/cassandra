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

import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CompactionControllerTest extends SchemaLoader
{
    private static final String KEYSPACE = "CompactionControllerTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.Builder.create(KEYSPACE, CF1, true, false, false)
                                                      .addPartitionKey("pk", AsciiType.instance)
                                                      .addClusteringColumn("ck", AsciiType.instance)
                                                      .addRegularColumn("val", AsciiType.instance)
                                                      .build(),
                                    CFMetaData.Builder.create(KEYSPACE, CF2, true, false, false)
                                                      .addPartitionKey("pk", AsciiType.instance)
                                                      .addClusteringColumn("ck", AsciiType.instance)
                                                      .addRegularColumn("val", AsciiType.instance)
                                                      .build());
    }

    @Test
    public void testMaxPurgeableTimestamp()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF1);
        cfs.truncateBlocking();

        DecoratedKey key = Util.dk("k1");

        long timestamp1 = FBUtilities.timestampMicros(); // latest timestamp
        long timestamp2 = timestamp1 - 5;
        long timestamp3 = timestamp2 - 5; // oldest timestamp

        // add to first memtable
        applyMutation(cfs.metadata, key, timestamp1);

        // check max purgeable timestamp without any sstables
        try(CompactionController controller = new CompactionController(cfs, null, 0))
        {
            assertEquals(timestamp1, controller.maxPurgeableTimestamp(key)); //memtable only

            cfs.forceBlockingFlush();
            assertEquals(Long.MAX_VALUE, controller.maxPurgeableTimestamp(key)); //no memtables and no sstables
        }

        Set<SSTableReader> compacting = Sets.newHashSet(cfs.getLiveSSTables()); // first sstable is compacting

        // create another sstable
        applyMutation(cfs.metadata, key, timestamp2);
        cfs.forceBlockingFlush();

        // check max purgeable timestamp when compacting the first sstable with and without a memtable
        try (CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            assertEquals(timestamp2, controller.maxPurgeableTimestamp(key)); //second sstable only

            applyMutation(cfs.metadata, key, timestamp3);

            assertEquals(timestamp3, controller.maxPurgeableTimestamp(key)); //second sstable and second memtable
        }

        // check max purgeable timestamp again without any sstables but with different insertion orders on the memtable
        cfs.forceBlockingFlush();

        //newest to oldest
        try (CompactionController controller = new CompactionController(cfs, null, 0))
        {
            applyMutation(cfs.metadata, key, timestamp1);
            applyMutation(cfs.metadata, key, timestamp2);
            applyMutation(cfs.metadata, key, timestamp3);

            assertEquals(timestamp3, controller.maxPurgeableTimestamp(key)); //memtable only
        }

        cfs.forceBlockingFlush();

        //oldest to newest
        try (CompactionController controller = new CompactionController(cfs, null, 0))
        {
            applyMutation(cfs.metadata, key, timestamp3);
            applyMutation(cfs.metadata, key, timestamp2);
            applyMutation(cfs.metadata, key, timestamp1);

            assertEquals(timestamp3, controller.maxPurgeableTimestamp(key)); //memtable only
        }
    }

    @Test
    public void testGetFullyExpiredSSTables()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);
        cfs.truncateBlocking();

        DecoratedKey key = Util.dk("k1");

        long timestamp1 = FBUtilities.timestampMicros(); // latest timestamp
        long timestamp2 = timestamp1 - 5;
        long timestamp3 = timestamp2 - 5; // oldest timestamp

        // create sstable with tombstone that should be expired in no older timestamps
        applyDeleteMutation(cfs.metadata, key, timestamp2);
        cfs.forceBlockingFlush();

        // first sstable with tombstone is compacting
        Set<SSTableReader> compacting = Sets.newHashSet(cfs.getLiveSSTables());

        // create another sstable with more recent timestamp
        applyMutation(cfs.metadata, key, timestamp1);
        cfs.forceBlockingFlush();

        // second sstable is overlapping
        Set<SSTableReader> overlapping = Sets.difference(Sets.newHashSet(cfs.getLiveSSTables()), compacting);

        // the first sstable should be expired because the overlapping sstable is newer and the gc period is later
        int gcBefore = (int) (System.currentTimeMillis() / 1000) + 5;
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore);
        assertNotNull(expired);
        assertEquals(1, expired.size());
        assertEquals(compacting.iterator().next(), expired.iterator().next());

        // however if we add an older mutation to the memtable then the sstable should not be expired
        applyMutation(cfs.metadata, key, timestamp3);
        expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore);
        assertNotNull(expired);
        assertEquals(0, expired.size());
    }

    private void applyMutation(CFMetaData cfm, DecoratedKey key, long timestamp)
    {
        ByteBuffer val = ByteBufferUtil.bytes(1L);

        new RowUpdateBuilder(cfm, timestamp, key)
        .clustering("ck")
        .add("val", val)
        .build()
        .applyUnsafe();
    }

    private void applyDeleteMutation(CFMetaData cfm, DecoratedKey key, long timestamp)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(cfm, key, timestamp, FBUtilities.nowInSeconds()))
        .applyUnsafe();
    }
}
