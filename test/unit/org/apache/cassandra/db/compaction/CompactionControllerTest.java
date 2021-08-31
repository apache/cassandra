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
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.TableMetadata;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
                                    TableMetadata.builder(KEYSPACE, CF1)
                                                 .addPartitionKeyColumn("pk", AsciiType.instance)
                                                 .addClusteringColumn("ck", AsciiType.instance)
                                                 .addRegularColumn("val", AsciiType.instance),
                                    TableMetadata.builder(KEYSPACE, CF2)
                                                 .addPartitionKeyColumn("pk", AsciiType.instance)
                                                 .addClusteringColumn("ck", AsciiType.instance)
                                                 .addRegularColumn("val", AsciiType.instance));
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
        applyMutation(cfs.metadata(), key, timestamp1);

        // check max purgeable timestamp without any sstables
        try(CompactionController controller = new CompactionController(cfs, null, 0))
        {
            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp1); //memtable only

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            assertTrue(controller.getPurgeEvaluator(key).test(Long.MAX_VALUE)); //no memtables and no sstables
        }

        Set<SSTableReader> compacting = Sets.newHashSet(cfs.getLiveSSTables()); // first sstable is compacting

        // create another sstable
        applyMutation(cfs.metadata(), key, timestamp2);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // check max purgeable timestamp when compacting the first sstable with and without a memtable
        try (CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp2);

            applyMutation(cfs.metadata(), key, timestamp3);

            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp3); //second sstable and second memtable
        }

        // check max purgeable timestamp again without any sstables but with different insertion orders on the memtable
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        //newest to oldest
        try (CompactionController controller = new CompactionController(cfs, null, 0))
        {
            applyMutation(cfs.metadata(), key, timestamp1);
            applyMutation(cfs.metadata(), key, timestamp2);
            applyMutation(cfs.metadata(), key, timestamp3);

            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp3); //memtable only
        }

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        //oldest to newest
        try (CompactionController controller = new CompactionController(cfs, null, 0))
        {
            applyMutation(cfs.metadata(), key, timestamp3);
            applyMutation(cfs.metadata(), key, timestamp2);
            applyMutation(cfs.metadata(), key, timestamp1);

            assertPurgeBoundary(controller.getPurgeEvaluator(key), timestamp3);
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
        applyDeleteMutation(cfs.metadata(), key, timestamp2);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // first sstable with tombstone is compacting
        Set<SSTableReader> compacting = Sets.newHashSet(cfs.getLiveSSTables());

        // create another sstable with more recent timestamp
        applyMutation(cfs.metadata(), key, timestamp1);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // second sstable is overlapping
        Set<SSTableReader> overlapping = Sets.difference(Sets.newHashSet(cfs.getLiveSSTables()), compacting);

        // the first sstable should be expired because the overlapping sstable is newer and the gc period is later
        int gcBefore = (int) (System.currentTimeMillis() / 1000) + 5;
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore);
        assertNotNull(expired);
        assertEquals(1, expired.size());
        assertEquals(compacting.iterator().next(), expired.iterator().next());

        // however if we add an older mutation to the memtable then the sstable should not be expired
        applyMutation(cfs.metadata(), key, timestamp3);
        expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore);
        assertNotNull(expired);
        assertEquals(0, expired.size());

        // Now if we explicitly ask to ignore overlaped sstables, we should get back our expired sstable
        expired = CompactionController.getFullyExpiredSSTables(cfs, compacting, overlapping, gcBefore, true);
        assertNotNull(expired);
        assertEquals(1, expired.size());
    }

    private void applyMutation(TableMetadata cfm, DecoratedKey key, long timestamp)
    {
        ByteBuffer val = ByteBufferUtil.bytes(1L);

        new RowUpdateBuilder(cfm, timestamp, key)
        .clustering("ck")
        .add("val", val)
        .build()
        .applyUnsafe();
    }

    private void applyDeleteMutation(TableMetadata cfm, DecoratedKey key, long timestamp)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(cfm, key, timestamp, FBUtilities.nowInSeconds()))
        .applyUnsafe();
    }

    private void assertPurgeBoundary(LongPredicate evaluator, long boundary)
    {
        assertFalse(evaluator.test(boundary));
        assertTrue(evaluator.test(boundary - 1));
    }
}
