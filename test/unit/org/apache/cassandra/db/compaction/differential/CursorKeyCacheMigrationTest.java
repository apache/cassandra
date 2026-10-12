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

package org.apache.cassandra.db.compaction.differential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A key that is hot in the originals migrates into the compaction output's key cache, at the same
 * data position, on the cursor path. BIG format only.
 */
public class CursorKeyCacheMigrationTest extends DifferentialCompactionTester
{
    /** Enough partitions that a corrupted key shows up as a miss. */
    private static final int PARTITIONS = 200;

    /** A key cache capacity the scenario's entries fit inside. */
    private static final long KEY_CACHE_CAPACITY_BYTES = 1L << 20;

    /** Small enough that the fixture's partitions span several index blocks, so entries are indexed. */
    private static final int COLUMN_INDEX_SIZE_KIB = 1;

    /** Large enough that a fixture partition's index blocks fit, so the iterator path caches a full entry. */
    private static final int COLUMN_INDEX_CACHE_SIZE_KIB = 2;

    /** Every Nth partition gets a surviving partition-level tombstone. */
    private static final int DELETE_STRIDE = 7;

    /** Deletion timestamps: older than the rows, so the rows survive and the deletion is retained. */
    private static final long DELETE_TIMESTAMP_BASE = 1000L;

    private SSTableFormat<?, ?> originalFormat;
    private boolean originalMigrate;
    private int originalColumnIndexCacheSize;
    private int originalColumnIndexSize;
    private long originalKeyCacheCapacity;

    @Before
    public void selectBigAndEnableMigration()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        originalMigrate = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(true);
        // Forces a promoted row index.
        originalColumnIndexCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        // Small index blocks, so the fixture's partitions span more than one block and their cached
        // entries carry the partition deletion.
        originalColumnIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(COLUMN_INDEX_SIZE_KIB);
        // Enable the key cache regardless of the yaml, since this scenario is about what it holds.
        originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
        if (originalKeyCacheCapacity == 0)
            CacheService.instance.keyCache.setCapacity(KEY_CACHE_CAPACITY_BYTES);
        CacheService.instance.invalidateKeyCache();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setColumnIndexCacheSize(originalColumnIndexCacheSize);
        DatabaseDescriptor.setColumnIndexSizeInKiB(originalColumnIndexSize);
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(originalMigrate);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
        CacheService.instance.invalidateKeyCache();
    }

    @Test
    public void hotKeysMigrateIntoTheOutputOnCursorPath() throws Throwable
    {
        assertHotKeysMigrate(true);
    }

    /** The same expectation on the iterator path. */
    @Test
    public void hotKeysMigrateIntoTheOutputOnIteratorPath() throws Throwable
    {
        assertHotKeysMigrate(false);
    }

    private void assertHotKeysMigrate(boolean cursor) throws Throwable
    {
        ColumnFamilyStore cfs = twoSSTablesWithMultiBlockPartitions();

        List<DecoratedKey> hot = readEveryPartition(cfs);
        assertTrue("the scenario must warm the key cache before it compacts, or maybeCacheKey " +
                   "never reaches its put", cachedAnywhere(cfs, hot) > 0);

        // keepOriginals false, so the live set is the output.
        commitThroughFactory(cfs, cursor,
                             (store, txn, gcBefore) -> new CompactionTask(store, txn, gcBefore, false));

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("expected one compaction output", 1, outputs.size());
        SSTableReader output = outputs.get(0);
        assertOutputFormatIsSelected(output);

        int migrated = 0;
        int deletionsVerified = 0;
        for (int pk = 0; pk < hot.size(); pk++)
        {
            DecoratedKey key = hot.get(pk);
            AbstractRowIndexEntry cached = ((KeyCacheSupport<?>) output).getCachedPosition(key, false);
            if (cached == null)
                continue;
            migrated++;

            // A multi-block entry carries the partition deletion. Compare it against what this test
            // wrote, not against getRowIndexEntry: that method consults the same key cache and would
            // hand back this very entry, so the check would compare the value to itself. A reused
            // DeletionTime aliased into the cache reports a later partition's value, not this one's.
            if (cached.isIndexed())
            {
                DeletionTime cachedDeletion = cached.deletionTime();
                if (pk % DELETE_STRIDE == 0)
                {
                    assertEquals("the key cache entry for pk " + pk + " lost its own partition " +
                                 "deletion; a reused DeletionTime leaked into the cache",
                                 DELETE_TIMESTAMP_BASE + pk, cachedDeletion.markedForDeleteAt());
                    deletionsVerified++;
                }
                else
                {
                    assertTrue("the key cache entry for pk " + pk + " gained a partition deletion it " +
                               "never had; a reused DeletionTime leaked into the cache",
                               cachedDeletion.isLive());
                }
            }
        }

        assertTrue("no hot key reached the output's key cache, so BigTableWriter.maybeCacheKey " +
                   "never stored anything and this scenario proved nothing", migrated > 0);
        assertTrue("no hot, multi-block, partition-deleted key reached the cache, so this scenario " +
                   "did not exercise the DeletionTime aliasing path", deletionsVerified > 0);
    }

    /**
     * With the index blocks small enough to fit column_index_cache_size, the iterator path caches a full entry
     * that holds the blocks. The cursor path must cache the same entry, byte for byte.
     */
    @Test
    public void migratedEntriesMatchIteratorPath() throws Throwable
    {
        DatabaseDescriptor.setColumnIndexCacheSize(COLUMN_INDEX_CACHE_SIZE_KIB);
        ColumnFamilyStore cfs = twoSSTablesWithMultiBlockPartitions();
        List<DecoratedKey> hot = readEveryPartition(cfs);
        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());

        List<String> iterator = migratedEntries(cfs, inputs, false, hot);
        List<String> cursor = migratedEntries(cfs, inputs, true, hot);

        assertTrue("no partition cached a full entry on the iterator path, so this scenario proved nothing",
                   iterator.stream().anyMatch(e -> e.startsWith("IndexedEntry")));
        assertEquals("the cursor path cached different key cache entries than the iterator path", iterator, cursor);
    }

    /** Compacts the inputs, keeping them live, and describes the entry the output cached for each key. */
    private List<String> migratedEntries(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor,
                                         List<DecoratedKey> keys) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        Set<SSTableReader> liveBefore = new HashSet<>(cfs.getLiveSSTables());
        // the first compaction may have replaced the input instances; find them again by descriptor
        Set<SSTableReader> current = new HashSet<>();
        for (SSTableReader live : liveBefore)
            for (SSTableReader in : inputs)
                if (live.descriptor.equals(in.descriptor))
                    current.add(live);
        assertEquals("inputs lost across the first compaction", inputs.size(), current.size());
        LifecycleTransaction txn = cfs.getTracker().tryModify(current, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting", txn);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        new CompactionTask(cfs, txn, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()), true).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        outputs.removeAll(liveBefore);
        assertEquals("expected one compaction output", 1, outputs.size());
        KeyCacheSupport<?> output = (KeyCacheSupport<?>) outputs.get(0);

        List<String> entries = new ArrayList<>();
        for (DecoratedKey key : keys)
        {
            AbstractRowIndexEntry cached = output.getCachedPosition(key, false);
            if (cached == null)
            {
                entries.add("none");
                continue;
            }
            try (DataOutputBuffer forCache = new DataOutputBuffer(); DataOutputBuffer forIndex = new DataOutputBuffer())
            {
                RowIndexEntry entry = (RowIndexEntry) cached;
                entry.serializeForCache(forCache);
                String described = entry.getClass().getSimpleName() + ' ' + ByteBufferUtil.bytesToHex(forCache.asNewBuffer());
                // the full entry also carries the block offsets, which only the index serialization writes
                if (entry.getClass().getSimpleName().equals("IndexedEntry"))
                {
                    entry.serialize(forIndex, null);
                    described += ' ' + ByteBufferUtil.bytesToHex(forIndex.asNewBuffer());
                }
                entries.add(described);
            }
        }
        return entries;
    }

    /** How many of these keys any live sstable currently holds a cached position for. */
    private static int cachedAnywhere(ColumnFamilyStore cfs, List<DecoratedKey> keys)
    {
        int found = 0;
        for (SSTableReader reader : cfs.getLiveSSTables())
            if (reader instanceof KeyCacheSupport<?>)
                for (DecoratedKey key : keys)
                    if (((KeyCacheSupport<?>) reader).getCachedPosition(key, false) != null)
                        found++;
        return found;
    }

    /** Point-reads every partition, which is what puts its position in the originals' key cache. */
    private List<DecoratedKey> readEveryPartition(ColumnFamilyStore cfs) throws Throwable
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS);
        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            execute("SELECT * FROM %s WHERE pk = ?", pk);
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk)));
        }
        return keys;
    }

    private ColumnFamilyStore twoSSTablesWithMultiBlockPartitions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "v".repeat(400);
        for (int round = 0; round < 2; round++)
        {
            for (int pk = 0; pk < PARTITIONS; pk++)
                for (int ck = 0; ck < 8; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck + round * 8, padding);
            flush();
        }

        // Give a spread of partitions a partition-level tombstone with a distinct, old timestamp.
        // Older than the rows, so the rows survive and the partition keeps a non-LIVE deletion; that
        // is the entry the key cache must snapshot rather than alias to the cursor's reused instance.
        for (int pk = 0; pk < PARTITIONS; pk += DELETE_STRIDE)
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", DELETE_TIMESTAMP_BASE + pk, pk);
        flush();

        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }
}
