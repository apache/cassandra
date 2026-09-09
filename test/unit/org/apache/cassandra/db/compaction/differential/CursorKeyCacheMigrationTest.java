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
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.service.CacheService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@code BigTableWriter.maybeCacheKey}, which carries a key that is hot in the originals into
 * the compaction output's key cache. BIG format only; it is the only format with a key cache.
 *
 * The method reaches its {@code cachedKeys.put} only when an original already holds the key, which
 * needs a point read before the compaction. No other test does that on the cursor path, so the put
 * ran in no test at all: every {@code getCachedPosition} returned null and the loop fell through.
 *
 * That matters because the writer stores the key it is handed. The cursor's own key is a reusable
 * instance the next partition overwrites, so a key stored without a copy would corrupt the map it
 * is a key of, and then the real key cache that {@code BigTableWriter.openInternal} drains it into.
 * Nothing about the sstable's own bounds would look wrong, so no other assertion in the tree
 * catches it.
 *
 * The oracle here is per key and positional, not a count. A count survives a corrupted key: the map
 * still holds one entry per put.
 */
public class CursorKeyCacheMigrationTest extends DifferentialCompactionTester
{
    /** Enough partitions that a corrupted key shows up as a miss rather than by luck. */
    private static final int PARTITIONS = 200;

    private SSTableFormat<?, ?> originalFormat;
    private boolean originalMigrate;
    private int originalColumnIndexCacheSize;

    @Before
    public void selectBigAndEnableMigration()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat("big");
        originalMigrate = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(true);
        // Forces a promoted row index, so the cached entry is the shallow kind the cursor path
        // writes rather than the full one the iterator path builds.
        // The getter returns bytes and the setter takes KiB; save the KiB one or the restore overflows.
        originalColumnIndexCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        CacheService.instance.invalidateKeyCache();
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setColumnIndexCacheSize(originalColumnIndexCacheSize);
        DatabaseDescriptor.setMigrateKeycacheOnCompaction(originalMigrate);
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
        CacheService.instance.invalidateKeyCache();
    }

    @Test
    public void hotKeysMigrateIntoTheOutputOnCursorPath() throws Throwable
    {
        assertHotKeysMigrate(true);
    }

    /** The same expectation on the iterator path, so a failure above reads as a cursor defect. */
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

        // keepOriginals false: the originals must really be replaced, so the live set is the output.
        commitThroughFactory(cfs, cursor,
                             (store, txn, gcBefore) -> new CompactionTask(store, txn, gcBefore, false));

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("expected one compaction output", 1, outputs.size());
        SSTableReader output = outputs.get(0);
        assertOutputFormatIsSelected(output);

        int migrated = 0;
        for (DecoratedKey key : hot)
        {
            AbstractRowIndexEntry cached = ((KeyCacheSupport<?>) output).getCachedPosition(key, false);
            if (cached == null)
                continue;
            migrated++;

            // The cached entry must name the same data position a fresh index lookup does. A key
            // stored while it was still reusable lands under whatever bytes it later held, so its
            // entry describes a different partition.
            AbstractRowIndexEntry looked = output.getRowIndexEntry(key, SSTableReader.Operator.EQ);
            assertNotNull("the migrated key " + key + " is not in the output's index at all", looked);
            assertEquals("the key cache entry for " + key + " points at a different partition than " +
                         "the output's own index does", looked.position, cached.position);
        }

        assertTrue("no hot key reached the output's key cache, so BigTableWriter.maybeCacheKey " +
                   "never stored anything and this scenario proved nothing", migrated > 0);
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
            keys.add(cfs.getPartitioner().decorateKey(org.apache.cassandra.utils.ByteBufferUtil.bytes(pk)));
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
        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }
}
