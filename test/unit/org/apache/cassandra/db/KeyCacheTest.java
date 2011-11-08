package org.apache.cassandra.db;
/*
 * 
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
 * 
 */


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static junit.framework.Assert.assertEquals;

public class KeyCacheTest extends CleanupHelper
{
    private static final String TABLE1 = "KeyCacheSpace";
    private static final String COLUMN_FAMILY1 = "Standard1";
    private static final String COLUMN_FAMILY2 = "Standard2";
    private static final String COLUMN_FAMILY3 = "Standard3";

    @Test
    public void testKeyCache50() throws IOException, ExecutionException, InterruptedException
    {
        testKeyCache(COLUMN_FAMILY1, 64);
    }

    @Test
    public void testKeyCache100() throws IOException, ExecutionException, InterruptedException
    {
        testKeyCache(COLUMN_FAMILY2, 128);
    }

    @Test
    public void testKeyCacheLoad() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore store = Table.open(TABLE1).getColumnFamilyStore(COLUMN_FAMILY3);

        // empty the cache
        store.invalidateKeyCache();
        assert store.getKeyCacheSize() == 0;

        // insert data and force to disk
        insertData(TABLE1, COLUMN_FAMILY3, 0, 100);
        store.forceBlockingFlush();

        // populate the cache
        readData(TABLE1, COLUMN_FAMILY3, 0, 100);
        assertEquals(100, store.getKeyCacheSize());

        // really? our caches don't implement the map interface? (hence no .addAll)
        Map<Pair<Descriptor, DecoratedKey>, Long> savedMap = new HashMap<Pair<Descriptor, DecoratedKey>, Long>();
        for (Pair<Descriptor, DecoratedKey> k : store.getKeyCache().getKeySet())
        {
            savedMap.put(k, store.getKeyCache().get(k));
        }

        // force the cache to disk
        store.keyCache.submitWrite(Integer.MAX_VALUE).get();

        // empty the cache again to make sure values came from disk
        store.invalidateKeyCache();
        assert store.getKeyCacheSize() == 0;

        // load the cache from disk.  unregister the old mbean so we can recreate a new CFS object.
        // but don't invalidate() the old CFS, which would nuke the data we want to try to load
        store.unregisterMBean();
        ColumnFamilyStore newStore = ColumnFamilyStore.createColumnFamilyStore(Table.open(TABLE1), COLUMN_FAMILY3);
        assertEquals(100, newStore.getKeyCacheSize());

        assertEquals(100, savedMap.size());
        for (Map.Entry<Pair<Descriptor, DecoratedKey>, Long> entry : savedMap.entrySet())
        {
            assert newStore.getKeyCache().get(entry.getKey()).equals(entry.getValue());
        }
    }

    public void testKeyCache(String cfName, int expectedCacheSize) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        // KeyCache should start at size 1 if we're caching X% of zero data.
        int keyCacheSize = cfs.getKeyCacheCapacity();
        assert keyCacheSize == 1 : keyCacheSize;

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key1.key);
        rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("1")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        rm = new RowMutation(TABLE1, key2.key);
        rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("2")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();

        // deletes
        rm = new RowMutation(TABLE1, key1.key);
        rm.delete(new QueryPath(cfName, null, ByteBufferUtil.bytes("1")), 1);
        rm.apply();
        rm = new RowMutation(TABLE1, key2.key);
        rm.delete(new QueryPath(cfName, null, ByteBufferUtil.bytes("2")), 1);
        rm.apply();

        // After a flush, the cache should expand to be X% of indices * INDEX_INTERVAL.
        cfs.forceBlockingFlush();
        keyCacheSize = cfs.getKeyCacheCapacity();
        assert keyCacheSize == expectedCacheSize : keyCacheSize;

        // After a compaction, the cache should expand to be X% of zero data.
        Util.compactAll(cfs).get();
        keyCacheSize = cfs.getKeyCacheCapacity();
        assert keyCacheSize == 1 : keyCacheSize;
    }
}
